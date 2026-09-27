"""播控核心：开场握手、心跳、暂停/恢复、紧急切断、结束、离线合并与策略联动。

线程模型：所有公共方法均在 ``store.lock`` 这一把全局 RLock 内执行，
因此「校验—发命令—改状态」是原子的，后台巡检线程与 HTTP 请求线程
不会交错产生重复命令。

重复命令防护（两道独立机制）：
1. 边缘重试防护——开场握手携带幂等键，UNIQUE(show_id,venue_id,idempotency_key)
   使重试只返回原命令，绝不二次开场；
2. 中心抖动防护——命令序号为场次内全局单调序列，边缘严格按序执行，
   重连时只拉取 delivered_seq 之后的命令，重复投递被就地丢弃。

离线自治：边缘节点短时离线时可按最后持有的授权状态继续本地播控，
事件按边缘本地序号缓冲；重连后整批上报，中心按 (会话, seq) 去重并
检查连续性，出现缺口即隔离该会话，等待人工核对，绝不按错误顺序折叠状态。
"""

from __future__ import annotations

import json
import threading
import time
import uuid

from store import (
    CMD_ACKED,
    CMD_CUT,
    CMD_DELIVERED,
    CMD_FINISH,
    CMD_OPEN,
    CMD_PAUSE,
    CMD_PENDING,
    CMD_RESUME,
    CMD_ROTATE_KEY,
    CMD_SUPERSEDED,
    EV_CUT,
    EV_FINISHED,
    EV_HEARTBEAT,
    EV_OPENED,
    EV_PAUSED,
    EV_RESUMED,
    KEY_ACTIVE,
    KEY_PENDING,
    KEY_REVOKED,
    KEY_ROTATING,
    SHOW_CUT,
    SHOW_ENDED,
    SHOW_PAUSED,
    SHOW_PENDING_AUTH,
    SHOW_PLAYING,
    SHOW_READY,
    VENUE_BRIEFLY_OFFLINE,
    VENUE_FINISHED,
    VENUE_ONLINE,
    VENUE_QUARANTINED,
    Conflict,
    Forbidden,
    NotFound,
    Store,
)

# 心跳超过该秒数判定为短时离线
DEFAULT_HEARTBEAT_TIMEOUT = 15.0

_EVENT_TO_CMD = {
    EV_OPENED: CMD_OPEN,
    EV_PAUSED: CMD_PAUSE,
    EV_RESUMED: CMD_RESUME,
    EV_CUT: CMD_CUT,
    EV_FINISHED: CMD_FINISH,
}
_TERMINAL_EVENT_STATUS = {EV_CUT: SHOW_CUT, EV_FINISHED: SHOW_ENDED}


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class PlayoutController:
    def __init__(self, store: Store, heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT,
                 clock=time.time):
        self.store = store
        self.heartbeat_timeout = heartbeat_timeout
        self._clock = clock
        # 边缘拉取等待（长轮询）用的条件变量，复用 store.lock
        self._cond = threading.Condition(store.lock)

    def now(self) -> float:
        return self._clock()

    # =====================================================================
    # 一、配置面：场次 / 放映点 / 权利窗口 / 密钥
    # =====================================================================

    def create_show(self, show_id, title, starts_at, planned_end_at):
        if self.store.get_show(show_id) is not None:
            raise Conflict(f"场次 {show_id} 已存在")
        if planned_end_at <= starts_at:
            raise Conflict("计划结束时间必须晚于开场时间")
        self.store.create_show(show_id, title, starts_at, planned_end_at)
        return dict(self.store.get_show(show_id))

    def register_venue(self, venue_id, name, region):
        if self.store.get_venue(venue_id) is not None:
            raise Conflict(f"放映点 {venue_id} 已存在")
        self.store.create_venue(venue_id, name, region)
        return dict(self.store.get_venue(venue_id))

    def create_key_batch(self, show_id, batch_id=None, activate=False):
        show = self._require_show(show_id)
        batch_id = batch_id or _new_id("key")
        self.store.create_key_batch(batch_id, show_id,
                                    status=KEY_ACTIVE if activate else KEY_PENDING)
        if activate:
            self._revoke_other_batches(show_id, batch_id, reason="新批次启用，旧批次轮换")
            self.store.commit()
        return dict(self.store.get_key_batch(batch_id))

    def activate_key_batch(self, show_id, batch_id):
        self._require_show(show_id)
        batch = self.store.get_key_batch(batch_id)
        if batch is None or batch["show_id"] != show_id:
            raise NotFound("密钥批次不存在或不属于该场次")
        if batch["status"] == KEY_REVOKED:
            raise Conflict("已吊销批次不能重新启用")
        with self.store.lock:
            self._revoke_other_batches(show_id, batch_id, reason="新批次启用，旧批次轮换")
            self.store.set_key_status(batch_id, KEY_ACTIVE)
            # 未开始点位：绑定到新批次
            self.store.conn.execute(
                "UPDATE entitlements SET key_batch_id=? WHERE show_id=? AND active=1",
                (batch_id, show_id),
            )
            # 进行中会话：下发轮换命令
            for row in self.store.list_active_sessions():
                if row["show_id"] != show_id:
                    continue
                self._issue(
                    show_id, row["venue_id"], CMD_ROTATE_KEY,
                    payload={"key_batch_id": batch_id},
                    reason="密钥批次轮换",
                )
            self.store.add_disposition(
                show_id, None, "密钥轮换", "重绑未开始点位并通知进行中会话",
                applied_to_session=False, trigger_ref=batch_id,
            )
            self.store.commit()
        return dict(self.store.get_key_batch(batch_id))

    def grant_entitlement(self, show_id, venue_id, window_start, window_end,
                          key_batch_id=None):
        self._require_show(show_id)
        self._require_venue(venue_id)
        if window_end <= window_start:
            raise Conflict("权利窗口结束时间必须晚于开始时间")
        existing = self.store.get_entitlement(show_id, venue_id)
        if existing is not None:
            raise Conflict("该放映点对本场次已有有效权利窗口")
        if key_batch_id is None:
            active = self.store.get(
                "SELECT * FROM key_batches WHERE show_id=? AND status=?",
                (show_id, KEY_ACTIVE),
            )
            if active is None:
                raise Conflict("场次尚无有效密钥批次，无法发放授权")
            key_batch_id = active["id"]
        else:
            batch = self.store.get_key_batch(key_batch_id)
            if batch is None or batch["show_id"] != show_id:
                raise NotFound("密钥批次不存在或不属于该场次")
        ent_id = _new_id("ent")
        self.store.create_entitlement(
            ent_id, show_id, venue_id, key_batch_id, window_start, window_end
        )
        with self.store.lock:
            self.store.init_session(show_id, venue_id, key_batch_id)
            # 授权齐套：待授权 -> 待开场（改期冻结的场次不在此处解冻）
            show = self.store.get_show(show_id)
            if show["status"] == SHOW_PENDING_AUTH and not show["reschedule_reason"]:
                self.store.set_show_status(show_id, SHOW_READY)
            self.store.commit()
        return dict(self.store.get_entitlement(show_id, venue_id))

    def rebook_entitlement(self, show_id, venue_id, window_start, window_end,
                           key_batch_id=None):
        """场次改期后为某点位重新发放权利窗口（绑定新时间窗/新批次）。"""
        self._require_show(show_id)
        self._require_venue(venue_id)
        if window_end <= window_start:
            raise Conflict("权利窗口结束时间必须晚于开始时间")
        ent = self.store.get_entitlement(show_id, venue_id)
        if ent is None:
            raise NotFound("该放映点对本场次无既有授权，应直接发放")
        if key_batch_id is None:
            active = self.store.get(
                "SELECT * FROM key_batches WHERE show_id=? AND status=?",
                (show_id, KEY_ACTIVE),
            )
            if active is None:
                raise Conflict("场次尚无有效密钥批次")
            key_batch_id = active["id"]
        with self.store.lock:
            self.store.conn.execute(
                "UPDATE entitlements SET window_start=?,window_end=?,key_batch_id=?"
                " WHERE id=?",
                (window_start, window_end, key_batch_id, ent["id"]),
            )
            self.store.commit()
        return dict(self.store.get_entitlement(show_id, venue_id))

    # =====================================================================
    # 二、控制面：开场 / 暂停 / 恢复 / 切断 / 结束
    # =====================================================================

    def _require_show(self, show_id):
        show = self.store.get_show(show_id)
        if show is None:
            raise NotFound(f"场次 {show_id} 不存在")
        return show

    def _require_venue(self, venue_id):
        venue = self.store.get_venue(venue_id)
        if venue is None:
            raise NotFound(f"放映点 {venue_id} 不存在")
        return venue

    def _require_session(self, show_id, venue_id):
        sess = self.store.get_session(show_id, venue_id)
        if sess is None:
            raise NotFound("该放映点未获得本场上映授权")
        return sess

    def _revoke_other_batches(self, show_id, keep_batch_id, reason):
        for row in self.store.list_key_batches(show_id):
            if row["id"] == keep_batch_id or row["status"] == KEY_REVOKED:
                continue
            self.store.set_key_status(row["id"], KEY_REVOKED, reason=reason)

    def _issue(self, show_id, venue_id, cmd, payload=None, reason=None,
               idempotency_key=None):
        """持锁状态下发命令；若会话已隔离或已终结则拒绝并记录。"""
        sess = self.store.get_session(show_id, venue_id)
        if sess is not None:
            if sess["status"] in (SHOW_ENDED, SHOW_CUT):
                self.store.add_disposition(
                    show_id, venue_id, "命令忽略", f"{cmd}（会话已终结）",
                    applied_to_session=True, detail=reason or "",
                )
                return None, False
            if sess["conn_state"] == VENUE_QUARANTINED and cmd != CMD_CUT:
                self.store.add_disposition(
                    show_id, venue_id, "命令挂起", f"{cmd}（会话隔离中）",
                    applied_to_session=True, detail=reason or "",
                )
                return None, False
        command, replayed = self.store.issue_command(
            show_id, venue_id, cmd, payload=payload, reason=reason,
            idempotency_key=idempotency_key,
        )
        if replayed:
            return command, True
        # 新命令即时影响会话状态（中心侧先行，边缘以 ACK 对齐）
        if sess is not None and cmd in (CMD_PAUSE, CMD_RESUME, CMD_CUT, CMD_FINISH):
            new_status = {
                CMD_PAUSE: SHOW_PAUSED,
                CMD_RESUME: SHOW_PLAYING,
                CMD_CUT: SHOW_CUT,
                CMD_FINISH: SHOW_ENDED,
            }[cmd]
            updates = {"status": new_status}
            if cmd == CMD_CUT:
                updates["conn_state"] = VENUE_QUARANTINED
                updates["quarantined_at"] = self.now()
                updates["quarantine_reason"] = reason or "紧急切断"
            elif cmd == CMD_FINISH:
                updates["conn_state"] = VENUE_FINISHED
            self.store.update_session(show_id, venue_id, **updates)
        if cmd in (CMD_PAUSE, CMD_CUT, CMD_FINISH):
            self._recompute_show_status(show_id)
        return command, False

    def _recompute_show_status(self, show_id):
        """场次状态跟随各点状态聚合：全部终结 -> 终结；任一播出 -> 播出中；
        其余（含暂停）-> 已暂停。

        待授权（含改期冻结）与已终结是管理面状态，聚合不得覆盖它们。
        """
        show = self.store.get_show(show_id)
        if show is None or show["status"] in (
                SHOW_PENDING_AUTH, SHOW_ENDED, SHOW_CUT):
            return
        sessions = self.store.list_sessions(show_id)
        active = [s for s in sessions if s["status"] not in (SHOW_ENDED, SHOW_CUT)]
        if sessions and not active:
            status = SHOW_CUT if all(s["status"] == SHOW_CUT for s in sessions) else SHOW_ENDED
            self.store.set_show_status(show_id, status)
        elif any(s["status"] == SHOW_PLAYING for s in active):
            self.store.set_show_status(show_id, SHOW_PLAYING)
        elif any(s["status"] in (SHOW_PAUSED, SHOW_PLAYING) for s in sessions):
            self.store.set_show_status(show_id, SHOW_PAUSED)

    # ---- 开场握手（边缘调用，幂等） ------------------------------------

    def handshake(self, show_id, venue_id, idempotency_key):
        """边缘开场握手。

        - 重复握手（同一幂等键）：返回原命令，不产生第二次开场；
        - 权利窗口外 / 区域禁播 / 场次已改期 / 批次吊销：拒绝（403）并留痕；
        - 通过：按场次全局单调序号下发「开场」命令（首次），会话转播出中。
        """
        show = self._require_show(show_id)
        venue = self._require_venue(venue_id)
        sess = self._require_session(show_id, venue_id)
        now = self.now()

        with self.store.lock:
            # 已终结的会话：幂等返回既有开场命令，不允许借握手复活
            if sess["status"] in (SHOW_ENDED, SHOW_CUT):
                prior = self.store.get(
                    "SELECT * FROM commands WHERE show_id=? AND venue_id=? AND cmd=?"
                    " ORDER BY seq LIMIT 1",
                    (show_id, venue_id, CMD_OPEN),
                )
                if prior is not None:
                    return {
                        "result": "already_terminal",
                        "command": self.store._command_out(prior),
                        "session_status": sess["status"],
                    }
                raise Forbidden("会话已终结，不能开场")

            # 同幂等键重放 -> 原命令（核心防重：网络抖动重试绝不二次开场）
            if idempotency_key:
                prior = self.store.get(
                    "SELECT * FROM commands WHERE show_id=? AND venue_id=?"
                    " AND idempotency_key=?",
                    (show_id, venue_id, idempotency_key),
                )
                if prior is not None:
                    return {"result": "replayed", "command": self.store._command_out(prior),
                            "session_status": sess["status"]}

            # 已在进行中但换了幂等键：返回最近一条开场命令，不重复开场
            if sess["status"] in (SHOW_PLAYING, SHOW_PAUSED):
                prior = self.store.get(
                    "SELECT * FROM commands WHERE show_id=? AND venue_id=? AND cmd=?"
                    " ORDER BY seq DESC LIMIT 1",
                    (show_id, venue_id, CMD_OPEN),
                )
                return {"result": "already_open", "command": self.store._command_out(prior),
                        "session_status": sess["status"]}

            # 隔离中的会话不允许重新开场
            if sess["conn_state"] == VENUE_QUARANTINED:
                raise Forbidden(f"会话隔离中：{sess['quarantine_reason']}")

            # 授权检查
            self._assert_can_open(show, venue, sess, now)
            ent = self.store.get_entitlement(show_id, venue_id)

            command, _replayed = self.store.issue_command(
                show_id, venue_id, CMD_OPEN,
                payload={"key_batch_id": ent["key_batch_id"],
                         "window_end": ent["window_end"]},
                reason="开场握手",
                idempotency_key=idempotency_key,
            )
            self.store.update_session(
                show_id, venue_id,
                status=SHOW_PLAYING, conn_state=VENUE_ONLINE,
                key_batch_id=ent["key_batch_id"],
                opened_at=now, last_heartbeat=now,
            )
            if show["status"] in (SHOW_READY, SHOW_PENDING_AUTH):
                self.store.set_show_status(show_id, SHOW_PLAYING)
            self.store.add_disposition(
                show_id, venue_id, "开场握手", "授权通过，下发开场命令",
                applied_to_session=True, trigger_ref=idempotency_key,
                detail=f"命令序号 {command['seq']}",
            )
            self.store.commit()
            self._cond.notify_all()
            return {"result": "opened", "command": command,
                    "session_status": SHOW_PLAYING}

    def _window_end(self, show_id, venue_id):
        ent = self.store.get_entitlement(show_id, venue_id)
        return ent["window_end"] if ent else None

    def _assert_can_open(self, show, venue, sess, now):
        """开场前的全部授权条件；不满足则记处置并抛 Forbidden。"""
        blockers: list[tuple[str, str]] = []  # (trigger_type, detail)

        if show["status"] in (SHOW_ENDED, SHOW_CUT):
            blockers.append(("场次终结", f"场次状态={show['status']}"))
        if show["status"] == SHOW_PENDING_AUTH and show["reschedule_reason"]:
            blockers.append(("场次改期", show["reschedule_reason"]))

        ent = self.store.get_entitlement(show["id"], venue["id"])
        if ent is None:
            blockers.append(("授权缺失", "无有效权利窗口"))
        else:
            if now < ent["window_start"]:
                blockers.append(("权利未生效",
                                 f"窗口 {ent['window_start']} 起开放"))
            if now >= ent["window_end"]:
                blockers.append(("权利到期",
                                 f"窗口已于 {ent['window_end']} 到期"))
            batch = self.store.get_key_batch(ent["key_batch_id"]) if ent["key_batch_id"] else None
            if batch is None:
                blockers.append(("密钥缺失", "权利窗口未绑定密钥批次"))
            elif batch["status"] in (KEY_REVOKED,):
                blockers.append(("密钥吊销", f"批次 {batch['id']} 已吊销"))
            elif batch["status"] == KEY_ROTATING:
                blockers.append(("密钥轮换中", f"批次 {batch['id']} 轮换未完成"))

        ban = self.store.active_ban(venue["region"], show["id"])
        if ban is not None:
            blockers.append(("区域禁播",
                             f"{venue['region']}：{ban['reason'] or '未注明原因'}"))

        if blockers:
            for trigger, detail in blockers:
                self.store.add_disposition(
                    show["id"], venue["id"], trigger, "拒绝开场",
                    applied_to_session=True, detail=detail,
                )
            self.store.commit()
            raise Forbidden("；".join(f"{t}:{d}" for t, d in blockers))

    # ---- 中心控制命令 --------------------------------------------------

    def control(self, show_id, venue_id, cmd, reason=None):
        """运维对单点下发暂停/恢复/切断/结束。"""
        self._require_show(show_id)
        self._require_venue(venue_id)
        sess = self._require_session(show_id, venue_id)
        if cmd not in (CMD_PAUSE, CMD_RESUME, CMD_CUT, CMD_FINISH):
            raise Conflict(f"不支持的控制命令：{cmd}")
        with self.store.lock:
            if sess["status"] in (SHOW_ENDED, SHOW_CUT):
                raise Conflict(f"会话已终结（{sess['status']}），不能再下发{cmd}")
            if cmd == CMD_PAUSE and sess["status"] != SHOW_PLAYING:
                raise Conflict(f"仅播出中可暂停，当前={sess['status']}")
            if cmd == CMD_RESUME and sess["status"] != SHOW_PAUSED:
                raise Conflict(f"仅已暂停可恢复，当前={sess['status']}")
            if cmd == CMD_PAUSE and sess["conn_state"] == VENUE_QUARANTINED:
                raise Conflict("隔离会话不可暂停")
            result = self._issue(show_id, venue_id, cmd, reason=reason)
            command, _replayed = result
            self.store.commit()
            self._cond.notify_all()
            return command

    def emergency_cut(self, show_id, reason, venue_ids=None, region=None):
        """紧急切断：可指定单点、多个点，或整个区域。返回各点命令。"""
        self._require_show(show_id)
        targets = self._resolve_targets(show_id, venue_ids, region)
        results = []
        with self.store.lock:
            for venue_id in targets:
                sess = self.store.get_session(show_id, venue_id)
                if sess is None:
                    continue
                if sess["status"] in (SHOW_ENDED, SHOW_CUT):
                    results.append({"venue_id": venue_id, "skipped": "已终结"})
                    continue
                command, _ = self._issue(show_id, venue_id, CMD_CUT, reason=reason)
                results.append({"venue_id": venue_id, "command": command})
            self.store.add_disposition(
                show_id, None, "紧急切断", reason or "未注明",
                applied_to_session=bool(targets),
                detail=",".join(targets),
            )
            self.store.commit()
            self._cond.notify_all()
        return results

    def finish_show(self, show_id, reason=None):
        """整场结束：对所有未终结点位下发结束命令。"""
        self._require_show(show_id)
        results = []
        with self.store.lock:
            for sess in self.store.list_sessions(show_id):
                if sess["status"] in (SHOW_ENDED, SHOW_CUT):
                    results.append({"venue_id": sess["venue_id"], "skipped": "已终结"})
                    continue
                command, _ = self._issue(
                    show_id, sess["venue_id"], CMD_FINISH, reason=reason or "整场结束"
                )
                results.append({"venue_id": sess["venue_id"], "command": command})
            self.store.commit()
            self._cond.notify_all()
        return results

    def _resolve_targets(self, show_id, venue_ids, region):
        targets = list(venue_ids or [])
        if region is not None:
            for v in self.store.list_venues(region):
                if self.store.get_entitlement(show_id, v["id"]) is not None:
                    targets.append(v["id"])
        # 去重保序
        seen, ordered = set(), []
        for t in targets:
            if t not in seen:
                seen.add(t)
                ordered.append(t)
        for t in ordered:
            self._require_venue(t)
        return ordered

    # =====================================================================
    # 三、策略联动：区域禁播 / 场次改期 / 密钥轮换 / 权利到期
    # =====================================================================

    def issue_region_ban(self, region, reason, show_id=None):
        """区域禁播即时生效：

        - 未开始点位：标记授权阻断，开场握手将被拒绝；
        - 已开始会话：立即下发紧急切断并隔离，形成明确处置记录。
        """
        ban_id = _new_id("ban")
        self.store.issue_ban(ban_id, region, reason, show_id)
        affected = []
        with self.store.lock:
            rows = self.store.list_active_sessions()
            for sess in rows:
                if show_id is not None and sess["show_id"] != show_id:
                    continue
                if sess["region"] != region:
                    continue
                command, _ = self._issue(
                    sess["show_id"], sess["venue_id"], CMD_CUT,
                    reason=f"区域禁播：{reason}",
                )
                self.store.add_disposition(
                    sess["show_id"], sess["venue_id"], "区域禁播",
                    "已开始会话紧急切断并隔离",
                    applied_to_session=True, trigger_ref=ban_id, detail=reason,
                )
                affected.append({"show_id": sess["show_id"],
                                 "venue_id": sess["venue_id"], "command": command})
            # 未开始点位无需逐点留痕：禁令本身记录在 bans 表，
            # 各点开场握手被拒时会在其处置流中写明「区域禁播」。
            self.store.commit()
            self._cond.notify_all()
        return {"ban_id": ban_id, "region": region, "cut_sessions": affected}

    def lift_region_ban(self, ban_id):
        self.store.lift_ban(ban_id)
        self.store.commit()
        return {"ban_id": ban_id, "lifted": True}

    def reschedule_show(self, show_id, new_starts_at, new_end_at, reason):
        """场次改期：未开始点位授权冻结（拒绝开场）；已开始会话不强行中断，
        但形成处置记录并进入人工确认队列（暂停待处置）。

        改期后须为各点重发权利窗口（rebook_entitlement）并调用
        confirm_reschedule 解冻，未确认前任何点位都不能开场。
        """
        show = self._require_show(show_id)
        if show["status"] in (SHOW_ENDED, SHOW_CUT):
            raise Conflict(f"{show['status']}场次不能改期")
        if new_end_at <= new_starts_at:
            raise Conflict("改期后的结束时间必须晚于开始时间")
        with self.store.lock:
            self.store.reschedule_show(show_id, new_starts_at, new_end_at, reason)
            affected = []
            for sess in self.store.list_sessions(show_id):
                if sess["status"] in (SHOW_ENDED, SHOW_CUT, SHOW_READY):
                    self.store.add_disposition(
                        show_id, sess["venue_id"], "场次改期", "未开始，授权冻结",
                        applied_to_session=False, detail=reason,
                    )
                    continue
                # 已开始：暂停并挂起（不直接切断，由运维选择继续或切断）
                if sess["status"] == SHOW_PLAYING:
                    command, _ = self._issue(
                        show_id, sess["venue_id"], CMD_PAUSE,
                        reason=f"场次改期待处置：{reason}",
                    )
                else:
                    command = None
                self.store.update_session(
                    show_id, sess["venue_id"], quarantine_reason="场次改期待人工处置"
                )
                self.store.add_disposition(
                    show_id, sess["venue_id"], "场次改期",
                    "已开始会话暂停待人工处置",
                    applied_to_session=True, detail=reason,
                )
                affected.append({"venue_id": sess["venue_id"], "command": command})
            self.store.commit()
            self._cond.notify_all()
        return {"show_id": show_id, "held_sessions": affected,
                "new_starts_at": new_starts_at, "new_end_at": new_end_at}

    def confirm_reschedule(self, show_id):
        """全部点位权利窗口重发完毕后，解除改期冻结（待授权 -> 待开场）。"""
        show = self._require_show(show_id)
        if show["status"] != SHOW_PENDING_AUTH or not show["reschedule_reason"]:
            raise Conflict("该场次不处于改期冻结状态")
        # 未开始点位的有效权利窗口必须已对齐新档期（起点不早于新开场时间）；
        # 已开始并暂停待处置的会话沿用旧授权，不参与解冻校验。
        stale = self.store.all(
            "SELECT e.venue_id FROM entitlements e JOIN sessions s"
            " ON s.show_id=e.show_id AND s.venue_id=e.venue_id"
            " WHERE e.show_id=? AND e.active=1 AND s.status=?"
            " AND e.window_start < ?",
            (show_id, SHOW_READY, show["starts_at"]),
        )
        if stale:
            raise Conflict(
                "仍有放映点未按新档期重发权利窗口："
                + ",".join(r["venue_id"] for r in stale))
        with self.store.lock:
            self.store.set_show_status(show_id, SHOW_READY)
            self.store.conn.execute(
                "UPDATE shows SET reschedule_reason=NULL WHERE id=?", (show_id,))
            self.store.add_disposition(
                show_id, None, "场次改期", "改期确认完成，解除授权冻结",
                applied_to_session=False,
            )
            self.store.commit()
        return {"show_id": show_id, "status": SHOW_READY}

    def rotate_key(self, show_id, reason="例行密钥轮换"):
        """生成新有效批次、吊销旧批次；未开始点位重绑，进行中下发轮换命令。"""
        self._require_show(show_id)
        batch = self.create_key_batch(show_id, activate=True)
        with self.store.lock:
            # create_key_batch(activate=True) 已吊销旧批次；显式补记原因
            for row in self.store.list_key_batches(show_id):
                if row["status"] == KEY_REVOKED and row["id"] != batch["id"]:
                    self.store.set_key_status(row["id"], KEY_REVOKED, reason=reason)
            self.store.conn.execute(
                "UPDATE entitlements SET key_batch_id=? WHERE show_id=? AND active=1",
                (batch["id"], show_id),
            )
            notified = []
            for sess in self.store.list_active_sessions():
                if sess["show_id"] != show_id:
                    continue
                command, _ = self._issue(
                    show_id, sess["venue_id"], CMD_ROTATE_KEY,
                    payload={"key_batch_id": batch["id"]}, reason=reason,
                )
                notified.append({"venue_id": sess["venue_id"], "command": command})
                self.store.update_session(
                    show_id, sess["venue_id"], key_batch_id=batch["id"]
                )
            self.store.add_disposition(
                show_id, None, "密钥轮换",
                "未开始点位重绑新批次；进行中会话下发轮换",
                applied_to_session=False, trigger_ref=batch["id"], detail=reason,
            )
            self.store.commit()
            self._cond.notify_all()
        return {"new_batch": batch, "notified_sessions": notified}

    def expire_entitlements(self):
        """权利窗口到期：进行中会话立即切断，杜绝到期后仍能拉流。"""
        now = self.now()
        acted = []
        with self.store.lock:
            for sess in self.store.list_active_sessions():
                ent = self.store.get_entitlement(sess["show_id"], sess["venue_id"])
                if ent is None or now >= ent["window_end"]:
                    reason = "权利窗口到期，强制停止"
                    command, _ = self._issue(
                        sess["show_id"], sess["venue_id"], CMD_CUT, reason=reason,
                    )
                    self.store.add_disposition(
                        sess["show_id"], sess["venue_id"], "权利到期",
                        "到期切断", applied_to_session=True,
                        detail=f"window_end={ent['window_end'] if ent else None}",
                    )
                    acted.append({"show_id": sess["show_id"],
                                  "venue_id": sess["venue_id"], "command": command})
            self.store.commit()
            self._cond.notify_all()
        return acted

    # =====================================================================
    # 四、边缘面：心跳 / 命令拉取 / 事件合并 / 重连
    # =====================================================================

    def heartbeat(self, show_id, venue_id, observed_seq=None):
        self._require_show(show_id)
        self._require_venue(venue_id)
        sess = self._require_session(show_id, venue_id)
        now = self.now()
        with self.store.lock:
            conn = sess["conn_state"]
            if sess["conn_state"] == VENUE_BRIEFLY_OFFLINE:
                conn = VENUE_ONLINE
                self.store.update_session(
                    show_id, venue_id, conn_state=VENUE_ONLINE, last_heartbeat=now
                )
                self.store.add_disposition(
                    show_id, venue_id, "连接恢复", "心跳恢复，退出短时离线",
                    applied_to_session=True,
                )
            elif sess["conn_state"] == VENUE_ONLINE:
                self.store.update_session(show_id, venue_id, last_heartbeat=now)
            self.store.commit()
            # 命令一律走 /commands 拉取通道，保证按序号投递与去重
            return {"now": now, "conn_state": conn,
                    "session_status": sess["status"],
                    "delivered_seq": sess["delivered_seq"]}

    def poll_commands(self, show_id, venue_id, after_seq=None, wait=0.0):
        """边缘拉取 after_seq 之后的命令（长轮询，wait 秒内有新命令即返回）。

        严格按场次单调序号返回，保证边缘只会顺序执行；重复拉取幂等。
        """
        self._require_show(show_id)
        self._require_venue(venue_id)
        sess = self._require_session(show_id, venue_id)
        cursor = sess["delivered_seq"] if after_seq is None else after_seq
        with self._cond:
            def _pending():
                return [dict(c) for c in self.store.list_commands(
                    show_id, venue_id, after_seq=cursor)]
            pending = _pending()
            if not pending and wait > 0:
                self._cond.wait(timeout=wait)
                pending = _pending()
            if pending:
                new_max = max(c["seq"] for c in pending)
                if new_max > sess["delivered_seq"]:
                    self.store.update_session(
                        show_id, venue_id, delivered_seq=new_max
                    )
                    for c in pending:
                        if c["status"] == CMD_PENDING:
                            self.store.mark_command(
                                show_id, c["seq"], CMD_DELIVERED
                            )
                    self.store.commit()
            return {"after_seq": cursor, "commands": pending,
                    "delivered_seq": max(sess["delivered_seq"],
                                         max((c["seq"] for c in pending), default=cursor))}

    def report_events(self, show_id, venue_id, events):
        """合并边缘上报的一批事件（离线缓冲或在线实时）。

        序号规则（边缘会话内单调）：
        - seq 必须 == last_event_seq+1（逐事件检查），允许重放已合并的序号；
        - 整批中出现缺口（如已有 1..3，上报 [5,6] 缺 4）：隔离会话，
          只保留连续前缀，等待边缘补传；
        - 重复序号（中心网络抖动导致重发）：去重丢弃，绝不二次开场；
        - 终态事件（已切断/已结束）后不接受任何后续状态事件。
        """
        self._require_show(show_id)
        self._require_venue(venue_id)
        sess = self._require_session(show_id, venue_id)
        if not isinstance(events, list) or not events:
            raise Conflict("events 必须为非空数组")

        result = {"merged": [], "duplicates": [], "rejected": [], "quarantined": False}
        with self.store.lock:
            # 终态以「边缘已回报的终态事件」为准——中心可能已乐观置为切断，
            # 但边缘缓冲的暂停/切断 ACK 仍必须正常合并。
            terminal = bool(self.store.get(
                "SELECT 1 FROM events WHERE show_id=? AND venue_id=? AND kind IN (?,?)",
                (show_id, venue_id, EV_CUT, EV_FINISHED)))
            expected = sess["last_event_seq"] + 1
            new_last_seq = sess["last_event_seq"]
            gap_detected = False
            known = set(self.store.event_seqs(show_id, venue_id))

            for ev in events:
                seq = ev.get("seq")
                kind = ev.get("kind")
                valid_kind = kind in _EVENT_TO_CMD or kind == EV_HEARTBEAT
                if not isinstance(seq, int) or seq < 1 or not valid_kind:
                    result["rejected"].append({"seq": seq, "reason": "事件类型非法"})
                    continue
                occurred_at = ev.get("occurred_at", self.now())
                buffered = bool(ev.get("offline_buffered", False))

                # 已合并 -> 重复上报，直接去重（不产生任何状态变化）
                if seq in known:
                    result["duplicates"].append(seq)
                    continue
                # 缺口 -> 从这里起整段隔离，拒绝合并
                if seq != expected:
                    gap_detected = True
                    result["rejected"].append(
                        {"seq": seq, "reason": f"序号缺口，期望 {expected}"}
                    )
                    continue
                if terminal and kind != EV_HEARTBEAT:
                    result["rejected"].append(
                        {"seq": seq, "reason": f"会话已终结({sess['status']})，事件丢弃"}
                    )
                    continue
                inserted = self.store.record_event(
                    show_id, venue_id, seq, kind, occurred_at, buffered,
                    payload=ev.get("payload"),
                )
                if not inserted:
                    result["duplicates"].append(seq)
                    continue
                known.add(seq)
                new_last_seq = seq
                expected = seq + 1
                result["merged"].append(seq)

                # 心跳事件（在线或离线缓冲）：仅刷新心跳时间
                if kind == EV_HEARTBEAT:
                    self.store.update_session(
                        show_id, venue_id, last_heartbeat=occurred_at
                    )
                    continue

                # 状态事件 -> 推进会话状态并 ACK 对应命令
                self._apply_edge_event(show_id, venue_id, kind, occurred_at, seq)
                if kind in _TERMINAL_EVENT_STATUS:
                    terminal = True

            if gap_detected:
                self._quarantine(
                    show_id, venue_id,
                    f"事件序列缺口（下一个期望序号 {expected}），等待补传核对",
                )
                result["quarantined"] = True
            elif new_last_seq != sess["last_event_seq"]:
                self.store.update_session(
                    show_id, venue_id, last_event_seq=new_last_seq
                )

            # 有事件到达说明连接正常（上报动作本身即存活证据，
            # 不能用缓冲事件的旧 occurred_at 当心跳，否则会立刻又被判定离线）
            if result["merged"]:
                self.store.update_session(
                    show_id, venue_id, last_heartbeat=self.now()
                )
            if sess["conn_state"] == VENUE_BRIEFLY_OFFLINE and not gap_detected:
                self.store.update_session(show_id, venue_id, conn_state=VENUE_ONLINE)
                self.store.add_disposition(
                    show_id, venue_id, "连接恢复",
                    "离线缓冲事件已按单调序列合并",
                    applied_to_session=True,
                )
            self.store.commit()
            self._cond.notify_all()
        return result

    def _apply_edge_event(self, show_id, venue_id, kind, occurred_at, seq):
        sess = self.store.get_session(show_id, venue_id)
        cmd_name = _EVENT_TO_CMD[kind]
        # ACK 匹配：最近一条同类型且未确认的命令
        match = self.store.get(
            "SELECT * FROM commands WHERE show_id=? AND venue_id=? AND cmd=?"
            " AND status!=? ORDER BY seq DESC LIMIT 1",
            (show_id, venue_id, cmd_name, CMD_SUPERSEDED),
        )
        if match is not None and match["status"] != CMD_ACKED:
            self.store.mark_command(show_id, match["seq"], CMD_ACKED, when=occurred_at)

        updates = {"last_event_seq": seq, "last_heartbeat": occurred_at}
        if kind in _TERMINAL_EVENT_STATUS:
            updates["status"] = _TERMINAL_EVENT_STATUS[kind]
            if kind == EV_CUT:
                updates["conn_state"] = VENUE_QUARANTINED
                updates["quarantined_at"] = occurred_at
                updates["quarantine_reason"] = "边缘确认切断"
            else:
                updates["conn_state"] = VENUE_FINISHED
        elif kind == EV_PAUSED:
            # 中心若已处于终态（切断命令先于缓冲 ACK 到达），不得回降级
            if sess["status"] not in (SHOW_CUT, SHOW_ENDED):
                updates["status"] = SHOW_PAUSED
        elif kind in (EV_OPENED, EV_RESUMED):
            if sess["status"] not in (SHOW_CUT, SHOW_ENDED, SHOW_PAUSED):
                updates["status"] = SHOW_PLAYING
        self.store.update_session(show_id, venue_id, **updates)
        self._recompute_show_status(show_id)

    def reconcile(self, show_id, venue_id, last_event_seq, delivered_seq,
                  session_status, commands_acked):
        """重连握手：边缘上报本地视图，中心按单调序列对账。

        - 补传边缘缺失的命令（delivered_seq 之后全部重放，边缘按序幂等执行）；
        - 拉取边缘缓冲事件由随后 report_events 完成；
        - 双方状态冲突时以中心授权状态为准，下发纠偏命令并记录偏差。
        """
        self._require_show(show_id)
        self._require_venue(venue_id)
        sess = self._require_session(show_id, venue_id)
        with self.store.lock:
            # 中心可能在离线期间发过命令：全部重放给边缘
            missed = [dict(c) for c in self.store.list_commands(
                show_id, venue_id, after_seq=delivered_seq)]
            corrections = []
            if not missed:
                # 状态对齐纠偏
                if session_status == SHOW_PLAYING and sess["status"] == SHOW_PAUSED:
                    cmd, _ = self._issue(
                        show_id, venue_id, CMD_PAUSE, reason="重连对账：中心为暂停态"
                    )
                    corrections.append(cmd)
                elif session_status == SHOW_PLAYING and sess["status"] in (SHOW_CUT, SHOW_ENDED):
                    cmd, _ = self._issue(
                        show_id, venue_id,
                        CMD_CUT if sess["status"] == SHOW_CUT else CMD_FINISH,
                        reason=f"重连对账：中心为{sess['status']}态",
                    )
                    corrections.append(cmd)
            self.store.add_disposition(
                show_id, venue_id, "重连对账",
                f"补发命令 {len(missed)} 条，纠偏 {len(corrections)} 条",
                applied_to_session=True,
                detail=f"edge(last_event={last_event_seq},delivered={delivered_seq},"
                       f"status={session_status})",
            )
            if sess["conn_state"] == VENUE_BRIEFLY_OFFLINE:
                self.store.update_session(show_id, venue_id, conn_state=VENUE_ONLINE)
            self.store.commit()
            self._cond.notify_all()
            return {
                "center_session_status": sess["status"],
                "center_last_event_seq": sess["last_event_seq"],
                "center_delivered_seq": sess["delivered_seq"],
                "missed_commands": missed,
                "corrections": [c for c in corrections if c],
                "quarantined": sess["conn_state"] == VENUE_QUARANTINED,
            }

    def _quarantine(self, show_id, venue_id, reason):
        """持锁状态下隔离会话并留痕（一致性保护，不自行折叠状态）。"""
        self.store.update_session(
            show_id, venue_id,
            conn_state=VENUE_QUARANTINED,
            quarantined_at=self.now(), quarantine_reason=reason,
        )
        self.store.add_disposition(
            show_id, venue_id, "事件序列缺口", "隔离会话，等待人工核对补传",
            applied_to_session=True, detail=reason,
        )

    def release_quarantine(self, show_id, venue_id, note="人工核对完成"):
        """人工核对后解除隔离（仅允许在已切断/已结束终态之外恢复管理）。"""
        sess = self._require_session(show_id, venue_id)
        with self.store.lock:
            if sess["conn_state"] != VENUE_QUARANTINED:
                raise Conflict("该会话未处于隔离状态")
            new_conn = VENUE_FINISHED if sess["status"] in (SHOW_CUT, SHOW_ENDED) \
                else VENUE_ONLINE
            self.store.update_session(
                show_id, venue_id, conn_state=new_conn,
                quarantine_reason=f"已解除：{note}",
            )
            self.store.add_disposition(
                show_id, venue_id, "解除隔离", note, applied_to_session=True,
            )
            self.store.commit()
            return {"show_id": show_id, "venue_id": venue_id, "conn_state": new_conn}

    # =====================================================================
    # 五、后台巡检：离线判定 / 权利到期
    # =====================================================================

    def sweep(self) -> dict:
        """一次巡检：心跳超时 -> 短时离线；窗口到期 -> 切断。"""
        now = self.now()
        offline = []
        with self.store.lock:
            for sess in self.store.list_active_sessions():
                if sess["conn_state"] not in (VENUE_ONLINE, VENUE_BRIEFLY_OFFLINE):
                    continue
                # 暂停态边缘仍维持心跳，使用同一超时阈值
                last = sess["last_heartbeat"]
                if last is None or now - last > self.heartbeat_timeout:
                    if sess["conn_state"] == VENUE_ONLINE:
                        self.store.update_session(
                            sess["show_id"], sess["venue_id"],
                            conn_state=VENUE_BRIEFLY_OFFLINE,
                        )
                        self.store.add_disposition(
                            sess["show_id"], sess["venue_id"], "连接中断",
                            f"心跳 {now - last:.1f}s 超时，进入短时离线自治",
                            applied_to_session=True,
                        )
                        offline.append(f"{sess['show_id']}/{sess['venue_id']}")
            self.store.commit()
        expired = self.expire_entitlements()
        return {"offline": offline, "expired": expired, "at": now}

    # =====================================================================
    # 六、查询面：时间线与偏差解释
    # =====================================================================

    def show_status(self, show_id):
        show = self._require_show(show_id)
        with self.store.lock:
            sessions = []
            for s in self.store.list_sessions(show_id):
                d = dict(s)
                ent = self.store.get_entitlement(show_id, s["venue_id"])
                d["window"] = ({"start": ent["window_start"], "end": ent["window_end"]}
                               if ent else None)
                sessions.append(d)
            return {"show": dict(show), "sessions": sessions,
                    "key_batches": [dict(k) for k in self.store.list_key_batches(show_id)]}

    def venue_timeline(self, show_id, venue_id):
        """某城市实际看到的时间线：中心命令流与边缘事件流合并，附偏差解释。"""
        self._require_show(show_id)
        self._require_venue(venue_id)
        sess = self._require_session(show_id, venue_id)
        with self.store.lock:
            commands = [dict(c) for c in self.store.list_commands(show_id, venue_id)]
            events = [dict(e) for e in self.store.list_events(show_id, venue_id)]
            dispositions = [dict(d) for d in self.store.list_dispositions(show_id, venue_id)]
            venue = dict(self.store.get_venue(venue_id))
            deviations = self._explain_deviations(
                show_id, venue_id, sess, commands, events, dispositions
            )
            timeline = self._build_timeline(commands, events, dispositions)
            return {
                "show_id": show_id,
                "venue": venue,
                "session": dict(sess),
                "timeline": timeline,
                "deviations": deviations,
            }

    def _build_timeline(self, commands, events, dispositions):
        items = []
        for c in commands:
            payload = c["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload or "{}")
            items.append({
                "at": c["issued_at"], "direction": "center->edge",
                "seq": c["seq"], "type": c["cmd"], "status": c["status"],
                "reason": c["reason"], "payload": payload,
            })
        for e in events:
            payload = e["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload or "{}")
            items.append({
                "at": e["occurred_at"], "direction": "edge",
                "seq": e["seq"], "type": e["kind"],
                "buffered_during_offline": bool(e["offline_buffered"]),
                "received_at": e["received_at"],
                "payload": payload,
            })
        for d in dispositions:
            items.append({
                "at": d["created_at"], "direction": "system",
                "type": d["trigger_type"], "action": d["action"],
                "detail": d["detail"],
            })
        items.sort(key=lambda x: (x["at"], {"system": 0, "center->edge": 1, "edge": 2}[x["direction"]]))
        return items

    def _explain_deviations(self, show_id, venue_id, sess, commands, events, dispositions):
        """对照命令与 ACK，解释每个偏差的来源。"""
        notes = []
        by_kind: dict[str, list] = {}
        for c in commands:
            by_kind.setdefault(c["cmd"], []).append(c)
        ev_kinds = {e["kind"] for e in events}

        for c in commands:
            if c["status"] == CMD_PENDING and c["cmd"] != CMD_ROTATE_KEY:
                # 仍未送达：离线或隔离
                why = {
                    VENUE_BRIEFLY_OFFLINE: "放映点短时离线，命令待重连补发",
                    VENUE_QUARANTINED: "会话隔离中，命令挂起待人工处置",
                    VENUE_FINISHED: "放映点已退场，命令不再送达",
                }.get(sess["conn_state"], "命令尚未送达")
                notes.append({"type": "命令未确认", "seq": c["seq"], "cmd": c["cmd"],
                              "explanation": why, "source": "连接状态"})
            elif c["status"] == CMD_DELIVERED and c["cmd"] in (
                    CMD_OPEN, CMD_PAUSE, CMD_RESUME, CMD_CUT, CMD_FINISH):
                expect_ev = {CMD_OPEN: EV_OPENED, CMD_PAUSE: EV_PAUSED,
                             CMD_RESUME: EV_RESUMED, CMD_CUT: EV_CUT,
                             CMD_FINISH: EV_FINISHED}[c["cmd"]]
                if expect_ev not in ev_kinds:
                    lag = self.now() - c["delivered_at"] if c["delivered_at"] else None
                    notes.append({
                        "type": "已送达未确认", "seq": c["seq"], "cmd": c["cmd"],
                        "explanation": f"边缘已收到但未回报{expect_ev}"
                                       + (f"，已延迟 {lag:.0f}s" if lag is not None else ""),
                        "source": "ACK 缺口",
                    })

        # 离线缓冲导致的时间线偏差
        buffered = [e for e in events if e["offline_buffered"]]
        if buffered:
            first, last = buffered[0], buffered[-1]
            lag = max(e["received_at"] - e["occurred_at"] for e in buffered)
            notes.append({
                "type": "离线自治窗口",
                "explanation": (f"边缘在离线期间本地继续播控，{len(buffered)} 个事件"
                                f"（序号 {first['seq']}..{last['seq']}）缓冲后补传，"
                                f"中心视图最大滞后 {lag:.0f}s"),
                "source": "边缘缓冲",
            })

        for d in dispositions:
            if d["trigger_type"] in ("区域禁播", "权利到期", "密钥轮换", "场次改期",
                                      "命令忽略", "事件序列缺口", "连接中断",
                                      "连接恢复", "重连对账", "中心重启恢复"):
                notes.append({
                    "type": d["trigger_type"], "explanation": d["action"],
                    "detail": d["detail"], "source": "策略联动",
                })

        if sess["conn_state"] == VENUE_QUARANTINED:
            notes.append({
                "type": "隔离",
                "explanation": f"会话处于隔离：{sess['quarantine_reason']}",
                "source": "一致性保护",
            })
        return notes
