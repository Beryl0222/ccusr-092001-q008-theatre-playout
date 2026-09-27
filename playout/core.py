"""播控核心：场次协调、幂等命令、离线自治合并与权利窗口执行。

设计要点：

- 每个场次拥有一条单调递增的控制事件日志（``sessions.last_seq`` 分配序号），
  中心命令、边缘确认与离线合并事件都进入同一条日志；
- 所有写入口都有幂等键：中心命令用 ``command_id``，边缘请求用
  ``edge_event_id``，网络抖动造成的重试只返回首次结果，绝不二次开场；
- 边缘短时离线期间的自治事件按 ``edge_seq`` 单调合并回中心日志，
  与中心状态冲突时以中心为准，并留下冲突偏差记录；
- 权利窗口在握手、心跳与巡检三处强制执行，窗口到期立即切断并吊销流令牌；
- 全部状态持久化在 SQLite，中心重启后通过 :meth:`PlayoutCore.recover`
  恢复管理仍在进行的场次。
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from typing import Callable, Optional

from . import models as m
from .store import Store


class CoreError(Exception):
    """业务错误：携带 HTTP 状态码、错误码与中文说明。"""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message

    def body(self) -> dict:
        return {"error": {"code": self.code, "message": self.message}}


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


DEFAULT_CONFIG = {
    "token_ttl": 60.0,          # 流令牌有效期（秒），心跳续期且不超过权利窗口终点
    "offline_after": 15.0,      # 心跳缺失多久记为「短时离线」
    "isolate_after": 120.0,     # 心跳缺失多久记为「隔离」
    "propagation_slo": 2.0,     # 命令传播延迟偏差阈值（秒）
    "key_grace": 30.0,          # 密钥轮换默认宽限（秒）
    "open_skew_epsilon": 0.5,   # 开场偏差记录阈值（秒）
}


class PlayoutCore:
    """协调中心。所有写操作经 ``self._lock`` 串行化，状态以事件日志为准。"""

    def __init__(
        self,
        store: Store,
        clock: Optional[Callable[[], float]] = None,
        config: Optional[dict] = None,
    ):
        self.store = store
        self.clock = clock or time.time
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self._lock = threading.RLock()

    # ================================================================
    # 基础设施
    # ================================================================

    def _mutate(self, fn):
        """在锁与事务中执行写操作；异常回滚。"""
        with self._lock:
            with self.store.conn:
                return fn()

    def _edge_call(self, site_id: str, edge_event_id: str, endpoint: str, fn):
        """边缘请求的幂等包装：同一 (site_id, edge_event_id) 只执行一次。

        首次执行的结果（含业务拒绝）被持久化，重试直接返回缓存，
        这是「网络抖动不造成二次开场」的第一道防线。
        """
        with self._lock:
            cached = self.store.one(
                "SELECT status, response FROM edge_requests WHERE site_id=? AND edge_event_id=?",
                (site_id, edge_event_id),
            )
            if cached is not None:
                body = json.loads(cached["response"])
                if cached["status"] >= 400:
                    err = body.get("error", {})
                    raise CoreError(cached["status"], err.get("code", "错误"), err.get("message", ""))
                return body
            try:
                with self.store.conn:
                    body = fn()
                    self._store_edge_response(site_id, edge_event_id, endpoint, 200, body)
                return body
            except CoreError as exc:
                with self.store.conn:
                    self._store_edge_response(site_id, edge_event_id, endpoint, exc.status, exc.body())
                raise

    def _store_edge_response(self, site_id, edge_event_id, endpoint, status, body):
        self.store.run(
            "INSERT INTO edge_requests (site_id, edge_event_id, endpoint, status, response, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (site_id, edge_event_id, endpoint, status, json.dumps(body, ensure_ascii=False), self.clock()),
        )

    # ---- 行访问 ----

    def _session(self, session_id: str) -> dict:
        row = self.store.one("SELECT * FROM sessions WHERE session_id=?", (session_id,))
        if row is None:
            raise CoreError(404, "场次不存在", f"场次 {session_id} 不存在")
        return dict(row)

    def _site(self, site_id: str) -> dict:
        row = self.store.one("SELECT * FROM sites WHERE site_id=?", (site_id,))
        if row is None:
            raise CoreError(404, "放映点不存在", f"放映点 {site_id} 不存在")
        return dict(row)

    def _point(self, session_id: str, site_id: str) -> dict:
        row = self.store.one(
            "SELECT * FROM points WHERE session_id=? AND site_id=?", (session_id, site_id)
        )
        if row is None:
            raise CoreError(404, "点位不存在", f"放映点 {site_id} 未参加场次 {session_id}")
        return dict(row)

    def _points(self, session_id: str) -> list:
        return [dict(r) for r in self.store.all(
            "SELECT * FROM points WHERE session_id=? ORDER BY site_id", (session_id,))]

    def _window(self, session_id: str, region: str) -> Optional[dict]:
        row = self.store.one(
            "SELECT * FROM rights_windows WHERE session_id=? AND region=?", (session_id, region))
        return dict(row) if row else None

    def _active_ban(self, session_id: str, region: str) -> Optional[dict]:
        row = self.store.one(
            "SELECT * FROM bans WHERE session_id=? AND region=? AND lifted_at IS NULL"
            " ORDER BY created_at DESC LIMIT 1",
            (session_id, region),
        )
        return dict(row) if row else None

    def _latest_key(self, session_id: str) -> Optional[dict]:
        row = self.store.one(
            "SELECT * FROM key_batches WHERE session_id=? AND status=?"
            " ORDER BY generation DESC LIMIT 1",
            (session_id, m.KEY_ACTIVE),
        )
        return dict(row) if row else None

    # ---- 状态写回 ----

    def _save_session(self, s: dict):
        self.store.run(
            "UPDATE sessions SET title=?, status=?, scheduled_start=?, scheduled_end=?, updated_at=?"
            " WHERE session_id=?",
            (s["title"], s["status"], s["scheduled_start"], s["scheduled_end"],
             s["updated_at"], s["session_id"]),
        )

    def _save_site(self, site: dict):
        self.store.run(
            "UPDATE sites SET status=?, last_heartbeat=?, updated_at=? WHERE site_id=?",
            (site["status"], site["last_heartbeat"], site["updated_at"], site["site_id"]),
        )

    def _save_point(self, p: dict):
        self.store.run(
            "UPDATE points SET state=?, permit_id=?, permit_seq=?, stream_token=?,"
            " token_valid_until=?, key_batch_id=?, opened_at=?, ended_at=?, exited_at=?,"
            " applied_seq=?, updated_at=? WHERE session_id=? AND site_id=?",
            (p["state"], p["permit_id"], p["permit_seq"], p["stream_token"],
             p["token_valid_until"], p["key_batch_id"], p["opened_at"], p["ended_at"],
             p["exited_at"], p["applied_seq"], p["updated_at"], p["session_id"], p["site_id"]),
        )

    def _touch_site(self, site: dict, now: float):
        """任何边缘联系都视为在线证据。"""
        site["last_heartbeat"] = now
        site["status"] = m.SITE_ONLINE
        site["updated_at"] = now
        self._save_site(site)

    # ---- 事件日志 ----

    def _next_seq(self, session_id: str) -> int:
        self.store.run(
            "UPDATE sessions SET last_seq = last_seq + 1 WHERE session_id=?", (session_id,))
        return self.store.one(
            "SELECT last_seq FROM sessions WHERE session_id=?", (session_id,))["last_seq"]

    def _event(self, session_id, kind, site_id=None, origin="center", ref_seq=None, payload=None) -> dict:
        seq = self._next_seq(session_id)
        event = {
            "session_id": session_id,
            "seq": seq,
            "event_id": _uid("ev"),
            "kind": kind,
            "site_id": site_id,
            "origin": origin,
            "ref_seq": ref_seq,
            "payload": payload or {},
            "created_at": self.clock(),
        }
        self.store.run(
            "INSERT INTO events (session_id, seq, event_id, kind, site_id, origin, ref_seq, payload, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (session_id, seq, event["event_id"], kind, site_id, origin, ref_seq,
             json.dumps(event["payload"], ensure_ascii=False), event["created_at"]),
        )
        return event

    def _record_deviation(self, session_id, site_id, kind, seconds, detail):
        self.store.run(
            "INSERT INTO deviations (deviation_id, session_id, site_id, kind, seconds, detail, created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (_uid("dev"), session_id, site_id, kind, float(seconds), detail, self.clock()),
        )

    def _disposition(self, session_id, site_id, policy_type, policy_id, action, reason):
        self.store.run(
            "INSERT INTO dispositions (disposition_id, session_id, site_id, policy_type, policy_id,"
            " action, reason, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (_uid("disp"), session_id, site_id, policy_type, policy_id, action, reason, self.clock()),
        )

    # ---- 派生状态 ----

    def _refresh_session_status(self, session_id: str) -> str:
        """由点位状态推导场次状态；只前进不回退，终态不再变化。"""
        session = self._session(session_id)
        current = session["status"]
        if current in m.SESSION_TERMINAL:
            return current
        states = [p["state"] for p in self._points(session_id)]
        new = None
        if states and all(s in m.POINT_TERMINAL for s in states):
            if current != m.SESSION_PENDING_AUTH:  # 未授权场次不自动收尾
                if any(s in (m.POINT_CUT, m.POINT_BLOCKED) for s in states):
                    new = m.SESSION_CUT
                else:
                    new = m.SESSION_ENDED
        elif any(s == m.POINT_PLAYING for s in states):
            new = m.SESSION_PLAYING
        elif any(s == m.POINT_PAUSED for s in states):
            new = m.SESSION_PAUSED
        if new and new != current:
            session["status"] = new
            session["updated_at"] = self.clock()
            self._save_session(session)
            self._event(session_id, m.EV_SESSION_STATUS, payload={"from": current, "to": new})
        return session["status"]

    def _open_skew(self, session: dict, opened_at: float, site_id: str):
        seconds = opened_at - session["scheduled_start"]
        if abs(seconds) >= self.config["open_skew_epsilon"]:
            direction = "延后" if seconds > 0 else "提前"
            self._record_deviation(
                session["session_id"], site_id, m.DEV_OPEN_SKEW, seconds,
                f"实际开场较计划{direction} {abs(seconds):.1f} 秒")

    def _maybe_site_exited(self, site: dict, session_id: str):
        """该站点没有待开场/进行中点位时，标记观众退场完成。"""
        row = self.store.one(
            "SELECT COUNT(*) AS c FROM points WHERE site_id=? AND state IN (?,?,?,?)",
            (site["site_id"], m.POINT_PENDING, *m.POINT_ACTIVE),
        )
        if row["c"] == 0:
            site["status"] = m.SITE_EXITED
            site["updated_at"] = self.clock()
            self._save_site(site)
            self._event(session_id, m.EV_SITE_EXIT, site_id=site["site_id"])

    # ================================================================
    # 资源管理：放映点 / 场次 / 授权 / 改期
    # ================================================================

    def create_site(self, site_id, name, city, region):
        def _do():
            if self.store.one("SELECT 1 AS x FROM sites WHERE site_id=?", (site_id,)):
                raise CoreError(409, "放映点已存在", f"放映点 {site_id} 已存在")
            now = self.clock()
            self.store.run(
                "INSERT INTO sites (site_id, name, city, region, status, last_heartbeat, created_at, updated_at)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (site_id, name, city, region, m.SITE_ONLINE, None, now, now),
            )
            return self.get_site(site_id)
        return self._mutate(_do)

    def get_site(self, site_id):
        with self._lock:
            site = self._site(site_id)
            rows = self.store.all(
                "SELECT p.session_id, p.state, p.opened_at, p.ended_at, s.title"
                " FROM points p JOIN sessions s ON s.session_id = p.session_id"
                " WHERE p.site_id=? ORDER BY p.updated_at DESC",
                (site_id,),
            )
            site["sessions"] = [dict(r) for r in rows]
            return site

    def list_sites(self):
        with self._lock:
            return {"items": [dict(r) for r in self.store.all("SELECT * FROM sites ORDER BY site_id")]}

    def create_session(self, session_id, title, scheduled_start, scheduled_end,
                       windows=None, site_ids=()):
        windows = windows or []
        site_ids = list(dict.fromkeys(site_ids or []))

        def _do():
            # 先校验，再落库，避免半截数据
            if self.store.one("SELECT 1 AS x FROM sessions WHERE session_id=?", (session_id,)):
                raise CoreError(409, "场次已存在", f"场次 {session_id} 已存在")
            if scheduled_end <= scheduled_start:
                raise CoreError(400, "时间非法", "scheduled_end 必须晚于 scheduled_start")
            if not site_ids:
                raise CoreError(400, "缺少放映点", "场次至少需要一个放映点")
            for w in windows:
                if w["not_after"] <= w["not_before"]:
                    raise CoreError(400, "窗口非法", "权利窗口 not_after 必须晚于 not_before")
            for sid in site_ids:
                self._site(sid)
            now = self.clock()
            self.store.run(
                "INSERT INTO sessions (session_id, title, status, scheduled_start, scheduled_end,"
                " last_seq, created_at, updated_at) VALUES (?,?,?,?,?,0,?,?)",
                (session_id, title, m.SESSION_PENDING_AUTH, scheduled_start, scheduled_end, now, now),
            )
            for w in windows:
                self.store.run(
                    "INSERT INTO rights_windows (session_id, region, not_before, not_after) VALUES (?,?,?,?)",
                    (session_id, w["region"], w["not_before"], w["not_after"]),
                )
            for sid in site_ids:
                self.store.run(
                    "INSERT INTO points (session_id, site_id, state, applied_seq, updated_at)"
                    " VALUES (?,?,?,?,?)",
                    (session_id, sid, m.POINT_PENDING, 0, now),
                )
            self._event(session_id, m.EV_SESSION_CREATED,
                        payload={"title": title, "sites": site_ids})
            return self.get_session(session_id)
        return self._mutate(_do)

    def get_session(self, session_id):
        with self._lock:
            session = self._session(session_id)
            session["windows"] = [dict(r) for r in self.store.all(
                "SELECT region, not_before, not_after FROM rights_windows"
                " WHERE session_id=? ORDER BY region", (session_id,))]
            session["points"] = [dict(r) for r in self.store.all(
                "SELECT p.site_id, p.state, p.opened_at, p.ended_at, p.exited_at, p.applied_seq,"
                " p.key_batch_id, p.token_valid_until, p.permit_id,"
                " s.name, s.city, s.region"
                " FROM points p JOIN sites s ON s.site_id = p.site_id"
                " WHERE p.session_id=? ORDER BY p.site_id", (session_id,))]
            return session

    def list_sessions(self):
        with self._lock:
            return {"items": [dict(r) for r in self.store.all(
                "SELECT session_id, title, status, scheduled_start, scheduled_end, last_seq, updated_at"
                " FROM sessions ORDER BY created_at")]}

    def authorize_session(self, session_id):
        def _do():
            session = self._session(session_id)
            if session["status"] != m.SESSION_PENDING_AUTH:
                raise CoreError(409, "状态非法", f"场次状态「{session['status']}」，仅「待授权」可授权")
            if not self.store.all("SELECT 1 AS x FROM rights_windows WHERE session_id=?", (session_id,)):
                raise CoreError(400, "缺少权利窗口", "授权前至少配置一个权利窗口")
            now = self.clock()
            session["status"] = m.SESSION_READY
            session["updated_at"] = now
            self._save_session(session)
            self._event(session_id, m.EV_SESSION_AUTHORIZED)
            batch_id = _uid("key")
            self.store.run(
                "INSERT INTO key_batches (batch_id, session_id, generation, status, secret,"
                " grace_until, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
                (batch_id, session_id, 1, m.KEY_ACTIVE, _uid("secret"), None, now, now),
            )
            self._event(session_id, m.EV_KEY_BATCH_CREATED,
                        payload={"batch_id": batch_id, "generation": 1})
            return self.get_session(session_id)
        return self._mutate(_do)

    def reschedule_session(self, session_id, scheduled_start, scheduled_end=None, windows=None):
        def _do():
            session = self._session(session_id)
            if session["status"] in m.SESSION_TERMINAL:
                raise CoreError(409, "场次已终止", "已终止场次不可改期")
            new_end = scheduled_end if scheduled_end is not None else session["scheduled_end"]
            if new_end <= scheduled_start:
                raise CoreError(400, "时间非法", "scheduled_end 必须晚于 scheduled_start")
            if windows is not None:
                for w in windows:
                    if w["not_after"] <= w["not_before"]:
                        raise CoreError(400, "窗口非法", "权利窗口 not_after 必须晚于 not_before")
            old = {"scheduled_start": session["scheduled_start"],
                   "scheduled_end": session["scheduled_end"]}
            session["scheduled_start"] = scheduled_start
            session["scheduled_end"] = new_end
            session["updated_at"] = self.clock()
            self._save_session(session)
            if windows is not None:
                self.store.run("DELETE FROM rights_windows WHERE session_id=?", (session_id,))
                for w in windows:
                    self.store.run(
                        "INSERT INTO rights_windows (session_id, region, not_before, not_after)"
                        " VALUES (?,?,?,?)",
                        (session_id, w["region"], w["not_before"], w["not_after"]),
                    )
            self._event(session_id, m.EV_RESCHEDULED, payload={
                "old": old,
                "new": {"scheduled_start": scheduled_start, "scheduled_end": new_end},
                "windows_replaced": windows is not None,
            })
            # 尚未开始的点位即时生效（握手按新窗口校验）；
            # 已开始的点位形成明确处置记录，按原窗口继续播出。
            for point in self._points(session_id):
                if point["state"] in m.POINT_ACTIVE:
                    self._disposition(
                        session_id, point["site_id"], m.POLICY_RESCHEDULE, None,
                        "进行中场次按原权利窗口继续播出", "场次改期")
            return self.get_session(session_id)
        return self._mutate(_do)

    # ================================================================
    # 中心命令（幂等）
    # ================================================================

    def _issue_point_command(self, session, point, ctype, command_id, reason, params=None) -> dict:
        return self._event(
            session["session_id"], m.EV_COMMAND_ISSUED, site_id=point["site_id"],
            payload={"command_id": command_id, "type": ctype,
                     "reason": reason, "params": params or {}})

    def _cut_point(self, session, point, reason, kind=m.EV_POINT_CUT):
        """紧急切断：中心立即生效（吊销令牌），命令同时下发供边缘执行。

        返回 (事件或 None, 说明)。待开场点位无需下发命令，直接标记。
        """
        now = self.clock()
        if point["state"] == m.POINT_PENDING:
            point["state"] = m.POINT_CUT
            point["ended_at"] = now
            point["updated_at"] = now
            self._save_point(point)
            self._event(session["session_id"], kind, site_id=point["site_id"],
                        payload={"reason": reason, "note": "未开场即切断"})
            return None, "未开场，直接切断"
        command_id = _uid("cmd")
        ev = self._issue_point_command(session, point, m.CMD_CUT, command_id, reason,
                                       params={"immediate": True})
        point["state"] = m.POINT_CUT
        point["stream_token"] = None
        point["token_valid_until"] = None
        point["ended_at"] = now
        point["updated_at"] = now
        self._save_point(point)
        self._event(session["session_id"], kind, site_id=point["site_id"],
                    ref_seq=ev["seq"], payload={"reason": reason})
        self._event(session["session_id"], m.EV_TOKEN_REVOKED, site_id=point["site_id"],
                    payload={"reason": reason})
        return ev, None

    def _end_point(self, session, point, reason):
        """结束：中心立即生效（停流），边缘确认后完成退场。返回 (事件或 None, 说明)。"""
        now = self.clock()
        if point["state"] == m.POINT_PENDING:
            point["state"] = m.POINT_ENDED
            point["ended_at"] = now
            point["updated_at"] = now
            self._save_point(point)
            self._event(session["session_id"], m.EV_POINT_ENDED, site_id=point["site_id"],
                        payload={"reason": reason, "note": "未开场"})
            return None, "未开场，直接结案"
        command_id = _uid("cmd")
        ev = self._issue_point_command(session, point, m.CMD_END, command_id, reason,
                                       params={"immediate": True})
        point["state"] = m.POINT_ENDED
        point["stream_token"] = None
        point["token_valid_until"] = None
        point["ended_at"] = now
        point["updated_at"] = now
        self._save_point(point)
        self._event(session["session_id"], m.EV_POINT_ENDED, site_id=point["site_id"],
                    ref_seq=ev["seq"], payload={"reason": reason})
        self._event(session["session_id"], m.EV_TOKEN_REVOKED, site_id=point["site_id"],
                    payload={"reason": reason})
        return ev, None

    def issue_command(self, session_id, ctype, command_id=None, site_ids=None, reason=None):
        if ctype not in m.OPERATOR_COMMANDS:
            raise CoreError(400, "未知命令", f"不支持的命令类型 {ctype!r}，可选 {list(m.OPERATOR_COMMANDS)}")

        def _do():
            if command_id:
                cached = self.store.one(
                    "SELECT session_id, response FROM commands WHERE command_id=?", (command_id,))
                if cached is not None:
                    if cached["session_id"] != session_id:
                        raise CoreError(409, "幂等键冲突",
                                        f"command_id {command_id} 已用于场次 {cached['session_id']}")
                    # 网络抖动/运维重试：返回首次结果，不重复生效
                    response = json.loads(cached["response"])
                    response["duplicate"] = True
                    self._record_deviation(
                        session_id, None, m.DEV_DUPLICATE, 0.0,
                        f"重复命令 {command_id}（{ctype}）已抑制，未二次执行")
                    return response
            session = self._session(session_id)
            if session["status"] in m.SESSION_TERMINAL:
                raise CoreError(409, "场次已终止", f"场次状态「{session['status']}」，不可再下发命令")
            if ctype in (m.CMD_PAUSE, m.CMD_RESUME) and session["status"] not in (
                    m.SESSION_PLAYING, m.SESSION_PAUSED):
                raise CoreError(409, "状态非法",
                                f"场次状态「{session['status']}」，不可执行「{m.COMMAND_LABELS[ctype]}」")
            cid = command_id or _uid("cmd")
            final_reason = reason or {
                m.CMD_PAUSE: "运维暂停", m.CMD_RESUME: "运维恢复",
                m.CMD_CUT: "运维紧急切断", m.CMD_END: "运维结束场次",
            }[ctype]
            points = self._points(session_id)
            if site_ids is not None:
                wanted = set(site_ids)
                known = {p["site_id"] for p in points}
                missing = wanted - known
                if missing:
                    raise CoreError(404, "点位不存在", f"点位未参加场次: {sorted(missing)}")
                points = [p for p in points if p["site_id"] in wanted]
            issued, affected, skipped = [], [], []
            for point in points:
                state = point["state"]
                ev, note = None, None
                if ctype == m.CMD_PAUSE and state == m.POINT_PLAYING:
                    ev = self._issue_point_command(session, point, ctype, cid, final_reason)
                elif ctype == m.CMD_RESUME and state == m.POINT_PAUSED:
                    ev = self._issue_point_command(session, point, ctype, cid, final_reason)
                elif ctype == m.CMD_CUT and state not in m.POINT_TERMINAL:
                    ev, note = self._cut_point(session, point, final_reason)
                elif ctype == m.CMD_END and state not in m.POINT_TERMINAL:
                    ev, note = self._end_point(session, point, final_reason)
                else:
                    skipped.append({"site_id": point["site_id"],
                                    "reason": f"点位状态「{state}」不适用"})
                    continue
                if ev is not None:
                    issued.append({"site_id": point["site_id"], "seq": ev["seq"]})
                else:
                    affected.append({"site_id": point["site_id"], "note": note})
            self._refresh_session_status(session_id)
            response = {"command_id": cid, "type": ctype, "issued": issued,
                        "affected": affected, "skipped": skipped, "duplicate": False}
            self.store.run(
                "INSERT INTO commands (command_id, session_id, type, request, response, created_at)"
                " VALUES (?,?,?,?,?,?)",
                (cid, session_id, ctype,
                 json.dumps({"site_ids": site_ids, "reason": reason}, ensure_ascii=False),
                 json.dumps(response, ensure_ascii=False), self.clock()),
            )
            return response
        return self._mutate(_do)

    # ================================================================
    # 边缘协议：握手 / 心跳 / 确认 / 同步
    # ================================================================

    def _permit_view(self, point, token, valid_until, idempotent, secret=None):
        view = {
            "permit_id": point["permit_id"],
            "seq": point["permit_seq"],
            "session_id": point["session_id"],
            "site_id": point["site_id"],
            "stream_token": token,
            "token_valid_until": valid_until,
            "key_batch_id": point["key_batch_id"],
            "point_state": point["state"],
            "idempotent": idempotent,
        }
        if secret is not None:
            view["key_secret"] = secret
        return view

    def _refresh_token(self, point, window, now):
        """续期流令牌；权利窗口内才发令牌，且有效期不越过窗口终点。"""
        if window is None:
            return None, None
        valid_until = min(window["not_after"], now + self.config["token_ttl"])
        if valid_until <= now:
            return None, None
        if point["stream_token"] is None:
            point["stream_token"] = _uid("tok")
        point["token_valid_until"] = valid_until
        point["updated_at"] = now
        self._save_point(point)
        return point["stream_token"], valid_until

    def handshake(self, site_id, session_id, edge_event_id=None):
        """开场握手：校验场次状态、区域禁播、权利窗口与密钥批次后签发许可。

        重复握手（网络抖动重试）复用既有许可，绝不二次开场。
        """
        edge_event_id = edge_event_id or _uid("edge")

        def _do():
            site = self._site(site_id)
            session = self._session(session_id)
            point = self._point(session_id, site_id)
            now = self.clock()
            self._touch_site(site, now)
            # 幂等：已有进行中的许可，重复请求返回同一张许可
            if point["state"] in m.POINT_ACTIVE and point["permit_id"]:
                window = self._window(session_id, site["region"])
                if window is None or now > window["not_after"]:
                    # 进行中点位权利已到期：强制切断而非续发令牌
                    self._cut_point(session, point, "权利窗口到期", kind=m.EV_RIGHTS_EXPIRED_CUT)
                    self._record_deviation(
                        session_id, site_id, m.DEV_RIGHTS_EXPIRED, 0.0,
                        "重复握手时权利窗口已到期，强制切断")
                    view = self._permit_view(point, None, None, idempotent=True)
                    view["must_stop"] = True
                    return view
                token, valid_until = self._refresh_token(point, window, now)
                self._record_deviation(
                    session_id, site_id, m.DEV_DUPLICATE, 0.0,
                    "重复握手请求，复用既有许可，未二次开场")
                return self._permit_view(point, token, valid_until, idempotent=True)
            if point["state"] in m.POINT_TERMINAL and point["state"] != m.POINT_BLOCKED:
                raise CoreError(409, "点位已终止", f"点位状态「{point['state']}」，无法开场")
            ban = self._active_ban(session_id, site["region"])
            if ban:
                if point["state"] != m.POINT_BLOCKED:
                    point["state"] = m.POINT_BLOCKED
                    point["updated_at"] = now
                    self._save_point(point)
                    self._event(session_id, m.EV_POINT_BLOCKED, site_id=site_id,
                                payload={"reason": ban["reason"], "ban_id": ban["ban_id"]})
                    self._refresh_session_status(session_id)
                # 拒绝也要留痕：先提交拒绝事件，再返回错误
                self.store.conn.commit()
                raise CoreError(403, "区域禁播", f"区域「{site['region']}」禁播：{ban['reason']}")
            if point["state"] == m.POINT_BLOCKED:
                # 禁播已解除但点位未恢复（防御性分支，正常流程不会到达）
                raise CoreError(409, "点位已终止", "点位处于「已禁播」，无法开场")
            if session["status"] == m.SESSION_PENDING_AUTH:
                raise CoreError(409, "场次未授权", "场次仍为「待授权」，请先授权")
            if session["status"] in m.SESSION_TERMINAL:
                raise CoreError(409, "场次已终止", f"场次状态「{session['status']}」，无法开场")
            window = self._window(session_id, site["region"])
            if window is None:
                self._reject_open(session_id, site_id, "无权利窗口", {})
                raise CoreError(403, "无权利窗口", f"区域「{site['region']}」未配置权利窗口")
            if now < window["not_before"]:
                self._reject_open(session_id, site_id, "权利窗口未生效",
                                  {"not_before": window["not_before"]})
                raise CoreError(403, "权利窗口未生效", "尚未到达权利窗口起点")
            if now > window["not_after"]:
                self._reject_open(session_id, site_id, "权利窗口已过期",
                                  {"not_after": window["not_after"]})
                raise CoreError(403, "权利窗口已过期", "权利窗口已结束，禁止拉流")
            batch = self._latest_key(session_id)
            if batch is None:
                self._reject_open(session_id, site_id, "无有效密钥批次", {})
                raise CoreError(403, "无有效密钥", "场次没有有效密钥批次")
            point["permit_id"] = _uid("permit")
            point["key_batch_id"] = batch["batch_id"]
            point["stream_token"] = _uid("tok")
            point["token_valid_until"] = min(window["not_after"], now + self.config["token_ttl"])
            point["state"] = m.POINT_HANDSHAKING
            point["updated_at"] = now
            ev = self._event(session_id, m.EV_HANDSHAKE_PERMIT, site_id=site_id,
                             payload={"permit_id": point["permit_id"],
                                      "batch_id": batch["batch_id"],
                                      "token_valid_until": point["token_valid_until"]})
            point["permit_seq"] = ev["seq"]
            self._save_point(point)
            return self._permit_view(point, point["stream_token"], point["token_valid_until"],
                                     idempotent=False, secret=batch["secret"])
        return self._edge_call(site_id, edge_event_id, "handshake", _do)

    def _reject_open(self, session_id, site_id, reason, extra):
        """记录开场拒绝并立即提交——拒绝本身就是要留痕的结果。"""
        self._event(session_id, m.EV_OPEN_REJECTED, site_id=site_id,
                    payload={"reason": reason, **extra})
        self.store.conn.commit()

    def _pending_commands(self, session_id, site_id, after_seq):
        rows = self.store.all(
            "SELECT seq, payload, created_at FROM events"
            " WHERE session_id=? AND site_id=? AND kind=? AND seq>? ORDER BY seq",
            (session_id, site_id, m.EV_COMMAND_ISSUED, after_seq or 0),
        )
        commands = []
        for r in rows:
            p = json.loads(r["payload"])
            commands.append({
                "seq": r["seq"],
                "type": p["type"],
                "command_id": p.get("command_id"),
                "reason": p.get("reason"),
                "params": p.get("params") or {},
                "issued_at": r["created_at"],
            })
        return commands

    def heartbeat(self, site_id, session_id, state=None, applied_seq=0, edge_event_id=None):
        """心跳：站点在线证据 + 权利窗口强制 + 令牌续期 + 命令下发。"""
        edge_event_id = edge_event_id or _uid("edge")

        def _do():
            site = self._site(site_id)
            session = self._session(session_id)
            point = self._point(session_id, site_id)
            now = self.clock()
            was = site["status"]
            offline_for = 0.0 if site["last_heartbeat"] is None else max(0.0, now - site["last_heartbeat"])
            self._touch_site(site, now)
            if was in (m.SITE_OFFLINE, m.SITE_ISOLATED) and point["state"] in m.POINT_ACTIVE:
                self._record_deviation(
                    session_id, site_id, m.DEV_OFFLINE_WINDOW, offline_for,
                    f"站点离线 {offline_for:.1f} 秒后恢复心跳")
            # 迟到的退场确认
            if (state in (m.SITE_EXITED, m.POINT_ENDED)
                    and point["state"] in m.POINT_TERMINAL and point["exited_at"] is None):
                point["exited_at"] = now
                point["updated_at"] = now
                self._save_point(point)
                self._maybe_site_exited(site, session_id)
            # 权利窗口强制：到期即切断并吊销令牌
            must_stop = False
            window = self._window(session_id, site["region"])
            if point["state"] in (m.POINT_PLAYING, m.POINT_PAUSED):
                if window is None or now > window["not_after"]:
                    self._cut_point(session, point, "权利窗口到期", kind=m.EV_RIGHTS_EXPIRED_CUT)
                    self._record_deviation(
                        session_id, site_id, m.DEV_RIGHTS_EXPIRED, 0.0,
                        "心跳时权利窗口已到期，强制切断并吊销令牌")
                    must_stop = True
            token = valid_until = None
            if not must_stop and point["state"] in m.POINT_ACTIVE:
                token, valid_until = self._refresh_token(point, window, now)
            commands = self._pending_commands(session_id, site_id, applied_seq)
            # 状态上报偏差：边缘视角与中心不一致且无在途命令可解释
            is_exit_report = (state in (m.SITE_EXITED, m.POINT_ENDED)
                              and point["state"] in m.POINT_TERMINAL)
            if (state and not is_exit_report and state != point["state"] and not commands):
                self._record_deviation(
                    session_id, site_id, m.DEV_STATE_MISMATCH, 0.0,
                    f"边缘上报「{state}」，中心记录「{point['state']}」")
            return {
                "session_status": self._session(session_id)["status"],
                "point_state": point["state"],
                "commands": commands,
                "stream_token": token,
                "token_valid_until": valid_until,
                "must_stop": must_stop,
            }
        return self._edge_call(site_id, edge_event_id, "heartbeat", _do)

    def _acked(self, session_id, seq) -> bool:
        return self.store.one(
            "SELECT 1 AS x FROM events WHERE session_id=? AND ref_seq=? AND kind=?",
            (session_id, seq, m.EV_COMMAND_ACKED),
        ) is not None

    def ack(self, site_id, session_id, seq, edge_event_id=None, payload=None):
        """确认控制事件：应用状态迁移；重复确认安全忽略。"""
        edge_event_id = edge_event_id or _uid("edge")
        payload = payload or {}

        def _do():
            site = self._site(site_id)
            session = self._session(session_id)
            point = self._point(session_id, site_id)
            now = self.clock()
            ev = self.store.one(
                "SELECT * FROM events WHERE session_id=? AND seq=?", (session_id, seq))
            if ev is None or ev["site_id"] != site_id:
                raise CoreError(404, "事件不存在", f"场次 {session_id} 不存在该点位的序号 {seq}")
            if ev["kind"] not in (m.EV_COMMAND_ISSUED, m.EV_HANDSHAKE_PERMIT):
                raise CoreError(400, "不可确认", "该事件不是待确认命令")
            if self._acked(session_id, seq):
                self._record_deviation(
                    session_id, site_id, m.DEV_DUPLICATE, 0.0,
                    f"重复确认序号 {seq}，已抑制")
                return {"acked": True, "duplicate": True, "point_state": point["state"],
                        "session_status": session["status"]}
            self._touch_site(site, now)
            body = json.loads(ev["payload"])
            ctype = "open" if ev["kind"] == m.EV_HANDSHAKE_PERMIT else body.get("type")
            if ctype == "open":
                if point["state"] == m.POINT_HANDSHAKING:
                    point["state"] = m.POINT_PLAYING
                    point["opened_at"] = now
                    self._event(session_id, m.EV_POINT_OPENED, site_id=site_id,
                                ref_seq=seq, payload={"source": "edge_ack"})
                    self._open_skew(session, now, site_id)
            elif ctype == m.CMD_PAUSE:
                if point["state"] == m.POINT_PLAYING:
                    point["state"] = m.POINT_PAUSED
            elif ctype == m.CMD_RESUME:
                if point["state"] == m.POINT_PAUSED:
                    point["state"] = m.POINT_PLAYING
            elif ctype == m.CMD_CUT:
                pass  # 签发时中心已切断，此处仅确认执行
            elif ctype == m.CMD_END:
                if payload.get("exit_completed"):
                    point["exited_at"] = now
                    self._maybe_site_exited(site, session_id)
            elif ctype == m.CMD_ROTATE_KEY:
                batch_id = (body.get("params") or {}).get("batch_id")
                if batch_id:
                    point["key_batch_id"] = batch_id
                    self._event(session_id, m.EV_KEY_ROTATED, site_id=site_id,
                                ref_seq=seq, payload={"batch_id": batch_id})
            self._event(session_id, m.EV_COMMAND_ACKED, site_id=site_id, ref_seq=seq,
                        payload={"command_id": body.get("command_id"), "type": ctype})
            delay = now - ev["created_at"]
            if ctype != "open" and delay > self.config["propagation_slo"]:
                self._record_deviation(
                    session_id, site_id, m.DEV_PROPAGATION, delay,
                    f"命令「{m.COMMAND_LABELS.get(ctype, ctype)}」从签发到确认耗时 {delay:.1f} 秒")
            point["applied_seq"] = max(point["applied_seq"], seq)
            point["updated_at"] = now
            self._save_point(point)
            self._maybe_revoke_rotated(session_id)
            status = self._refresh_session_status(session_id)
            return {"acked": True, "duplicate": False,
                    "point_state": point["state"], "session_status": status}
        return self._edge_call(site_id, edge_event_id, "ack", _do)

    def sync(self, site_id, session_id, sync_id=None, last_acked_seq=0, offline_events=None):
        """断线重连合并：边缘按 edge_seq 单调上报离线期间的自治事件。

        同一 sync_id 重试返回缓存；相同 edge_seq 的事件判重；
        与中心状态冲突的事件以中心为准并记录偏差。
        """
        sync_id = sync_id or _uid("sync")
        offline_events = offline_events or []

        def _do():
            site = self._site(site_id)
            session = self._session(session_id)
            self._point(session_id, site_id)  # 确认点位存在
            now = self.clock()
            was = site["status"]
            offline_for = 0.0 if site["last_heartbeat"] is None else max(0.0, now - site["last_heartbeat"])
            self._touch_site(site, now)
            if was in (m.SITE_OFFLINE, m.SITE_ISOLATED):
                self._record_deviation(
                    session_id, site_id, m.DEV_OFFLINE_WINDOW, offline_for,
                    f"站点离线 {offline_for:.1f} 秒后重新同步")
            merged = duplicates = 0
            conflicts = []
            ordered = sorted(offline_events, key=lambda e: e.get("edge_seq") or 0)
            for ev in ordered:
                edge_seq = ev.get("edge_seq")
                if edge_seq is None:
                    conflicts.append({"edge_seq": None, "type": ev.get("type"),
                                      "reason": "缺少边缘序号"})
                    continue
                if self.store.one(
                        "SELECT 1 AS x FROM edge_merged WHERE site_id=? AND session_id=? AND edge_seq=?",
                        (site_id, session_id, edge_seq)):
                    duplicates += 1
                    continue
                ok, note = self._apply_edge_event(session, site, ev)
                self.store.run(
                    "INSERT INTO edge_merged (site_id, session_id, edge_seq, created_at) VALUES (?,?,?,?)",
                    (site_id, session_id, edge_seq, now))
                if ok:
                    merged += 1
                else:
                    conflicts.append({"edge_seq": edge_seq, "type": ev.get("type"), "reason": note})
            point = self._point(session_id, site_id)
            return {
                "merged": merged,
                "duplicates": duplicates,
                "conflicts": conflicts,
                "commands": self._pending_commands(session_id, site_id, last_acked_seq),
                "point_state": point["state"],
                "session_status": self._session(session_id)["status"],
            }
        return self._edge_call(site_id, sync_id, "sync", _do)

    def _apply_edge_event(self, session, site, ev):
        """应用一条边缘离线自治事件；返回 (是否采纳, 冲突说明)。"""
        session_id = session["session_id"]
        site_id = site["site_id"]
        point = self._point(session_id, site_id)
        etype = ev.get("type")
        occurred = ev.get("occurred_at") or self.clock()
        edge_seq = ev.get("edge_seq")
        state = point["state"]

        def note_merged():
            self._event(session_id, m.EV_EDGE_MERGED, site_id=site_id, origin="edge",
                        payload={"edge_seq": edge_seq, "type": etype, "occurred_at": occurred})
            self._record_deviation(
                session_id, site_id, m.DEV_OFFLINE_AUTONOMY,
                max(0.0, self.clock() - occurred),
                f"边缘离线期间自治执行「{etype}」，恢复连接后按序号 {edge_seq} 合并")

        applied = True
        if etype == "open" and state == m.POINT_HANDSHAKING:
            point["state"] = m.POINT_PLAYING
            point["opened_at"] = occurred
            self._event(session_id, m.EV_POINT_OPENED, site_id=site_id, origin="edge",
                        payload={"occurred_at": occurred, "source": "edge_sync"})
            self._open_skew(session, occurred, site_id)
            note_merged()
        elif etype == "pause" and state == m.POINT_PLAYING:
            point["state"] = m.POINT_PAUSED
            note_merged()
        elif etype == "resume" and state == m.POINT_PAUSED:
            point["state"] = m.POINT_PLAYING
            note_merged()
        elif etype == "end" and state in m.POINT_ACTIVE:
            point["state"] = m.POINT_ENDED
            point["ended_at"] = occurred
            point["stream_token"] = None
            point["token_valid_until"] = None
            note_merged()
        elif etype == "cut" and state in m.POINT_ACTIVE:
            point["state"] = m.POINT_CUT
            point["ended_at"] = occurred
            point["stream_token"] = None
            point["token_valid_until"] = None
            note_merged()
        elif etype == "exit" and state in m.POINT_TERMINAL:
            point["exited_at"] = occurred
            self._maybe_site_exited(site, session_id)
            note_merged()
        else:
            note = f"边缘事件「{etype}」与中心状态「{state}」冲突，以中心为准"
            self._event(session_id, m.EV_EDGE_CONFLICT, site_id=site_id, origin="edge",
                        payload={"edge_seq": edge_seq, "type": etype, "center_state": state})
            self._record_deviation(session_id, site_id, m.DEV_CONFLICT, 0.0, note)
            return False, note
        point["updated_at"] = self.clock()
        self._save_point(point)
        self._refresh_session_status(session_id)
        return applied, ""

    # ================================================================
    # 政策：区域禁播 / 密钥轮换
    # ================================================================

    def declare_ban(self, session_id, region, reason, mode="immediate", ban_id=None):
        if mode not in ("immediate", "grace"):
            raise CoreError(400, "模式非法", "禁播模式只能是 immediate 或 grace")

        def _do():
            if ban_id:
                existing = self.store.one("SELECT * FROM bans WHERE ban_id=?", (ban_id,))
                if existing is not None:
                    return {"ban_id": ban_id, "duplicate": True, "affected": []}
            session = self._session(session_id)
            if session["status"] in m.SESSION_TERMINAL:
                raise CoreError(409, "场次已终止", "已终止场次不可再禁播")
            now = self.clock()
            bid = ban_id or _uid("ban")
            self.store.run(
                "INSERT INTO bans (ban_id, session_id, region, reason, mode, created_at, lifted_at)"
                " VALUES (?,?,?,?,?,?,NULL)",
                (bid, session_id, region, reason, mode, now),
            )
            self._event(session_id, m.EV_BAN_DECLARED,
                        payload={"ban_id": bid, "region": region, "mode": mode, "reason": reason})
            affected = []
            for point in self._points(session_id):
                site = self._site(point["site_id"])
                if site["region"] != region:
                    continue
                state = point["state"]
                if state == m.POINT_PENDING:
                    point["state"] = m.POINT_BLOCKED
                    point["updated_at"] = now
                    self._save_point(point)
                    self._event(session_id, m.EV_POINT_BLOCKED, site_id=point["site_id"],
                                payload={"reason": reason, "ban_id": bid})
                    self._disposition(session_id, point["site_id"], m.POLICY_BAN, bid,
                                      "禁播未开场点位", reason)
                    affected.append({"site_id": point["site_id"], "action": "禁播"})
                elif state == m.POINT_HANDSHAKING:
                    point["state"] = m.POINT_BLOCKED
                    point["stream_token"] = None
                    point["token_valid_until"] = None
                    point["updated_at"] = now
                    self._save_point(point)
                    self._event(session_id, m.EV_POINT_BLOCKED, site_id=point["site_id"],
                                payload={"reason": reason, "ban_id": bid})
                    self._event(session_id, m.EV_TOKEN_REVOKED, site_id=point["site_id"],
                                payload={"reason": f"区域禁播：{reason}"})
                    self._disposition(session_id, point["site_id"], m.POLICY_BAN, bid,
                                      "禁播握手中点位，许可作废", reason)
                    affected.append({"site_id": point["site_id"], "action": "禁播"})
                elif state in (m.POINT_PLAYING, m.POINT_PAUSED):
                    if mode == "immediate":
                        self._cut_point(session, point, f"区域禁播：{reason}")
                        self._disposition(session_id, point["site_id"], m.POLICY_BAN, bid,
                                          "紧急切断进行中场次", reason)
                        affected.append({"site_id": point["site_id"], "action": "紧急切断"})
                    else:
                        self._disposition(session_id, point["site_id"], m.POLICY_BAN, bid,
                                          "允许播完当前场次，禁止新的开场", reason)
                        affected.append({"site_id": point["site_id"], "action": "播完为止"})
            self._refresh_session_status(session_id)
            return {"ban_id": bid, "session_id": session_id, "region": region, "mode": mode,
                    "reason": reason, "affected": affected, "duplicate": False}
        return self._mutate(_do)

    def lift_ban(self, session_id, ban_id):
        def _do():
            row = self.store.one("SELECT * FROM bans WHERE ban_id=?", (ban_id,))
            if row is None or row["session_id"] != session_id:
                raise CoreError(404, "禁播不存在", f"禁播 {ban_id} 不存在")
            ban = dict(row)
            if ban["lifted_at"] is not None:
                return {"ban_id": ban_id, "lifted": False, "duplicate": True, "restored": []}
            now = self.clock()
            self.store.run("UPDATE bans SET lifted_at=? WHERE ban_id=?", (now, ban_id))
            self._event(session_id, m.EV_BAN_LIFTED, payload={"ban_id": ban_id})
            restored = []
            for point in self._points(session_id):
                site = self._site(point["site_id"])
                if site["region"] == ban["region"] and point["state"] == m.POINT_BLOCKED:
                    point["state"] = m.POINT_PENDING
                    point["updated_at"] = now
                    self._save_point(point)
                    self._event(session_id, m.EV_POINT_UNBLOCKED, site_id=point["site_id"],
                                payload={"ban_id": ban_id})
                    restored.append(point["site_id"])
            # 若场次因全员禁播被收尾，点位恢复后回到待开场
            session = self._session(session_id)
            if session["status"] == m.SESSION_CUT and restored:
                session["status"] = m.SESSION_READY
                session["updated_at"] = now
                self._save_session(session)
                self._event(session_id, m.EV_SESSION_STATUS,
                            payload={"from": m.SESSION_CUT, "to": m.SESSION_READY,
                                     "note": "禁播解除"})
            self._refresh_session_status(session_id)
            return {"ban_id": ban_id, "lifted": True, "duplicate": False, "restored": restored}
        return self._mutate(_do)

    def rotate_keys(self, session_id, grace_seconds=None):
        """密钥轮换：新批次立即生效；未开场点位握手时自动使用新批次，
        进行中点位收到轮换命令并形成处置记录；旧批次宽限到期吊销。"""
        def _do():
            session = self._session(session_id)
            if session["status"] in m.SESSION_TERMINAL:
                raise CoreError(409, "场次已终止", "已终止场次不可轮换密钥")
            old = self._latest_key(session_id)
            if old is None:
                raise CoreError(409, "无有效密钥", "没有可轮换的有效密钥批次")
            now = self.clock()
            grace = self.config["key_grace"] if grace_seconds is None else float(grace_seconds)
            new_id = _uid("key")
            self.store.run(
                "INSERT INTO key_batches (batch_id, session_id, generation, status, secret,"
                " grace_until, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
                (new_id, session_id, old["generation"] + 1, m.KEY_ACTIVE,
                 _uid("secret"), None, now, now),
            )
            self.store.run(
                "UPDATE key_batches SET status=?, grace_until=?, updated_at=? WHERE batch_id=?",
                (m.KEY_ROTATING, now + grace, now, old["batch_id"]),
            )
            self._event(session_id, m.EV_KEY_ROTATION_STARTED,
                        payload={"old_batch": old["batch_id"], "new_batch": new_id,
                                 "grace_until": now + grace})
            notified = []
            for point in self._points(session_id):
                if point["state"] in m.POINT_ACTIVE:
                    ev = self._issue_point_command(
                        session, point, m.CMD_ROTATE_KEY, _uid("cmd"), "密钥轮换",
                        params={"batch_id": new_id})
                    self._disposition(session_id, point["site_id"], m.POLICY_KEY_ROTATION, new_id,
                                      "进行中场次轮换密钥批次", "密钥轮换")
                    notified.append({"site_id": point["site_id"], "seq": ev["seq"]})
            return {"old_batch": old["batch_id"], "new_batch": new_id,
                    "grace_until": now + grace, "notified": notified,
                    "note": "未开场点位将在握手时自动使用新批次"}
        return self._mutate(_do)

    def _maybe_revoke_rotated(self, session_id):
        """所有进行中点位都切走后，吊销轮换中的旧批次。"""
        for batch in self.store.all(
                "SELECT * FROM key_batches WHERE session_id=? AND status=?",
                (session_id, m.KEY_ROTATING)):
            row = self.store.one(
                "SELECT COUNT(*) AS c FROM points WHERE session_id=? AND key_batch_id=?"
                " AND state IN (?,?,?)",
                (session_id, batch["batch_id"], *m.POINT_ACTIVE))
            if row["c"] == 0:
                self.store.run(
                    "UPDATE key_batches SET status=?, updated_at=? WHERE batch_id=?",
                    (m.KEY_REVOKED, self.clock(), batch["batch_id"]))
                self._event(session_id, m.EV_KEY_REVOKED,
                            payload={"batch_id": batch["batch_id"],
                                     "reason": "全部点位已切换到新批次"})

    def revoke_key_batch(self, session_id, batch_id):
        def _do():
            row = self.store.one(
                "SELECT * FROM key_batches WHERE batch_id=? AND session_id=?",
                (batch_id, session_id))
            if row is None:
                raise CoreError(404, "密钥批次不存在", f"密钥批次 {batch_id} 不存在")
            batch = dict(row)
            if batch["status"] == m.KEY_REVOKED:
                return {"batch_id": batch_id, "status": m.KEY_REVOKED, "duplicate": True}
            if batch["status"] == m.KEY_ACTIVE:
                row = self.store.one(
                    "SELECT COUNT(*) AS c FROM points WHERE session_id=? AND key_batch_id=?"
                    " AND state IN (?,?,?)",
                    (session_id, batch_id, *m.POINT_ACTIVE))
                if row["c"] > 0:
                    raise CoreError(409, "批次使用中",
                                    "仍有进行中点位使用该批次，请先轮换密钥")
            self.store.run(
                "UPDATE key_batches SET status=?, updated_at=? WHERE batch_id=?",
                (m.KEY_REVOKED, self.clock(), batch_id))
            self._event(session_id, m.EV_KEY_REVOKED,
                        payload={"batch_id": batch_id, "reason": "运维吊销"})
            return {"batch_id": batch_id, "status": m.KEY_REVOKED, "duplicate": False}
        return self._mutate(_do)

    def list_keys(self, session_id):
        with self._lock:
            self._session(session_id)
            return {"items": [dict(r) for r in self.store.all(
                "SELECT batch_id, session_id, generation, status, grace_until, created_at, updated_at"
                " FROM key_batches WHERE session_id=? ORDER BY generation", (session_id,))]}

    # ================================================================
    # 巡检与恢复
    # ================================================================

    def sweep(self):
        """惰性巡检：每次请求前执行，保证政策与窗口「即时」生效。

        - 心跳缺失的站点降级为短时离线 / 隔离；
        - 权利窗口到期的进行中点位强制切断并吊销令牌；
        - 过期的开场许可作废，点位回到待开场；
        - 轮换宽限到期的旧密钥批次吊销；
        - 所有窗口过期且无人播出的场次收尾。
        """
        def _do():
            now = self.clock()
            for site in self.store.all(
                    "SELECT * FROM sites WHERE status IN (?,?)", (m.SITE_ONLINE, m.SITE_OFFLINE)):
                if site["last_heartbeat"] is None:
                    continue
                elapsed = now - site["last_heartbeat"]
                new = None
                if elapsed > self.config["isolate_after"]:
                    new = m.SITE_ISOLATED
                elif elapsed > self.config["offline_after"]:
                    new = m.SITE_OFFLINE
                if new and new != site["status"]:
                    self.store.run(
                        "UPDATE sites SET status=?, updated_at=? WHERE site_id=?",
                        (new, now, site["site_id"]))
            for row in self.store.all(
                    "SELECT * FROM points WHERE state IN (?,?)",
                    (m.POINT_PLAYING, m.POINT_PAUSED)):
                session = self._session(row["session_id"])
                if session["status"] in m.SESSION_TERMINAL:
                    continue
                site = self._site(row["site_id"])
                window = self._window(row["session_id"], site["region"])
                if window is None or now > window["not_after"]:
                    self._cut_point(session, dict(row), "权利窗口到期",
                                    kind=m.EV_RIGHTS_EXPIRED_CUT)
                    self._record_deviation(
                        row["session_id"], row["site_id"], m.DEV_RIGHTS_EXPIRED, 0.0,
                        "巡检发现权利窗口到期，强制切断")
            for row in self.store.all(
                    "SELECT * FROM points WHERE state=?", (m.POINT_HANDSHAKING,)):
                if row["token_valid_until"] is not None and now > row["token_valid_until"]:
                    self.store.run(
                        "UPDATE points SET state=?, permit_id=NULL, permit_seq=NULL,"
                        " stream_token=NULL, token_valid_until=NULL, updated_at=?"
                        " WHERE session_id=? AND site_id=?",
                        (m.POINT_PENDING, now, row["session_id"], row["site_id"]))
                    self._event(row["session_id"], m.EV_PERMIT_EXPIRED, site_id=row["site_id"])
            for batch in self.store.all(
                    "SELECT * FROM key_batches WHERE status=? AND grace_until IS NOT NULL",
                    (m.KEY_ROTATING,)):
                if now > batch["grace_until"]:
                    self.store.run(
                        "UPDATE key_batches SET status=?, updated_at=? WHERE batch_id=?",
                        (m.KEY_REVOKED, now, batch["batch_id"]))
                    self._event(batch["session_id"], m.EV_KEY_REVOKED,
                                payload={"batch_id": batch["batch_id"],
                                         "reason": "轮换宽限到期"})
            for row in self.store.all(
                    "SELECT session_id FROM sessions WHERE status NOT IN (?,?)",
                    (m.SESSION_ENDED, m.SESSION_CUT)):
                self._close_if_exhausted(row["session_id"])
            return {"swept_at": now}
        return self._mutate(_do)

    def _close_if_exhausted(self, session_id):
        """所有权利窗口过期且没有进行中点位时，关闭场次。"""
        session = self._session(session_id)
        if session["status"] == m.SESSION_PENDING_AUTH:
            return
        points = self._points(session_id)
        if any(p["state"] in m.POINT_ACTIVE for p in points):
            return
        windows = self.store.all(
            "SELECT * FROM rights_windows WHERE session_id=?", (session_id,))
        if not windows:
            return
        now = self.clock()
        if any(now <= w["not_after"] for w in windows):
            return
        for p in points:
            if p["state"] == m.POINT_PENDING:
                self.store.run(
                    "UPDATE points SET state=?, ended_at=?, updated_at=?"
                    " WHERE session_id=? AND site_id=?",
                    (m.POINT_ENDED, now, now, session_id, p["site_id"]))
                self._event(session_id, m.EV_POINT_ENDED, site_id=p["site_id"],
                            payload={"reason": "权利窗口到期", "note": "未开场"})
        self._refresh_session_status(session_id)

    def recover(self):
        """中心重启后的恢复：为仍在进行的场次留痕，并立即巡检一次。"""
        def _do():
            recovered = []
            for row in self.store.all(
                    "SELECT session_id FROM sessions WHERE status NOT IN (?,?)",
                    (m.SESSION_ENDED, m.SESSION_CUT)):
                sid = row["session_id"]
                self._event(sid, m.EV_CENTER_RESTART,
                            payload={"note": "中心服务重启，恢复管理进行中场次"})
                self._record_deviation(sid, None, m.DEV_RESTART, 0.0,
                                       "中心服务重启后继续管理")
                recovered.append(sid)
            return recovered
        recovered = self._mutate(_do)
        self.sweep()
        return {"recovered_sessions": recovered}

    # ================================================================
    # 运维查询：时间线 / 偏差 / 处置记录
    # ================================================================

    def timeline(self, session_id, site_id=None):
        """城市（点位）实际看到的时间线：观众可见状态 + 事件摘要。"""
        with self._lock:
            session = self._session(session_id)
            if site_id:
                rows = self.store.all(
                    "SELECT * FROM events WHERE session_id=? AND (site_id=? OR site_id IS NULL)"
                    " ORDER BY seq", (session_id, site_id))
            else:
                rows = self.store.all(
                    "SELECT * FROM events WHERE session_id=? ORDER BY seq", (session_id,))
            entries = []
            for r in rows:
                payload = json.loads(r["payload"])
                audience, summary = self._summarize(r, payload)
                entries.append({
                    "seq": r["seq"],
                    "at": r["created_at"],
                    "effective_at": payload.get("occurred_at", r["created_at"]),
                    "kind": r["kind"],
                    "site_id": r["site_id"],
                    "origin": r["origin"],
                    "audience_state": audience,
                    "summary": summary,
                })
            return {
                "session_id": session_id,
                "site_id": site_id,
                "status": session["status"],
                "scheduled_start": session["scheduled_start"],
                "scheduled_end": session["scheduled_end"],
                "entries": entries,
            }

    @staticmethod
    def _summarize(row, payload):
        """把事件翻译成运维可读摘要，并标注观众可见状态。"""
        kind = row["kind"]
        label = m.COMMAND_LABELS.get(payload.get("type"), payload.get("type", ""))
        if kind == m.EV_SESSION_CREATED:
            return None, "场次已创建"
        if kind == m.EV_SESSION_AUTHORIZED:
            return None, "场次已授权，初始密钥批次就绪"
        if kind == m.EV_SESSION_STATUS:
            return None, f"场次状态：{payload.get('from')} → {payload.get('to')}"
        if kind == m.EV_RESCHEDULED:
            return None, f"场次改期：开场调整为 {payload.get('new', {}).get('scheduled_start')}"
        if kind == m.EV_HANDSHAKE_PERMIT:
            return None, f"开场许可下发（密钥批次 {payload.get('batch_id')}）"
        if kind == m.EV_OPEN_REJECTED:
            return None, f"开场被拒绝：{payload.get('reason')}"
        if kind == m.EV_PERMIT_EXPIRED:
            return None, "开场许可过期，点位回到待开场"
        if kind == m.EV_POINT_OPENED:
            source = payload.get("source")
            note = "边缘确认" if source == "edge_ack" else "离线自治，恢复后合并"
            return m.POINT_PLAYING, f"开场（{note}）"
        if kind == m.EV_COMMAND_ISSUED:
            return None, f"中心签发命令：{label}（{payload.get('reason')}）"
        if kind == m.EV_COMMAND_ACKED:
            ctype = payload.get("type")
            return {
                "open": (None, "开场许可已确认"),
                m.CMD_PAUSE: (m.POINT_PAUSED, "暂停生效"),
                m.CMD_RESUME: (m.POINT_PLAYING, "恢复播出"),
                m.CMD_CUT: (m.POINT_CUT, "切断已确认"),
                m.CMD_END: (m.POINT_ENDED, "结束已确认"),
                m.CMD_ROTATE_KEY: (None, "密钥轮换已确认"),
            }.get(ctype, (None, f"命令已确认：{ctype}"))
        if kind == m.EV_POINT_CUT:
            return m.POINT_CUT, f"紧急切断：{payload.get('reason')}"
        if kind == m.EV_RIGHTS_EXPIRED_CUT:
            return m.POINT_CUT, "权利窗口到期，强制切断"
        if kind == m.EV_POINT_ENDED:
            note = f"（{payload.get('note')}）" if payload.get("note") else ""
            return m.POINT_ENDED, f"场次结束{note}"
        if kind == m.EV_POINT_BLOCKED:
            return m.POINT_BLOCKED, f"区域禁播：{payload.get('reason')}"
        if kind == m.EV_POINT_UNBLOCKED:
            return m.POINT_PENDING, "禁播解除，恢复待开场"
        if kind == m.EV_TOKEN_REVOKED:
            return None, "流令牌已吊销"
        if kind == m.EV_EDGE_MERGED:
            state = {"open": m.POINT_PLAYING, "pause": m.POINT_PAUSED,
                     "resume": m.POINT_PLAYING, "end": m.POINT_ENDED,
                     "cut": m.POINT_CUT}.get(payload.get("type"))
            return state, f"离线自治事件合并：{payload.get('type')}（序号 {payload.get('edge_seq')}）"
        if kind == m.EV_EDGE_CONFLICT:
            return None, f"边缘事件冲突，以中心为准：{payload.get('type')}"
        if kind == m.EV_SITE_EXIT:
            return m.SITE_EXITED, "观众退场完成"
        if kind == m.EV_BAN_DECLARED:
            return None, f"区域禁播生效：{payload.get('region')}（{payload.get('mode')}）"
        if kind == m.EV_BAN_LIFTED:
            return None, "区域禁播解除"
        if kind == m.EV_KEY_BATCH_CREATED:
            return None, f"密钥批次就绪：{payload.get('batch_id')}（第 {payload.get('generation')} 代）"
        if kind == m.EV_KEY_ROTATION_STARTED:
            return None, f"密钥轮换：{payload.get('old_batch')} → {payload.get('new_batch')}"
        if kind == m.EV_KEY_ROTATED:
            return None, f"点位已切换到新密钥批次 {payload.get('batch_id')}"
        if kind == m.EV_KEY_REVOKED:
            return None, f"密钥批次已吊销：{payload.get('batch_id')}（{payload.get('reason')}）"
        if kind == m.EV_CENTER_RESTART:
            return None, "中心服务重启，恢复管理"
        return None, kind

    def deviations(self, session_id, site_id=None):
        """偏差清单：解释每个城市实际时间线与计划/中心意图的差异来源。"""
        with self._lock:
            self._session(session_id)
            if site_id:
                rows = self.store.all(
                    "SELECT * FROM deviations WHERE session_id=? AND (site_id=? OR site_id IS NULL)"
                    " ORDER BY created_at, deviation_id", (session_id, site_id))
            else:
                rows = self.store.all(
                    "SELECT * FROM deviations WHERE session_id=? ORDER BY created_at, deviation_id",
                    (session_id,))
            return {"session_id": session_id, "site_id": site_id,
                    "items": [dict(r) for r in rows]}

    def list_dispositions(self, session_id):
        """处置记录：政策变化对已开始会话的明确处置。"""
        with self._lock:
            self._session(session_id)
            return {"session_id": session_id, "items": [dict(r) for r in self.store.all(
                "SELECT * FROM dispositions WHERE session_id=? ORDER BY created_at, disposition_id",
                (session_id,))]}
