"""播控核心场景测试。

覆盖：完整生命周期、幂等防重复开场、权利窗口强制、离线自治合并、
区域禁播、场次改期、密钥轮换、时间线与偏差、中心重启恢复。
"""

import os
import tempfile
import unittest

from playout import models as m
from playout.core import CoreError, PlayoutCore
from playout.store import Store


class FakeClock:
    def __init__(self, t=1_700_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


def make_core(config=None):
    clock = FakeClock()
    core = PlayoutCore(Store(":memory:"), clock=clock, config=config)
    return core, clock


def add_site(core, site_id, region="华东", city=None):
    return core.create_site(site_id, f"影城{site_id}", city or f"城市{site_id}", region)


def add_session(core, clock, session_id="S1", site_ids=("A",), regions=("华东",),
                start_offset=100.0, length=1000.0):
    t0 = clock.t
    windows = [{"region": r, "not_before": t0 + start_offset,
                "not_after": t0 + start_offset + length} for r in regions]
    core.create_session(session_id, f"剧目{session_id}", t0 + start_offset,
                        t0 + start_offset + length, windows, list(site_ids))
    core.authorize_session(session_id)
    return t0 + start_offset, t0 + start_offset + length


def open_point(core, clock, site_id, session_id, tag=""):
    """完成一次握手 + 确认，返回许可。"""
    permit = core.handshake(site_id, session_id, edge_event_id=f"hs-{site_id}{tag}")
    core.ack(site_id, session_id, permit["seq"], edge_event_id=f"ack-open-{site_id}{tag}")
    return permit


def point_state(core, session_id, site_id):
    for p in core.get_session(session_id)["points"]:
        if p["site_id"] == site_id:
            return p["state"]
    raise AssertionError(f"点位不存在: {site_id}")


def event_kinds(core, session_id):
    return [e["kind"] for e in core.timeline(session_id)["entries"]]


def deviation_kinds(core, session_id):
    return [d["kind"] for d in core.deviations(session_id)["items"]]


class LifecycleTest(unittest.TestCase):
    def test_full_lifecycle(self):
        """授权 → 握手 → 开场 → 暂停 → 恢复 → 结束 → 退场。"""
        core, clock = make_core()
        add_site(core, "A")
        start, _ = add_session(core, clock)
        clock.advance(100)  # 到达计划开场
        permit = core.handshake("A", "S1", edge_event_id="e1")
        self.assertEqual(permit["point_state"], m.POINT_HANDSHAKING)
        self.assertIsNotNone(permit["stream_token"])
        ack = core.ack("A", "S1", permit["seq"], edge_event_id="e2")
        self.assertEqual(ack["point_state"], m.POINT_PLAYING)
        self.assertEqual(core.get_session("S1")["status"], m.SESSION_PLAYING)

        pause = core.issue_command("S1", m.CMD_PAUSE, command_id="c-pause")
        self.assertEqual(len(pause["issued"]), 1)
        hb = core.heartbeat("A", "S1", state=m.POINT_PLAYING,
                            applied_seq=permit["seq"], edge_event_id="e3")
        self.assertEqual([c["type"] for c in hb["commands"]], [m.CMD_PAUSE])
        core.ack("A", "S1", hb["commands"][0]["seq"], edge_event_id="e4")
        self.assertEqual(core.get_session("S1")["status"], m.SESSION_PAUSED)

        resume = core.issue_command("S1", m.CMD_RESUME, command_id="c-resume")
        core.ack("A", "S1", resume["issued"][0]["seq"], edge_event_id="e5")
        self.assertEqual(core.get_session("S1")["status"], m.SESSION_PLAYING)

        end = core.issue_command("S1", m.CMD_END, command_id="c-end")
        self.assertEqual(core.get_session("S1")["status"], m.SESSION_ENDED)
        core.ack("A", "S1", end["issued"][0]["seq"], edge_event_id="e6",
                 payload={"exit_completed": True})
        self.assertEqual(core.get_site("A")["status"], m.SITE_EXITED)

    def test_unauthorized_session_cannot_open(self):
        core, clock = make_core()
        add_site(core, "A")
        t0 = clock.t
        core.create_session("S1", "剧目", t0, t0 + 100,
                            [{"region": "华东", "not_before": t0, "not_after": t0 + 100}],
                            ["A"])
        with self.assertRaises(CoreError) as ctx:
            core.handshake("A", "S1", edge_event_id="e1")
        self.assertEqual(ctx.exception.code, "场次未授权")

    def test_late_exit_confirmed_via_heartbeat(self):
        """结束确认后观众缓慢退场：心跳补报退场状态。"""
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock)
        clock.advance(100)
        permit = open_point(core, clock, "A", "S1")
        end = core.issue_command("S1", m.CMD_END, command_id="c-end")
        # 结束确认时观众尚未退场
        core.ack("A", "S1", end["issued"][0]["seq"], edge_event_id="e-end",
                 payload={"exit_completed": False})
        self.assertNotEqual(core.get_site("A")["status"], m.SITE_EXITED)
        # 之后心跳补报「退场完成」
        core.heartbeat("A", "S1", state=m.SITE_EXITED,
                       applied_seq=permit["seq"], edge_event_id="h-exit")
        self.assertEqual(core.get_site("A")["status"], m.SITE_EXITED)
        self.assertIn(m.EV_SITE_EXIT, event_kinds(core, "S1"))


class IdempotencyTest(unittest.TestCase):
    """事故回归：中心网络抖动不得造成部分城市重复开场。"""

    def test_duplicate_handshake_never_double_opens(self):
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock)
        clock.advance(100)
        permit1 = core.handshake("A", "S1", edge_event_id="e1")
        # 抖动重试：同一边缘事件 ID 原样重发
        permit2 = core.handshake("A", "S1", edge_event_id="e1")
        self.assertEqual(permit1, permit2)
        # 抖动重试：边缘换了请求 ID
        permit3 = core.handshake("A", "S1", edge_event_id="e1-retry")
        self.assertEqual(permit3["permit_id"], permit1["permit_id"])
        self.assertTrue(permit3["idempotent"])
        # 确认开场后再握手，仍复用同一张许可
        core.ack("A", "S1", permit1["seq"], edge_event_id="e2")
        permit4 = core.handshake("A", "S1", edge_event_id="e3")
        self.assertEqual(permit4["permit_id"], permit1["permit_id"])
        # 全场只有一张许可、一次开场
        kinds = event_kinds(core, "S1")
        self.assertEqual(kinds.count(m.EV_HANDSHAKE_PERMIT), 1)
        self.assertEqual(kinds.count(m.EV_POINT_OPENED), 1)

    def test_duplicate_command_id_is_suppressed(self):
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock)
        clock.advance(100)
        open_point(core, clock, "A", "S1")
        r1 = core.issue_command("S1", m.CMD_PAUSE, command_id="c1")
        r2 = core.issue_command("S1", m.CMD_PAUSE, command_id="c1")
        self.assertFalse(r1["duplicate"])
        self.assertTrue(r2["duplicate"])
        self.assertEqual(r1["issued"], r2["issued"])
        kinds = event_kinds(core, "S1")
        self.assertEqual(kinds.count(m.EV_COMMAND_ISSUED), 1)
        self.assertIn(m.DEV_DUPLICATE, deviation_kinds(core, "S1"))

    def test_duplicate_ack_is_suppressed(self):
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock)
        clock.advance(100)
        permit = core.handshake("A", "S1", edge_event_id="e1")
        # 同一序号确认两次：第一次生效，第二次判重
        first = core.ack("A", "S1", permit["seq"], edge_event_id="a1")
        second = core.ack("A", "S1", permit["seq"], edge_event_id="a2")
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        kinds = event_kinds(core, "S1")
        self.assertEqual(kinds.count(m.EV_POINT_OPENED), 1)

    def test_command_id_reused_across_sessions_rejected(self):
        core, clock = make_core()
        add_site(core, "A")
        add_site(core, "B")
        add_session(core, clock, session_id="S1", site_ids=("A",))
        add_session(core, clock, session_id="S2", site_ids=("B",))
        clock.advance(100)
        open_point(core, clock, "A", "S1")
        open_point(core, clock, "B", "S2")
        core.issue_command("S1", m.CMD_PAUSE, command_id="shared-id")
        with self.assertRaises(CoreError) as ctx:
            core.issue_command("S2", m.CMD_PAUSE, command_id="shared-id")
        self.assertEqual(ctx.exception.code, "幂等键冲突")


class RightsWindowTest(unittest.TestCase):
    """事故回归：权利到期后不得继续拉流。"""

    def test_handshake_outside_window_rejected(self):
        core, clock = make_core()
        add_site(core, "A")
        start, end = add_session(core, clock)  # [t0+100, t0+1100]
        with self.assertRaises(CoreError) as ctx:
            core.handshake("A", "S1", edge_event_id="e1")
        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(ctx.exception.code, "权利窗口未生效")
        clock.advance(1101)  # 越过窗口终点 [t0+100, t0+1100]
        with self.assertRaises(CoreError) as ctx:
            core.handshake("A", "S1", edge_event_id="e2")
        self.assertEqual(ctx.exception.code, "权利窗口已过期")
        # 拒绝留痕 + 同一请求重放得到同一拒绝
        self.assertIn(m.EV_OPEN_REJECTED, event_kinds(core, "S1"))
        with self.assertRaises(CoreError) as ctx:
            core.handshake("A", "S1", edge_event_id="e2")
        self.assertEqual(ctx.exception.code, "权利窗口已过期")

    def test_expiry_cut_on_heartbeat(self):
        core, clock = make_core()
        add_site(core, "A")
        start, end = add_session(core, clock, length=200.0)
        clock.advance(100)
        permit = open_point(core, clock, "A", "S1")
        clock.advance(150)  # 窗口内
        hb = core.heartbeat("A", "S1", state=m.POINT_PLAYING,
                            applied_seq=permit["seq"], edge_event_id="h1")
        self.assertFalse(hb["must_stop"])
        self.assertEqual(hb["token_valid_until"], end)  # 令牌不越过窗口终点
        clock.advance(51)   # 越过窗口终点
        hb2 = core.heartbeat("A", "S1", state=m.POINT_PLAYING,
                             applied_seq=permit["seq"], edge_event_id="h2")
        self.assertTrue(hb2["must_stop"])
        self.assertIsNone(hb2["stream_token"])
        self.assertEqual(hb2["point_state"], m.POINT_CUT)
        self.assertIn(m.EV_RIGHTS_EXPIRED_CUT, event_kinds(core, "S1"))
        self.assertIn(m.DEV_RIGHTS_EXPIRED, deviation_kinds(core, "S1"))

    def test_expiry_cut_by_sweep_without_heartbeat(self):
        """边缘沉默时，巡检同样强制到期切断。"""
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock, length=200.0)
        clock.advance(100)
        open_point(core, clock, "A", "S1")
        clock.advance(201)
        core.sweep()
        self.assertEqual(point_state(core, "S1", "A"), m.POINT_CUT)
        self.assertEqual(core.get_session("S1")["status"], m.SESSION_CUT)

    def test_rehandshake_after_expiry_cuts_instead_of_issuing_token(self):
        """进行中点位窗口到期后再握手：切断并停止发令牌，不得继续拉流。"""
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock, length=200.0)
        clock.advance(100)
        open_point(core, clock, "A", "S1")
        clock.advance(201)  # 越过窗口终点
        view = core.handshake("A", "S1", edge_event_id="late-retry")
        self.assertTrue(view["must_stop"])
        self.assertIsNone(view["stream_token"])
        self.assertEqual(point_state(core, "S1", "A"), m.POINT_CUT)


class OfflineMergeTest(unittest.TestCase):
    def test_offline_events_merge_in_monotonic_order(self):
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock)
        clock.advance(100)
        permit = open_point(core, clock, "A", "S1")
        t0 = clock.t
        clock.advance(20)  # 无心跳超过阈值
        core.sweep()
        self.assertEqual(core.get_site("A")["status"], m.SITE_OFFLINE)
        # 边缘离线自治：先暂停后恢复；上报时刻意乱序，中心按 edge_seq 归并
        result = core.sync(
            "A", "S1", sync_id="sync-1", last_acked_seq=permit["seq"],
            offline_events=[
                {"edge_seq": 2, "type": "resume", "occurred_at": t0 + 18},
                {"edge_seq": 1, "type": "pause", "occurred_at": t0 + 15},
            ])
        self.assertEqual(result["merged"], 2)
        self.assertEqual(result["duplicates"], 0)
        self.assertEqual(result["point_state"], m.POINT_PLAYING)
        # 同一 sync_id 重试：返回缓存，不重复合并
        again = core.sync("A", "S1", sync_id="sync-1", last_acked_seq=permit["seq"],
                          offline_events=[])
        self.assertEqual(again, result)
        # 相同边缘事件换 sync_id 重传：判重
        third = core.sync(
            "A", "S1", sync_id="sync-2", last_acked_seq=permit["seq"],
            offline_events=[
                {"edge_seq": 1, "type": "pause", "occurred_at": t0 + 15},
                {"edge_seq": 2, "type": "resume", "occurred_at": t0 + 18},
            ])
        self.assertEqual(third["merged"], 0)
        self.assertEqual(third["duplicates"], 2)
        self.assertIn(m.DEV_OFFLINE_AUTONOMY, deviation_kinds(core, "S1"))

    def test_offline_open_with_valid_permit(self):
        """断线前已拿到许可的边缘，离线期间按计划开场，恢复后合并。"""
        core, clock = make_core()
        add_site(core, "A")
        start, _ = add_session(core, clock)
        clock.advance(100)
        permit = core.handshake("A", "S1", edge_event_id="e1")  # 未确认即断线
        clock.advance(30)
        core.sweep()
        opened_at = clock.t - 10  # 离线期间实际开场时刻
        result = core.sync(
            "A", "S1", sync_id="s1", last_acked_seq=0,
            offline_events=[{"edge_seq": 1, "type": "open", "occurred_at": opened_at}])
        self.assertEqual(result["merged"], 1)
        self.assertEqual(result["point_state"], m.POINT_PLAYING)
        self.assertEqual(core.get_session("S1")["status"], m.SESSION_PLAYING)
        # 开场偏差按真实发生时刻计算
        skews = [d for d in core.deviations("S1")["items"] if d["kind"] == m.DEV_OPEN_SKEW]
        self.assertEqual(len(skews), 1)
        self.assertAlmostEqual(skews[0]["seconds"], opened_at - start, places=3)

    def test_conflict_center_wins(self):
        """离线期间中心已切断，边缘迟到的暂停事件冲突，以中心为准。"""
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock)
        clock.advance(100)
        permit = open_point(core, clock, "A", "S1")
        clock.advance(20)
        core.sweep()
        core.issue_command("S1", m.CMD_CUT, command_id="c-cut", reason="测试切断")
        result = core.sync(
            "A", "S1", sync_id="s1", last_acked_seq=permit["seq"],
            offline_events=[{"edge_seq": 1, "type": "pause", "occurred_at": clock.t - 5}])
        self.assertEqual(result["merged"], 0)
        self.assertEqual(len(result["conflicts"]), 1)
        self.assertEqual(result["point_state"], m.POINT_CUT)
        # 切断命令仍会随同步下发给边缘
        self.assertEqual([c["type"] for c in result["commands"]], [m.CMD_CUT])
        self.assertIn(m.DEV_CONFLICT, deviation_kinds(core, "S1"))


class PolicyTest(unittest.TestCase):
    def _three_site_session(self, core, clock):
        add_site(core, "A", region="华北", city="北京")
        add_site(core, "B", region="华北", city="天津")
        add_site(core, "C", region="华南", city="广州")
        add_session(core, clock, site_ids=("A", "B", "C"), regions=("华北", "华南"))
        clock.advance(100)

    def test_ban_immediate_cuts_playing_and_blocks_pending(self):
        core, clock = make_core()
        self._three_site_session(core, clock)
        open_point(core, clock, "A", "S1")  # A 播出中，B/C 待开场
        ban = core.declare_ban("S1", "华北", "内容紧急下架", mode="immediate", ban_id="ban-1")
        actions = {a["site_id"]: a["action"] for a in ban["affected"]}
        self.assertEqual(actions, {"A": "紧急切断", "B": "禁播"})
        self.assertEqual(point_state(core, "S1", "A"), m.POINT_CUT)
        self.assertEqual(point_state(core, "S1", "B"), m.POINT_BLOCKED)
        # 被禁播点位握手被拒
        with self.assertRaises(CoreError) as ctx:
            core.handshake("B", "S1", edge_event_id="b1")
        self.assertEqual(ctx.exception.code, "区域禁播")
        # 其他区域不受影响
        open_point(core, clock, "C", "S1")
        # 处置记录完整
        disps = core.list_dispositions("S1")["items"]
        self.assertTrue(any(d["policy_type"] == m.POLICY_BAN and d["site_id"] == "A"
                            and d["action"] == "紧急切断进行中场次" for d in disps))
        self.assertTrue(any(d["site_id"] == "B" for d in disps))
        # 解除禁播后 B 恢复待开场
        lifted = core.lift_ban("S1", "ban-1")
        self.assertEqual(lifted["restored"], ["B"])
        self.assertEqual(point_state(core, "S1", "B"), m.POINT_PENDING)

    def test_ban_grace_lets_playing_finish(self):
        core, clock = make_core()
        self._three_site_session(core, clock)
        open_point(core, clock, "C", "S1")
        core.declare_ban("S1", "华南", "临时管控", mode="grace")
        self.assertEqual(point_state(core, "S1", "C"), m.POINT_PLAYING)  # 允许播完
        disps = core.list_dispositions("S1")["items"]
        self.assertTrue(any(d["action"] == "允许播完当前场次，禁止新的开场" for d in disps))

    def test_reschedule_affects_pending_and_records_started(self):
        core, clock = make_core()
        add_site(core, "A")
        add_site(core, "B")
        add_session(core, clock, site_ids=("A", "B"))
        clock.advance(100)
        open_point(core, clock, "A", "S1")  # A 已开场，B 未开场
        t0 = clock.t
        core.reschedule_session(
            "S1", scheduled_start=t0 + 200, scheduled_end=t0 + 1200,
            windows=[{"region": "华东", "not_before": t0 + 200, "not_after": t0 + 1200}])
        # 未开场点位立即按新窗口执行：现在握手被拒
        with self.assertRaises(CoreError) as ctx:
            core.handshake("B", "S1", edge_event_id="b1")
        self.assertEqual(ctx.exception.code, "权利窗口未生效")
        # 已开场点位形成处置记录，按原窗口继续
        disps = core.list_dispositions("S1")["items"]
        self.assertTrue(any(d["policy_type"] == m.POLICY_RESCHEDULE and d["site_id"] == "A"
                            for d in disps))
        self.assertEqual(point_state(core, "S1", "A"), m.POINT_PLAYING)
        # 到达新窗口后 B 可以开场
        clock.advance(200)
        open_point(core, clock, "B", "S1", tag="-new")
        self.assertEqual(point_state(core, "S1", "B"), m.POINT_PLAYING)

    def test_key_rotation(self):
        core, clock = make_core()
        add_site(core, "A")
        add_site(core, "B")
        add_session(core, clock, site_ids=("A", "B"))
        clock.advance(100)
        permit_a = open_point(core, clock, "A", "S1")  # A 播出中，B 待开场
        rot = core.rotate_keys("S1", grace_seconds=30)
        old_batch, new_batch = rot["old_batch"], rot["new_batch"]
        self.assertNotEqual(old_batch, new_batch)
        # 进行中点位收到轮换命令
        hb = core.heartbeat("A", "S1", state=m.POINT_PLAYING,
                            applied_seq=permit_a["seq"], edge_event_id="h1")
        rotate_cmds = [c for c in hb["commands"] if c["type"] == m.CMD_ROTATE_KEY]
        self.assertEqual(len(rotate_cmds), 1)
        self.assertEqual(rotate_cmds[0]["params"]["batch_id"], new_batch)
        # 未开场点位握手自动使用新批次
        permit_b = core.handshake("B", "S1", edge_event_id="hs-B")
        self.assertEqual(permit_b["key_batch_id"], new_batch)
        # A 确认轮换后旧批次吊销
        core.ack("A", "S1", rotate_cmds[0]["seq"], edge_event_id="a-rot")
        keys = {k["batch_id"]: k["status"] for k in core.list_keys("S1")["items"]}
        self.assertEqual(keys[old_batch], m.KEY_REVOKED)
        self.assertEqual(keys[new_batch], m.KEY_ACTIVE)
        # 处置记录
        disps = core.list_dispositions("S1")["items"]
        self.assertTrue(any(d["policy_type"] == m.POLICY_KEY_ROTATION and d["site_id"] == "A"
                            for d in disps))

    def test_key_rotation_grace_expiry_revokes(self):
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock)
        clock.advance(100)
        open_point(core, clock, "A", "S1")
        rot = core.rotate_keys("S1", grace_seconds=30)
        clock.advance(31)  # 宽限到期，A 未确认轮换
        core.sweep()
        keys = {k["batch_id"]: k["status"] for k in core.list_keys("S1")["items"]}
        self.assertEqual(keys[rot["old_batch"]], m.KEY_REVOKED)


class TimelineTest(unittest.TestCase):
    def test_timeline_shows_city_view_and_deviations(self):
        core, clock = make_core()
        add_site(core, "A", city="上海")
        add_session(core, clock)
        clock.advance(100)
        permit = core.handshake("A", "S1", edge_event_id="e1")
        clock.advance(6)  # 迟到 6 秒确认开场
        core.ack("A", "S1", permit["seq"], edge_event_id="e2")
        core.issue_command("S1", m.CMD_PAUSE, command_id="c1")
        hb = core.heartbeat("A", "S1", state=m.POINT_PLAYING,
                            applied_seq=permit["seq"], edge_event_id="e3")
        clock.advance(5)  # 传播延迟超过阈值
        core.ack("A", "S1", hb["commands"][0]["seq"], edge_event_id="e4")

        timeline = core.timeline("S1", site_id="A")
        states = [e["audience_state"] for e in timeline["entries"] if e["audience_state"]]
        self.assertEqual(states, [m.POINT_PLAYING, m.POINT_PAUSED])
        # 序号严格单调
        seqs = [e["seq"] for e in timeline["entries"]]
        self.assertEqual(seqs, sorted(seqs))
        # 偏差解释：开场偏差 + 传播延迟
        kinds = deviation_kinds(core, "S1")
        self.assertIn(m.DEV_OPEN_SKEW, kinds)
        self.assertIn(m.DEV_PROPAGATION, kinds)
        skew = [d for d in core.deviations("S1")["items"]
                if d["kind"] == m.DEV_OPEN_SKEW][0]
        self.assertAlmostEqual(skew["seconds"], 6.0, places=3)

    def test_event_sequence_is_strictly_monotonic(self):
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock)
        clock.advance(100)
        open_point(core, clock, "A", "S1")
        core.issue_command("S1", m.CMD_PAUSE, command_id="c1")
        core.issue_command("S1", m.CMD_END, command_id="c2")
        seqs = [e["seq"] for e in core.timeline("S1")["entries"]]
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))


class SiteStatusTest(unittest.TestCase):
    def test_offline_isolation_and_recovery(self):
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock)
        clock.advance(100)
        permit = open_point(core, clock, "A", "S1")
        clock.advance(16)
        core.sweep()
        self.assertEqual(core.get_site("A")["status"], m.SITE_OFFLINE)
        clock.advance(120)
        core.sweep()
        self.assertEqual(core.get_site("A")["status"], m.SITE_ISOLATED)
        hb = core.heartbeat("A", "S1", state=m.POINT_PLAYING,
                            applied_seq=permit["seq"], edge_event_id="h-back")
        self.assertEqual(core.get_site("A")["status"], m.SITE_ONLINE)
        self.assertIn(m.DEV_OFFLINE_WINDOW, deviation_kinds(core, "S1"))

    def test_state_mismatch_deviation(self):
        core, clock = make_core()
        add_site(core, "A")
        add_session(core, clock)
        clock.advance(100)
        permit = open_point(core, clock, "A", "S1")
        # 边缘上报与中心记录不一致，且无在途命令可解释
        core.heartbeat("A", "S1", state=m.POINT_PAUSED,
                       applied_seq=permit["seq"], edge_event_id="h1")
        self.assertIn(m.DEV_STATE_MISMATCH, deviation_kinds(core, "S1"))


class RecoveryTest(unittest.TestCase):
    def test_restart_recovers_ongoing_sessions(self):
        """中心重启后从 SQLite 恢复，继续管理进行中的场次。"""
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "playout.db")
            core1 = PlayoutCore(Store(path), clock=clock)
            add_site(core1, "A")
            add_session(core1, clock)
            clock.advance(100)
            open_point(core1, clock, "A", "S1")
            before = [e["seq"] for e in core1.timeline("S1")["entries"]]

            # 模拟中心重启：同一数据库文件新建核心实例
            core2 = PlayoutCore(Store(path), clock=clock)
            recovered = core2.recover()
            self.assertIn("S1", recovered["recovered_sessions"])
            self.assertEqual(core2.get_session("S1")["status"], m.SESSION_PLAYING)
            # 重启后继续管理：暂停命令照常下发，序号延续
            pause = core2.issue_command("S1", m.CMD_PAUSE, command_id="after-restart")
            self.assertEqual(len(pause["issued"]), 1)
            self.assertGreater(pause["issued"][0]["seq"], max(before))
            kinds = event_kinds(core2, "S1")
            self.assertIn(m.EV_CENTER_RESTART, kinds)
            self.assertIn(m.DEV_RESTART, deviation_kinds(core2, "S1"))


if __name__ == "__main__":
    unittest.main()
