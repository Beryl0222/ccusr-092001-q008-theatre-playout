"""播控核心场景测试。

用可注入的虚拟时钟驱动：授权窗口、心跳超时、权利到期、重启恢复均可确定性验证。
"""

import os
import tempfile
import unittest

from controller import PlayoutController
from store import (
    CMD_ACKED,
    EV_CUT,
    EV_HEARTBEAT,
    EV_OPENED,
    EV_PAUSED,
    KEY_REVOKED,
    SHOW_CUT,
    SHOW_ENDED,
    SHOW_PAUSED,
    SHOW_PLAYING,
    SHOW_READY,
    VENUE_BRIEFLY_OFFLINE,
    VENUE_FINISHED,
    VENUE_ONLINE,
    VENUE_QUARANTINED,
    Conflict,
    Forbidden,
    Store,
)

T0 = 1_700_000_000.0


class Clock:
    def __init__(self, t=T0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt
        return self.t


class PlayoutTestBase(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.store = Store(":memory:", clock=self.clock)
        self.c = PlayoutController(self.store, heartbeat_timeout=10,
                                   clock=self.clock)
        self._bootstrap()

    def _bootstrap(self, show="s1", venues=(("v1", "北京", "华北"),
                                            ("v2", "上海", "华东"))):
        self.c.create_show(show, "剧目", self.clock.t, self.clock.t + 7200)
        for vid, name, region in venues:
            self.c.register_venue(vid, name, region)
        self.c.create_key_batch(show, "k1", activate=True)
        for vid, _n, _r in venues:
            self.c.grant_entitlement(show, vid,
                                     self.clock.t - 60, self.clock.t + 7200)

    def open(self, venue="v1", show="s1", key="idem"):
        r = self.c.handshake(show, venue, key)
        self.assertEqual(r["result"], "opened")
        return r

    def edge_report(self, venue, seq_kind_pairs, show="s1", buffered=False):
        events = [{"seq": seq, "kind": kind,
                   "occurred_at": self.clock.t, "offline_buffered": buffered}
                  for seq, kind in seq_kind_pairs]
        return self.c.report_events(show, venue, events)

    def sess(self, venue="v1", show="s1"):
        return self.c.store.get_session(show, venue)


class HandshakeTest(PlayoutTestBase):
    def test_show_moves_to_ready_after_grant(self):
        self.assertEqual(self.store.get_show("s1")["status"], SHOW_READY)

    def test_handshake_opens_and_acks_flow(self):
        r = self.open()
        self.assertEqual(r["command"]["seq"], 1)
        self.assertEqual(self.sess()["status"], SHOW_PLAYING)
        # 边缘回报已开场 -> 命令 ACK
        res = self.edge_report("v1", [(1, EV_OPENED)])
        self.assertEqual(res["merged"], [1])
        self.assertEqual(self.sess()["status"], SHOW_PLAYING)
        cmd = self.store.get(
            "SELECT * FROM commands WHERE show_id='s1' AND venue_id='v1'")
        self.assertEqual(cmd["status"], CMD_ACKED)

    def test_repeated_handshake_same_idempotency_key_never_reopens(self):
        """中心网络抖动导致边缘重复握手：必须幂等，绝不二次开场。"""
        r1 = self.c.handshake("s1", "v1", "dup-key")
        r2 = self.c.handshake("s1", "v1", "dup-key")
        r3 = self.c.handshake("s1", "v1", "dup-key")
        self.assertEqual(r1["command"]["seq"], r2["command"]["seq"])
        self.assertEqual(r2["result"], "replayed")
        self.assertEqual(r3["result"], "replayed")
        cmds = self.store.all(
            "SELECT * FROM commands WHERE show_id='s1' AND venue_id='v1' AND cmd='开场'")
        self.assertEqual(len(cmds), 1)

    def test_handshake_after_playing_with_new_key_returns_existing_open(self):
        self.open()
        r = self.c.handshake("s1", "v1", "another-key")
        self.assertEqual(r["result"], "already_open")
        self.assertEqual(r["command"]["seq"], 1)

    def test_handshake_before_window_rejected(self):
        self.c.create_show("s2", "新剧", self.clock.t + 3600, self.clock.t + 7200)
        self.c.register_venue("v3", "广州", "华南")
        self.c.create_key_batch("s2", "k2", activate=True)
        self.c.grant_entitlement("s2", "v3",
                                 self.clock.t + 1800, self.clock.t + 7200)
        with self.assertRaises(Forbidden):
            self.c.handshake("s2", "v3", "k")
        # 拒绝留痕
        disps = self.store.list_dispositions("s2", "v3")
        self.assertTrue(any(d["trigger_type"] == "权利未生效" for d in disps))

    def test_handshake_after_entitlement_expiry_rejected(self):
        self.clock.advance(8000)
        with self.assertRaises(Forbidden) as ctx:
            self.c.handshake("s1", "v1", "late")
        self.assertIn("权利到期", str(ctx.exception))

    def test_region_ban_blocks_unopened_venue(self):
        self.c.issue_region_ban("华北", "上级通知禁播")
        with self.assertRaises(Forbidden):
            self.c.handshake("s1", "v1", "k")
        # 其他区域不受影响
        r = self.c.handshake("s1", "v2", "k2")
        self.assertEqual(r["result"], "opened")

    def test_lifted_ban_allows_open(self):
        ban = self.c.issue_region_ban("华北", "临时禁播")
        self.c.lift_region_ban(ban["ban_id"])
        r = self.c.handshake("s1", "v1", "k")
        self.assertEqual(r["result"], "opened")

    def test_revoked_key_batch_blocks_open(self):
        self.c.rotate_key("s1", "泄漏处置")
        # 旧批次吊销：未开始的 v1 不能再用旧批次（授权已重绑新批次，可正常开）
        r = self.c.handshake("s1", "v1", "k")
        self.assertEqual(r["result"], "opened")
        self.assertNotEqual(r["command"]["payload"]["key_batch_id"], "k1")
        # 若人为把授权绑回吊销批次则拒绝
        self.store.conn.execute(
            "UPDATE entitlements SET key_batch_id='k1' WHERE show_id='s1' AND venue_id='v2'")
        self.store.commit()
        with self.assertRaises(Forbidden) as ctx:
            self.c.handshake("s1", "v2", "k2")
        self.assertIn("密钥吊销", str(ctx.exception))


class MonotonicMergeTest(PlayoutTestBase):
    def test_offline_buffered_events_merge_in_order(self):
        self.open()
        self.edge_report("v1", [(1, EV_OPENED)])
        # 边缘离线：中心暂停、边缘本地确认（缓冲）
        self.c.control("s1", "v1", "暂停", "现场设备检修")
        self.clock.advance(12)
        sweep = self.c.sweep()
        self.assertIn("s1/v1", sweep["offline"])
        self.assertEqual(self.sess()["conn_state"], VENUE_BRIEFLY_OFFLINE)
        # 离线期间边缘缓冲 [2:已暂停, 3:心跳]
        res = self.edge_report("v1", [(2, EV_PAUSED), (3, EV_HEARTBEAT)],
                               buffered=True)
        self.assertEqual(res["merged"], [2, 3])
        self.assertFalse(res["quarantined"])
        self.assertEqual(self.sess()["conn_state"], VENUE_ONLINE)
        self.assertEqual(self.sess()["status"], SHOW_PAUSED)

    def test_duplicate_events_are_deduplicated(self):
        self.open()
        batch = [{"seq": 1, "kind": EV_OPENED, "occurred_at": self.clock.t}]
        first = self.c.report_events("s1", "v1", batch)
        second = self.c.report_events("s1", "v1", [dict(batch[0])])
        self.assertEqual(first["merged"], [1])
        self.assertEqual(second["duplicates"], [1])
        self.assertEqual(len(self.store.list_events("s1", "v1")), 1)

    def test_sequence_gap_quarantines_and_prefix_kept(self):
        self.open()
        # 边缘丢失序号 2，直接上报 3 -> 隔离，但 1 已正常合并
        self.edge_report("v1", [(1, EV_OPENED)])
        res = self.edge_report("v1", [(3, EV_PAUSED)])
        self.assertTrue(res["quarantined"])
        self.assertEqual(self.sess()["conn_state"], VENUE_QUARANTINED)
        # 隔离期间控制命令挂起（除切断外不下发）
        with self.assertRaises(Conflict):
            self.c.control("s1", "v1", "暂停")
        # 补传缺口序号 2 后（仍隔离，需人工解除）；此前被拒的 seq 3 现在连续，正常合并
        res2 = self.edge_report("v1", [(2, EV_PAUSED), (3, EV_PAUSED)])
        self.assertIn(2, res2["merged"])
        self.assertIn(3, res2["merged"])
        released = self.c.release_quarantine("s1", "v1", "日志核对连续")
        self.assertEqual(released["conn_state"], VENUE_ONLINE)

    def test_repeated_control_command_sequence_not_duplicated(self):
        """中心重发/边缘重连拉取：同序号命令只执行一次。"""
        self.open()
        self.edge_report("v1", [(1, EV_OPENED)])
        self.c.control("s1", "v1", "暂停")
        # 边缘用旧游标重复拉取，得到同一序号命令；中心不产生新命令
        poll1 = self.c.poll_commands("s1", "v1", after_seq=1)
        poll2 = self.c.poll_commands("s1", "v1", after_seq=1)
        self.assertEqual([c["seq"] for c in poll1["commands"]], [2])
        self.assertEqual([c["seq"] for c in poll2["commands"]], [2])
        total = self.store.all("SELECT * FROM commands WHERE show_id='s1'")
        self.assertEqual([r["seq"] for r in total], [1, 2])


class ReconnectTest(PlayoutTestBase):
    def test_missed_commands_delivered_on_reconnect(self):
        self.open()
        self.edge_report("v1", [(1, EV_OPENED)])
        # 边缘离线后，中心下发暂停 + 紧急切断
        self.clock.advance(12)
        self.c.sweep()
        self.c.control("s1", "v1", "暂停", "离线期间调度")
        self.c.emergency_cut("s1", "导演中止", venue_ids=["v1"])
        # 边缘以旧游标重连对账
        rec = self.c.reconcile("s1", "v1", last_event_seq=1, delivered_seq=1,
                               session_status=SHOW_PLAYING, commands_acked=[])
        seqs = [c["seq"] for c in rec["missed_commands"]]
        self.assertEqual(seqs, [2, 3])
        self.assertEqual(rec["center_session_status"], SHOW_CUT)
        # 边缘按序执行后回报
        res = self.edge_report("v1", [(2, EV_PAUSED), (3, EV_CUT)], buffered=True)
        self.assertEqual(res["merged"], [2, 3])
        self.assertEqual(self.sess()["status"], SHOW_CUT)

    def test_reconnect_corrects_edge_still_playing_when_center_paused(self):
        self.open()
        self.edge_report("v1", [(1, EV_OPENED)])
        self.c.control("s1", "v1", "暂停")
        # 边缘未收到暂停（命令仍 pending，未拉取），重连时状态为播出中
        rec = self.c.reconcile("s1", "v1", 1, 1, SHOW_PLAYING, [])
        # 暂停命令此前已下发（seq 2），missed 中直接带回放，无需额外纠偏
        self.assertEqual([c["seq"] for c in rec["missed_commands"]], [2])


class PolicyLinkageTest(PlayoutTestBase):
    def test_region_ban_cuts_active_session_records_disposition(self):
        self.open("v1")
        self.edge_report("v1", [(1, EV_OPENED)])
        res = self.c.issue_region_ban("华北", "区域临时管控")
        self.assertEqual(res["cut_sessions"][0]["venue_id"], "v1")
        self.assertEqual(self.sess("v1")["status"], SHOW_CUT)
        self.assertEqual(self.sess("v1")["conn_state"], VENUE_QUARANTINED)
        disps = self.store.list_dispositions("s1", "v1")
        self.assertTrue(any(d["trigger_type"] == "区域禁播" for d in disps))
        # 边缘确认切断
        self.edge_report("v1", [(2, EV_CUT)])

    def test_emergency_cut_by_region(self):
        self.c.register_venue("v3", "天津", "华北")
        self.c.grant_entitlement("s1", "v3", self.clock.t - 10, self.clock.t + 999)
        self.open("v1")
        self.open("v3")
        self.open("v2")
        self.c.emergency_cut("s1", "华北全网切断", region="华北")
        self.assertEqual(self.sess("v1")["status"], SHOW_CUT)
        self.assertEqual(self.sess("v3")["status"], SHOW_CUT)
        self.assertEqual(self.sess("v2")["status"], SHOW_PLAYING)

    def test_reschedule_holds_opened_freezes_unopened(self):
        self.open("v1")
        self.edge_report("v1", [(1, EV_OPENED)])
        res = self.c.reschedule_show(
            "s1", self.clock.t + 86400, self.clock.t + 86400 + 7200, "主演调整")
        self.assertEqual(len(res["held_sessions"]), 1)
        self.assertEqual(self.sess("v1")["status"], SHOW_PAUSED)
        # v2 未开始：握手被拒（改期冻结）
        with self.assertRaises(Forbidden) as ctx:
            self.c.handshake("s1", "v2", "k2")
        self.assertIn("场次改期", str(ctx.exception))
        # 未重发权利窗口不能确认解冻
        with self.assertRaises(Conflict):
            self.c.confirm_reschedule("s1")
        # 重发 v2 窗口后确认解冻，可正常开场；终态场次不可改期
        self.c.rebook_entitlement(
            "s1", "v2", self.clock.t + 86400 + 60, self.clock.t + 86400 + 7200)
        self.c.confirm_reschedule("s1")
        # v1 已开始的会话保持暂停待人工处置，不被解冻影响
        self.assertEqual(self.sess("v1")["status"], SHOW_PAUSED)

    def test_key_rotation_rebinds_unopened_and_notifies_active(self):
        self.open("v1")
        self.edge_report("v1", [(1, EV_OPENED)])
        res = self.c.rotate_key("s1", "例行轮换")
        new_batch = res["new_batch"]["id"]
        self.assertEqual(new_batch[:4], "key_")
        # 旧批次吊销
        self.assertEqual(self.store.get_key_batch("k1")["status"], KEY_REVOKED)
        # 进行中会话收到轮换命令并更新绑定
        notified = res["notified_sessions"][0]
        self.assertEqual(notified["venue_id"], "v1")
        self.assertEqual(self.sess("v1")["key_batch_id"], new_batch)
        # 未开始点位授权已重绑
        ent = self.store.get_entitlement("s1", "v2")
        self.assertEqual(ent["key_batch_id"], new_batch)
        # v2 用新批次正常开场
        r = self.c.handshake("s1", "v2", "k2")
        self.assertEqual(r["command"]["payload"]["key_batch_id"], new_batch)

    def test_entitlement_expiry_cuts_active_session(self):
        self.open("v1")
        self.edge_report("v1", [(1, EV_OPENED)])
        self.clock.advance(7300)  # 超过 window_end
        acted = self.c.expire_entitlements()
        self.assertEqual(acted[0]["venue_id"], "v1")
        self.assertEqual(self.sess("v1")["status"], SHOW_CUT)
        disps = self.store.list_dispositions("s1", "v1")
        self.assertTrue(any(d["trigger_type"] == "权利到期" for d in disps))

    def test_sweep_marks_offline_then_heartbeat_recovers(self):
        self.open("v1")
        self.edge_report("v1", [(1, EV_OPENED)])
        self.clock.advance(11)
        self.c.sweep()
        self.assertEqual(self.sess("v1")["conn_state"], VENUE_BRIEFLY_OFFLINE)
        self.c.heartbeat("s1", "v1")
        self.assertEqual(self.sess("v1")["conn_state"], VENUE_ONLINE)

    def test_finish_show_closes_all_venues(self):
        self.open("v1")
        self.open("v2")
        results = self.c.finish_show("s1", "演出结束")
        self.assertEqual(len(results), 2)
        self.assertEqual(self.sess("v1")["conn_state"], VENUE_FINISHED)
        self.assertEqual(self.sess("v2")["conn_state"], VENUE_FINISHED)
        self.assertEqual(self.store.get_show("s1")["status"], SHOW_ENDED)
        # 终态后忽略新命令
        with self.assertRaises(Conflict):
            self.c.control("s1", "v1", "切断")


class TimelineTest(PlayoutTestBase):
    def test_timeline_explains_offline_lag_and_missing_ack(self):
        self.open("v1")
        self.edge_report("v1", [(1, EV_OPENED)])
        self.c.control("s1", "v1", "暂停")
        self.c.poll_commands("s1", "v1")  # 边缘在线时已拉取（已送达）
        self.clock.advance(12)
        self.c.sweep()
        tl = self.c.venue_timeline("s1", "v1")
        explanations = " ".join(
            d.get("explanation", "") for d in tl["deviations"])
        self.assertIn("短时离线", explanations)
        self.assertIn("已暂停", explanations)  # 已送达未确认
        # 时间线含中心命令、边缘事件、系统处置三类
        dirs = {item["direction"] for item in tl["timeline"]}
        self.assertEqual(dirs, {"center->edge", "edge", "system"})

    def test_timeline_commands_globally_ordered_across_venues(self):
        """场次内序号全局单调：v1 开场(1)、v2 开场(2)、v1 暂停(3)。"""
        self.open("v1", key="k1")
        self.open("v2", key="k2")
        self.c.control("s1", "v1", "暂停")
        seq1 = [c["seq"] for c in self.store.list_commands("s1", "v1")]
        seq2 = [c["seq"] for c in self.store.list_commands("s1", "v2")]
        self.assertEqual(seq1, [1, 3])
        self.assertEqual(seq2, [2])


class PersistenceRestartTest(PlayoutTestBase):
    def test_restart_recovers_active_sessions_and_keeps_sequence(self):
        db_fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)
        try:
            store = Store(db_path, clock=self.clock)
            c1 = PlayoutController(store, heartbeat_timeout=10, clock=self.clock)
            c1.create_show("s9", "复排版", self.clock.t, self.clock.t + 7200)
            c1.register_venue("vx", "成都", "西南")
            c1.create_key_batch("s9", "kx", activate=True)
            c1.grant_entitlement("s9", "vx", self.clock.t - 10, self.clock.t + 7200)
            r = c1.handshake("s9", "vx", "k")
            self.assertEqual(r["command"]["seq"], 1)
            c1.report_events("s9", "vx", [
                {"seq": 1, "kind": EV_OPENED, "occurred_at": self.clock.t}])
            store.close()

            # 中心重启
            store2 = Store(db_path, clock=self.clock)
            c2 = PlayoutController(store2, heartbeat_timeout=10, clock=self.clock)
            from service import recover
            info = recover(c2)
            self.assertEqual(len(info["recovered_sessions"]), 1)
            # 继续管理：暂停命令序号必须接着 1 分配为 2
            cmd = c2.control("s9", "vx", "暂停", "重启后调度")
            self.assertEqual(cmd["seq"], 2)
            sess = store2.get_session("s9", "vx")
            self.assertEqual(sess["status"], SHOW_PAUSED)
            # 时间线可查
            tl = c2.venue_timeline("s9", "vx")
            self.assertTrue(any(d["type"] == "中心重启恢复" for d in tl["timeline"]
                                if d["direction"] == "system"))
            store2.close()
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
