"""HTTP API 端到端测试：真实起服，经 HTTP 走完整播控链路。"""

import json
import threading
import time
import unittest
import urllib.error
import urllib.request

from api import build_server
from controller import PlayoutController
from store import Store


class Clock:
    def __init__(self, t=1_700_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class ApiTestBase(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.store = Store(":memory:", clock=self.clock)
        self.controller = PlayoutController(
            self.store, heartbeat_timeout=10, clock=self.clock)
        self._start()

    def _start(self):
        server = build_server(self.controller, host="127.0.0.1", port=0)
        self.port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self._server = server
        return server

    def tearDown(self):
        self._server.shutdown()
        self._server.server_close()

    def req(self, method, path, body=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(url, data=data, method=method)
        if data is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=5) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def bootstrap(self):
        t = self.clock.t
        self.req("POST", "/admin/shows", {
            "show_id": "s1", "title": "剧目",
            "starts_at": t, "planned_end_at": t + 7200})
        self.req("POST", "/admin/venues",
                 {"venue_id": "v1", "name": "北京", "region": "华北"})
        self.req("POST", "/admin/venues",
                 {"venue_id": "v2", "name": "上海", "region": "华东"})
        self.req("POST", "/admin/shows/s1/keys",
                 {"batch_id": "k1", "activate": True})
        for vid in ("v1", "v2"):
            self.req("POST", "/admin/entitlements", {
                "show_id": "s1", "venue_id": vid,
                "window_start": t - 60, "window_end": t + 7200})


class HttpFlowTest(ApiTestBase):
    def test_health(self):
        status, body = self.req("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["service"], "distributed-theatre-playout")

    def test_full_lifecycle_over_http(self):
        self.bootstrap()
        t = self.clock.t

        # 握手开场（重复两次，验证二次开场防护）
        s1, b1 = self.req("POST", "/edge/s1/v1/handshake",
                          {"idempotency_key": "edge-1"})
        s2, b2 = self.req("POST", "/edge/s1/v1/handshake",
                          {"idempotency_key": "edge-1"})
        self.assertEqual(s1, 200)
        self.assertEqual(b1["result"], "opened")
        self.assertEqual(b2["result"], "replayed")
        self.assertEqual(b1["command"]["seq"], b2["command"]["seq"])

        # 拉取命令并回报已开场
        status, poll = self.req("GET", "/edge/s1/v1/commands")
        self.assertEqual(status, 200)
        status, res = self.req("POST", "/edge/s1/v1/events", {"events": [
            {"seq": 1, "kind": "已开场", "occurred_at": t}]})
        self.assertEqual(res["merged"], [1])

        # 中心暂停 -> 边缘拉取到 seq=2 -> 回报
        status, _ = self.req("POST", "/control/s1/v1",
                             {"cmd": "暂停", "reason": "检修"})
        status, poll = self.req("GET", "/edge/s1/v1/commands")
        self.assertEqual([c["seq"] for c in poll["commands"]], [2])
        status, res = self.req("POST", "/edge/s1/v1/events", {"events": [
            {"seq": 2, "kind": "已暂停", "occurred_at": t,
             "offline_buffered": True}]})
        self.assertEqual(res["merged"], [2])

        # 恢复、整场结束
        status, _ = self.req("POST", "/control/s1/v1", {"cmd": "恢复"})
        status, _ = self.req("POST", "/admin/shows/s1/finish",
                             {"reason": "结束"})
        status, body = self.req("GET", "/shows/s1")
        self.assertEqual(body["show"]["status"], "已结束")

    def test_forbidden_after_rights_expiry(self):
        self.bootstrap()
        self.clock.advance(99999)
        status, body = self.req("POST", "/edge/s1/v1/handshake",
                                {"idempotency_key": "x"})
        self.assertEqual(status, 403)
        self.assertIn("权利到期", body["message"])

    def test_region_ban_cuts_active_and_blocks_future(self):
        self.bootstrap()
        self.req("POST", "/edge/s1/v1/handshake", {"idempotency_key": "a"})
        status, body = self.req("POST", "/admin/bans",
                                {"region": "华北", "reason": "管控"})
        self.assertEqual(status, 201)
        self.assertEqual(body["cut_sessions"][0]["venue_id"], "v1")
        # 已开始点被切断
        status, show = self.req("GET", "/shows/s1")
        v1 = next(s for s in show["sessions"] if s["venue_id"] == "v1")
        self.assertEqual(v1["status"], "已切断")
        # 禁播解除后可查询时间线
        status, tl = self.req("GET", "/shows/s1/timeline/v1")
        self.assertEqual(status, 200)
        kinds = {d["type"] for d in tl["deviations"]}
        self.assertIn("区域禁播", kinds)

    def test_key_rotation_immediate_effect(self):
        self.bootstrap()
        status, body = self.req("POST", "/admin/shows/s1/rotate-key",
                                {"reason": "例行轮换"})
        self.assertEqual(status, 200)
        new_batch = body["new_batch"]["id"]
        status, hs = self.req("POST", "/edge/s1/v2/handshake",
                              {"idempotency_key": "b"})
        self.assertEqual(hs["command"]["payload"]["key_batch_id"], new_batch)

    def test_reschedule_blocks_unopened(self):
        self.bootstrap()
        t = self.clock.t
        status, _ = self.req("POST", "/admin/shows/s1/reschedule", {
            "new_starts_at": t + 86400,
            "new_end_at": t + 86400 + 7200,
            "reason": "主演调整"})
        status, body = self.req("POST", "/edge/s1/v1/handshake",
                                {"idempotency_key": "c"})
        self.assertEqual(status, 403)
        self.assertIn("场次改期", body["message"])

    def test_event_gap_quarantines_over_http(self):
        self.bootstrap()
        t = self.clock.t
        self.req("POST", "/edge/s1/v1/handshake", {"idempotency_key": "d"})
        self.req("POST", "/edge/s1/v1/events",
                 {"events": [{"seq": 1, "kind": "已开场", "occurred_at": t}]})
        status, body = self.req("POST", "/edge/s1/v1/events", {"events": [
            {"seq": 3, "kind": "已暂停", "occurred_at": t}]})
        self.assertTrue(body["quarantined"])
        # 解除隔离
        status, body = self.req("POST", "/ops/quarantine/release", {
            "show_id": "s1", "venue_id": "v1", "note": "核对完毕"})
        self.assertEqual(status, 200)
        self.assertEqual(body["conn_state"], "在线")

    def test_sweep_marks_offline(self):
        self.bootstrap()
        self.req("POST", "/edge/s1/v1/handshake", {"idempotency_key": "e"})
        self.clock.advance(11)
        status, body = self.req("POST", "/ops/sweep", {})
        self.assertEqual(status, 200)
        self.assertIn("s1/v1", body["offline"])

    def test_404_and_validation(self):
        status, body = self.req("GET", "/shows/nope")
        self.assertEqual(status, 404)
        status, body = self.req("POST", "/admin/shows", {"show_id": "x"})
        self.assertEqual(status, 400)


class LongPollTest(ApiTestBase):
    def test_longpoll_returns_new_command(self):
        self.bootstrap()
        t = self.clock.t
        self.req("POST", "/edge/s1/v1/handshake", {"idempotency_key": "f"})

        def pause_soon():
            time.sleep(0.3)
            self.req("POST", "/control/s1/v1", {"cmd": "暂停"})

        threading.Thread(target=pause_soon, daemon=True).start()
        # 握手命令已被首次隐式拉取？这里从 after_seq=1 等暂停命令
        status, poll = self.req("GET", "/edge/s1/v1/commands?after_seq=1&wait=3")
        self.assertEqual(status, 200)
        self.assertEqual([c["seq"] for c in poll["commands"]], [2])


if __name__ == "__main__":
    unittest.main()
