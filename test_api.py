"""HTTP API 冒烟测试：通过真实 socket 走一遍完整播控流程。"""

import http.client
import json
import threading
import unittest

from playout.api import make_server
from playout.core import PlayoutCore
from playout.store import Store
from service import health
from test_playout import FakeClock


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.clock = FakeClock()
        cls.core = PlayoutCore(Store(":memory:"), clock=cls.clock)
        cls.server = make_server("127.0.0.1", 0, cls.core, health)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def call(self, method, path, body=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        payload = json.dumps(body).encode() if body is not None else None
        conn.request(method, path, body=payload,
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        data = json.loads(resp.read().decode())
        conn.close()
        return resp.status, data

    def test_health(self):
        status, data = self.call("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(data, {"status": "ok", "service": "distributed-theatre-playout"})

    def test_full_flow_over_http(self):
        t0 = self.clock.t
        status, _ = self.call("POST", "/admin/sites", {
            "site_id": "H1", "name": "HTTP影城", "city": "杭州", "region": "华东"})
        self.assertEqual(status, 201)
        status, _ = self.call("POST", "/admin/sessions", {
            "session_id": "P1", "title": "HTTP场次",
            "scheduled_start": t0 + 50, "scheduled_end": t0 + 1000,
            "windows": [{"region": "华东", "not_before": t0 + 50, "not_after": t0 + 1000}],
            "site_ids": ["H1"]})
        self.assertEqual(status, 201)
        status, _ = self.call("POST", "/admin/sessions/P1/authorize")
        self.assertEqual(status, 200)
        self.clock.advance(50)
        # 边缘握手 → 确认 → 心跳 → 暂停 → 确认
        status, permit = self.call("POST", "/edge/sites/H1/sessions/P1/handshake",
                                   {"edge_event_id": "h1"})
        self.assertEqual(status, 200)
        self.assertIn("stream_token", permit)
        status, ack = self.call("POST", f"/edge/sites/H1/sessions/P1/acks/{permit['seq']}",
                                {"edge_event_id": "h2"})
        self.assertEqual(ack["point_state"], "播出中")
        status, cmd = self.call("POST", "/admin/sessions/P1/commands",
                                {"type": "pause", "command_id": "http-pause"})
        self.assertEqual(len(cmd["issued"]), 1)
        # 同一 command_id 重发 → 幂等
        status, cmd2 = self.call("POST", "/admin/sessions/P1/commands",
                                 {"type": "pause", "command_id": "http-pause"})
        self.assertTrue(cmd2["duplicate"])
        status, hb = self.call("POST", "/edge/sites/H1/sessions/P1/heartbeat",
                               {"state": "播出中", "applied_seq": permit["seq"],
                                "edge_event_id": "h3"})
        self.assertEqual(hb["commands"][0]["type"], "pause")
        status, _ = self.call("POST",
                              f"/edge/sites/H1/sessions/P1/acks/{hb['commands'][0]['seq']}",
                              {"edge_event_id": "h4"})
        # 运维查询
        status, timeline = self.call("GET", "/admin/sessions/P1/timeline?site_id=H1")
        self.assertEqual(status, 200)
        states = [e["audience_state"] for e in timeline["entries"] if e["audience_state"]]
        self.assertEqual(states, ["播出中", "已暂停"])
        status, session = self.call("GET", "/admin/sessions/P1")
        self.assertEqual(session["status"], "已暂停")

    def test_error_format(self):
        status, data = self.call("GET", "/admin/sessions/NOPE")
        self.assertEqual(status, 404)
        self.assertIn("error", data)
        status, data = self.call("POST", "/admin/sessions/NOPE/commands", {"type": "pause"})
        self.assertEqual(status, 404)
        status, data = self.call("POST", "/admin/sessions", {"session_id": "X"})
        self.assertEqual(status, 400)
        status, data = self.call("GET", "/no-such-path")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
