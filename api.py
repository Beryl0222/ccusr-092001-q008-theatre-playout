"""HTTP API：管理面、控制面、边缘面与运维查询面。

路由概览：
  管理面  POST /admin/shows | /admin/venues | /admin/entitlements
                 /admin/shows/{id}/keys | /admin/shows/{id}/reschedule
                 /admin/shows/{id}/rotate-key | /admin/shows/{id}/finish
          POST /admin/bans ; DELETE /admin/bans/{ban_id}
  控制面  POST /control/{show_id}/{venue_id}            {cmd, reason}
          POST /shows/{id}/emergency-cut                {reason, venue_ids?, region?}
  边缘面  POST /edge/{show_id}/{venue_id}/handshake     {idempotency_key}
          POST /edge/{show_id}/{venue_id}/heartbeat
          GET  /edge/{show_id}/{venue_id}/commands?after_seq=&wait=
          POST /edge/{show_id}/{venue_id}/events        {events:[...]}
          POST /edge/{show_id}/{venue_id}/reconcile
  查询面  GET  /shows | /shows/{id} | /shows/{id}/timeline/{venue_id} | /venues
  运维面  POST /ops/sweep ; POST /ops/quarantine/release
  健康    GET  /health
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from controller import PlayoutController
from store import Conflict, Forbidden, NotFound

MAX_LONGPOLL = 25.0


def _make_handler(controller: PlayoutController):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        # ---- 基础工具 ---------------------------------------------------

        def _send(self, status, payload):
            body = json.dumps(payload, ensure_ascii=False, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self):
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise _BadRequest(f"请求体不是合法 JSON：{exc}")
            if not isinstance(data, dict):
                raise _BadRequest("请求体必须是 JSON 对象")
            return data

        def _query(self):
            return {k: v[-1] for k, v in parse_qs(urlparse(self.path).query).items()}

        def _handle_errors(self, fn):
            try:
                fn()
            except NotFound as exc:
                self._send(404, {"error": "not_found", "message": str(exc)})
            except Forbidden as exc:
                self._send(403, {"error": "forbidden", "message": str(exc)})
            except Conflict as exc:
                self._send(409, {"error": "conflict", "message": str(exc)})
            except _BadRequest as exc:
                self._send(400, {"error": "bad_request", "message": str(exc)})
            except (KeyError, TypeError, ValueError) as exc:
                self._send(400, {"error": "bad_request", "message": str(exc)})

        def do_GET(self):
            self._handle_errors(lambda: self._route_get())

        def do_POST(self):
            self._handle_errors(lambda: self._route_post())

        def do_DELETE(self):
            self._handle_errors(lambda: self._route_delete())

        def log_message(self, *_args):
            return

        # ---- GET --------------------------------------------------------

        def _route_get(self):
            path = urlparse(self.path).path.rstrip("/") or "/"
            q = self._query()
            c = controller

            if path == "/health":
                self._send(200, {"status": "ok", "service": "distributed-theatre-playout"})
            elif path == "/shows":
                self._send(200, [dict(r) for r in c.store.list_shows()])
            elif path == "/venues":
                region = q.get("region")
                self._send(200, [dict(r) for r in c.store.list_venues(region)])
            else:
                parts = [p for p in path.split("/") if p]
                # /shows/{id}
                if len(parts) == 2 and parts[0] == "shows":
                    self._send(200, c.show_status(parts[1]))
                # /shows/{id}/timeline/{venue_id}
                elif len(parts) == 4 and parts[0] == "shows" and parts[2] == "timeline":
                    self._send(200, c.venue_timeline(parts[1], parts[3]))
                # /edge/{show_id}/{venue_id}/commands
                elif (len(parts) == 4 and parts[0] == "edge"
                      and parts[3] == "commands"):
                    after = q.get("after_seq")
                    after_seq = int(after) if after is not None else None
                    wait = min(float(q.get("wait", "0")), MAX_LONGPOLL)
                    self._send(200, c.poll_commands(
                        parts[1], parts[2], after_seq=after_seq, wait=wait))
                else:
                    self._send(404, {"error": "not_found", "message": path})

        # ---- POST -------------------------------------------------------

        def _route_post(self):
            path = urlparse(self.path).path.rstrip("/") or "/"
            body = self._body()
            c = controller
            parts = [p for p in path.split("/") if p]

            if path == "/admin/shows":
                self._send(201, c.create_show(
                    body["show_id"], body["title"],
                    _ts(body["starts_at"]), _ts(body["planned_end_at"])))
            elif path == "/admin/venues":
                self._send(201, c.register_venue(
                    body["venue_id"], body["name"], body["region"]))
            elif (len(parts) == 4 and parts[0] == "admin" and parts[1] == "shows"
                  and parts[3] == "keys"):
                self._send(201, c.create_key_batch(
                    parts[2], body.get("batch_id"),
                    activate=bool(body.get("activate", False))))
            elif path == "/admin/entitlements":
                self._send(201, c.grant_entitlement(
                    body["show_id"], body["venue_id"],
                    _ts(body["window_start"]), _ts(body["window_end"]),
                    body.get("key_batch_id")))
            elif path == "/admin/entitlements/rebook":
                self._send(200, c.rebook_entitlement(
                    body["show_id"], body["venue_id"],
                    _ts(body["window_start"]), _ts(body["window_end"]),
                    body.get("key_batch_id")))
            elif path == "/admin/bans":
                self._send(201, c.issue_region_ban(
                    body["region"], body["reason"], body.get("show_id")))
            elif (len(parts) == 4 and parts[0] == "admin" and parts[1] == "shows"
                  and parts[3] == "reschedule"):
                self._send(200, c.reschedule_show(
                    parts[2], _ts(body["new_starts_at"]),
                    _ts(body["new_end_at"]), body["reason"]))
            elif (len(parts) == 4 and parts[0] == "admin" and parts[1] == "shows"
                  and parts[3] == "confirm-reschedule"):
                self._send(200, c.confirm_reschedule(parts[2]))
            elif (len(parts) == 4 and parts[0] == "admin" and parts[1] == "shows"
                  and parts[3] == "rotate-key"):
                self._send(200, c.rotate_key(parts[2], body.get("reason", "例行密钥轮换")))
            elif (len(parts) == 4 and parts[0] == "admin" and parts[1] == "shows"
                  and parts[3] == "finish"):
                self._send(200, c.finish_show(parts[2], body.get("reason")))
            elif len(parts) == 3 and parts[0] == "control":
                self._send(201, c.control(
                    parts[1], parts[2], body["cmd"], body.get("reason")))
            elif len(parts) == 3 and parts[0] == "shows" and parts[2] == "emergency-cut":
                self._send(200, c.emergency_cut(
                    parts[1], body.get("reason", "紧急切断"),
                    venue_ids=body.get("venue_ids"), region=body.get("region")))
            elif (len(parts) == 4 and parts[0] == "edge" and parts[3] == "handshake"):
                self._send(200, c.handshake(
                    parts[1], parts[2], body["idempotency_key"]))
            elif (len(parts) == 4 and parts[0] == "edge" and parts[3] == "heartbeat"):
                self._send(200, c.heartbeat(
                    parts[1], parts[2], body.get("observed_seq")))
            elif (len(parts) == 4 and parts[0] == "edge" and parts[3] == "events"):
                self._send(200, c.report_events(
                    parts[1], parts[2], body["events"]))
            elif (len(parts) == 4 and parts[0] == "edge" and parts[3] == "reconcile"):
                self._send(200, c.reconcile(
                    parts[1], parts[2],
                    int(body["last_event_seq"]), int(body["delivered_seq"]),
                    body["session_status"], body.get("commands_acked", [])))
            elif path == "/ops/sweep":
                self._send(200, c.sweep())
            elif path == "/ops/quarantine/release":
                self._send(200, c.release_quarantine(
                    body["show_id"], body["venue_id"], body.get("note", "人工核对完成")))
            else:
                self._send(404, {"error": "not_found", "message": path})

        # ---- DELETE -----------------------------------------------------

        def _route_delete(self):
            path = urlparse(self.path).path.rstrip("/") or "/"
            parts = [p for p in path.split("/") if p]
            if len(parts) == 3 and parts[0] == "admin" and parts[1] == "bans":
                self._send(200, controller.lift_region_ban(parts[2]))
            else:
                self._send(404, {"error": "not_found", "message": path})

    return Handler


class _BadRequest(Exception):
    pass


def _ts(value):
    """时间戳入参：数字（epoch 秒）或纯数字字符串。"""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value)
    raise _BadRequest(f"非法时间戳：{value!r}")


def build_server(controller: PlayoutController, host="0.0.0.0", port=8000):
    handler = _make_handler(controller)
    return ThreadingHTTPServer((host, port), handler)
