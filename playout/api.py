"""HTTP 路由层：运维管理 API（/admin/*）与边缘节点协议（/edge/*）。

仅依赖标准库；每个请求前执行一次惰性巡检（sweep），
保证区域禁播、权利到期、密钥宽限等政策无需后台线程即可即时生效。
"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .core import CoreError, PlayoutCore

# ---------- 路由模式 ----------
_SITE = r"(?P<site_id>[A-Za-z0-9_.\-~]+)"
_SESSION = r"(?P<session_id>[A-Za-z0-9_.\-~]+)"
_SEQ = r"(?P<seq>\d+)"
_BAN = r"(?P<ban_id>[A-Za-z0-9_.\-~]+)"
_BATCH = r"(?P<batch_id>[A-Za-z0-9_.\-~]+)"

ROUTES = [
    # 运维
    ("POST",   r"^/admin/sites$", "create_site"),
    ("GET",    r"^/admin/sites$", "list_sites"),
    ("GET",    rf"^/admin/sites/{_SITE}$", "get_site"),
    ("POST",   r"^/admin/sessions$", "create_session"),
    ("GET",    r"^/admin/sessions$", "list_sessions"),
    ("GET",    rf"^/admin/sessions/{_SESSION}$", "get_session"),
    ("POST",   rf"^/admin/sessions/{_SESSION}/authorize$", "authorize_session"),
    ("POST",   rf"^/admin/sessions/{_SESSION}/reschedule$", "reschedule_session"),
    ("POST",   rf"^/admin/sessions/{_SESSION}/commands$", "issue_command"),
    ("POST",   rf"^/admin/sessions/{_SESSION}/bans$", "declare_ban"),
    ("POST",   rf"^/admin/sessions/{_SESSION}/bans/{_BAN}/lift$", "lift_ban"),
    ("POST",   rf"^/admin/sessions/{_SESSION}/keys/rotate$", "rotate_keys"),
    ("POST",   rf"^/admin/sessions/{_SESSION}/keys/{_BATCH}/revoke$", "revoke_key"),
    ("GET",    rf"^/admin/sessions/{_SESSION}/keys$", "list_keys"),
    ("GET",    rf"^/admin/sessions/{_SESSION}/timeline$", "timeline"),
    ("GET",    rf"^/admin/sessions/{_SESSION}/deviations$", "deviations"),
    ("GET",    rf"^/admin/sessions/{_SESSION}/dispositions$", "dispositions"),
    ("POST",   r"^/admin/recover$", "recover"),
    # 边缘
    ("POST",   rf"^/edge/sites/{_SITE}/sessions/{_SESSION}/handshake$", "edge_handshake"),
    ("POST",   rf"^/edge/sites/{_SITE}/sessions/{_SESSION}/heartbeat$", "edge_heartbeat"),
    ("POST",   rf"^/edge/sites/{_SITE}/sessions/{_SESSION}/acks/{_SEQ}$", "edge_ack"),
    ("POST",   rf"^/edge/sites/{_SITE}/sessions/{_SESSION}/sync$", "edge_sync"),
]


def build_handler(core: PlayoutCore, health=None):
    """构造绑定到指定核心实例的 Handler 类。"""

    class Handler(BaseHTTPRequestHandler):
        server_version = "TheatrePlayout/1.0"

        # ---- 基础收发 ----
        def _send(self, status, body):
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise CoreError(400, "请求体非法", f"JSON 解析失败: {exc}")
            if not isinstance(data, dict):
                raise CoreError(400, "请求体非法", "请求体必须是 JSON 对象")
            return data

        def _query(self) -> dict:
            from urllib.parse import parse_qs, urlparse
            parsed = urlparse(self.path)
            return {k: v[0] for k, v in parse_qs(parsed.query).items()}

        # ---- 入口 ----
        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        def _dispatch(self, method):
            from urllib.parse import urlparse
            path = urlparse(self.path).path
            if path == "/health":
                self._send(200, health() if health else {"status": "ok"})
                return
            try:
                core.sweep()  # 惰性巡检：政策与窗口即时生效
                for verb, pattern, action in ROUTES:
                    if verb != method:
                        continue
                    match = re.match(pattern, path)
                    if match:
                        getattr(self, f"_do_{action}")(match.groupdict())
                        return
                self._send(404, {"error": {"code": "接口不存在", "message": f"{method} {path}"}})
            except CoreError as exc:
                self._send(exc.status, exc.body())
            except (KeyError, TypeError) as exc:
                self._send(400, {"error": {"code": "参数缺失", "message": str(exc)}})

        def log_message(self, *_args):
            return

        # ---- 数字时间参数 ----
        @staticmethod
        def _num(body, key, required=True):
            if key not in body or body[key] is None:
                if required:
                    raise CoreError(400, "参数缺失", f"缺少参数 {key}")
                return None
            try:
                return float(body[key])
            except (TypeError, ValueError):
                raise CoreError(400, "参数非法", f"参数 {key} 必须是数字时间戳")

        # ================= 运维 API =================
        def _do_create_site(self, _p):
            b = self._body()
            for key in ("site_id", "name", "city", "region"):
                if not b.get(key):
                    raise CoreError(400, "参数缺失", f"缺少参数 {key}")
            self._send(201, core.create_site(b["site_id"], b["name"], b["city"], b["region"]))

        def _do_list_sites(self, _p):
            self._send(200, core.list_sites())

        def _do_get_site(self, p):
            self._send(200, core.get_site(p["site_id"]))

        def _do_create_session(self, _p):
            b = self._body()
            sid = b.get("session_id")
            if not sid:
                raise CoreError(400, "参数缺失", "缺少参数 session_id")
            start = self._num(b, "scheduled_start")
            end = self._num(b, "scheduled_end")
            windows = []
            for w in b.get("windows") or []:
                windows.append({
                    "region": w["region"],
                    "not_before": self._num(w, "not_before"),
                    "not_after": self._num(w, "not_after"),
                })
            site_ids = b.get("site_ids") or []
            if not isinstance(site_ids, list):
                raise CoreError(400, "参数非法", "site_ids 必须是数组")
            self._send(201, core.create_session(
                sid, b.get("title", sid), start, end, windows, site_ids))

        def _do_list_sessions(self, _p):
            self._send(200, core.list_sessions())

        def _do_get_session(self, p):
            self._send(200, core.get_session(p["session_id"]))

        def _do_authorize_session(self, p):
            self._send(200, core.authorize_session(p["session_id"]))

        def _do_reschedule_session(self, p):
            b = self._body()
            start = self._num(b, "scheduled_start")
            end = self._num(b, "scheduled_end", required=False)
            windows = None
            if "windows" in b:
                windows = [{
                    "region": w["region"],
                    "not_before": self._num(w, "not_before"),
                    "not_after": self._num(w, "not_after"),
                } for w in (b["windows"] or [])]
            self._send(200, core.reschedule_session(
                p["session_id"], start, end, windows))

        def _do_issue_command(self, p):
            b = self._body()
            ctype = b.get("type")
            if not ctype:
                raise CoreError(400, "参数缺失", "缺少参数 type")
            site_ids = b.get("site_ids")
            if site_ids is not None and not isinstance(site_ids, list):
                raise CoreError(400, "参数非法", "site_ids 必须是数组")
            result = core.issue_command(
                p["session_id"], ctype, command_id=b.get("command_id"),
                site_ids=site_ids, reason=b.get("reason"))
            self._send(200, result)

        def _do_declare_ban(self, p):
            b = self._body()
            if not b.get("region"):
                raise CoreError(400, "参数缺失", "缺少参数 region")
            self._send(200, core.declare_ban(
                p["session_id"], b["region"], b.get("reason", "未说明原因"),
                mode=b.get("mode", "immediate"), ban_id=b.get("ban_id")))

        def _do_lift_ban(self, p):
            self._send(200, core.lift_ban(p["session_id"], p["ban_id"]))

        def _do_rotate_keys(self, p):
            b = self._body()
            grace = self._num(b, "grace_seconds", required=False) if b else None
            self._send(200, core.rotate_keys(p["session_id"], grace))

        def _do_revoke_key(self, p):
            self._send(200, core.revoke_key_batch(p["session_id"], p["batch_id"]))

        def _do_list_keys(self, p):
            self._send(200, core.list_keys(p["session_id"]))

        def _do_timeline(self, p):
            q = self._query()
            self._send(200, core.timeline(p["session_id"], q.get("site_id")))

        def _do_deviations(self, p):
            q = self._query()
            self._send(200, core.deviations(p["session_id"], q.get("site_id")))

        def _do_dispositions(self, p):
            self._send(200, core.list_dispositions(p["session_id"]))

        def _do_recover(self, _p):
            self._body()  # 允许空 POST
            self._send(200, core.recover())

        # ================= 边缘协议 =================
        def _do_edge_handshake(self, p):
            b = self._body()
            self._send(200, core.handshake(
                p["site_id"], p["session_id"], b.get("edge_event_id")))

        def _do_edge_heartbeat(self, p):
            b = self._body()
            applied = b.get("applied_seq") or 0
            try:
                applied = int(applied)
            except (TypeError, ValueError):
                raise CoreError(400, "参数非法", "applied_seq 必须是整数")
            self._send(200, core.heartbeat(
                p["site_id"], p["session_id"], state=b.get("state"),
                applied_seq=applied, edge_event_id=b.get("edge_event_id")))

        def _do_edge_ack(self, p):
            b = self._body()
            self._send(200, core.ack(
                p["site_id"], p["session_id"], int(p["seq"]),
                edge_event_id=b.get("edge_event_id"), payload=b.get("payload")))

        def _do_edge_sync(self, p):
            b = self._body()
            events = b.get("offline_events") or []
            for ev in events:
                if "edge_seq" not in ev or "type" not in ev:
                    raise CoreError(400, "参数非法",
                                    "offline_events 每项必须含 edge_seq 与 type")
            try:
                last = int(b.get("last_acked_seq") or 0)
            except (TypeError, ValueError):
                raise CoreError(400, "参数非法", "last_acked_seq 必须是整数")
            self._send(200, core.sync(
                p["site_id"], p["session_id"], sync_id=b.get("sync_id"),
                last_acked_seq=last, offline_events=events))

    return Handler


def make_server(host: str, port: int, core: PlayoutCore, health=None) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), build_handler(core, health))
