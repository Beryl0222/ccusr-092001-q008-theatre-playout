"""剧场异地同步播控服务入口。

- ``python3 service.py --check``：配置自检；
- ``python3 service.py --port 8000 --db playout.db``：启动 HTTP 服务，
  启动时自动执行恢复（recover），继续管理仍在进行的场次。
"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from playout.api import make_server
from playout.core import PlayoutCore
from playout.store import Store

SERVICE_ID = "distributed-theatre-playout"


def health():
    """返回稳定的服务身份。"""
    return {"status": "ok", "service": SERVICE_ID}


def check() -> bool:
    """配置自检：领域词汇可加载、内存存储可初始化、基本流程可跑通。"""
    store = Store(":memory:")
    core = PlayoutCore(store)
    now = core.clock()
    core.create_site("s-check", "自检影城", "自检市", "自检区")
    core.create_session(
        "p-check", "自检场次", now - 10, now + 3600,
        windows=[{"region": "自检区", "not_before": now - 10, "not_after": now + 3600}],
        site_ids=["s-check"])
    core.authorize_session("p-check")
    core.handshake("s-check", "p-check", edge_event_id="check-handshake")
    core.sweep()
    store.close()
    print("基础检查通过")
    return True


class Handler(BaseHTTPRequestHandler):
    """仅保留健康检查的独立入口；完整 API 由 playout.api 提供。"""

    def do_GET(self):
        if self.path != "/health":
            self.send_error(404)
            return
        body = json.dumps(health(), ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


def main(argv=None):
    parser = argparse.ArgumentParser(description="剧场异地同步播控")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--db", default="playout.db", help="SQLite 数据库路径，默认 playout.db")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    if args.check:
        check()
        return
    store = Store(args.db)
    core = PlayoutCore(store)
    recovered = core.recover()
    if recovered["recovered_sessions"]:
        print(f"恢复管理进行中场次: {', '.join(recovered['recovered_sessions'])}")
    server = make_server(args.host, args.port, core, health)
    print(f"播控服务监听 {args.host}:{args.port}，数据库 {args.db}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()
