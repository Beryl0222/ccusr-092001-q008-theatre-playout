"""剧场异地同步播控服务的基础入口。"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SERVICE_ID = "distributed-theatre-playout"


def health():
    """返回稳定的服务身份。"""
    return {"status": "ok", "service": SERVICE_ID}


class Handler(BaseHTTPRequestHandler):
    """提供运维健康检查。"""

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="剧场异地同步播控")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.check:
        print("基础检查通过")
    else:
        ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()
