"""剧场异地同步播控服务入口。

用法：
  python3 service.py --check                # 配置自检
  python3 service.py --port 8000 --db playout.db
  python3 service.py --sweep-interval 5     # 后台巡检间隔（秒）

服务以 SQLite (WAL) 持久化全部控制状态，重启后自动继续管理仍在进行的场次；
后台巡检线程周期性完成心跳超时（短时离线判定）与权利窗口到期切断。
"""

import argparse
import json
import threading

from api import build_server
from controller import PlayoutController
from store import Store

SERVICE_ID = "distributed-theatre-playout"

# 默认数据库路径；":memory:" 仅用于测试
DEFAULT_DB = "playout.db"
DEFAULT_SWEEP_INTERVAL = 5.0


def health():
    """返回稳定的服务身份。"""
    return {"status": "ok", "service": SERVICE_ID}


class Sweeper(threading.Thread):
    """后台巡检：心跳超时转短时离线、权利到期强制切断。"""

    def __init__(self, controller: PlayoutController, interval: float):
        super().__init__(daemon=True, name="sweeper")
        self.controller = controller
        self.interval = interval
        self._stop = threading.Event()

    def run(self):
        while not self._stop.wait(self.interval):
            try:
                self.controller.sweep()
            except Exception as exc:  # 巡检异常不应杀死进程
                print(f"[sweeper] 巡检失败：{exc}", flush=True)

    def stop(self):
        self._stop.set()


def build_controller(db_path: str = DEFAULT_DB) -> PlayoutController:
    store = Store(db_path)
    return PlayoutController(store)


def recover(controller: PlayoutController) -> dict:
    """重启恢复：加载进行中的会话，记录恢复处置并立即执行一次巡检。

    - 播出中/已暂停会话重新纳入管理，其命令历史与事件序列均来自磁盘；
    - 离线边缘重连时凭单调序号对账补发，无需中心保持内存状态；
    - 启动时先跑一次 sweep，使重启期间到期的权利立刻生效。
    """
    store = controller.store
    active = store.list_active_sessions()
    with store.lock:
        for sess in active:
            store.add_disposition(
                sess["show_id"], sess["venue_id"], "中心重启恢复",
                "会话重新纳入中心管理", applied_to_session=True,
                detail=f"last_event_seq={sess['last_event_seq']},"
                       f"delivered_seq={sess['delivered_seq']}",
            )
        store.commit()
    sweep_result = controller.sweep()
    return {"recovered_sessions": [
        {"show_id": s["show_id"], "venue_id": s["venue_id"],
         "status": s["status"], "conn_state": s["conn_state"]}
        for s in active], "startup_sweep": sweep_result}


def main():
    parser = argparse.ArgumentParser(description="剧场异地同步播控")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite 路径（默认 playout.db）")
    parser.add_argument("--sweep-interval", type=float, default=DEFAULT_SWEEP_INTERVAL)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    if args.check:
        # 自检：内存库建表 + 一次最小生命周期，确认配置与代码可用
        controller = build_controller(":memory:")
        assert health()["service"] == SERVICE_ID
        info = recover(controller)
        print("基础检查通过")
        print(json.dumps(info, ensure_ascii=False))
        return

    controller = build_controller(args.db)
    sweeper = Sweeper(controller, args.sweep_interval)
    sweeper.start()
    server = build_server(controller, host=args.host, port=args.port)
    print(f"播控服务启动：http://{args.host}:{args.port}  db={args.db}"
          f"  sweep={args.sweep_interval}s", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        sweeper.stop()
        server.server_close()
        controller.store.close()


if __name__ == "__main__":
    main()
