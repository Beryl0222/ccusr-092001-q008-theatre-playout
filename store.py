"""SQLite 持久化层。

所有控制状态均落库（WAL 模式），中心服务重启后可继续管理进行中的场次。

关键约定：
- commands.seq 为「场次内全局单调递增」序号，由数据库在同事务内分配，
  UNIQUE(show_id, seq) 保证同一序号绝不会分配两次；
- events 的 UNIQUE(show_id, venue_id, seq) 保证边缘重连重放的事件只合并一次；
- commands 的 UNIQUE(show_id, venue_id, idempotency_key) 保证开场握手等
  命令在网络抖动重试时不产生第二条命令（杜绝二次开场）。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time

# 场次状态
SHOW_PENDING_AUTH = "待授权"
SHOW_READY = "待开场"
SHOW_PLAYING = "播出中"
SHOW_PAUSED = "已暂停"
SHOW_ENDED = "已结束"
SHOW_CUT = "已切断"
SHOW_STATES = (
    SHOW_PENDING_AUTH,
    SHOW_READY,
    SHOW_PLAYING,
    SHOW_PAUSED,
    SHOW_ENDED,
    SHOW_CUT,
)

# 放映点状态
VENUE_ONLINE = "在线"
VENUE_BRIEFLY_OFFLINE = "短时离线"
VENUE_QUARANTINED = "隔离"
VENUE_FINISHED = "退场完成"
VENUE_STATES = (
    VENUE_ONLINE,
    VENUE_BRIEFLY_OFFLINE,
    VENUE_QUARANTINED,
    VENUE_FINISHED,
)

# 密钥批次状态
KEY_PENDING = "待启用"
KEY_ACTIVE = "有效"
KEY_ROTATING = "轮换中"
KEY_REVOKED = "已吊销"
KEY_STATES = (KEY_PENDING, KEY_ACTIVE, KEY_ROTATING, KEY_REVOKED)

# 命令生命周期
CMD_PENDING = "待送达"
CMD_DELIVERED = "已送达"
CMD_ACKED = "已确认"
CMD_SUPERSEDED = "已废弃"
CMD_STATES = (CMD_PENDING, CMD_DELIVERED, CMD_ACKED, CMD_SUPERSEDED)

# 控制命令类型
CMD_OPEN = "开场"
CMD_PAUSE = "暂停"
CMD_RESUME = "恢复"
CMD_CUT = "切断"
CMD_FINISH = "结束"
CMD_ROTATE_KEY = "密钥轮换"
TERMINAL_CMDS = (CMD_CUT, CMD_FINISH)
CONTROL_CMDS = (CMD_OPEN, CMD_PAUSE, CMD_RESUME, CMD_CUT, CMD_FINISH, CMD_ROTATE_KEY)

# 边缘事件类型
EV_OPENED = "已开场"
EV_HEARTBEAT = "心跳"
EV_PAUSED = "已暂停"
EV_RESUMED = "已恢复"
EV_CUT = "已切断"
EV_FINISHED = "已结束"
TERMINAL_EVENTS = (EV_CUT, EV_FINISHED)

SCHEMA = """
CREATE TABLE IF NOT EXISTS shows (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    starts_at REAL NOT NULL,
    planned_end_at REAL NOT NULL,
    status TEXT NOT NULL,
    reschedule_reason TEXT,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS key_batches (
    id TEXT PRIMARY KEY,
    show_id TEXT NOT NULL REFERENCES shows(id),
    status TEXT NOT NULL,
    activated_at REAL,
    revoked_at REAL,
    rotate_reason TEXT,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS venues (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    region TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS entitlements (
    id TEXT PRIMARY KEY,
    show_id TEXT NOT NULL REFERENCES shows(id),
    venue_id TEXT NOT NULL REFERENCES venues(id),
    key_batch_id TEXT REFERENCES key_batches(id),
    window_start REAL NOT NULL,
    window_end REAL NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS bans (
    id TEXT PRIMARY KEY,
    region TEXT NOT NULL,
    show_id TEXT,
    reason TEXT,
    issued_at REAL NOT NULL,
    lifted_at REAL
);
CREATE TABLE IF NOT EXISTS sessions (
    show_id TEXT NOT NULL REFERENCES shows(id),
    venue_id TEXT NOT NULL REFERENCES venues(id),
    status TEXT NOT NULL,
    conn_state TEXT NOT NULL,
    key_batch_id TEXT,
    opened_at REAL,
    last_heartbeat REAL,
    last_event_seq INTEGER NOT NULL DEFAULT 0,
    delivered_seq INTEGER NOT NULL DEFAULT 0,
    gap_from INTEGER,
    gap_to INTEGER,
    quarantined_at REAL,
    quarantine_reason TEXT,
    PRIMARY KEY (show_id, venue_id)
);
CREATE TABLE IF NOT EXISTS commands (
    seq INTEGER NOT NULL,
    show_id TEXT NOT NULL REFERENCES shows(id),
    venue_id TEXT NOT NULL REFERENCES venues(id),
    cmd TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    reason TEXT,
    status TEXT NOT NULL,
    idempotency_key TEXT,
    issued_at REAL NOT NULL,
    delivered_at REAL,
    acked_at REAL,
    PRIMARY KEY (show_id, seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cmd_idem
    ON commands(show_id, venue_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS events (
    show_id TEXT NOT NULL,
    venue_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    occurred_at REAL NOT NULL,
    received_at REAL NOT NULL,
    offline_buffered INTEGER NOT NULL DEFAULT 0,
    payload TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (show_id, venue_id, seq)
);
CREATE TABLE IF NOT EXISTS dispositions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    show_id TEXT NOT NULL,
    venue_id TEXT,
    trigger_type TEXT NOT NULL,
    trigger_ref TEXT,
    action TEXT NOT NULL,
    applied_to_session INTEGER NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL
);
"""


class Conflict(Exception):
    """请求与当前状态或唯一性约束冲突（映射 HTTP 409）。"""


class NotFound(Exception):
    """引用的实体不存在（映射 HTTP 404）。"""


class Forbidden(Exception):
    """当前授权/禁令状态拒绝该操作（映射 HTTP 403）。"""


class Store:
    """线程安全的 SQLite 封装：单连接 + 串行写事务。"""

    def __init__(self, path: str = ":memory:", clock=time.time):
        self._lock = threading.RLock()
        self._clock = clock
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    # ---- 基础工具 -------------------------------------------------------

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def now(self) -> float:
        return self._clock()

    def get(self, sql: str, args=()) -> sqlite3.Row | None:
        cur = self.conn.execute(sql, args)
        return cur.fetchone()

    def all(self, sql: str, args=()) -> list[sqlite3.Row]:
        cur = self.conn.execute(sql, args)
        return cur.fetchall()

    # ---- 场次 / 放映点 / 密钥 / 权利 / 禁令 -----------------------------

    def create_show(self, show_id, title, starts_at, planned_end_at, status=SHOW_PENDING_AUTH):
        with self._lock:
            self.conn.execute(
                "INSERT INTO shows(id,title,starts_at,planned_end_at,status,created_at)"
                " VALUES(?,?,?,?,?,?)",
                (show_id, title, starts_at, planned_end_at, status, self.now()),
            )
            self.commit()

    def get_show(self, show_id):
        return self.get("SELECT * FROM shows WHERE id=?", (show_id,))

    def list_shows(self):
        return self.all("SELECT * FROM shows ORDER BY starts_at")

    def set_show_status(self, show_id, status, reschedule_reason=None):
        with self._lock:
            self.conn.execute(
                "UPDATE shows SET status=?, reschedule_reason=COALESCE(?,reschedule_reason)"
                " WHERE id=?",
                (status, reschedule_reason, show_id),
            )

    def reschedule_show(self, show_id, new_starts_at, new_end_at, reason):
        with self._lock:
            self.conn.execute(
                "UPDATE shows SET starts_at=?, planned_end_at=?, status=?, reschedule_reason=?"
                " WHERE id=?",
                (new_starts_at, new_end_at, SHOW_PENDING_AUTH, reason, show_id),
            )

    def create_venue(self, venue_id, name, region):
        with self._lock:
            self.conn.execute(
                "INSERT INTO venues(id,name,region,created_at) VALUES(?,?,?,?)",
                (venue_id, name, region, self.now()),
            )
            self.commit()

    def get_venue(self, venue_id):
        return self.get("SELECT * FROM venues WHERE id=?", (venue_id,))

    def list_venues(self, region=None):
        if region is not None:
            return self.all("SELECT * FROM venues WHERE region=? ORDER BY id", (region,))
        return self.all("SELECT * FROM venues ORDER BY id")

    def create_key_batch(self, batch_id, show_id, status=KEY_PENDING):
        with self._lock:
            self.conn.execute(
                "INSERT INTO key_batches(id,show_id,status,created_at) VALUES(?,?,?,?)",
                (batch_id, show_id, status, self.now()),
            )
            self.commit()

    def get_key_batch(self, batch_id):
        return self.get("SELECT * FROM key_batches WHERE id=?", (batch_id,))

    def set_key_status(self, batch_id, status, reason=None):
        with self._lock:
            if status == KEY_ACTIVE:
                self.conn.execute(
                    "UPDATE key_batches SET status=?, activated_at=COALESCE(activated_at,?) WHERE id=?",
                    (status, self.now(), batch_id),
                )
            elif status == KEY_REVOKED:
                self.conn.execute(
                    "UPDATE key_batches SET status=?, revoked_at=?, rotate_reason=COALESCE(?,rotate_reason)"
                    " WHERE id=?",
                    (status, self.now(), reason, batch_id),
                )
            else:
                self.conn.execute("UPDATE key_batches SET status=? WHERE id=?", (status, batch_id))

    def list_key_batches(self, show_id):
        return self.all(
            "SELECT * FROM key_batches WHERE show_id=? ORDER BY created_at", (show_id,)
        )

    def create_entitlement(self, ent_id, show_id, venue_id, key_batch_id,
                           window_start, window_end, active=1):
        with self._lock:
            self.conn.execute(
                "INSERT INTO entitlements(id,show_id,venue_id,key_batch_id,"
                "window_start,window_end,active) VALUES(?,?,?,?,?,?,?)",
                (ent_id, show_id, venue_id, key_batch_id, window_start, window_end, active),
            )
            self.commit()

    def get_entitlement(self, show_id, venue_id):
        return self.get(
            "SELECT * FROM entitlements WHERE show_id=? AND venue_id=? AND active=1",
            (show_id, venue_id),
        )

    def rebind_key_batch(self, show_id, venue_id, batch_id):
        with self._lock:
            self.conn.execute(
                "UPDATE entitlements SET key_batch_id=? WHERE show_id=? AND venue_id=? AND active=1",
                (batch_id, show_id, venue_id),
            )

    def issue_ban(self, ban_id, region, reason, show_id=None):
        with self._lock:
            self.conn.execute(
                "INSERT INTO bans(id,region,show_id,reason,issued_at) VALUES(?,?,?,?,?)",
                (ban_id, region, show_id, reason, self.now()),
            )
            self.commit()

    def lift_ban(self, ban_id):
        with self._lock:
            self.conn.execute("UPDATE bans SET lifted_at=? WHERE id=? AND lifted_at IS NULL",
                              (self.now(), ban_id))

    def active_ban(self, region, show_id):
        return self.get(
            "SELECT * FROM bans WHERE region=? AND lifted_at IS NULL"
            " AND (show_id IS NULL OR show_id=?) ORDER BY issued_at DESC LIMIT 1",
            (region, show_id),
        )

    # ---- 会话 -----------------------------------------------------------

    def get_session(self, show_id, venue_id):
        return self.get(
            "SELECT * FROM sessions WHERE show_id=? AND venue_id=?", (show_id, venue_id)
        )

    def list_sessions(self, show_id):
        return self.all(
            "SELECT s.*, v.name AS venue_name, v.region AS region FROM sessions s"
            " JOIN venues v ON v.id=s.venue_id WHERE s.show_id=? ORDER BY s.venue_id",
            (show_id,),
        )

    def list_active_sessions(self):
        """播出中/已暂停的会话（重启恢复与后台巡检的目标集合）。"""
        return self.all(
            "SELECT s.*, v.name AS venue_name, v.region AS region FROM sessions s"
            " JOIN venues v ON v.id=s.venue_id"
            " WHERE s.status IN (?,?)",
            (SHOW_PLAYING, SHOW_PAUSED),
        )

    def init_session(self, show_id, venue_id, key_batch_id):
        with self._lock:
            self.conn.execute(
                "INSERT INTO sessions(show_id,venue_id,status,conn_state,key_batch_id)"
                " VALUES(?,?,?,?,?)",
                (show_id, venue_id, SHOW_READY, VENUE_ONLINE, key_batch_id),
            )

    def update_session(self, show_id, venue_id, **fields):
        if not fields:
            return
        with self._lock:
            cols = ", ".join(f"{k}=?" for k in fields)
            self.conn.execute(
                f"UPDATE sessions SET {cols} WHERE show_id=? AND venue_id=?",
                (*fields.values(), show_id, venue_id),
            )

    # ---- 命令：场次内全局单调序号 + 幂等 --------------------------------

    def issue_command(self, show_id, venue_id, cmd, payload=None, reason=None,
                      idempotency_key=None):
        """在调用方事务内分配下一个场次序号并插入命令。

        必须已持有 self.lock。若幂等键已存在，返回既有命令（replayed=1）。
        返回 (row_dict, replayed)。
        """
        payload_json = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)
        if idempotency_key is not None:
            existing = self.get(
                "SELECT * FROM commands WHERE show_id=? AND venue_id=? AND idempotency_key=?",
                (show_id, venue_id, idempotency_key),
            )
            if existing is not None:
                return self._command_out(existing), True
        row = self.get(
            "SELECT COALESCE(MAX(seq),0) AS m FROM commands WHERE show_id=?", (show_id,)
        )
        next_seq = row["m"] + 1
        self.conn.execute(
            "INSERT INTO commands(seq,show_id,venue_id,cmd,payload,reason,status,"
            "idempotency_key,issued_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (next_seq, show_id, venue_id, cmd, payload_json, reason,
             CMD_PENDING, idempotency_key, self.now()),
        )
        created = self.get(
            "SELECT * FROM commands WHERE show_id=? AND seq=?", (show_id, next_seq)
        )
        return self._command_out(created), False

    @staticmethod
    def _command_out(row) -> dict:
        d = dict(row)
        try:
            d["payload"] = json.loads(d.get("payload") or "{}")
        except (json.JSONDecodeError, TypeError):
            d["payload"] = {}
        return d

    def list_commands(self, show_id, venue_id=None, after_seq=None):
        sql = "SELECT * FROM commands WHERE show_id=?"
        args: list = [show_id]
        if venue_id is not None:
            sql += " AND venue_id=?"
            args.append(venue_id)
        if after_seq is not None:
            sql += " AND seq>?"
            args.append(after_seq)
        sql += " ORDER BY seq"
        return [self._command_out(r) for r in self.all(sql, args)]

    def mark_command(self, show_id, seq, status, when=None):
        with self._lock:
            if status == CMD_DELIVERED:
                self.conn.execute(
                    "UPDATE commands SET status=?, delivered_at=? WHERE show_id=? AND seq=?",
                    (status, when or self.now(), show_id, seq),
                )
            elif status == CMD_ACKED:
                self.conn.execute(
                    "UPDATE commands SET status=?, delivered_at=COALESCE(delivered_at,?),"
                    " acked_at=? WHERE show_id=? AND seq=?",
                    (status, when or self.now(), when or self.now(), show_id, seq),
                )
            else:
                self.conn.execute(
                    "UPDATE commands SET status=? WHERE show_id=? AND seq=?",
                    (status, show_id, seq),
                )

    # ---- 边缘事件：单会话单调序号，重放自动去重 -------------------------

    def record_event(self, show_id, venue_id, seq, kind, occurred_at,
                     offline_buffered, payload=None, received_at=None):
        """插入边缘上报事件。重复 (会话, seq) 返回 False（已存在）。"""
        with self._lock:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO events(show_id,venue_id,seq,kind,occurred_at,"
                "received_at,offline_buffered,payload) VALUES(?,?,?,?,?,?,?,?)",
                (show_id, venue_id, seq, kind, occurred_at, received_at or self.now(),
                 1 if offline_buffered else 0,
                 json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)),
            )
            return cur.rowcount > 0

    def list_events(self, show_id, venue_id=None):
        if venue_id is not None:
            return self.all(
                "SELECT * FROM events WHERE show_id=? AND venue_id=? ORDER BY venue_id,seq",
                (show_id, venue_id),
            )
        return self.all(
            "SELECT * FROM events WHERE show_id=? ORDER BY venue_id,seq", (show_id,)
        )

    def event_seqs(self, show_id, venue_id):
        rows = self.all(
            "SELECT seq FROM events WHERE show_id=? AND venue_id=? ORDER BY seq",
            (show_id, venue_id),
        )
        return [r["seq"] for r in rows]

    # ---- 处置记录 -------------------------------------------------------

    def add_disposition(self, show_id, venue_id, trigger_type, action,
                        applied_to_session, trigger_ref=None, detail=""):
        with self._lock:
            cur = self.conn.execute(
                "INSERT INTO dispositions(show_id,venue_id,trigger_type,trigger_ref,"
                "action,applied_to_session,detail,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (show_id, venue_id, trigger_type, trigger_ref, action,
                 1 if applied_to_session else 0, detail, self.now()),
            )
            return cur.lastrowid

    def list_dispositions(self, show_id, venue_id=None):
        if venue_id is not None:
            return self.all(
                "SELECT * FROM dispositions WHERE show_id=? AND venue_id=? ORDER BY id",
                (show_id, venue_id),
            )
        return self.all("SELECT * FROM dispositions WHERE show_id=? ORDER BY id", (show_id,))

    def close(self):
        with self._lock:
            self.conn.close()
