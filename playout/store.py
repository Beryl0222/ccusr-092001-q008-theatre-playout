"""SQLite 持久化层：模式定义与基础访问。

所有表都以事件日志为核心，状态表是日志的物化视图；
中心重启后直接从库中恢复，无需额外快照。
"""

from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
  session_id      TEXT PRIMARY KEY,
  title           TEXT NOT NULL,
  status          TEXT NOT NULL,
  scheduled_start REAL NOT NULL,
  scheduled_end   REAL NOT NULL,
  last_seq        INTEGER NOT NULL DEFAULT 0,
  created_at      REAL NOT NULL,
  updated_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS rights_windows (
  session_id TEXT NOT NULL,
  region     TEXT NOT NULL,
  not_before REAL NOT NULL,
  not_after  REAL NOT NULL,
  PRIMARY KEY (session_id, region)
);

CREATE TABLE IF NOT EXISTS sites (
  site_id        TEXT PRIMARY KEY,
  name           TEXT NOT NULL,
  city           TEXT NOT NULL,
  region         TEXT NOT NULL,
  status         TEXT NOT NULL,
  last_heartbeat REAL,
  created_at     REAL NOT NULL,
  updated_at     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS points (
  session_id        TEXT NOT NULL,
  site_id           TEXT NOT NULL,
  state             TEXT NOT NULL,
  permit_id         TEXT,
  permit_seq        INTEGER,
  stream_token      TEXT,
  token_valid_until REAL,
  key_batch_id      TEXT,
  opened_at         REAL,
  ended_at          REAL,
  exited_at         REAL,
  applied_seq       INTEGER NOT NULL DEFAULT 0,
  updated_at        REAL NOT NULL,
  PRIMARY KEY (session_id, site_id)
);

CREATE TABLE IF NOT EXISTS key_batches (
  batch_id    TEXT PRIMARY KEY,
  session_id  TEXT NOT NULL,
  generation  INTEGER NOT NULL,
  status      TEXT NOT NULL,
  secret      TEXT NOT NULL,
  grace_until REAL,
  created_at  REAL NOT NULL,
  updated_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  session_id TEXT NOT NULL,
  seq        INTEGER NOT NULL,
  event_id   TEXT NOT NULL,
  kind       TEXT NOT NULL,
  site_id    TEXT,
  origin     TEXT NOT NULL,
  ref_seq    INTEGER,
  payload    TEXT NOT NULL,
  created_at REAL NOT NULL,
  PRIMARY KEY (session_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_events_site ON events (session_id, site_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_ref  ON events (session_id, ref_seq);

CREATE TABLE IF NOT EXISTS commands (
  command_id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  type       TEXT NOT NULL,
  request    TEXT NOT NULL,
  response   TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS edge_requests (
  site_id       TEXT NOT NULL,
  edge_event_id TEXT NOT NULL,
  endpoint      TEXT NOT NULL,
  status        INTEGER NOT NULL,
  response      TEXT NOT NULL,
  created_at    REAL NOT NULL,
  PRIMARY KEY (site_id, edge_event_id)
);

CREATE TABLE IF NOT EXISTS edge_merged (
  site_id    TEXT NOT NULL,
  session_id TEXT NOT NULL,
  edge_seq   INTEGER NOT NULL,
  created_at REAL NOT NULL,
  PRIMARY KEY (site_id, session_id, edge_seq)
);

CREATE TABLE IF NOT EXISTS bans (
  ban_id     TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  region     TEXT NOT NULL,
  reason     TEXT NOT NULL,
  mode       TEXT NOT NULL,
  created_at REAL NOT NULL,
  lifted_at  REAL
);

CREATE TABLE IF NOT EXISTS dispositions (
  disposition_id TEXT PRIMARY KEY,
  session_id     TEXT NOT NULL,
  site_id        TEXT,
  policy_type    TEXT NOT NULL,
  policy_id      TEXT,
  action         TEXT NOT NULL,
  reason         TEXT NOT NULL,
  created_at     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS deviations (
  deviation_id TEXT PRIMARY KEY,
  session_id   TEXT NOT NULL,
  site_id      TEXT,
  kind         TEXT NOT NULL,
  seconds      REAL NOT NULL,
  detail       TEXT NOT NULL,
  created_at   REAL NOT NULL
);
"""


class Store:
    """对 sqlite3 连接的薄封装；事务由核心层通过 ``with store.conn`` 管理。"""

    def __init__(self, path: str = ":memory:"):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        if path != ":memory:":
            self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def one(self, sql, params=()):
        return self.conn.execute(sql, params).fetchone()

    def all(self, sql, params=()):
        return self.conn.execute(sql, params).fetchall()

    def run(self, sql, params=()):
        self.conn.execute(sql, params)

    def close(self):
        self.conn.close()
