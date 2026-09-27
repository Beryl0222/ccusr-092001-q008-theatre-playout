"""领域词汇与状态机常量，与 domain.json 对齐。"""

# ---- 场次状态 ----
SESSION_PENDING_AUTH = "待授权"
SESSION_READY = "待开场"
SESSION_PLAYING = "播出中"
SESSION_PAUSED = "已暂停"
SESSION_ENDED = "已结束"
SESSION_CUT = "已切断"
SESSION_STATES = (
    SESSION_PENDING_AUTH,
    SESSION_READY,
    SESSION_PLAYING,
    SESSION_PAUSED,
    SESSION_ENDED,
    SESSION_CUT,
)
SESSION_TERMINAL = (SESSION_ENDED, SESSION_CUT)

# ---- 放映点状态 ----
SITE_ONLINE = "在线"
SITE_OFFLINE = "短时离线"
SITE_ISOLATED = "隔离"
SITE_EXITED = "退场完成"
SITE_STATES = (SITE_ONLINE, SITE_OFFLINE, SITE_ISOLATED, SITE_EXITED)

# ---- 密钥批次状态 ----
KEY_PENDING = "待启用"
KEY_ACTIVE = "有效"
KEY_ROTATING = "轮换中"
KEY_REVOKED = "已吊销"
KEY_STATES = (KEY_PENDING, KEY_ACTIVE, KEY_ROTATING, KEY_REVOKED)

# ---- 点位状态（场次 × 放映点）----
POINT_PENDING = "待开场"
POINT_HANDSHAKING = "握手中"
POINT_PLAYING = "播出中"
POINT_PAUSED = "已暂停"
POINT_ENDED = "已结束"
POINT_CUT = "已切断"
POINT_BLOCKED = "已禁播"
POINT_STATES = (
    POINT_PENDING,
    POINT_HANDSHAKING,
    POINT_PLAYING,
    POINT_PAUSED,
    POINT_ENDED,
    POINT_CUT,
    POINT_BLOCKED,
)
POINT_TERMINAL = (POINT_ENDED, POINT_CUT, POINT_BLOCKED)
POINT_ACTIVE = (POINT_HANDSHAKING, POINT_PLAYING, POINT_PAUSED)

# ---- 控制命令类型 ----
CMD_PAUSE = "pause"
CMD_RESUME = "resume"
CMD_CUT = "cut"
CMD_END = "end"
CMD_ROTATE_KEY = "rotate_key"
COMMAND_TYPES = (CMD_PAUSE, CMD_RESUME, CMD_CUT, CMD_END, CMD_ROTATE_KEY)
OPERATOR_COMMANDS = (CMD_PAUSE, CMD_RESUME, CMD_CUT, CMD_END)
COMMAND_LABELS = {
    "open": "开场",
    CMD_PAUSE: "暂停",
    CMD_RESUME: "恢复",
    CMD_CUT: "紧急切断",
    CMD_END: "结束",
    CMD_ROTATE_KEY: "密钥轮换",
}

# ---- 控制事件种类（场次内单调序号日志）----
EV_SESSION_CREATED = "session_created"
EV_SESSION_AUTHORIZED = "session_authorized"
EV_SESSION_STATUS = "session_status"
EV_RESCHEDULED = "rescheduled"
EV_HANDSHAKE_PERMIT = "handshake_permit"
EV_OPEN_REJECTED = "open_rejected"
EV_PERMIT_EXPIRED = "permit_expired"
EV_POINT_OPENED = "point_opened"
EV_COMMAND_ISSUED = "command_issued"
EV_COMMAND_ACKED = "command_acked"
EV_POINT_CUT = "point_cut"
EV_RIGHTS_EXPIRED_CUT = "rights_expired_cut"
EV_POINT_ENDED = "point_ended"
EV_POINT_BLOCKED = "point_blocked"
EV_POINT_UNBLOCKED = "point_unblocked"
EV_TOKEN_REVOKED = "stream_token_revoked"
EV_EDGE_MERGED = "edge_event_merged"
EV_EDGE_CONFLICT = "edge_event_conflict"
EV_SITE_EXIT = "site_exit_confirmed"
EV_BAN_DECLARED = "ban_declared"
EV_BAN_LIFTED = "ban_lifted"
EV_KEY_BATCH_CREATED = "key_batch_created"
EV_KEY_ROTATION_STARTED = "key_rotation_started"
EV_KEY_ROTATED = "key_rotated"
EV_KEY_REVOKED = "key_batch_revoked"
EV_CENTER_RESTART = "center_restart"

# ---- 偏差种类（解释城市时间线偏差来源）----
DEV_OPEN_SKEW = "开场偏差"
DEV_PROPAGATION = "传播延迟"
DEV_OFFLINE_AUTONOMY = "离线自治"
DEV_OFFLINE_WINDOW = "离线窗口"
DEV_DUPLICATE = "重复抑制"
DEV_CONFLICT = "状态冲突"
DEV_RIGHTS_EXPIRED = "权利到期"
DEV_RESTART = "中心重启"
DEV_STATE_MISMATCH = "状态上报偏差"

# ---- 处置记录的策略类型 ----
POLICY_BAN = "区域禁播"
POLICY_RESCHEDULE = "场次改期"
POLICY_KEY_ROTATION = "密钥轮换"
