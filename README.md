# 剧场异地同步播控

国家级剧院现场演出同步到多省多城放映点的完整播控后端。系统依据已有的场次、放映点、
密钥批次与权利窗口定义，协调开场握手、心跳、暂停、恢复、紧急切断和结束确认；
支持边缘节点短时离线自治，恢复连接后按单调序列合并事件；区域禁播、场次改期与
密钥轮换即时影响尚未开始的点位，对已开始会话形成明确处置记录；运维可查询每个城市
实际看到的时间线与偏差来源，并在中心服务重启后继续管理仍在进行的场次。

## 运行

```bash
python3 service.py --check           # 配置自检（内存库跑通握手）
python3 service.py --port 8000 --db playout.db   # 启动服务，启动时自动恢复
python3 -m unittest -v               # 运行全部 25 个测试
```

启动后通过 `/health` 巡检。所有状态持久化在单个 SQLite 文件中，中心重启自动执行
`recover()`：为未终止场次追加「中心重启」事件并立即巡检，继续管理进行中场次。

## 核心设计

### 1. 单调事件序列

每个场次一条严格递增的事件日志（`events` 表，序号由 `sessions.last_seq` 分配），
中心命令、边缘确认、离线合并事件、政策变化都进入同一条日志。点位保存
`applied_seq`（已确认的最大中心序号）与 `permit_seq`（当前许可序号），
心跳/同步时按序号拉取未应用的命令（`applied_seq` 水位线）。

### 2. 幂等：重复命令绝不二次开场

事故回归的两道防线：

- **中心命令**用 `command_id` 去重（`commands` 表）：重复下发返回首次结果并标记
  `duplicate: true`，只产生「重复抑制」偏差记录，不产生新事件；
- **边缘请求**用 `edge_event_id` 去重（`edge_requests` 表）：网络抖动导致的
  握手/心跳/确认/同步重试返回首次响应（含首次的拒绝结果），绝不二次生效；
- 开场另有状态守卫：进行中点位重复握手复用同一张许可（`permit_id` 不变、
  `idempotent: true`），确认命令重复确认安全忽略，全场只有一次 `point_opened`。

### 3. 权利窗口强制执行

握手、心跳、巡检三处校验，窗口外拉流只有拒绝：

- **握手时**：窗口未生效/已过期/缺失 → 403 并记录 `open_rejected`，同一请求重放得到同一拒绝；
- **心跳时**：播放中窗口到期 → `must_stop: true`，中心立即切断、吊销流令牌；
- **巡检（sweep）**：即使边缘沉默，到期同样强制切断；流令牌有效期不越过窗口终点
  （`min(token_ttl, not_after)`），窗口外不存在有效令牌；
- 所有权利窗口过期且无人播出时自动收尾未开场点位。

### 4. 离线自治与单调合并

- 心跳缺失 15s → 「短时离线」，120s → 「隔离」（阈值可配置）；
- 边缘离线期间可基于已有许可自治执行 open/pause/resume/end/cut/exit，
  恢复连接后通过 `/sync` 携带 `edge_seq`（边缘本地单调序号）批量上报；
- 中心按 `edge_seq` 排序应用，已合并序号记入 `edge_merged` 判重，同一 `sync_id`
  重试返回缓存；与中心状态冲突（如中心已切断）的事件**以中心为准**，事件仍下发给边缘，
  同时产生「状态冲突」偏差；
- 每个被合并的自治事件产生「离线自治」偏差，记录边缘执行时刻与合并时刻之差；
- 心跳/sync 恢复联系时自动回到「在线」，并记录「离线窗口」长度。

### 5. 政策即时生效与处置记录

| 政策 | 对尚未开始的点位 | 对已开始的会话 |
|---|---|---|
| 区域禁播（immediate） | 立即置「已禁播」、作废旧许可 | 紧急切断 + 吊销令牌 |
| 区域禁播（grace） | 禁止新开场 | 允许播完，形成处置记录 |
| 场次改期 | 握手立即按新窗口/新时间校验 | 按原窗口继续，逐点位记录 |
| 密钥轮换 | 握手自动签发新批次 | 下发轮换命令，旧批次宽限后吊销 |

每条政策对每个点位的处理都写入 `dispositions`（政策类型、动作、原因），
政策本身进入场次事件日志；解除禁播恢复待开场状态。

### 6. 城市时间线与偏差解释

- `GET /admin/sessions/{id}/timeline?site_id=`：按序号返回事件流，每条标注
  **观众可见状态**（播出中/已暂停/已切断…）与中文摘要；
- `GET /admin/sessions/{id}/deviations`：偏差清单，解释偏差来源：
  开场偏差（实际 vs 计划）、传播延迟、离线窗口、离线自治、重复抑制、状态冲突、
  权利到期、中心重启、状态上报偏差；
- `GET /admin/sessions/{id}/dispositions`：政策处置记录。

## API 总览

运维侧（`/admin`）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/admin/sites` | 创建放映点 |
| GET | `/admin/sites`、`/admin/sites/{id}` | 查询放映点 |
| POST | `/admin/sessions` | 创建场次（含权利窗口、点位列表） |
| GET | `/admin/sessions`、`/admin/sessions/{id}` | 查询场次与点位状态 |
| POST | `/admin/sessions/{id}/authorize` | 授权并生成初始密钥批次 |
| POST | `/admin/sessions/{id}/reschedule` | 改期（可替换权利窗口） |
| POST | `/admin/sessions/{id}/commands` | 下发 `pause`/`resume`/`cut`/`end`（支持 `command_id` 幂等） |
| POST | `/admin/sessions/{id}/bans` | 区域禁播（immediate/grace） |
| POST | `/admin/sessions/{id}/bans/{bid}/lift` | 解除禁播 |
| POST | `/admin/sessions/{id}/keys/rotate` | 密钥轮换 |
| POST | `/admin/sessions/{id}/keys/{batch}/revoke` | 吊销批次 |
| GET | `/admin/sessions/{id}/keys` | 密钥批次状态 |
| GET | `/admin/sessions/{id}/timeline` | 城市时间线 |
| GET | `/admin/sessions/{id}/deviations` | 偏差清单 |
| GET | `/admin/sessions/{id}/dispositions` | 处置记录 |
| POST | `/admin/recover` | 手动触发恢复 |

边缘侧（`/edge`，每次调用带 `edge_event_id` 幂等键）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/edge/sites/{site}/sessions/{session}/handshake` | 开场握手，返回许可、流令牌与密钥 |
| POST | `/edge/sites/{site}/sessions/{session}/heartbeat` | 心跳：续期令牌、返回待应用命令、窗口到期 `must_stop` |
| POST | `/edge/sites/{site}/sessions/{session}/acks/{seq}` | 确认控制事件 |
| POST | `/edge/sites/{site}/sessions/{session}/sync` | 重连合并离线自治事件（`edge_seq` 单调） |

## 代码结构

```
service.py            入口：--check / 启动（自动 recover）/ health
playout/models.py     领域词汇（场次/放映点/点位/密钥状态、事件与偏差类型）
playout/store.py      SQLite 模式与连接封装
playout/core.py       PlayoutCore：状态机、幂等、窗口强制、离线合并、政策、查询
playout/api.py        标准库 HTTP 路由（/admin 运维 + /edge 边缘协议）
test_playout.py       核心场景测试（含两次事故的回归用例）
test_api.py           HTTP 冒烟测试
domain.json           状态词汇表
```

## 典型流程

```
运维建场 → 配置权利窗口与点位 → 授权（初始密钥批次）
  → 边缘 handshake（窗口/禁播/密钥校验，签发许可+流令牌）
  → 边缘 acks/{permit_seq}（实际开场，记录开场偏差）
  → 周期性 heartbeat（令牌续期、命令下发、窗口强制）
  → 网络中断：边缘自治，恢复后 sync（edge_seq 合并、冲突以中心为准）
  → pause/resume/cut/end 命令经心跳送达，acks 确认
  → end 确认含 exit_completed → 站点「退场完成」
运维随时查 timeline/deviations/dispositions；中心重启自动 recover。
```

关键配置项（`PlayoutCore(config=...)`）：`token_ttl`（默认 60s）、
`offline_after`（15s）、`isolate_after`（120s）、`propagation_slo`（2s）、
`key_grace`（30s）。
