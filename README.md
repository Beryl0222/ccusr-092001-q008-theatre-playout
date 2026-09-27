# 剧场异地同步播控

国家级剧院现场演出同步至多省多城放映点的播控后端。基于场次、放映点、密钥批次与
权利窗口定义，协调**开场握手、心跳、暂停、恢复、紧急切断与结束确认**，支持边缘
短时离线自治、按单调序列合并事件，并在中心服务重启后继续管理进行中的场次。

- 纯 Python 3.11+ 标准库（`http.server` + SQLite WAL），无外部运行时依赖
- 全部控制状态落库，重启即恢复；36 个单元/端到端测试覆盖关键事故场景

## 解决的核心问题

| 事故 | 系统行为 |
| --- | --- |
| 中心网络抖动，边缘重复开场 | 开场握手携带幂等键，`UNIQUE(场次,点,幂等键)` 保证重试只回原命令，绝不二次开场 |
| 重复命令造成二次动作 | 命令为**场次内全局单调序号**，边缘严格按序执行；重连只拉 `after_seq` 之后命令，重复投递就地丢弃 |
| 权利到期后仍能拉流 | 后台巡检 + 开场握手双重校验，到期的进行中会话被**立即强制切断并隔离**，留痕 |
| 边缘短时离线 | 进入「短时离线」自治，边缘按本地序号缓冲事件；重连后整批上报，按 `(会话,seq)` 去重合并 |
| 缓冲事件丢了一段 | 序号缺口 → **隔离会话**，只保留连续前缀，拒绝按错误顺序折叠状态，等待补传与人工解除 |
| 区域禁播 | 未开始点位握手即拒；已开始会话立即紧急切断，均形成处置记录 |
| 场次改期 | 未开始点位授权冻结（拒绝开场）；已开始会话暂停待人工处置；重发窗口并确认后解冻 |
| 密钥轮换 | 旧批次吊销、未开始点位授权即时重绑新批次；进行中会话下发「密钥轮换」命令 |
| 中心重启 | 进行中会话重新纳入管理，命令/事件序号从磁盘续写；启动即跑一次巡检（含到期切断） |
| 排查「某城市实际看到了什么」 | `GET /shows/{id}/timeline/{venue}` 返回命令流/事件流/处置记录合并时间线与偏差解释 |

## 运行

```bash
python3 service.py --check                       # 配置自检
python3 service.py --port 8000 --db playout.db  # 启动（默认每 5s 巡检）
python3 -m unittest discover -v                 # 测试
curl http://127.0.0.1:8000/health
```

## 数据模型（SQLite）

- `shows` 场次（待授权/待开场/播出中/已暂停/已结束/已切断；含改期原因）
- `venues` 放映点（所属区域）、`entitlements` 权利窗口（时间窗 + 密钥批次）
- `key_batches` 密钥批次（待启用/有效/轮换中/已吊销）、`bans` 区域禁令
- `sessions` 每（场次,点）会话：播放/连接状态、`last_event_seq`、`delivered_seq`、隔离信息
- `commands` 中心命令：`(场次,seq)` 全局单调序号、幂等键、待送达/已送达/已确认/已废弃
- `events` 边缘事件：`(场次,点,seq)` 单调序号、发生时间/接收时间、离线缓冲标记
- `dispositions` 处置记录：每个策略动作（禁播/到期/改期/轮换/缺口/恢复…）的审计留痕

## API 速览

### 管理面
```
POST /admin/shows              {show_id,title,starts_at,planned_end_at}
POST /admin/venues             {venue_id,name,region}
POST /admin/shows/{id}/keys    {batch_id?,activate?}
POST /admin/entitlements       {show_id,venue_id,window_start,window_end,key_batch_id?}
POST /admin/entitlements/rebook          # 改期后重发窗口
POST /admin/shows/{id}/reschedule        {new_starts_at,new_end_at,reason}
POST /admin/shows/{id}/confirm-reschedule
POST /admin/shows/{id}/rotate-key        {reason}
POST /admin/shows/{id}/finish            {reason?}
POST /admin/bans               {region,reason,show_id?}
DELETE /admin/bans/{ban_id}
```

### 控制面
```
POST /control/{show}/{venue}                 {cmd:暂停|恢复|切断|结束, reason?}
POST /shows/{id}/emergency-cut               {reason, venue_ids?|region?}
```

### 边缘面（放映点节点调用）
```
POST /edge/{show}/{venue}/handshake   {idempotency_key}
       -> opened | replayed | already_open | already_terminal（403 见授权拒绝原因）
POST /edge/{show}/{venue}/heartbeat
GET  /edge/{show}/{venue}/commands?after_seq=&wait=   # 长轮询，按序返回
POST /edge/{show}/{venue}/events      {events:[{seq,kind,occurred_at,offline_buffered}]}
       -> {merged,duplicates,rejected,quarantined}
POST /edge/{show}/{venue}/reconcile   {last_event_seq,delivered_seq,session_status,...}
```

事件 `kind`：`已开场 / 心跳 / 已暂停 / 已恢复 / 已切断 / 已结束`。

### 查询与运维
```
GET  /shows | /shows/{id} | /shows/{id}/timeline/{venue} | /venues?region=
POST /ops/sweep                                   # 手动触发一次巡检
POST /ops/quarantine/release   {show_id,venue_id,note?}
GET  /health
```

## 典型时序

**正常开场与 ACK**
```
边缘 --handshake(idem=K)--> 中心  校验窗口/密钥/禁令，发 开场 seq=N（待送达）
边缘 --GET commands--------> 中心  返回 seq=N，标记已送达
边缘 --events[已开场 #1]---> 中心  合并事件，seq=N 标记已确认，会话=播出中
```

**中心抖动重试**：同一 `idempotency_key` 再握手 → 返回 `replayed` + 原 seq=N，
库里只有一条开场命令。

**离线自治与重连**
```
心跳超时 → 会话=短时离线（边缘继续本地播控，事件本地缓冲）
边缘重连 --reconcile(delivered_seq=旧)--> 中心补发其后全部命令（可能含暂停/切断）
边缘按序执行 --events[缓冲批次]--> 中心校验序号连续、去重、推进状态并 ACK
若批次中出现序号缺口 → quarantined=true，会话隔离，等待补传 + POST /ops/quarantine/release
```

## 目录

```
store.py        SQLite 表结构、单调序号与幂等唯一约束
controller.py   播控核心：握手/心跳/控制/合并/策略联动/巡检/时间线
api.py          HTTP 路由（管理/控制/边缘/查询/运维）
service.py      进程入口：持久化装配、后台巡检线程、重启恢复
test_controller.py / test_api.py   36 个场景与端到端测试
domain.json     统一领域称谓与一致性规则
```
