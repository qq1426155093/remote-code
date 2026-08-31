# Agent Query 回放设计 v1(事件持久化 + 续传)

## 1. 范围与目标

本文是 AgentService query 回放能力的详细设计:每个 query(一次 turn)获得稳定的
`query_id` 与逐帧 `sequence`;事件在 turn 执行过程中持久化到磁盘;客户端未接收完
response 时,可凭 `query_id` + sequence 续传,从指定 sequence 起继续拿到剩余事件流。

设计基准:remote-code @ `ddadf2b`(agent bridge 已落地);前置设计
[Agent Service 设计 v1](agent-service-design-v1.md)——本文修订其"断开即取消"决策。

### 1.1 已确认的关键决策

| 决策点 | 结论 | 理由 |
|---|---|---|
| 断开语义 | **一律 detach**:turn 一旦发起跑到底,Query 流只是观察窗口 | 与进程/日志语义一致(processes 不因观察者离开而停止);客户端心智最简单 |
| 取消手段 | 新增 `CancelQuery` RPC | 流关闭不再是取消;detached turn 需要显式出口 |
| 事件存储 | **磁盘持久化**(类似进程日志),非内存 | 内存不受长 turn / 大输出胁迫;保留期交给磁盘策略;底层 Claude Code 本就按 `CLAUDE_CONFIG_DIR` 在同一台机器落盘 transcript,未引入新暴露面 |
| 编号语义 | `sequence` 每 query 从 0 连续,合成帧(`session_started`/`completed`)也占号 | 与进程日志 offset 语义对齐(0 起、inclusive) |
| 跨 controller 重启 | **可回放已落定 query;running 标 lost** | agent 子进程随 controller 死、会话全 LOST,续会话是 v2 非目标;但落定事件可查(post-mortem) |

### 1.2 非目标(v1 明确不做)

- 跨重启续会话(`session/load`/`resume`)——沿用 v1 非目标;
- 回放事件的 MCP 暴露(仅 gRPC token);
- 存 prompt 文本本身(v1 已丢弃 `user_message_chunk`;回放方是原客户端,自知发过什么);
- 查询列表/检索 API(`ListQueries` 留待需要时)。

## 2. 语义变化(相对 agent-service-design-v1)

| 维度 | v1 | 本设计 |
|---|---|---|
| 客户端断开 Query 流 | 取消 turn(`session/cancel`) | **不影响 turn**;事件继续落盘,可 `ObserveQuery` 续看 |
| Ctrl-C(CLI) | 关流即取消 | 改调 `CancelQuery` |
| turn 终态 | 流随 turn 结束 | `state.json` 记录 settled(stop_reason 含 cancelled)/lost,可反复回放至保留期 |
| 内存 | 事件总线只转发不囤积(风险 #5) | 活跃 turn 只转发;保留交给磁盘 |

成本语义变化:**没人看的 turn 会跑完**(除非显式取消)。调用方发起即视为授权执行到底。

## 3. 存储

### 3.1 布局

```
<runtime-dir>/agent-events/<query-id>/
  events-<start-seq>.log   # 分段;文件名 = 段内首帧 sequence
  state.json               # 查询状态机与元数据
```

对照 `<runtime-dir>/file-transfers/`(files 服务先例):命名子目录,不占进程
`<uuid>/` 空间。目录与文件 0700/0600(runtime-dir 是操作者空间)。

### 3.2 记录格式(`events-<seq>.log`)

append-only;每记录:

```
uint32 payload_len | uint32 crc32c(payload) | payload
```

payload 为序列化的 `QueryResponse` 帧(含 `query_id`、`sequence`)。段写满
`segment_bytes` 轮转到下一段(新段以当前 sequence 命名)。头部淘汰 = 整段删除,
`earliest_retained` 前移,无原地改写竞争(镜像进程日志分段架构)。

崩溃容忍:缓冲写;轮转、turn 落定、状态迁移时 `Sync`。撕裂尾部记录靠 CRC 截断
(对照 `log_v2.go` 的做法);`state.json` 仍为 running 的查询重启后标 **lost**。

### 3.3 `state.json` 状态机

```
running ──prompt 落定──▶ settled(stop_reason)
   │                        │
   ├──CancelQuery───▶ settled(cancelled)
   │
   └──controller 重启/进程崩溃──▶ lost
```

字段:`format_version`、`query_id`、`session_id`、`working_directory`、`state`、
`stop_reason`、`next_sequence`、`earliest_sequence`、`created_at`、`settled_at`,
以及失败终态的 rpc error(code、reason、message)——回放末尾按原状态错误结束流。

### 3.4 保留与 GC

- 逐 query 字节上限(默认 16MB):超限头部淘汰整段,`earliest_sequence` 前移;
- 总量上限(默认 1GB)与落定后保留时长(默认 7 天):后台周期清理,优先删最旧;
- GC 只删已落定查询;活跃查询只受逐 query 上限约束。

对照进程日志默认值量级(`LogConfig`:64MB/进程、4GB 总量、7 天)。

### 3.5 读取与 live 扇出

- **已落定/lost 查询**:直接从分段文件顺序读,`from_sequence` 定位到所在段;
- **活跃查询**:turn 泵逐帧追加(互斥锁内写 + 入订阅者队列),`ObserveQuery` 先读盘到
  `next_sequence` 快照、再接管订阅队列,无缝续流;观察者上限默认 8(对照日志观察)。

## 4. gRPC API

### 4.1 proto 草案

```protobuf
service AgentService {
  rpc Query(QueryRequest) returns (stream QueryResponse);
  rpc ObserveQuery(ObserveQueryRequest) returns (stream ObserveQueryResponse);
  rpc CancelQuery(CancelQueryRequest) returns (CancelQueryResponse);
  rpc CloseSession(CloseSessionRequest) returns (CloseSessionResponse);
}

message QueryResponse {
  string query_id = 8;   // 本 turn 的稳定 id
  uint64 sequence = 9;   // 0 起逐帧连续
  oneof event { ... }    // 原样不动
}

message ObserveQueryRequest {
  string query_id = 1;
  // Inclusive;0 = 从保留的最早事件起。
  uint64 from_sequence = 2;
  // Follow a running query until it settles; settled queries drain to the end.
  optional bool follow = 3;
}

message ObserveQueryResponse {
  oneof payload {
    AgentQueryHeader header = 1;
    QueryResponse event = 2;      // 复用同一帧类型
    AgentQueryEnd end = 3;
  }
}

message AgentQueryHeader {
  string query_id = 1;
  string session_id = 2;
  AgentQueryState state = 3;             // RUNNING / SETTLED / LOST
  uint64 earliest_sequence = 4;          // 保留窗口
  uint64 snapshot_end_sequence = 5;      // 附着时已落盘的 next
  uint64 resolved_start_sequence = 6;
  bool history_truncated = 7;
  optional string stop_reason = 8;       // 已落定时
  bool follow = 9;
}

message AgentQueryEnd {
  uint64 next_sequence = 1;
  AgentQueryEndReason reason = 2;        // SETTLED / SNAPSHOT_COMPLETE / SHUTDOWN
}
```

失败/lost 的查询**不以 end 帧收尾**:其 `state.json` 持久化了 rpc error
(code、reason、message),回放末尾按原状态错误结束流——取消是
`SETTLED + stop_reason=cancelled`,不是独立的 end reason。

`AgentInfo` 追加 `optional AgentReplayInfo replay = 7`(available、format_version、
max_observers、保留上限),沿用 `file_transfers` 能力协商先例。

### 4.2 错误模型

新增 rpcerror reasons(只加不改;镜像 `pkg/client/reason.go`):

| reason | 场景 |
|---|---|
| `AGENT_QUERY_NOT_FOUND` | query id 未知或记录已删除 |
| `AGENT_QUERY_SEQUENCE_INVALID` | `from_sequence` > `next_sequence`(尚未存在;settled 记录同样校验) |
| `AGENT_QUERY_EVENTS_PRUNED` | `from_sequence` < `earliest_sequence`(已被淘汰) |
| `AGENT_QUERY_OBSERVER_LIMIT` | 活跃查询观察者达到上限(对照日志观察) |
| `AGENT_QUERY_OBSERVER_LAG` | live 观察者落后被踢;从最后收到的 sequence 重新 observe |

流中途查询目录被 GC 删除 → 以 `AGENT_QUERY_NOT_FOUND` 状态错误终止。

## 5. `internal/agent` 改动

- `query_store.go`(新):`QueryStore`——打开/恢复 `<runtime-dir>/agent-events/`,
  逐 query 的 `QueryWriter`(append + 轮转 + 状态迁移 + 订阅扇出)与
  `Open settled`(只读回放),GC 与恢复;
- `service.go`:`StartTurn` 分配 query id(UUID v4,`crypto/rand`),泵产出
  `*QueryResponse` 帧(query_id + sequence 已填)——**一处转换点**,原 Query 流、
  落盘、ObserveQuery 三方共用;emit 失败只放弃该消费者,不再 `beginCancel`;
  `CancelQuery` 复用 beginCancel 路径(幂等:已落定直接成功);
- `session.go`:turn 增加 query 标识与写句柄;`TurnStream.events` 改传 proto 帧;
- `grpc.go`:`ObserveQuery`/`CancelQuery` 适配;`Info` 报 replay 能力;
- `New` 签名改为可失败(恢复扫描),`internal/server` 装配处透传错误。

## 6. 配置

`[agent]` 内新增可选子表(带默认值,v9 内非破坏;对照 `[process_logs]` 命名):

```toml
[agent.events]
max_bytes_per_query = 16777216   # 16MB
max_total_bytes     = 1073741824 # 1GB
segment_bytes       = 1048576    # 1MB
retention_after_settle = "168h"  # 7 天
max_observers       = 8
```

`--check-config` 走 `agent.ValidateConfig`(内含 `ValidateEventLogConfig`
量级校验,对照 `processservice.ValidateLogConfig`)。

## 7. CLI / client

- `pkg/client`:`AgentQuery` 流帧带 `QueryId`/`Sequence`;新增
  `ObserveAgentQuery(ctx, ObserveAgentQueryOptions)` 与 `CancelAgentQuery`;
- CLI 新命令(集中注册 `commandSpec`):
  - `agent-observe <QUERY_ID> [--from N] [--no-follow]`——渲染复用
    `agentEventRenderer`;running 查询默认 follow,`--no-follow` 只排空快照;
    每个命令输出头部一行 `query: <id>` 供回放引用;
  - `agent-cancel <QUERY_ID>`;
- `agent`(query)Ctrl-C 改调 `CancelQuery`,中断后提示
  `cancelled turn <id>; replay with 'agent-observe <id>'`。

## 8. 安全不变量对照

| 不变量 | 落实 |
|---|---|
| 诊断日志不落 prompts/消息内容 | 不变:controllerlog/slog 只记 id 与状态;`agent-events/` 是数据存储不是日志 |
| 用户内容落盘的信任级别 | 与进程日志一致(`LogConfig` 持久化进程 stdout/stderr 同级):runtime-dir 操作者空间、0600、gRPC token 保护、不经 MCP |
| 未新增暴露面 | 底层 claude-agent-acp→Claude Code 本就按 `CLAUDE_CONFIG_DIR` 在同一机器写 transcript |
| 不存凭证 | 事件内容为 agent 观察帧;不含 env 值 |

CLAUDE.md 的"Never log tokens, prompts..."措辞按此收窄为诊断日志范围。

## 9. 测试策略(不需要 Claude 凭证)

- **存储层单测**:记录读写与 CRC、轮转与头部淘汰、状态机迁移、撕裂尾部截断、
  重启恢复(running→lost、settled 可回放)、GC(总量/时长/活跃豁免)、并发订阅;
- **service 单测**(假 agent):detach(断开后 turn 跑完并落盘)、CancelQuery 幂等、
  ObserveQuery live follow / 落定快照 / pruned / not found、并发观察者;
- **集成**:in-process server 全链路(query→断开→observe 续传→cancel);
- 全量 `make test-race`。

## 10. 实施顺序与估算

| 步骤 | 内容 | 估算 |
|---|---|---|
| 1 | proto + rpcerror + 生成 | ~0.5 天 |
| 2 | QueryStore(写/读/恢复/GC)+ 单测 | ~2 天 |
| 3 | service 改造(query id、泵、detach、Cancel/Observe)+ 单测 | ~1.5 天 |
| 4 | 配置 + server 装配 | ~0.5 天 |
| 5 | pkg/client + CLI | ~1 天 |
| 6 | 集成测试 + race + 文档收尾 | ~1 天 |

合计约 6-7 天。

## 11. 风险与开放问题

1. **GC 与读取竞争**:读取中目录被删 → `AGENT_QUERY_NOT_FOUND` 终止,客户端可重试
   (数据确已过期);
2. **磁盘占用**:依赖默认上限 + 操作者可调;`GetInfo` 暴露上限供监控;
3. **观察者落干**(live follow 落后于头部淘汰):v1 单查询段淘汰只发生在逐 query
   上限触顶,超大概率不会发生在 follow 窗口内;若发生,订阅者收到 pruned 错误;
4. **detach 成本**:无人消费的 turn 跑完——文档明示,CLI 提示可 `agent-cancel`。

## 12. 实现修订记录(2026-08-31)

按本设计落地(`feat(agent): persist turns and serve replay over ObserveQuery`
等提交)时确认的偏差:

1. **失败终态不走 end 帧**:end reason 只有 `SETTLED` / `SNAPSHOT_COMPLETE` /
   `SHUTDOWN`;失败/lost 查询以持久化的 rpc 状态错误结束回放流(§4.1 已改)。
2. **lost 恢复补终态错误**:controller 重启把 running 记录标 lost 时,合成
   `AGENT_PROCESS_LOST` 终态错误,回放以状态错误收尾而不是静默截断。
3. **settled 记录同样校验 `from_sequence`**:Attach 只对活跃 writer 锁定
   `next_sequence`;service 层对快照统一校验,越界一律
   `AGENT_QUERY_SEQUENCE_INVALID`(此前 settled 路径会静默回放空窗口)。
4. **turn 槽位先于落盘释放**:结果一到就 `endTurn`,记录 Settle(fsync)在
   后面慢慢做——客户端收到 `completed` 帧立即复用/关闭会话不会被
   `AGENT_TURN_ACTIVE` 拒绝。
5. **新增观察者 reasons**:`AGENT_QUERY_OBSERVER_LIMIT` / `AGENT_QUERY_OBSERVER_LAG`
   (§4.2 已补)。
6. **CLI 默认 follow**:`agent-observe` 对 running 查询默认续流,`--no-follow`
   排空快照即止;`agent` 命令每 turn 打印 `query: <id>` 供回放引用。
