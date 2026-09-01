# Agent Service 设计 v1(claude-agent-acp 桥接)

## 1. 范围与目标

本文是 AgentService 的详细设计:controller 通过社区 Go SDK
[`coder/acp-go-sdk`](https://github.com/coder/acp-go-sdk) 以 ACP 客户端身份启动并驱动
`claude-agent-acp` 子进程,向 gRPC 调用方提供流式问答能力。

设计基准:remote-code @ `62fe13a`;acp-go-sdk v0.13.5(Apache-2.0,零第三方依赖,
`go.mod` 仅 `go 1.21`);claude-agent-acp v0.70.0(依赖 `@agentclientprotocol/sdk`
1.3.0,ACP 协议 v1 稳定版)。协议面调研见
[ACP 客户端迁移方案](research/acp-client-migration-plan.md),agent 行为分析见
[claude-agent-acp 源码分析](research/claude-agent-acp-analysis.md)。

### 1.1 已确认的关键决策

| 决策点 | 结论 | 理由 |
|---|---|---|
| Query RPC 形态 | **服务端流式** `Query(QueryRequest) returns (stream QueryResponse)` | 贴合 ACP turn 模型;~~客户端断开流即取消 turn~~(修订:断开一律 detach,取消走 `CancelQuery`) |
| 权限审批 | **自动放行**(选 `allow_once` 类选项) | gRPC token 持有者本可直接 `StartProcess` 执行任意命令,审批在该层只是 UX 不是隔离;与既有开放信任模型一致 |
| 进程模型 | **单进程共享**:一个 claude-agent-acp 子进程承载全部会话 | 资源占用低(Node 常驻数百 MB),与 Zed 等编辑器用法一致;崩溃重启,会话标 LOST |
| 进程承载 | **复用进程注册表**(新增内部 raw-pipe 入口) | 统一可观测性(记录/日志/LOST 语义);协议 stdout 不落盘 |
| 协议实现 | 直接引用 `coder/acp-go-sdk`(非 vendor) | 零依赖、客户端角色一等公民、有 claude 对接示例;版本进 go.mod |

### 1.2 非目标(v1 明确不做)

- 交互式权限审批(流内事件 + 独立应答)——自动放行已覆盖信任模型,留 v2;
- elicitation、steering、`session/load`/`resume` 跨重启续会话;
- 多模态 ContentBlock 输入(图片/音频)、`session/set_mode`(plan 模式);
- 每会话/每工作区独立 agent 进程;
- MCP server 注入(`session/new.mcpServers`)。

## 2. 总体架构

```
CLI / 外部 gRPC 客户端(bearer token)
  │
  ▼
AgentService(internal/agent/service.go)──── 新 gRPC 服务,internal/server 装配
  │  Query(stream) / CloseSession
  ▼
internal/agent 会话层
  ├─ process.go    懒启动共享 agent 进程;崩溃检测与重启;Initialize 能力快照
  ├─ session.go    sessionId → 会话状态(事件总线 + turn 状态机)
  └─ client.go     实现 acp.Client(9 方法):权限自动放行、SessionUpdate 按 session 扇出
  │
  │  acp-go-sdk: acp.NewClientSideConnection(client, stdin, stdout)
  ▼
internal/process 注册表(新增内部 StartRawProcess)──── stdout 独占管道,不进分段日志
  │
  ▼
claude-agent-acp 子进程(stderr → 现有分段日志,可经 ObserveProcessLogs 观测)
```

初始化链(首次 Query 懒触发):spawn 进程 → `Initialize`(不广告 fs/terminal/elicitation
能力)→ 缓存能力快照 → `NewSession` → `session/prompt`。SDK 调用面(与
`example/claude-code/main.go` 一致):

```go
conn := acp.NewClientSideConnection(client, stdin, stdout)
initResp, err := conn.Initialize(ctx, acp.InitializeRequest{
    ProtocolVersion:    acp.ProtocolVersionNumber,
    ClientCapabilities: acp.ClientCapabilities{}, // 全部能力不广告
})
sess, err := conn.NewSession(ctx, acp.NewSessionRequest{Cwd: wsRoot, McpServers: []acp.McpServer{}})
_, err = conn.Prompt(ctx, acp.PromptRequest{SessionId: id, Prompt: []acp.ContentBlock{acp.TextBlock(prompt)}})
err = conn.Cancel(ctx, acp.CancelNotification{SessionId: id})
```

## 3. `internal/process`:raw-pipe 入口

唯一的现有包改动,内部 API,**不经 gRPC 暴露**。

### 3.1 接口

```go
type RawProcessSpec struct {
    Name             string            // 走既有命名校验
    Command          string
    Arguments        []string
    WorkingDirectory string            // workspace 内相对路径,复用 cleanWorkingDirectory
    Environment      map[string]string // 复用 buildEnvironment(继承 os.Environ + overrides)
}

type RawProcess struct {
    Stdin  io.WriteCloser // 协议写入端,调用方独占
    Stdout io.ReadCloser  // 协议读取端,不接分段日志 writer
    // 进程句柄:复用注册表记录(uuid、状态、收割、信号)
}

func (s *Service) StartRawProcess(ctx context.Context, spec RawProcessSpec) (*RawProcess, error)
```

### 3.2 语义

- 复用不变:校验与限额(argv/env 尺寸、NUL 拒绝)、`os.Root` 打开 cwd、`Setpgid`
  独立进程组、退出收割 goroutine 写 status、`<runtime-dir>/<uuid>/` 记录目录、
  controller 重启后 LOST 判定;
- 差异仅一处:PIPE 模式下 stdout 接管道而非日志 writer(对照
  `runner_unix.go:74-75` 现状);stderr 仍走日志;
- **stdin 防附着**:raw 进程对 gRPC `StreamProcessInput` 一律拒绝,新增 rpcerror
  reason `PROCESS_INPUT_RAW`——否则持有 token 的客户端可向协议流注入任意字节行,
  破坏 JSON-RPC 会话(虽然 token=RCE,但不该允许无意义地破坏共享 agent 进程,
  它承载着其他会话);
- `StartProcess`(gRPC 面)行为零变化;`SignalProcess`/`ObserveProcessLogs` 对
  raw 进程照常可用(前者是合法的运维手段,后者只见 stderr)。

### 3.3 安全不变量对照

| 不变量 | 落实 |
|---|---|
| Never log prompts / 上传内容 | 协议 stdout 为内存管道,不落盘;`<runtime-dir>/<uuid>/` 日志只有 stderr 诊断 |
| env 只存 key 不存值 | 沿用 `EnvironmentKeys`(`record.go`),凭证经 `buildEnvironment` 的 `os.Environ` 继承或 `[agent.environment]`(操作者配置文件,不进任何记录) |
| 退出/LOST 可观测 | 复用注册表 status/记录语义 |

## 4. `internal/agent` 包

```
internal/agent/
  service.go   AgentService gRPC 实现;进程与 会话的装配门面
  process.go   agentProcess:Start(懒)→ Initialize → 能力快照;Done 监控;Restart
  session.go   session:ACP sessionId、事件总线(订阅 channel)、turn 状态机
  client.go    acpClient:实现 acp.Client 9 方法
  events.go    ACP SessionNotification → QueryResponse 事件映射
```

### 4.1 进程管理(process.go)

- **懒启动**:首个 Query 触发 `StartRawProcess` + `Initialize`;失败映射
  `AGENT_START_FAILED`(含 stderr 摘要,不含 env 值)。
- **崩溃检测**:注册表 done/退出回调 → 全部会话标 LOST → 在途 Query 流以
  `AGENT_PROCESS_LOST` 终止(末帧带 exit code/signal)。
- **重启策略**:下次 Query 自动重新 spawn + Initialize(全新进程);旧 session_id
  一律 `AGENT_SESSION_LOST`。不做退避重试(调用方驱动即天然重试)。
- **关停**:controller `Shutdown` 序列中,先对所有活跃 turn 发 `session/cancel`,
  等 turn 落定(上限 5s)→ 关 agent stdin(claude-agent-acp 在 stdin EOF 时干净退出)
  → 超时 SIGTERM → 再超时 SIGKILL(对照 simple-client.ts 关停顺序)。

### 4.2 会话与 turn(session.go)

- 会话表 `map[acpSessionID]*session`,创建于 Query(`session_id` 为空)时,
  `NewSession` 的 cwd 取 workspace 根,或请求内 `working_directory`(复用 workspace
  相对路径校验);首帧 `session_started{session_id}` 下发供复用。
- **每会话单活跃 turn**:并发 Query 同一会话返回 `AGENT_TURN_ACTIVE`;不同会话可
  并行(agent 支持多会话)。SDK 连接的并发出站请求安全性以 race 测试验证。
- **turn 生命周期**:`Prompt` 在独立 ctx 上等待(不随客户端流 ctx 取消);
  SessionUpdate 通知按 sessionId 扇出到事件总线 → 活跃 Query 流转发;`Prompt` 返回
  后发末帧 `TurnCompleted{stop_reason}` 并结束流。
- **取消**(**已被 query 回放设计修订,见下**):客户端断开 gRPC 流 → 发
  `session/cancel` 通知 → 等 `Prompt` 以 `stopReason=cancelled` 落地(上限 30s,
  超时则记孤儿 turn 并强制终止流——对照 claude-agent-acp 自身的 30s 强杀兜底);
  协议要求 cancel 后到达的 update 仍要消费。
  > **修订(2026-08-31)**:断开改为 **一律 detach**——turn 跑到底,事件逐帧落盘
  > 可回放;显式取消走新的 `CancelQuery` RPC(内部仍是本条的 cancel 路径)。
  > 详见 [Agent Query 回放设计 v1](agent-query-replay-design-v1.md)。
- **CloseSession**:rpc `CloseSession` → 若能力快照含 `sessionCapabilities.close`
  (claude-agent-acp 有此能力)则调 SDK `CloseSession`(方法 `session/close`,
  `client_gen.go:267`)并移除本地会话表项;未广告该能力的 agent 仅做本地清理。
  有活跃 turn 的会话先拒绝(`AGENT_TURN_ACTIVE`)。

### 4.3 acp.Client 实现(client.go)

| 方法 | 行为 |
|---|---|
| `RequestPermission` | 自动放行:优先 `Kind == allow_once`,次选 `allow_always`,否则首个选项;无选项回 `cancelled`(协议要求 cancel 语义兜底)。**按 kind 挑、绝不硬编码 optionId**(ExitPlanMode 用权限模式名当 optionId) |
| `SessionUpdate` | 按 `params.SessionId` 扇出到会话事件总线 |
| `ReadTextFile`/`WriteTextFile` | 返回错误(未广告 fs 能力,正常不会收到) |
| 5 个 terminal 方法 | 存根返回错误(未广告 terminal 能力,不会收到) |

## 5. gRPC API

### 5.1 proto 草案

```protobuf
service AgentService {
  rpc Query(QueryRequest) returns (stream QueryResponse);
  rpc CloseSession(CloseSessionRequest) returns (CloseSessionResponse);
}

message QueryRequest {
  string prompt = 1;                      // 非空;MVP 纯文本
  optional string session_id = 2;         // 空 = 新建会话
  optional string working_directory = 3;  // 仅新建会话时有效;workspace 内相对路径
}

message QueryResponse {
  oneof event {
    SessionStarted session_started = 1;   // 首帧(新建时);含 session_id
    AgentMessage  message        = 2;     // agent 文本 chunk
    AgentThought  thought        = 3;     // 思考 chunk
    ToolCallEvent tool_call      = 4;     // 工具调用创建/状态更新
    PlanUpdate    plan           = 5;
    UsageUpdate   usage          = 6;     // token 计量
    TurnCompleted completed      = 7;     // 末帧:stop_reason + 错误详情
  }
}
```

`GetInfoResponse` 追加 `optional AgentInfo agent = 10;`(enabled、能力快照、进程
状态),沿用 `file_transfers` 能力协商先例,追加字段 v1 内非破坏。

### 5.2 sessionUpdate 映射(11 种)

| ACP update | QueryResponse 事件 |
|---|---|
| `agent_message_chunk` | `message`(text block) |
| `agent_thought_chunk` | `thought` |
| `tool_call` / `tool_call_update` | `tool_call`(含状态字段) |
| `plan` | `plan` |
| `usage_update` | `usage` |
| `user_message_chunk` | 丢弃(客户端已知自己发了什么) |
| `available_commands_update` / `current_mode_update` / `config_option_update` / `session_info_update` | v1 丢弃(对应功能未开放;`session_info_update` 中 agent 侧会话 id 变化时更新本地记录) |

### 5.3 错误模型

rpcerror reasons(只加不改惯例;客户端镜像 `pkg/client/reason.go`):
`AGENT_DISABLED`、`AGENT_START_FAILED`、`AGENT_SESSION_NOT_FOUND`、
`AGENT_SESSION_LOST`、`AGENT_TURN_ACTIVE`、`AGENT_PROCESS_LOST`、
`PROCESS_INPUT_RAW`。

SDK 的 `*acp.RequestError`(JSON-RPC 错误)映射为 gRPC `Unknown`,reason 透传原始
code/message,便于上层诊断(如未登录时的 authRequired)。

## 6. 配置

```toml
[agent]
enabled  = true                                  # 懒启动、不绑端口;默认开,可关
command  = "npx"                                 # PATH 解析;离线部署换绝对路径
arguments = ["@agentclientprotocol/claude-agent-acp"]
# environment = { ... }                          # 可选;凭证建议走 controller 进程
#                                                # 环境继承,而非写进配置
```

- TOML schema 版本 **v8 → v9**:新增 `controllerConfigVersionV9`,并按 workflows
  表先例(`cmd/controller/config.go:321-322`)门控——`version < 9` 的配置出现
  `[agent]` 表时报错 "controller config version %d does not support the agent
  table";
- `--check-config` / `Prepare()` 校验表结构(enabled=false 时其余字段可省);
  command 存在性不强制校验(懒启动,失败时报 `AGENT_START_FAILED` 即可);
- `internal/server`:Config 增字段 → Prepared 增 agent service → Shutdown 插入
  agent 关停(在进程注册表关停之前)。

## 7. 测试策略(不需要 Claude 凭证)

- **假 agent**:测试内用 acp-go-sdk 的 **agent 侧连接**(`agent_gen.go`,
  `NewAgentSideConnection`)经 `io.Pipe` 扮演 claude-agent-acp,脚本化:能力协商、
  update 序列、权限请求(验证自动放行的选项选择)、慢 turn、立即崩溃;
- **单元**:internal/agent 全路径(turn 生命周期、取消、崩溃、并发拒绝)+
  internal/process raw 入口(生命周期、防附着、stdout 大帧、`make test-race`);
- **集成**:`*_integration_test.go` 起 in-process server,`[agent].command` 指向
  假 agent 测试二进制,验证 gRPC 全链路;
- **端到端(可选)**:环境变量门控,对真实 `npx @agentclientprotocol/claude-agent-acp`
  跑一轮对话(风格对照该仓库 `RUN_INTEGRATION_TESTS` 惯例)。

## 8. 实施顺序与估算

| 步骤 | 内容 | 估算 |
|---|---|---|
| 1 | `go get github.com/coder/acp-go-sdk@v0.13.5` + raw-pipe 入口 + 测试 | ~2 天 |
| 2 | internal/agent(进程/会话/turn/client)+ 单测 | ~2 天 |
| 3 | proto + rpcerror + server 装配 + 配置 + 集成测试 | ~1 天 |
| 4 | pkg/client AgentSession + CLI 命令(`agent-query`/`agent-close`) | ~1 天 |

合计约 5-6 天出 MVP。

## 9. 风险与开放问题

1. **acp-go-sdk v0.x 无稳定性承诺**(最近更新 2026-06):API 变化风险由 go.mod
   锁定版本 + 薄封装(internal/agent 是唯一 import 点)控制;
2. **SDK 并发出站请求**:多会话并发 Prompt 依赖连接层并发安全,以 race 测试 +
   必要时在 process.go 串行化出站调用兜底;
3. **claude-agent-acp 私有扩展漂移**(steering/`_meta` 形状):v1 不依赖任何私有
   扩展,仅用稳定协议面;
4. **审批超时**:agent 侧权限请求在自动放行下即时应答,无挂起风险;若未来改交互式,
   需补超时与 cancel 联动;
5. **长期会话的内存增长**:事件总线仅转发不囤积(无订阅者时丢弃),turn 结束即释放。

## 10. 实现修订记录（2026-09-01）

- `AgentService` 新增分页 `ListSessions`，只返回当前 Agent 进程 generation 内可被 `Query.session_id`
  复用的 session；状态为 `IDLE` 或 `RUNNING`，运行态同时报告 `active_query_id`。
- session 快照补充 workspace-absolute 展示路径、generation、创建时间和最近活动时间。ACP 子进程崩溃、
  Controller 重启或 `CloseSession` 后记录从列表消失；不把 query 历史聚合成不可复用的伪 session。
- ACP 自身的 `session/list` 仍未直接暴露：它列出的持久化会话需要先实现 `LoadSession` 才能安全复用，
  与当前 generation 内 session 列表是不同契约。
- `AgentInfo.listing` 发布 query/session listing 与分页大小能力；CLI 对应
  `agent-queries` / `agent-sessions`。
