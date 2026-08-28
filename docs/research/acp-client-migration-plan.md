# 在 remote-code 中实现 ACP 客户端:与 claude-agent-acp 通讯的迁移方案

> 状态:设计提案(未实现)。基准:remote-code @ `62fe13a`;协议仓库
> `temp/agent-client-protocol` @ `d0370de`(ACP 协议 v1 稳定版);agent 参考实现
> `temp/claude-agent-acp` @ `14d192d`(v0.70.0,依赖 `@agentclientprotocol/sdk` 1.3.0)。
> 姊妹文档:[claude-agent-acp 源码分析](./claude-agent-acp-analysis.md)(下称"分析文档")。
> 本文行号引用:无前缀 = 对应 temp/ 仓库;`remote-code 内部/...` = 本仓库。

## 0. 背景与术语澄清

**agent-client-protocol 仓库里没有可直接"搬运"的 Go 代码。** 它是协议定义仓库:JSON
Schema(`schema/v1/schema.json`,draft 2020-12,由 Rust crate `agent-client-protocol-schema`
经 schemars 生成)+ 协议文档(`docs/protocol/v1/`)+ 官方 SDK 链接(Rust/TS/Python/Kotlin/
Java,**没有 Go**)。见仓库 README "Official Libraries" 一节。

因此"把 client 部分迁移到本项目"的真实含义是:**在 remote-code(Go)中实现 ACP v1
的客户端角色**,以子进程方式驱动 claude-agent-acp,经 stdio NDJSON JSON-RPC 通讯,并把
turn/更新/权限审批桥接到本项目的 gRPC API。协议仓库的价值是**权威规范**(`x-side`/
`x-method` 注解机器可读),claude-agent-acp 的价值是**参考实现 + 对接目标**。

## 1. 结论先行(TL;DR)

| 决策点 | 结论 |
|---|---|
| Go 协议实现 | **vendor 社区 SDK `coder/acp-go-sdk`(Apache-2.0,零第三方依赖,客户端角色一等公民)**,自有状态机放在薄封装里,保留替换为手写子集的能力(§3) |
| 协议版本 | 锚定 **v1 稳定 schema**;v2 仍是 Draft(2026-07-20 公告),不投入 |
| 进程承载 | 复用**进程模板**启动 claude-agent-acp(argv 消毒 + revision);但协议 stdio **不走现有日志管道**——需给 `internal/process` 新增内部 raw-pipe 入口(§4.1,唯一的硬改动) |
| gRPC 面 | 新增独立 **`AcpService`**(与 ProcessService 正交);`GetInfoResponse` 追加能力字段(追加 field,v1 内非破坏) |
| 权限审批 | server-streaming 事件流 + 一元应答,照抄日志流的 Header/checkpoint 重放模式,不轮询(§5.3) |
| 包结构 | 新增 `internal/acp/`(transport / agent / permission / bridge / record / service),装配与关停挂进 `internal/server`(§5.1) |
| 工作量预估 | 7 个阶段,核心是 Phase 1–4;最小可用(无 elicitation、无 resume)约一个人 1.5–2 周 |

## 2. 需要实现什么:ACP v1 客户端面

以下全部由本地 `schema/v1/schema.json` 的 `x-side`/`x-method` 注解直接枚举核对(方法
名去重后),非文档转述。

### 2.1 方法清单(按方向)

**client→agent(x-side=agent,客户端调用)— 12 个请求 + 1 个通知:**

| 方法 | 说明 | 门控 |
|---|---|---|
| `initialize` | 握手 + 能力协商 | 基线必选 |
| `session/new` | 建会话(`{cwd, mcpServers}`,`mcpServers` 是 required,schema.json:4772) | 基线必选 |
| `session/prompt` | 发起 turn,响应带 `stopReason` | 基线必选 |
| `session/cancel` | **通知**(非请求),取消运行中 turn | 基线必选 |
| `session/load` | 恢复历史会话 | `agentCapabilities.loadSession` |
| `session/set_mode` | 切换模式(plan/default 等) | 模式能力 |
| `session/set_config_option` | 会话级配置项 | `session.configOptions.boolean` |
| `session/list` / `session/delete` | 会话枚举/删除 | `sessionCapabilities.list/delete` |
| `session/resume` / `session/close` | 挂起/关闭会话 | `sessionCapabilities.resume/close` |
| `authenticate` / `logout` | 登录/登出 | `auth` 相关能力 |

**agent→client(x-side=client,客户端必须实现并应答)— 9 个请求 + 2 个通知:**

| 方法 | 说明 | 门控 |
|---|---|---|
| `session/request_permission` | **客户端基线唯一必选请求**;响应 `outcome: selected{optionId} \| cancelled`(schema.json:342/:5414) | 无 |
| `fs/read_text_file` / `fs/write_text_file` | 客户端文件 IO(claude-agent-acp 自己做 IO,**从不调用**,回存根即可) | `fs.*` 能力 |
| `terminal/create|output|release|wait_for_exit|kill` | 交互终端 | `terminal` 能力 |
| `elicitation/create` | 表单/URL 征询(不广告则 agent 禁用 AskUserQuestion 工具) | `elicitation.form/url` |
| 通知 `session/update` | turn 内流式更新,11 种 `sessionUpdate`:user/agent_message_chunk、agent_thought_chunk、tool_call、tool_call_update、plan、available_commands_update、current_mode_update、config_option_update、session_info_update、usage_update | 无 |
| 通知 `elicitation/complete` | URL 模式带外完成 | elicitation.url |

协议级:`$/cancel_request` 通知双向可用,收到后原请求须以 `-32800` 或部分结果应答。

`session/prompt` 响应的 `stopReason ∈ {end_turn, max_tokens, max_turn_requests, refusal,
cancelled}`(schema.json `StopReason` def,已逐一核对)。

### 2.2 握手与能力协商

- `protocolVersion` 是 uint16 **主版本号**:client 发自己支持的最新版;agent 支持则原样
  回,否则回自己最新版;client 不认识 agent 回的版本 SHOULD 断连(`docs/protocol/v1/initialization.mdx`)。当前 = 1。
- `clientCapabilities`(schema.json:4473):`fs{readTextFile,writeTextFile}`、`terminal`、
  `auth{terminal}`、`session.configOptions.boolean`、`elicitation{form,url}`。**省略一律视为
  不支持**;`elicitation: {}` 不等于支持 form。
- `agentCapabilities`(schema.json:2410):`loadSession`、`promptCapabilities`、
  `mcpCapabilities`、`auth{logout}`、`sessionCapabilities{list,delete,
  additionalDirectories,resume,close}`。调可选方法前 MUST 先查能力
  (`docs/protocol/v1/session-setup.mdx`)。
- `_meta` 惯例(`docs/protocol/v1/extensibility.mdx`):所有类型(含嵌套)都带
  `_meta: object`;能力对象内的 `_meta` 用于广告扩展;未识别的 `_` 前缀请求必须回
  `-32601`,未识别通知忽略。
- **claude-agent-acp 的私有扩展**(不在 unstable schema 中,靠 initialize 应答广告):
  - `agentCapabilities._meta.claudeCode.promptQueueing=true`(`src/acp-agent.ts:1706-1710`):
    可并发发 `session/prompt` 由 agent 排队;
  - **steering 在 initialize 应答的顶层 `_meta.steering.supported=true`**(`src/acp-agent.ts:1741-1748`,
    是 `agentCapabilities` 的兄弟字段):私有方法 `_session/steering {sessionId, prompt}` →
    `{outcome: injected|startedNewTurn}`,向运行中 turn 注入消息。

### 2.3 传输:JSON-RPC 2.0 over NDJSON stdio

`docs/protocol/v1/transports.mdx`:UTF-8;client 把 agent 作为子进程拉起;消息以 `\n`
分隔且**不得含内嵌换行**;agent MUST NOT 向 stdout 写任何非 ACP 消息,client 对 stdin
同理;stderr 仅作日志,client MAY 采集。claude-agent-acp 为此把 `console.log/info/warn/debug`
全部重定向到 stderr(`src/index.ts:57-60`),并在 stdin EOF 时干净退出(`src/index.ts:97-100`)。
`requestId` = `null|int64|string`,响应必须回同值。协议无请求超时——超时纯属客户端策略。

### 2.4 驱动 claude-agent-acp 的最小完整客户端清单

对照 `examples/simple-client.ts` 逐条归纳(这是迁移后 Go 侧必须复刻的行为契约):

1. spawn agent(pipe stdin/stdout),stderr 单独泵到日志;
2. 注册回调再连接:`request_permission`、fs 存根、`session/update`;
   权限应答**按 `kind` 挑选项、绝不硬编码 optionId**(ExitPlanMode 用权限模式名当
   optionId,`simple-client.ts:370-378`);
3. `initialize`(protocolVersion=1 + clientCapabilities)→ 读回能力;
4. `session/new {cwd, mcpServers: []}`;
5. turn 状态机:prompt 挂起期间收 update 流;并发 prompt 仅当 agent 广告
   promptQueueing,否则本地排队;
6. cancel 语义:发 `session/cancel` 通知;MUST 以 `cancelled` outcome 应答所有挂起的
   request_permission,继续接受 cancel 之后到达的 update(`docs/protocol/v1/prompt-turn.mdx`);
7. fs 存根(或干脆不广告 fs 能力);
8. elicitation 可选(第一版不做);
9. **关停顺序**:先 cancel 运行中 turn → 等落定(有上限)→ 关 agent stdin(触发其
   干净退出)→ 超时 SIGTERM/SIGKILL(`simple-client.ts:631-675`);
10. 孤儿 turn 与缓存重放的去重防御(分析文档 §3/§17,Go 侧同样要做)。

## 3. Go 实现路线评估

调研时点 2026-08-28;社区库列表见 `docs/libraries/community.mdx`。

| 路线 | 代表 | 优点 | 缺点 | 结论 |
|---|---|---|---|---|
| ① 社区 SDK | `coder/acp-go-sdk` | 零第三方依赖(go.mod 仅 go 1.21,与本项目 Go 1.26 兼容);客户端角色一等公民:`NewClientSideConnection(Client, io.Writer, io.Reader)` + 类型化出站方法;内置 stdio NDJSON、`$/cancel_request`、per-request goroutine;v1 稳定+不稳定方法全覆盖;有 example/claude-code;pkg.go.dev 收录、约 222 个包导入(docker、pomerium 等) | v0.x 无稳定性承诺;最近提交 2026-06-05(v0.13.5),约 3 个月无更新 | **推荐主路线,并 vendor 进仓库**锁定版本 |
| ② schema 生成 | quicktype 等 | 类型与 schema 同步 | 只得类型不得连接/状态机;schema 的邻接标签联合(`anyOf[{tag const} + allOf[$ref]]`,schemars 风格)让 quicktype 生成质量差(glideapps/quicktype#493);Go 无 sum type,仍要手写 `UnmarshalJSON` 按标签分发 | 不单独走;`x-side`/`x-method` 可作为自写小型生成器的输入,留作 SDK 退化时的备份 |
| ③ 纯手写 | — | 驱动 claude-agent-acp 仅需约 15 个消息类型 + 2 个回调接口,完全可控 | 连接层、取消、能力协商全要自己写与测 | 作为 ① 的 fallback;薄封装设计使替换成本可控 |

备选库:`ironpark/acp-go`(MIT,30 stars,2026-04 后停更)、`eino-contrib/acp`
(CloudWeGo,活跃但**未检出 LICENSE,复用前须核实**)。

**落地原则:自己的 turn/权限/队列状态机放在 `internal/acp` 薄封装里,不渗进 SDK
类型;SDK 只出现在 transport/agent 两个文件的边界上。**

## 4. remote-code 侧现状与缺口

### 4.1 核心缺口:PIPE 进程没有独占 stdout(必须改 `internal/process`)

现状(remote-code 内部):

- spawn 时 PIPE 模式的 stdout/stderr **直接接分段日志 writer,不是可读管道**
  (`internal/process/runner_unix.go:74-75`);`StdinPipe` 仅在 `input_mode=MANAGED` 时
  保留(`runner_unix.go:89-95`)。
- 读输出只有 `ObserveProcessLogs`(64KiB 二进制分帧落盘 + 行索引 + retention 裁剪,
  `internal/process/log_v2.go`)。
- 写 stdin 已可用:`StreamProcessInput` 双向流,独占 writer(`input_service.go:191-196`),
  严格 sequence、≤64KiB/帧,`CloseInput` 收尾。

**协议流不能走日志路径**,原因:(a) 全部协议帧(prompt 内容、agent 回复)持久化到
`<runtime-dir>/<uuid>/`,违背"Never log prompts"安全不变量的精神;(b) 64KiB 分帧 +
行索引开销;(c) janitor 按 retention 裁剪会中断长 follow。

**改法(提案)**:给 `internal/process` 新增**内部**构造入口(不经 gRPC 暴露),如
`StartRawProcess(spec) (*RawProcess, error)`——保留进程组(`Setpgid`)、退出收割、
LOST 判定、记录目录,但 stdin/stdout 返回 `io.ReadWriteCloser`,仅 stderr 接现有日志。
模板启动路径同样支持(`StartRawProcessFromTemplate`)。这是唯一动安全敏感包的改动,
需要按 CLAUDE.md 惯例配套表驱动测试(lifecycle/取消/并发)。

### 4.2 模板系统承载 agent 进程(复用,基本不改)

- 模板定义 `*.process-template.yaml` 必须**在 workspace 外**(`template_loader.go:36-80`);
  render 是纯 Expr,输出只允许 `{arguments, working_directory, environment}`
  (`template.go:576-611`);revision = 定义 canonical JSON 的 SHA-256,`ProcessInfo`
  经 `redactArguments` 只暴露模板名 + revision(`template_service.go:50-73`)。
- claude-agent-acp 模板草案:

  ```yaml
  # configs/process-templates/acp.process-template.yaml(workspace 外)
  version: 1
  language: expr
  templates:
    - name: claude-agent-acp
      description: ACP adapter for the Claude Agent SDK
      parameters_schema: {type: object, properties: {}, additionalProperties: false}
      command: {path: npx, arguments: ["@agentclientprotocol/claude-agent-acp"]}
      io: {mode: pipe, input: managed}   # 提案:raw 入口要求 pipe+managed
  ```

- **凭证不进模板参数**:`ANTHROPIC_API_KEY` 等放 `[process_templates.extra_parameters]`
  (操作者级,config 文件)或依赖 controller 进程环境继承(`buildEnvironment` 会合并
  `os.Environ()`,`internal/process/service.go:769-817`)。env 值**确认不持久化**——
  `metadata.json` 只存 `EnvironmentKeys`(`record.go:29-42`),与安全不变量一致。
- `command` 走 PATH 解析;npx 首次拉包需要网络与 npm 缓存目录,离线部署需预装
  (`npm i -g` 后 command 直接指 `claude-agent-acp` bin)。

### 4.3 装配与生命周期先例

- `Prepare()` 校验 + 编译(模板/MCP/workflow)不绑端口(`internal/server/server.go:84-130`,
  即 `--check-config`);`NewPrepared*()` 逐级装配,失败回滚(`server.go:133-255`);
  `Shutdown` 有既定顺序(`server.go:321-387`)。
- 桥接外部协议的先例是 `internal/mcp`:host **进程内**直接持有
  `*processservice.Service` 引用调用(`internal/mcp/host_controller.go:18-25`),而非
  gRPC loopback。ACP 与之结构不同:MCP 是被动 HTTP server,ACP 是**主动 spawn 子进程
  的 client**,生命周期更像 workflow(后台 goroutine + BeginShutdown/Close 挂进关停
  序列)。
- 新增子系统要动:`internal/server/server.go`(Config/Prepared/Shutdown)、
  `cmd/controller/config.go`(TOML 表 + schema-version gating,先例 `config.go:306-310`)、
  proto + rpcerror、pkg/client、`internal/cli/command.go`。

### 4.4 gRPC / CLI 扩展惯例

- 服务切分:Controller/File/Process 三服务;模板类 RPC 挂 ProcessService。
  **建议 ACP 用独立 `AcpService`**:turn/权限语义与进程管理正交,也避免 ProcessService
  膨胀;进程 spawn 仍复用模板机制。
- 流程:改 `api/remote/code/v1/remote_code.proto` → `make generate`(固定插件在
  `.tools/bin`)→ 提交生成物。v1 内只加字段/方法不改号,非破坏;能力协商先例
  `GetInfoResponse.file_transfers`(`proto:48-59`,装配于 `server.go:403-414`),ACP 加
  `acp` 能力字段即追加 field。
- rpcerror:Domain `remote.code.v1`,SCREAMING_SNAKE,按域分组注释,**只加不改不重用**;
  客户端镜像在 `pkg/client/reason.go`。ACP 新 reason 草案:
  `ACP_AGENT_NOT_RUNNING`、`ACP_SESSION_NOT_FOUND`、`ACP_SESSION_LOST`、
  `ACP_TURN_NOT_ACTIVE`、`ACP_PERMISSION_REQUEST_NOT_PENDING`、
  `ACP_PERMISSION_OPTION_INVALID`。
- CLI:命令集中注册为 `commandSpec`(`internal/cli/command.go:137-183`),新命令 = 加
  一行 + REPL 方法文件(参照 `internal/cli/template.go` 的 exec-template 流程)。

## 5. 目标架构(提案)

### 5.1 包结构

```
internal/acp/
  transport.go   # vendored SDK 的边界:spawn + NewClientSideConnection;stderr 泵;
                 #   重连/退避;协议错误 → rpcerror 映射
  agent.go       # 会话与 turn 状态机:prompt 入队、常驻 reader、每 turn done channel、
                 #   孤儿记账、取消(对照分析文档 §3 的多信号结算)、steering 转发
  permission.go  # pending 审批表(session/request_permission → 等待 gRPC 应答)、
                 #   optionId 校验(必须!分析文档 §17.7:不校验 = 任意权限注入)
  bridge.go      # ACP 通知 → gRPC 事件流的映射;工具元数据单一构造
  record.go      # <runtime-dir>/acp/<uuid>/{metadata,status}.json,抄 process/record.go
                 #   的 exclusive/atomic write 与 LOST 判定
  service.go     # gRPC AcpService 实现 + Shutdown/BeginShutdown
vendor/github.com/coder/acp-go-sdk/   # 锁定版本
```

### 5.2 数据流

```
CLI (internal/cli, 新命令 acp-*)
  │  pkg/client.AcpSession(封装 ObserveAcpSession 事件流 + Prompt/Respond 调用)
  ▼
gRPC AcpService (internal/acp/service.go)
  │  PromptAcpSession / RespondAcpPermission / ObserveAcpSession(server-stream)
  ▼
internal/acp 会话层(turn 状态机 + pending 审批表 + 事件序号/游标)
  │  vendored acp-go-sdk: NewClientSideConnection(Client, stdin, stdout)
  ▼
claude-agent-acp 子进程(internal/process raw-pipe 入口,模板启动,stderr→现有日志)
```

### 5.3 gRPC API 草案(提案,细节以 proto 评审为准)

```protobuf
service AcpService {
  rpc GetAcpInfo(GetAcpInfoRequest) returns (GetAcpInfoResponse);          // 能力/模板
  rpc ListAcpSessions(...) returns (...);                                  // 含 LOST
  rpc StartAcpSession(StartAcpSessionRequest) returns (...);               // 指定模板+参数+cwd
  rpc PromptAcpSession(...) returns (...);                                 // stopReason 应答
  rpc CancelAcpTurn(...) returns (...);
  rpc RespondAcpPermission(...) returns (...);                             // optionId 校验
  rpc ObserveAcpSession(...) returns (stream AcpSessionEvent);             // Header/游标重放
  rpc CloseAcpSession(...) returns (...);
}
// GetInfoResponse 追加: optional AcpInfo acp = 10;
```

`ObserveAcpSession` 事件流照抄日志流模式(`log_service.go:18-135`):Header 带会话快
照与 earliest/当前游标,事件带单调序号,断线重连按游标重放;权限请求作为事件下发,
`RespondAcpPermission` 一元应答,过期/未知 requestId →
`ACP_PERMISSION_REQUEST_NOT_PENDING`。**不轮询**——与文件传输 session 的持久 offset
先例(`internal/files/transfer_store.go`)同理。

### 5.4 配置草案

```toml
# controller.toml
[acp]
enabled = false                       # 默认关,与 MCP/workflow 同风格
default_agent = "claude-agent-acp"    # 引用进程模板名

[process_templates]
definition_files = ["configs/process-templates/acp.process-template.yaml"]
# ANTHROPIC_API_KEY 等凭证放 0600 的独立文件或进程环境,绝不进 parameters
```

### 5.5 持久化与恢复

- ACP 会话记录 `<runtime-dir>/acp/<uuid>/`:`metadata.json`(模板名+revision、claude
  会话 id、能力快照、事件游标)+ `status.json`(活跃 turn、pending 审批数)。
- controller 重启:agent 子进程按进程 LOST 语义不重新收养;ACP 会话标 `ACP_SESSION_LOST`,
  保留 claude 会话 id 供后续 `session/load`/`session/resume` 重建(第二阶段特性)。
- turn 级 durable 排队第一版不做;如需,复用 workflow 的 bbolt 模式
  (`internal/workflow/store.go:15`)。

## 6. 迁移步骤(分阶段)

每阶段可独立提交、可测试;Phase 1–4 构成最小可用版(MVP:prompt/update/权限审批/取消,
无 elicitation、无 resume、无 steering)。

### Phase 0 — 前置决策与依赖(0.5 天)

- [ ] 评审确认本方案的三项关键决策:vendor acp-go-sdk、process raw-pipe 入口、独立
      AcpService;
- [ ] `go mod vendor github.com/coder/acp-go-sdk@v0.13.5`(或评审时最新 tag),通读其
      `Client` 接口与 example/claude-code,确认回调面覆盖 §2.1 客户端侧方法;
- [ ] 在 temp/ 保留两个参考仓库(已在,gitignored);schema 版本锚定
      `schema/v1/schema.json` 的当前 meta 版本号并记录在 `internal/acp` 包注释里。

**验收**:`go build ./...` 通过;vendor diff 可审。

### Phase 1 — `internal/process` raw-pipe 入口(1.5–2 天,唯一动安全敏感包的阶段)

- [ ] `StartRawProcess`:复用现有校验/限额/进程组/收割(`service.go:689-860`、
      `runner_unix.go:39-101`),stdout 返回管道而非日志 writer;stderr 仍走日志;
- [ ] `StartRawProcessFromTemplate`(revision 校验 + argv 消毒路径复用
      `template_service.go:50-73`);
- [ ] 保持"gRPC 面不暴露 raw 模式"——`StartProcess` 行为完全不变;
- [ ] 表驱动测试:生命周期、退出收割、取消(信号整组)、并发 stdin 写、stdout 大流量
      (超过 64KiB 分帧阈值的单行 JSON-RPC 消息)、`make test-race` 通过。

**验收**:现有全部测试不回归;新测试覆盖上述路径;进程记录/LOST 语义不变。

### Phase 2 — `internal/acp` 骨架:transport + 会话层(2–3 天)

- [ ] `transport.go`:模板 spawn → raw pipe → `NewClientSideConnection`;stderr 泵到
      controllerlog;initialize 能力协商(protocolVersion=1,第一版 clientCapabilities:
      `fs{false,false}, terminal:false`,不广告 elicitation);
- [ ] `agent.go`:`session/new`、turn 状态机(入队 + 常驻消费 + 每 turn done;孤儿
      记账;cancel → `session/cancel` 通知 + 以 cancelled 应答挂起权限;关停顺序按
      §2.4-9);
- [ ] `bridge.go`:`session/update` 11 种 → 内部事件(第一版聚合 message chunk 与
      tool_call,thought/plan/usage 透传);
- [ ] fs 存根回调;`record.go` 持久化 + LOST 判定(抄 `process/record.go:285-323`);
- [ ] 单元测试:用脚本化假 agent(见 Phase 7)驱动;race 测试覆盖并发 prompt/cancel。

**验收**:进程内测试能完整跑通"initialize → session/new → prompt → update 流 →
stopReason"与取消路径。

### Phase 3 — gRPC `AcpService`(1–1.5 天)

- [ ] proto 定义(§5.3)+ `make generate` + 提交生成物;
- [ ] `internal/acp/service.go` 实现 + rpcerror reasons(§4.4 清单)+ 客户端镜像
      `pkg/client/reason.go`;
- [ ] `GetInfoResponse.acp` 能力字段,`internal/server` 装配(Prepare 校验模板存在、
      NewPrepared 构建、Shutdown 挂关停)+ `cmd/controller/config.go` TOML 表与版本
      gating(`--check-config` 必须覆盖新配置);
- [ ] 集成测试:`*_integration_test.go` 起 in-process server 验证能力字段与会话 RPC。

**验收**:`make lint && go test ./...` 通过;`--check-config` 校验新表。

### Phase 4 — 权限审批桥接(1–1.5 天)

- [ ] `permission.go`:pending 表(requestId → options + 效果快照)、超时策略(建议
      5 分钟,超时回 cancelled)、cancel 联动;
- [ ] `ObserveAcpSession` 事件流 + 游标重放;`RespondAcpPermission` 校验 optionId
      属于该请求的 options(**防权限注入**,分析文档 §17.7),非法 →
      `ACP_PERMISSION_OPTION_INVALID`;
- [ ] 测试:审批-应答往返、断线重连重放、cancel 后挂起请求的 cancelled 应答、
      optionId 伪造拒绝。

**验收**:审批全链路(含重连)测试通过。**至此 MVP 完成。**

### Phase 5 — `pkg/client` + CLI(1 天)

- [ ] `pkg/client` 增加 `AcpSession`(封装事件流 + Prompt/Respond,参照
      `ProcessAttachment` 的双流组合 `process_attach.go:45-163`);按 `GetInfo.acp`
      降级提示;
- [ ] CLI 命令(`commandSpec` 集中注册):`acp-start` / `acp-prompt` / `acp-cancel` /
      `acp-permissions` / `acp-sessions`;渲染参照现有 REPL 风格;
- [ ] 手工验收脚本:CLI 完整对话一轮 + 一次审批。

### Phase 6 — 增强(按需排期)

- [ ] steering 转发(读 initialize 顶层 `_meta.steering`,暴露
      `SteerAcpSession` RPC,把 `injected|startedNewTurn` 透传);
- [ ] elicitation(广告 `elicitation.form` + 实现 `elicitation/create`,桥接为第二类
      待应答事件——permission.go 泛化为 elicitation 表);
- [ ] 会话恢复:`session/load`/`resume` + LOST 重建;`session/set_mode`(plan 模式);
- [ ] usage_update 的 token 计量透传(对照分析文档 §13 的 `_meta.quota` 形状);
- [ ] MCP:把 remote-code 自己的 MCP server 列表经 `session/new.mcpServers` 注入
      (与 `internal/mcp` 打通,需注意 token 不对称问题,
      `docs/authorization-model-v1.md`)。

### Phase 7 — 测试基建(与 Phase 2 并行建,持续)

- **假 agent**:`internal/acp/acptest`(小的 Go 测试二进制或 in-process 脚本引擎,
  按脚本回 JSON-RPC:能力协商、update 序列、权限请求、慢 turn、cancel 竞态、
  非法 optionId)。所有单测用它,**不要求 Claude 凭证**(CLAUDE.md 测试纪律)。
- **集成测试**:in-process server + 假 agent 全链路;
- **端到端(可选,环境变量门控)**:`ACP_E2E=1` 时对真实 claude-agent-acp 跑一轮
  对话,风格对照 claude-agent-acp 自己的 `RUN_INTEGRATION_TESTS` 门控
  (`package.json` scripts)。
- 每阶段跑 `make test-race`(ACP 层有大量并发:reader goroutine、审批表、事件流)。

## 7. 安全考量(对齐 CLAUDE.md 安全不变量)

| 不变量 | 本方案的落实 |
|---|---|
| Never log prompts / 文件内容 | 协议 stdout **不落盘**(raw pipe 不进日志,§4.1);事件流只经内存转发;`<runtime-dir>/acp/` 只存元数据与游标 |
| env 只存 key 不存值 | 沿用 `EnvironmentKeys`(`record.go:29-42`);凭证走 extra_parameters(操作者 config)或 controller 环境继承,不进客户端可见面、不进模板参数 |
| 模板 argv 消毒 | agent 进程走 `StartRawProcessFromTemplate`,ProcessInfo 只暴露模板名 + revision |
| 凭证存在性不泄露值 | controllerlog 记"agent 凭证来自环境/extra_parameters",不记值 |
| token = RCE 的开放信任模型 | 不新增 app 层限制假象:ACP 会话本质就是以 controller 用户跑 Claude,权限审批只是 UX 不是隔离;隔离仍靠 OS 层 |
| 权限应答不可伪造 | `RespondAcpPermission` 校验 optionId 归属(§Phase 4),防任意 PermissionUpdate 注入 SDK |

另:ACP 帧上限——prompt 单帧建议沿用现有 16MiB 消息上限(`internal/server/server.go:31`)
作为 ObserveAcpSession/Prompt 的 gRPC 侧护栏;协议侧单行 JSON-RPC 无内嵌换行,天然有界
于行缓冲。

## 8. 风险与开放问题

1. **acp-go-sdk 维护节奏**(v0.x,~3 个月无提交):已用 vendor + 薄封装缓解;若停更,
   手写子集(§3 路线③)是既有 fallback,成本约 +2 天。
2. **raw-pipe 入口的设计评审**:改 `internal/process` 是全方案唯一的侵入性改动,需要
   评审确认内部 API 形状(返回 `io.ReadWriteCloser` vs 回调式 reader)。
3. **claude-agent-acp 的 SDK 内部表面漂移**(分析文档 §9/§17):steering、`_meta`
   扩展、token 计量形状都可能随版本变;建议把"能力探测"逻辑集中在一个函数里,版本
   升级只改一处。
4. **审批等待与 controller 重启的交互**:pending 审批在重启后必然丢失——应答必须在
   重启前完成或接受 cancelled;MVP 直接标 LOST,Phase 6 再考虑补偿。
5. **多客户端同时 Observe 一个 ACP 会话**:事件流广播 vs 独占——MVP 建议允许多观察者
   (日志流已有 MaxObservers 先例),写操作(prompt/respond)天然串行于 turn 状态机。
6. **Node/npx 部署依赖**:离线环境需预装;可在文档里给出全局安装变体。
7. **v2 协议**:仍是 Draft,锚定 v1;`protocolVersion` 协商机制已为升级留了钩子。

## 9. 参考

- 协议规范(本地一手来源):`temp/agent-client-protocol/docs/protocol/v1/`(initialization、
  session-setup、prompt-turn、cancellation、transports、extensibility);
  schema:`temp/agent-client-protocol/schema/v1/schema.json`(方法枚举见 §2.1)。
- 客户端参考实现:`temp/claude-agent-acp/examples/simple-client.ts`;
  agent 侧要求:`temp/claude-agent-acp/src/acp-agent.ts`。
- Go SDK:[coder/acp-go-sdk](https://github.com/coder/acp-go-sdk) ·
  [pkg.go.dev](https://pkg.go.dev/github.com/coder/acp-go-sdk) ·
  [社区库列表](https://agentclientprotocol.com/libraries/community)。
- 本仓库先例:`internal/mcp`(外部协议桥接)、`internal/files/transfer_store.go`
  (持久会话状态机)、`internal/process/log_service.go`(游标重放流)、
  `internal/workflow`(后台子系统生命周期)。
