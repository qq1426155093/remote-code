# 可配置 MCP Server 需求 v1

## 1. 文档状态

本文定义 `remote-code-controller` 可配置 MCP Server 的首版需求。仓库已按本文实现首版 MCP endpoint、
`.mcp.yaml` 文件加载、JSON Schema 校验、Expr 脚本、controller/file/process/template host capability 和
有界 workspace Resource；本文同时保留安全边界与兼容性要求，作为后续修改的验收依据。
对应的实现方案见[可配置 MCP Server 详细设计 v1](mcp-server-design-v1.md)。

本设计的规范基线为 Model Context Protocol `2026-07-28`：

- [MCP 基础协议](https://modelcontextprotocol.io/specification/2026-07-28/basic)
- [Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
- [MCP Tools](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)

`2026-07-28` 已移除旧版 Streamable HTTP 的协议级 session、独立 GET SSE stream 和
`initialize` 握手。controller 必须以无 session、逐请求携带协议元数据的模型作为主要实现，
不得依赖连接或历史请求保存客户端上下文。

## 2. 目标

controller 通过配置加载多个以 `.mcp.yaml` 为复合扩展名的定义文件，将这些文件中的 tool 聚合成一个
MCP Server，并通过 Streamable HTTP 供 Code Agent 调用。

每个 `.mcp.yaml` 文件表示一个逻辑 module，例如 `controller.mcp.yaml`、`file.mcp.yaml` 或
`process.mcp.yaml`；一个 module 可以定义多个 tool。每个 tool 至少包含：

- 唯一名称、标题和面向模型的功能描述；
- 使用 JSON Schema 表达的参数及参数描述；
- 可选的输出 JSON Schema；
- 一段 Expr 脚本；
- 脚本允许调用的 controller host capability；
- 超时、并发限制和 MCP tool annotations。

Expr 脚本通过经过 allowlist 的 host function 使用 controller 文件、进程和后续 Agent 能力。
host function 必须复用 controller 现有的工作区边界、进程注册表、并发限制、持久化与审计逻辑，
不得通过直接访问任意文件、调用 shell 或回连 gRPC 绕过这些边界。

## 3. 非目标

首版不包含：

- MCP Prompts、Sampling、Elicitation 或自定义 MCP extensions；
- JSON Schema `x-mcp-header` 参数镜像；
- 从 HTTP、Git 或其它远端位置下载 `.mcp.yaml` 定义；
- `.mcp.yaml` 文件热加载以及运行时增删 tool；
- 由 MCP 客户端上传或修改 Expr 脚本；
- 通用 JavaScript、Python、Lua 或 Starlark 运行时；
- 在一个 tool 脚本内实现任意多步骤事务或完整 workflow；
- OAuth authorization server、动态客户端注册或浏览器登录流程；
- 将进程日志 follow、PTY attach 或 stdin 双向流直接映射为长连接 tool；
- 对 MCP tool 的副作用提供分布式事务或 exactly-once 保证。

复杂、持久化、多步骤的 Agent 编排仍由 Go workflow 状态机负责。Expr 用于参数计算、条件、
结果整形和一次受控 controller 操作，不取代 workflow runner。

## 4. 总体行为

### 4.1 启用与启动

MCP Server 默认关闭。只有 controller 配置显式设置 `mcp.enabled = true` 时才加载定义并绑定
MCP listener。启用 MCP 时必须同时满足：

- 至少配置一个 `.mcp.yaml` 定义文件；
- 配置现有 bearer token 文件，MCP 不允许匿名访问；
- 所有定义文件、JSON Schema、Expr 脚本和 capability 声明均通过校验；
- MCP listener 满足与 gRPC listener 相同的 TLS/loopback 安全策略；
- MCP 与 gRPC 不得配置为同一监听地址。

任意定义错误都使 controller 拒绝启动，不允许静默跳过错误 tool 或以部分 registry 运行。
`--check-config` 必须完成全部 `.mcp.yaml` 文件读取、schema 编译、capability 校验和 Expr 编译，但不绑定
listener，也不得调用任何 host function。

首版 registry 在进程生命周期内不可变。修改 `.mcp.yaml` 文件后必须重启 controller，重启时一次性
构建并发布新的完整 registry。

### 4.2 MCP endpoint

MCP 使用独立于 gRPC 的 HTTP listener，默认监听 `127.0.0.1:9444`，默认 endpoint 为 `/mcp`。
使用独立 listener 的目的是保持 gRPC 与 HTTP 生命周期、超时和中间件边界清晰；首版不在同一端口
复用 gRPC 与普通 HTTP。

主要协议版本为 `2026-07-28`。同一 endpoint 应兼容只使用 POST request/response 子集的
`2025-11-25` 客户端，以覆盖尚未迁移的 Code Agent；兼容模式不提供旧版独立 GET SSE stream、
session resumability 或 DELETE session。GET、DELETE、PUT、PATCH 和未支持的 OPTIONS 请求返回
`405 Method Not Allowed`，并携带 `Allow: POST`。

首版实现以下 MCP 方法：

- `server/discover`：面向 `2026-07-28` 客户端公布版本与 `tools` capability；
- `tools/list`：分页、稳定排序地列出所有已授权 tool；
- `tools/call`：校验参数并执行 tool；
- `resources/templates/list` 与 `resources/read`：仅在全局允许 `files.read` 时发布并读取有界 workspace 文件；
- 仅在兼容旧协议时处理 `initialize` 和 `notifications/initialized`。

首版响应统一使用 `Content-Type: application/json`。客户端即使在 `Accept` 中声明
`text/event-stream`，服务端也不主动选择 SSE。HTTP 请求 context 取消必须向 tool、Expr host
function 和底层 controller service 传播。

### 4.3 Streamable HTTP 校验

对 `2026-07-28` 请求必须：

- 每次 POST 仅包含一个 JSON-RPC request；拒绝 batch 和客户端 JSON-RPC response；
- 要求 `Content-Type: application/json`；
- 要求 `Accept` 同时允许 `application/json` 和 `text/event-stream`；
- 要求 `MCP-Protocol-Version`、`Mcp-Method`，`tools/call` 还要求 `Mcp-Name`；
- 要求 `params._meta` 中包含 `io.modelcontextprotocol/protocolVersion` 与
  `io.modelcontextprotocol/clientCapabilities`，`io.modelcontextprotocol/clientInfo` 建议提供；
- 比较 header 与 JSON-RPC body 镜像字段，任何不一致返回 `HeaderMismatch`；
- 不创建、返回或依赖 `Mcp-Session-Id`；收到该 header 时忽略其 session 语义；
- 每个成功 result 包含 `resultType = "complete"` 和 server info。

body 超限应在完整解码前返回 `413 Payload Too Large`。JSON 解析拒绝 trailing value；请求 ID 只允许
非空字符串或数字，不允许 `null`。

### 4.4 Tool 列表

`tools/list` 返回的名称是 `<namespace>.<tool-name>`，例如 `file.list`、`process.start`。tool 按最终
名称字节序稳定排序。名称必须唯一，重复名称使启动失败，不存在“后加载覆盖先加载”。

列表支持分页，默认每页最多 100 个 tool。cursor 为 opaque value，必须绑定当前 registry 摘要和
offset，并由进程启动时生成的随机密钥做 HMAC；无效、过期或被篡改的 cursor 返回 invalid-params。
由于首版 registry 不热更新，
`tools.listChanged` 为 `false`。面向新协议时列表可声明私有缓存和有限 TTL。

### 4.5 Tool 调用

一次 `tools/call` 按以下顺序处理：

1. 完成 HTTP、认证和 JSON-RPC 结构校验；
2. 按完全限定名称查找 tool；
3. 对 `arguments` 应用 tool 的 input schema；
4. 获取全局和 tool 级并发名额；
5. 创建不超过 tool/global 上限的 deadline；
6. 构造隔离的 Expr environment 并执行一次脚本；
7. 将输出规范化为 JSON value；
8. 如配置 output schema，则验证输出；
9. 返回 `structuredContent`，并同时返回相同 JSON 的 text content 以兼容旧客户端。

tool 不存在或 JSON-RPC 结构不合法属于 protocol error。schema 校验失败、host function 返回错误、
超时和业务前置条件失败属于 tool execution error，通过 `isError = true` 返回可供模型修正的简短信息。
内部错误不得包含 controller 绝对路径、token、环境变量 value、脚本正文、文件内容或进程输入。

controller 不自动重试 Expr 或 host function。客户端断开时如果副作用已经发生，结果可能处于
“操作成功但响应未送达”的不确定状态。创建型 tool 应返回稳定 handle；跨调用状态必须通过明确的
process ID、Agent ID 或其它 opaque handle 传递，不能依赖 HTTP 连接或 MCP session。

## 5. Controller 配置

controller loader 当前接受 TOML schema v1/v2/v3：v1 将 MCP 解释为 disabled，v2 引入 MCP，v3 增加
process template 定义。启用本节全部能力的示例使用 v3：

```toml
version = 3
workspace = "/srv/remote-code/workspace"
listen_address = "127.0.0.1:9443"

[process_templates]
definition_files = [
  "/etc/remote-code/process-templates/code-agents.process-template.yaml",
]

[mcp]
enabled = true
listen_address = "127.0.0.1:9444"
endpoint_path = "/mcp"
definition_files = [
  "/etc/remote-code/mcp/controller.mcp.yaml",
  "/etc/remote-code/mcp/file.mcp.yaml",
  "/etc/remote-code/mcp/process.mcp.yaml",
]
allowed_origins = []
allowed_host_capabilities = [
  "controller.read",
  "files.read",
  "files.write",
  "processes.read",
  "processes.start",
  "processes.signal",
  "process_templates.read",
  "process_templates.start",
]
max_request_bytes = 1048576
max_response_bytes = 4194304
max_concurrent_calls = 16
requests_per_second = 20
request_burst = 40
default_tool_timeout = "30s"
max_tool_timeout = "5m"
tool_list_page_size = 100

[tls]
certificate_file = "/etc/remote-code/tls/server.crt"
key_file = "/etc/remote-code/tls/server.key"

[auth]
token_file = "/etc/remote-code/controller.token"
```

要求如下：

- `definition_files` 只接受显式文件列表，不做隐式目录扫描、glob 或环境变量展开；
- 文件名必须以 `.mcp.yaml` 结尾，必须是非空普通文件，拒绝符号链接和其它文件类型；
- 不接受 `.mcp.yml`、裸 `.mcp` 或按内容自动探测格式，避免同一版本出现多套解析规则；
- definition 文件解析后的物理路径必须位于 workspace 之外，即使 operator 显式配置也不放行；
- 相对路径延续 controller 现有规则，按进程工作目录解释；生产配置推荐绝对路径；
- 同一物理文件被重复配置时拒绝启动；
- `allowed_host_capabilities` 是整个 MCP Server 的权限上界，默认空列表；
- `allowed_origins` 默认空列表；没有 `Origin` header 的非浏览器请求可以继续，带有 Origin 的请求
  必须与 allowlist 中某一规范化 origin 精确匹配；
- MCP listener 使用全局 TLS certificate/key 和 bearer token，不在 `.mcp.yaml` 文件中保存凭据；
- 非 loopback 明文监听继续受 `allow_insecure_remote` 保护，但启用 MCP 时 bearer token 始终必需。

首版 MCP 配置只通过 TOML 提供，不新增一组数量庞大的命令行覆盖参数。

## 6. `.mcp.yaml` 文件格式

### 6.1 格式与顶层字段

`.mcp.yaml` 文件采用严格 YAML 1.2 JSON-compatible 子集，schema version 为 1：

```yaml
version: 1
namespace: file
description: Workspace file tools
language: expr
```

字段要求：

- `version` 必须为 `1`；
- `namespace` 必须匹配 `[A-Za-z0-9][A-Za-z0-9_-]{0,62}`；
- `description` 可选，仅用于运维和诊断，不直接拼接到每个 tool；
- `language` 必须为 `expr`，用于给未来脚本引擎版本留出显式演进点；
- 文件必须是 UTF-8 且只包含一个 YAML document；
- mapping key 必须是 string，未知字段、错误类型和重复 key 均为启动错误；
- 只允许 JSON-compatible 的 null、boolean、string、integer、finite number、sequence 和 mapping；
- 拒绝 anchor、alias、merge key、显式/自定义 tag、YAML directive、timestamp、binary 和非 JSON number；
- 不提供 include、环境变量插值、模板或其它配置期求值。

`.mcp.yaml` 文件是 controller 操作者提供的受信任代码配置，而 MCP tool arguments 始终是不可信输入。
定义文件不得包含 bearer token、私钥、环境变量 secret 或其它凭据。

### 6.2 Tool 字段

`tools` 是非空 sequence，每个元素包含：

```yaml
tools:
  - name: list
    title: List workspace files
    description: List direct children of a workspace-relative path.
    capabilities:
      - files.read
    timeout: 10s
    max_concurrency: 8

    annotations:
      read_only: true
      destructive: false
      idempotent: true
      open_world: false

    input_schema:
      type: object
      required:
        - path
      additionalProperties: false
      properties:
        path:
          type: string
          description: Workspace-relative directory path; use "." for the root.
          minLength: 1
          maxLength: 4096

    script: |-
      file_list(args.path)
```

要求如下：

- `name` 为 module 内局部名称；与 namespace 拼接后必须满足 MCP 的 1..128 字符名称约束，
  只允许 ASCII 字母、数字、下划线、连字符和点；
- `title` 与 `description` 必填，description 必须准确描述副作用、限制和返回 handle；
- `capabilities` 必填，即使为空也必须显式写出；
- `timeout` 可省略并使用全局默认值，但不得超过全局最大值；
- `max_concurrency` 可省略，默认不增加 tool 级限制；
- `script` 必填且必须是一个 Expr program；
- 多行 `script` 必须使用 YAML literal block `|` 或 `|-`，不得使用会折叠换行的 `>`；
- `input_schema` 必填，默认 dialect 为 JSON Schema 2020-12，根必须是 object；
- 每个公开 parameter 都必须具有非空 `description`；顶层必须显式设置
  `additionalProperties = false`；
- `output_schema` 可选，可描述任意 JSON value；
- annotations 必须与 capability 和脚本静态分析结果一致，不能把 destructive tool 声明为只读。

### 6.3 输出 schema 示例

```yaml
output_schema:
  type: array
  items:
    type: object
    required:
      - path
      - type
      - size
    additionalProperties: false
    properties:
      path:
        type: string
      type:
        type: string
      size:
        type: integer
        minimum: 0
```

`input_schema` 和 `output_schema` 在 YAML AST 校验后必须无损转换成 JSON-compatible tree，再进入
JSON Schema compiler。实际定义应优先使用简单、直接的 schema；组合关键字的深度和子 schema 总数
受资源限制。

### 6.4 Schema 约束

首版只支持 JSON Schema 2020-12；省略 `$schema` 时按 2020-12 处理，显式 `$schema` 必须是该 dialect
的 canonical URI。compiler 不注册网络或文件 loader。允许同一 schema 内的 fragment `$ref`、
fragment `$dynamicRef` 和受控内嵌 `$defs`；文件路径、HTTP(S) 或其它外部引用均使启动失败。

schema 编译和验证必须限制深度、子 schema 数量、正则长度和执行时间。schema 中的 `default`
仅作为文档，不自动修改 arguments；可选值由脚本使用 `??` 显式处理。

首版拒绝 schema 中任何位置的 `x-mcp-header`。该扩展会要求客户端把参数复制到
`Mcp-Param-*` HTTP header，并要求服务端执行额外的 header/body 一致性校验；在真正实现代理路由
需求前，不暴露一个只有声明而没有完整验证语义的配置入口。

## 7. Expr 脚本模型

### 7.1 Environment

每次调用至少提供：

- `args`：已经通过 input schema 的 `map[string]any`；
- `call`：只读调用元数据，包括 invocation ID、tool name 和安全的 client display info；
- 经 capability 过滤后的 host functions；
- 内部 context，用于 `expr.WithContext` 向 host function 传播取消与 deadline。

不向脚本提供 bearer token、controller 环境变量、绝对 workspace 路径、网络 client、任意文件句柄、
shell 或底层 registry mutex。

JSON 整数解析为 `int64`；超出范围时拒绝参数。带小数或指数的 number 解析为有限 `float64`；
NaN 和 Infinity 不属于合法 JSON。UUID、offset、sequence 等需要无损传递的值使用 string 或强类型
host result，不依赖浮点数。

### 7.2 Host function 与 capability

host function 使用稳定的 snake_case 名称，例如：

| Host function | Capability | 副作用分类 |
| --- | --- | --- |
| `controller_info` | `controller.read` | read |
| `file_stat`、`file_list`、`file_tree`、`file_read_text`、`file_read_range`、`file_search` | `files.read` | read |
| `file_mkdir`、`file_chmod` | `files.write` | mutate |
| `file_write_text`、`file_apply_patch`、`file_move` | `files.write` | destructive（可能覆盖已有数据） |
| `file_remove` | `files.delete` | destructive |
| `process_list`、`process_get`、`process_logs`、`process_logs_since` | `processes.read` | read |
| `process_start` | `processes.start` | mutate |
| `process_signal` | `processes.signal` | destructive |
| `process_delete` | `processes.delete` | destructive |
| `process_template_list`、`process_template_get` | `process_templates.read` | read |
| `process_template_start` | `process_templates.start` | mutate |

tool 只能使用其 `capabilities` 所授权且同时位于 controller
`allowed_host_capabilities` 中的函数。脚本引用其它函数时必须在 controller 启动阶段编译失败。
tool 声明的 capability 集合必须与脚本静态引用的 host function 所需集合完全一致；多声明同样是
启动错误。无 host call 的纯表达式 tool 必须显式写 `capabilities = []`。
未来新增 Agent host function 时必须使用新的 capability，不得复用宽泛的 `processes.start`。

host function 必须调用 controller 内部 service/facade，沿用现有路径、symlink、大小、进程并发、
命令参数、环境变量和状态转换校验。`.mcp.yaml` capability 不扩大底层 service 权限。

### 7.3 副作用规则

Expr 本身按无副作用表达式语言设计，而本项目的部分 host function 会产生副作用。为保持可预测性，
首版增加以下静态规则：

- 一个 tool script 最多出现一个 mutate/destructive host call site；
- mutate/destructive call 必须是所有 `let` 之后的最终表达式；
- host function 不得出现在 `map`、`filter`、`reduce`、`all`、`any` 等集合 predicate 中；
- 一个 script 的 host call site 总数最多 16；
- 禁止 range 运算符和 `repeat` 等可由小输入制造巨大中间值的操作；
- controller 不优化、重放或自动重试副作用调用。

只读 host function 可以为最终 mutation 计算参数，例如读取文件后构造一次写入，但多个 mutation
必须拆成多个 MCP tool call 或交给持久化 workflow runner。

### 7.4 执行限制

Expr program 保证终止并不等于资源使用无限小。必须同时限制：

- 单文件、全部定义、单 script 的字节数；
- Expr AST 节点数和嵌套深度；
- collection builtin 的数量和 lambda 嵌套深度；
- MCP request 和 arguments JSON 大小；
- input array/map 元素数量；
- host call 数量及单次 host 返回大小；
- tool deadline、全局和每 tool 并发数；
- 最终 JSON 与 HTTP response 大小。

context deadline 主要约束 host function；由于 Expr 内建求值不能依赖 context 强行中断，危险语法必须
在编译阶段拒绝，且输入集合必须有硬上限。禁止使用“后台 goroutine 执行 Expr，超时后遗弃 goroutine”
作为资源隔离方案。

## 8. 初始 host 能力

### 8.1 文件能力

首版文件 host function 至少覆盖：

- stat、非递归 list、受限 tree；
- 受最大字节数限制且要求合法 UTF-8 的完整与按行范围读取，以及同时限制扫描字节、条目、文件和结果数的
  literal search；
- 原子文本写入、带 expected SHA-256 的单文件 unified patch、mkdir、move、chmod；
- 显式 capability 保护的 remove。

所有路径均为 workspace-relative；拒绝绝对路径、`..`、NUL、根目录 destructive 操作和 symlink
escape。写入沿用同目录临时文件、校验、sync 和原子发布语义。Expr 不返回任意尺寸二进制；二进制读取
使用下述有界 workspace Resource。

### 8.2 进程能力

首版进程 host function 至少覆盖：

- start，返回稳定 UUID、name、PID 和状态；
- list active/history，并按精确 reference 获取一个快照；
- signal，可选等待；
- 删除终态历史；
- 有界日志 snapshot，可选 stream filter、tail lines 或可续传的 decimal offset；
- list/get/start operator-controlled process template，模板启动使用独立 capability。

`process_start` 继续接收具体 executable/arguments，不经 shell 拼接；其远程代码执行风险与 gRPC
`StartProcess` 相同。只有显式允许 `processes.start` 的 controller 才能加载调用它的 tool。
日志 tool 不支持 follow；长任务由 start 返回 UUID，再通过 list/logs/signal tool 操作。

首版不暴露 stdin、PTY attach 和 resize。若以后增加，必须采用显式 handle 和独立的有界交互模型，
不能把连接本身作为 session。

### 8.3 Workspace Resource

全局 allowlist 包含 `files.read` 时，server 发布 `workspace:///{+path}` Resource template。handler 只读取
workspace 内普通文件，继续复用 `os.Root`、路径和 symlink escape 校验，并以 MCP binary resource 返回。
单次原始字节上限由 `max_response_bytes` 扣除 JSON-RPC 固定余量后按 base64 的 4/3 膨胀比例反算；读取前
根据文件大小拒绝超限，不依赖 HTTP middleware 截断。Resource 不支持写入、目录打包或 subscription。

## 9. 安全与认证

### 9.1 HTTP 安全

- MCP 每个 HTTP 请求必须验证 `Authorization: Bearer <token>`，使用 constant-time comparison；
- 未认证返回 `401` 和 `WWW-Authenticate: Bearer`，不返回 token 细节；
- 对带 `Origin` 的请求执行精确 allowlist 校验，无效 origin 返回 `403`；
- 不发送宽泛 CORS header，首版不支持浏览器作为 MCP client；
- 默认只监听 loopback，远程监听必须 TLS，并由部署者提供网络和 OS 级隔离；
- 限制 header、body、并发、速率、deadline 和 response 大小；
- 使用受支持且已修复 DNS rebinding 问题的 MCP Go SDK 版本，并保留独立 Origin middleware。

静态 bearer token 是 controller 首版自有认证方案，不宣称实现 MCP OAuth authorization profile。
未来增加 OAuth 时不得降低现有每请求认证和 tool capability 检查。

### 9.2 脚本与输出安全

- `.mcp.yaml` 文件权限建议为 `0640` 或更严格；loader 必须拒绝物理路径位于 workspace 内的定义文件；
- 不从 workspace 自动加载 `.mcp.yaml` 文件，避免 Agent 修改 workspace 后扩大自身权限；
- input schema、capability 和底层 service 必须分层校验，不能只依赖模型遵守 description；
- tool annotations 只是给客户端的提示，不是服务端授权机制；
- tool 输出不得包含 token、环境变量 value、controller 绝对路径或未请求的文件内容；
- host errors 在 transport 边界转换为稳定、经过清理的消息。

## 10. 并发、取消与关闭

- tool registry 和已编译 Expr program 在发布后不可变，可供并发请求复用；
- 全局 semaphore、tool semaphore 和速率限制均在执行脚本前获取；
- 排队等待也受 HTTP request context 和 tool deadline 约束；
- 自定义 host function 通过 `expr.WithContext` 接收 context；
- HTTP 客户端断开、server shutdown 或 deadline 到达时，底层可取消操作应尽快结束；
- 已完成的文件 rename、已启动的进程或已发出的 signal 不做伪回滚；
- shutdown 先停止接受新 MCP 调用并 drain 在途 HTTP 请求，再关闭共享 process/file service；
- 到达 shutdown deadline 后取消剩余 tool context 并强制关闭 HTTP connection。

## 11. 审计与可观测性

每次 tool call 生成 controller 侧 invocation ID。普通日志至少记录：

- invocation ID；
- 完全限定 tool name；
- client info 的 name/version，仅用于显示和诊断；
- 使用的 capability 名称；
- 开始/结束时间、耗时、成功/失败/取消/超时分类；
- 对应 process/Agent 的公开稳定 ID（若结果产生），但不记录完整参数。

不得记录 bearer token、arguments 原文、Expr 源码、文件内容、prompt、进程 stdin、环境变量 value 或
完整 structured result。指标至少预留调用数、在途数、排队拒绝数、超时数和按 tool 的延迟统计接口。

## 12. 配额

首版默认硬上限：

| 项目 | 默认/上限 |
| --- | --- |
| 定义文件数 | 128 |
| 单个 `.mcp.yaml` 文件 | 1 MiB |
| 全部 `.mcp.yaml` 文件 | 8 MiB |
| tool 总数 | 512 |
| 单个 script | 64 KiB |
| tool name | 128 bytes（含 namespace） |
| description | 16 KiB |
| YAML AST 节点 | 100,000/file |
| 单个 YAML scalar | 256 KiB，script 另受 64 KiB 限制 |
| Expr AST 节点 | 10,000 |
| YAML/Expr/JSON 嵌套深度 | 64 |
| collection builtin call site | 32/script |
| collection lambda 嵌套 | 2 |
| 单个 JSON container 元素 | 10,000 |
| arguments/result JSON 节点 | 100,000 |
| schema 子 schema | 1,024/tool |
| HTTP request | 1 MiB，可向下配置 |
| HTTP response | 默认 4 MiB，可配置 16 KiB..64 MiB |
| host call sites | 16/script |
| mutation call sites | 1/script |
| 全局在途 tool call | 16，可配置 1..256 |
| 默认 tool timeout | 30 秒 |
| 最大 tool timeout | 5 分钟 |
| tool list page | 100，可配置 1..500 |

具体 file/process host function 还必须服从现有 service 的更小限制；多层限制取最小值。
host 累计结果与最终 tool result 都使用 `max_response_bytes`；最终校验按 structured/text 双份字段的实际
JSON 编码大小执行，并为 JSON-RPC envelope 保留固定余量，不能只校验 Expr 值本身。

## 13. 错误模型

| 场景 | HTTP / MCP 结果 |
| --- | --- |
| token 缺失或错误 | HTTP 401 |
| Origin 不允许 | HTTP 403 |
| body 超限 | HTTP 413 |
| 速率或并发入口拒绝 | HTTP 429，携带 `Retry-After` |
| JSON 无法解析 | JSON-RPC `-32700`，HTTP 400 |
| JSON-RPC 结构错误 | `-32600`，HTTP 400 |
| header/body 不一致 | `-32020 HeaderMismatch`，HTTP 400 |
| 不支持的协议版本 | `-32022 UnsupportedProtocolVersion`，HTTP 400 |
| MCP method 不存在 | `-32601`，HTTP 404 |
| tool 不存在 | `-32602` protocol error |
| arguments 不符合 schema | `CallToolResult.isError = true` |
| host 前置条件/业务错误 | `CallToolResult.isError = true` |
| tool deadline | `isError = true`；连接已断开时不再写响应 |
| script 输出不符合 output schema | 清理后的 internal tool error |
| panic 或不变量破坏 | `-32603`，同时记录无敏感数据的 server 日志 |

## 14. 示例 module

### 14.1 `file.mcp.yaml`

```yaml
version: 1
namespace: file
description: Workspace file tools
language: expr

tools:
  - name: list
    title: List workspace files
    description: List direct children of a workspace-relative directory.
    capabilities:
      - files.read
    timeout: 10s

    annotations:
      read_only: true
      destructive: false
      idempotent: true
      open_world: false

    input_schema:
      type: object
      required:
        - path
      additionalProperties: false
      properties:
        path:
          type: string
          description: Workspace-relative directory; use "." for the root.
          minLength: 1
          maxLength: 4096

    output_schema:
      type: array

    script: |-
      file_list(args.path)
```

### 14.2 `process.mcp.yaml`

```yaml
version: 1
namespace: process
description: Managed process tools
language: expr

tools:
  - name: start
    title: Start a managed process
    description: >-
      Start one command without shell expansion and return its stable process ID.
    capabilities:
      - processes.start
    timeout: 30s
    max_concurrency: 4

    annotations:
      read_only: false
      destructive: false
      idempotent: false
      open_world: true

    input_schema:
      type: object
      required:
        - command
      additionalProperties: false
      properties:
        name:
          type: string
          description: Optional unique logical process name.
          maxLength: 64
        command:
          type: string
          description: Concrete executable; no shell parsing is performed.
          minLength: 1
          maxLength: 4096
        arguments:
          type: array
          description: Arguments passed directly to the executable.
          maxItems: 256
          items:
            type: string
            maxLength: 4096
        working_directory:
          type: string
          description: Workspace-relative working directory.
          maxLength: 4096
        io_mode:
          type: string
          description: Process I/O mode.
          enum:
            - pipe
            - pty

    output_schema:
      type: object
      required:
        - id
        - name
        - pid
        - state
      additionalProperties: true

    script: |-
      process_start({
        name: args.name ?? "",
        command: args.command,
        arguments: args.arguments ?? [],
        working_directory: args.working_directory ?? ".",
        io_mode: args.io_mode ?? "pipe"
      })
```

## 15. 验收标准

- controller 可配置 `controller.mcp.yaml`、`file.mcp.yaml` 和 `process.mcp.yaml`，并在一个 endpoint 中列出
  三个 module 的 tool；允许 `files.read` 时同时列出 workspace Resource template；
- 任一文件缺失、非普通文件、schema 错误、Expr 编译错误、未授权 capability 或重复 tool 名都会使
  `--check-config` 和实际启动失败；
- 多 YAML document、duplicate key、非 string key、anchor/alias/merge/tag 和非 JSON scalar 都会使
  `--check-config` 和实际启动失败；
- `2026-07-28` Streamable HTTP 客户端可以完成 discover、tools/list 和 tools/call；
- 兼容客户端可以通过 POST-only `2025-11-25` 流程完成 initialize、list 和 call；
- tools/list 名称稳定排序、分页 cursor 正确，registry 不随连接变化；
- arguments 和 output 分别经过 JSON Schema 校验，structured 与 text JSON 结果一致；
- schema 中出现 `x-mcp-header` 时启动失败；
- 未带 token、错误 Origin、header/body mismatch、超限和未知 tool 的错误符合本文映射；
- tool capability 不能越过全局 allowlist，脚本不能调用未声明 host function；
- 文件 host function 不能通过绝对路径、`..` 或 symlink 逃逸 workspace；
- process host function 沿用活动上限、进程组、持久化和 signal 安全规则；
- HTTP 断开、deadline 和 shutdown 能传播取消，race test 不出现 registry 或 limiter 数据竞争；
- 日志不包含 token、arguments、脚本、文件内容、prompt、stdin 或环境变量 value；
- `go test ./...`、`go test -race ./...`、`go vet ./...` 和 `go build ./...` 全部通过。

## 16. 后续演进

- 原子热加载 registry，并通过 `subscriptions/listen` 发布 tools list change；
- OAuth 2.1 protected-resource 支持与按 token scope 的 tool 过滤；
- MCP Tasks extension，用于标准化长时间 Agent/workflow 操作；
- 独立 `ScriptEngine` 后端，在确有多步骤脚本需求时评估 Starlark；
- resource link、image/audio 等丰富 tool result，以及可选的 Resource 订阅；
- Agent 语义 host functions、审批策略和按 workspace/tool 的细粒度授权；
- 持久化 idempotency key 与调用审计索引。
