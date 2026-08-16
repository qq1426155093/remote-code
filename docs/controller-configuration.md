# Controller 配置文件

`remote-code-controller` 同时支持 TOML 配置文件和原有命令行参数。配置文件必须显式通过
`--config` 指定，不会从当前目录或系统目录隐式加载。

## 覆盖顺序

最终配置按以下顺序合并：

```text
内置默认值 < TOML 配置文件 < 显式命令行参数
```

例如：

```bash
remote-code-controller \
  --config /etc/remote-code/controller.toml \
  --max-processes 32
```

除 `max_processes` 被命令行覆盖外，其它值仍来自 TOML。布尔值可以显式反向覆盖，例如
`--allow-insecure-remote=false`。

## TOML schema v1、v2、v3、v4 与 v5

```toml
version = 5
workspace = "/srv/remote-code/workspace"
listen_address = "127.0.0.1:9443"
runtime_directory = "/var/run/remote-code-controller"
max_upload_bytes = 1073741824
max_processes = 16
allow_insecure_remote = false

[file_transfers]
resumable_enabled = true
upload_session_ttl = "24h"
completed_session_ttl = "1h"
max_active_upload_sessions = 64
max_staging_bytes = 4294967296
checkpoint_bytes = 4194304
checkpoint_interval = "1s"
max_concurrent_downloads = 16

[process_logs]
max_bytes_per_process = 67108864
max_total_bytes = 4294967296
segment_bytes = 4194304
retention_after_exit = "168h"
max_observers_per_process = 8

[tls]
certificate_file = "/etc/remote-code/tls/server.crt"
key_file = "/etc/remote-code/tls/server.key"

[auth]
token_file = "/etc/remote-code/controller.token"

[process_templates]
definition_files = [
  "/etc/remote-code/process-templates/code-agents.process-template.yaml",
]

[process_templates.extra_parameters]
default_model = "gpt-5"
common_arguments = ["--approval-mode", "never"]
environment = { HTTP_PROXY = "http://proxy.example" }

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
```

`version` 必须存在。schema v1 继续兼容，但不允许 `[mcp]`；schema v2 增加 MCP；schema v3 增加
`[process_templates]`；schema v4 增加 `process_templates.extra_parameters`。v3、v4 均可省略
`[process_templates]` 或配置空 `definition_files`，此时没有进程模板；v3 不接受
`extra_parameters`。TLS
certificate/key 必须同时配置。认证配置只接受 token 文件路径，
不允许直接把 token 放进 TOML。所有相对路径按 controller 进程的当前工作目录解释；生产
配置建议使用绝对路径。

解析采用严格模式：未知字段、错误类型、重复 key、缺失/不支持的 schema version 都会使
controller 拒绝启动。配置文件最大 1 MiB。

`file_transfers` 从 schema v5 开始可用。上传 session 元数据和下载 revision 密钥保存在
`<runtime_directory>/file-transfers/`；已经确认的上传 checkpoint 可以跨 controller 进程重启恢复。
若还要求跨主机重启恢复，`runtime_directory` 必须位于持久磁盘，不能依赖重启时会清空的 `/run`。
`max_staging_bytes` 必须不小于 `max_upload_bytes`，并按活动 session 的声明大小预留，避免未完成上传
耗尽磁盘。

## 字段映射

| TOML | 命令行覆盖参数 | 默认值 |
| --- | --- | --- |
| `workspace` | `--workspace` | 必填 |
| `listen_address` | `--listen-addr` | `127.0.0.1:9443` |
| `runtime_directory` | `--runtime-dir` | `/var/run/remote-code-controller` |
| `max_upload_bytes` | `--max-upload-bytes` | `1073741824` |
| `file_transfers.resumable_enabled` | `--disable-resumable-transfers`（反向开关） | `true` |
| `file_transfers.upload_session_ttl` | `--upload-session-ttl` | `24h` |
| `file_transfers.completed_session_ttl` | `--completed-upload-session-ttl` | `1h` |
| `file_transfers.max_active_upload_sessions` | `--max-upload-sessions` | `64` |
| `file_transfers.max_staging_bytes` | `--max-upload-staging-bytes` | `4294967296` |
| `file_transfers.checkpoint_bytes` | `--upload-checkpoint-bytes` | `4194304` |
| `file_transfers.checkpoint_interval` | `--upload-checkpoint-interval` | `1s` |
| `file_transfers.max_concurrent_downloads` | `--max-concurrent-downloads` | `16` |
| `max_processes` | `--max-processes` | `16` |
| `allow_insecure_remote` | `--allow-insecure-remote` | `false` |
| `process_logs.max_bytes_per_process` | `--process-log-max-bytes` | `67108864` |
| `process_logs.max_total_bytes` | `--process-log-max-total-bytes` | `4294967296` |
| `process_logs.segment_bytes` | `--process-log-segment-bytes` | `4194304` |
| `process_logs.retention_after_exit` | `--process-log-retention` | `168h` |
| `process_logs.max_observers_per_process` | `--process-log-max-observers` | `8` |
| `tls.certificate_file` | `--tls-cert` | 空 |
| `tls.key_file` | `--tls-key` | 空 |
| `auth.token_file` | `--token-file` | 空 |
| `process_templates.definition_files` | 无 | `[]` |
| `process_templates.extra_parameters` | 无 | `{}` |

`process_templates` 字段不提供命令行覆盖。每个 definition file 必须以
`.process-template.yaml` 结尾，是 workspace 外的普通文件，且最终路径不能是符号链接。模板文件使用
严格 YAML、JSON Schema Draft 2020-12 和纯 Expr 渲染；完整字段、安全边界和示例见
[进程模板详细设计](process-template-design-v1.md)以及
[`code-agents.process-template.yaml`](../configs/process-templates/code-agents.process-template.yaml)。

`extra_parameters` 是所有进程模板共享的、operator 控制的只读部署常量。Expr 通过
`extra_parameters.<key>` 或 `extra_parameters["key"]` 引用；顶层 key 必须是字面量并在 controller
启动时存在。它与 RPC 的 `parameters` 使用独立命名空间，不接受调用方覆盖，也不会加入公开的模板
Schema。它只对 `render` 表达式可见，不能改写静态 `command`、I/O 模式或输入模式。

值可使用 TOML string、integer、有限 float、boolean、array 和 table；日期/时间、`NaN`、`Inf`、NUL
及非法 UTF-8 会使准备失败。规范化值最多 4096 个节点、32 层，单个 collection 最多 4096 项，字符串
和 map key 总计最多 256 KiB。controller 在启动期深拷贝配置，之后所有并发渲染共享不可变副本。

任一非空 `extra_parameters` map 都会以规范化形式计入每个模板 revision；因此其中任意值变化都会更新
所有模板 revision，即使某个模板没有引用该 key。空 map 不改变旧模板 revision。值不会通过模板发现 API
返回，也不应写入日志或进程元数据。该字段不应保存 token 等凭据；当前若需要秘密，仍应使用受保护的
文件，而不是把明文写进 controller TOML。

MCP 字段首版不提供命令行覆盖，以免列表字段产生不明确的替换/追加语义。默认值如下：

| TOML | 默认值 |
| --- | --- |
| `mcp.enabled` | `false` |
| `mcp.listen_address` | `127.0.0.1:9444` |
| `mcp.endpoint_path` | `/mcp` |
| `mcp.allowed_origins` | `[]` |
| `mcp.allowed_host_capabilities` | `[]` |
| `mcp.max_request_bytes` | `1048576` |
| `mcp.max_response_bytes` | `4194304` |
| `mcp.max_concurrent_calls` | `16` |
| `mcp.requests_per_second` | `20` |
| `mcp.request_burst` | `40` |
| `mcp.default_tool_timeout` | `30s` |
| `mcp.max_tool_timeout` | `5m` |
| `mcp.tool_list_page_size` | `100` |

`mcp.max_response_bytes` 可配置范围为 `16384` 到 `67108864`；下限保证基础 JSON-RPC/tool error envelope
以及最小 binary Resource 都有可用空间。

启用 MCP 时 `definition_files` 至少包含一个以 `.mcp.yaml` 结尾的普通文件，且文件物理路径必须位于
workspace 之外；最终符号链接、重复物理文件以及 `.mcp.yml`/`.mcp` 扩展名都会被拒绝。MCP 强制要求
全局 token，并复用全局 TLS。MCP 与 gRPC 使用不同 listener；非 loopback 明文监听继续受
`allow_insecure_remote` 限制。可用 host capability 为 `controller.read`、`files.read`、`files.write`、
`files.delete`、`processes.read`、`processes.start`、`processes.signal`、`processes.delete`、
`process_templates.read` 和 `process_templates.start`。仓库示例不默认使用两个 delete capability。

全局允许 `files.read` 时，MCP endpoint 还会发布 `workspace:///{+path}` Resource template。单次读取只接受
workspace 内普通文件，内容以 binary resource 返回；原始文件字节上限根据 `max_response_bytes` 扣除
JSON-RPC/base64 开销后计算，因此不会依赖响应中间件截断。`file.read_text`、`file.read_range` 和
`process.logs*` 示例也使用较低的 tool 参数上限，最终 structured/text 双份结果仍会在发送前按实际编码大小校验。

首版安全 definition opener 使用 Linux `O_NOFOLLOW` 与 fd identity；在非 Linux 平台启用 MCP 会
fail closed，普通 gRPC controller 在 MCP 关闭时不受影响。

## 校验

以下命令解析、合并并校验配置，不启动 listener：

```bash
remote-code-controller --config /etc/remote-code/controller.toml --check-config
```

成功时输出 `configuration OK`。日志尺寸包含 segment 与索引；segment 和单进程上限不得小于
256 KiB，总日志上限不得小于单进程上限，保留周期不能为负数，单进程观察者上限为 1–1024。
校验还包含 schema、字段类型、范围、TLS 配对与证书内容、明文远端监听策略、workspace 目录、token
文件、全部进程模板 YAML/JSON Schema/Expr，以及全部 MCP YAML/JSON Schema/Expr/capability。listener
是否可绑定以及 runtime 目录创建仍在实际启动时验证；检查过程不会渲染模板、执行 tool 或调用 host
function。
