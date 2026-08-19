# 错误模型详细设计 v1

## 1. 范围

本文定义 controller 在 gRPC 与 MCP 两个入口返回错误时，如何让调用方以机器可读的方式识别**具体
条件**。它不改变任何 RPC 的请求/响应消息，也不改变现有的 status code 选择，只规定错误上附带的
detail。

认证与授权见[授权模型现状与演进](authorization-model-v1.md)；各服务的语义边界见对应的需求与
详细设计文档。

## 2. 问题

status code 标识失败的**类别**，不标识**条件**。当前实现中：

- `FailedPrecondition` 覆盖 20 余种互不相同的情况，仅 `ProcessService` 就包含"进程不在运行"、
  "输入未在启动时启用"、"输入已关闭"、"日志正在被观察"、"模板 revision 不匹配"；
- `AlreadyExists` 同时表示"活动进程名已占用"、"输入已有 attached writer"、"上传目标已存在"；
- `ResourceExhausted` 同时表示进程数、历史条目数、观察者数和各类字节上限。

调用方若要区分这些条件，只能匹配 message 文本。message 是给人看的、可以随时改写，把它当作契约会
在措辞调整时静默失效。

同时仓库里已经存在两套互不一致的结构化错误：

| 位置 | 形式 |
| --- | --- |
| `internal/process/log_service.go`、`internal/controllerlog/service.go` | `google.rpc.ErrorInfo`，`Reason` 为字符串常量，`Domain` 为 `remote.code.v1` |
| `internal/files/transfer_upload.go` | 自定义 `codev1.FileTransferError`，`reason` 为 proto enum |

两者都只覆盖各自的少数路径，其余错误没有任何机器可读标识。

## 3. 契约

**每一个调用方可能据以改变行为的错误**，都必须附带一个 `google.rpc.ErrorInfo` detail：

- `Domain` 恒为 `remote.code.v1`；
- `Reason` 是 `internal/rpcerror` 中声明的常量，`UPPER_SNAKE_CASE`；
- `Metadata` 可选，仅承载调用方恢复所需的结构化值（例如仍可读的 offset 区间）。**不得包含任何
  凭据、提示词或文件内容**——detail 与 message 跨越同一个边界。

规则：

1. **Reason 是 API 契约。** 可以新增，不得重命名，不得把既有值改指另一种条件。
2. **message 不是契约。** 保持人类可读，可以随时改写；调用方不应匹配它。
3. **code 不变。** 引入 reason 不改变任何既有 status code，避免破坏现有客户端。
4. **调用方必须容忍未知或缺失的 reason。** 新 controller 可能报告旧客户端不认识的值；纯校验类
   错误（多数 `InvalidArgument`）不携带 reason，因为调用方除了展示消息之外无事可做。
5. **一个错误至多一个 domain 内的 ErrorInfo。** 读取方取第一个 `Domain` 匹配的 detail。

服务端统一通过 `internal/rpcerror` 构造：

```go
return rpcerror.Errorf(codes.FailedPrecondition, rpcerror.ProcessInputClosed, "process input is closed")

return rpcerror.ErrorfWithMetadata(codes.OutOfRange, rpcerror.LogOffsetOutOfRange, map[string]string{
    "earliest_offset": "12", "next_offset": "40",
}, "%s", cause)
```

detail 序列化失败时降级为不带 detail 的 status，而不是让调用失败：code 与 message 仍然送达。

## 4. 客户端

`pkg/client` 导出：

```go
func Reason(err error) string
```

返回 controller 附带的 reason，没有则返回空字符串。同时导出与服务端一一对应的 `Reason*` 常量。
典型用法是在同一个 code 的多种条件之间分派：

```go
_, err := remote.OpenProcessInput(ctx, reference)
switch client.Reason(err) {
case client.ReasonProcessInputDisabled:
    // 进程启动时没有启用输入，重启进程才能修复。
case client.ReasonProcessInputAttached:
    // 已有 writer，等待或改为观察日志。
case client.ReasonProcessNotRunning:
    // 进程已退出。
}
```

## 5. 兼容性

- 既有的两个 reason 字符串 `LOG_OFFSET_OUT_OF_RANGE` 与 `CONTROLLER_LOG_OFFSET_OUT_OF_RANGE`
  已经在线上，值原样保留，迁移到 `rpcerror` 后 wire 输出不变。
- `FileTransferError` 保留在契约中，未废弃。传输错误现在**同时**携带该 enum 与 ErrorInfo：前者
  服务已经读取它的客户端，后者让传输错误与其它服务用同一种方式判别。
- 旧 controller 不返回 reason，`Reason()` 返回空字符串，调用方应回退到 code。
- 不涉及 proto 变更，因此没有 wire 兼容风险。

## 6. Reason 目录

| Reason | Code | 条件 |
| --- | --- | --- |
| `PROCESS_SERVICE_SHUTTING_DOWN` | `Unavailable` | controller 正在关闭，不再接受新进程 |
| `ACTIVE_PROCESS_LIMIT_REACHED` | `ResourceExhausted` | 活动进程数达到 `max_processes` |
| `PROCESS_NAME_IN_USE` | `AlreadyExists` | 逻辑进程名已被一个活动进程占用 |
| `PROCESS_HISTORY_LIMIT_REACHED` | `ResourceExhausted` | 进程历史条目数达到上限且无可回收项 |
| `PROCESS_NOT_RUNNING` | `FailedPrecondition` | 目标进程不处于 RUNNING |
| `PROCESS_NOT_TERMINAL` | `FailedPrecondition` | 进程仍活动，需先停止才能删除历史 |
| `PROCESS_ALREADY_EXITED` | `FailedPrecondition` | 发送信号时进程已退出 |
| `WORKING_DIRECTORY_OPEN_FAILED` | 随原因变化 | 无法打开工作目录 |
| `WORKING_DIRECTORY_NOT_DIRECTORY` | `FailedPrecondition` | 工作目录路径不是目录 |
| `PROCESS_NOT_PTY` | `FailedPrecondition` | 该操作要求 PTY，进程未使用 PTY |
| `PTY_INPUT_CLOSE_UNSUPPORTED` | `FailedPrecondition` | PTY 没有独立的写端关闭操作 |
| `PROCESS_INPUT_DISABLED` | `FailedPrecondition` | 进程启动时未启用受管输入 |
| `PROCESS_INPUT_CLOSED` | `FailedPrecondition` | 输入端点已关闭 |
| `PROCESS_INPUT_ATTACHED` | `AlreadyExists` | 输入已有 attached writer |
| `PROCESS_LOGS_UNAVAILABLE` | `FailedPrecondition` | 该进程没有持久化日志 |
| `PROCESS_LOGS_OBSERVED` | `FailedPrecondition` | 日志正在被观察，无法删除 |
| `PROCESS_LOG_OBSERVER_LIMIT_REACHED` | `ResourceExhausted` | 单进程观察者数达到上限 |
| `LOG_OFFSET_OUT_OF_RANGE` | `OutOfRange` | 请求的进程日志 offset 已不在保留区间 |
| `CONTROLLER_LOG_OFFSET_OUT_OF_RANGE` | `OutOfRange` | 请求的 controller 日志 offset 已不在保留区间 |
| `CONTROLLER_LOGS_UNAVAILABLE` | `FailedPrecondition` | controller 运行日志不可用 |
| `TEMPLATE_RENDER_FAILED` | `FailedPrecondition` | 模板未能渲染出合法的进程规格 |
| `TEMPLATE_REVISION_MISMATCH` | `FailedPrecondition` | 模板 revision 与请求期望不一致 |
| `TRANSFER_OFFSET_MISMATCH` | 随原因变化 | chunk offset 与已提交 offset 不一致；`metadata.expected_offset` 给出期望值 |
| `TRANSFER_FILE_CHANGED` | `FailedPrecondition` | 下载源在续传期间发生变化 |
| `TRANSFER_PREFIX_MISMATCH` | `FailedPrecondition` | 已下载前缀的摘要与远端文件不一致 |
| `TRANSFER_SESSION_STATE` | 随原因变化 | 上传 session 状态不允许该操作 |
| `TRANSFER_ACTIVE_TRANSFER` | `FailedPrecondition` | 路径上存在活动传输 |

## 7. 覆盖范围

本版本覆盖 `ProcessService`、`ControllerService` 的运行日志入口，以及文件传输的双 detail。

**尚未覆盖**：`internal/files` 中 `service.go`、`patch.go`、`range.go`、`text.go`、`resource.go`、
`transfer_download.go` 的非传输错误（约 30 处，含上传/写入目标已存在、目标不是普通文件、
patch 摘要不匹配、各类字节与树规模上限）。这些条件同样值得 reason，按同一契约在后续变更中补齐；
在此之前调用方对这些路径仍只能依赖 code。

多数 `InvalidArgument` 有意不携带 reason，见 3. 规则 4。

## 8. 验证要求

至少覆盖：reason 随 code 与 message 一并送达且可被提取；metadata 往返正确；nil、非 status 错误、
无 detail 的 status、以及其它 domain 的 ErrorInfo 都返回空 reason；reason 常量取值唯一且格式合法；
共用同一个 code 的两种条件在客户端可区分；传输错误同时携带 `FileTransferError` 与 ErrorInfo；
reason 能穿过真实 gRPC 边界。
