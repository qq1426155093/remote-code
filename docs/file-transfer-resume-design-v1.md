# 文件断点续传设计 v1

## 1. 范围与兼容性

断点续传是应用层协议能力，不依赖 gRPC 对流 RPC 的透明重试。原有 `Upload` 和 `Download` RPC
保持不变；新 RPC 是 `remote.code.v1.FileService` 的加法扩展。客户端通过
`GetInfoResponse.file_transfers` 判断能力，旧服务端字段为空时自动回退旧协议。

首版支持单个普通文件、顺序 chunk、网络断线、CLI 重新执行和 controller 进程重启。不支持并行分片、
乱序写、目录传输、压缩和增量同步。

## 2. 上传协议

上传使用服务端持久化 session：

1. `CreateUploadSession` 接收幂等 `request_id`、目标路径、大小、完整 SHA-256、mode 和覆盖选项。
2. 相同 request ID 与相同元数据返回原 session；元数据不同返回 `AlreadyExists`。
3. `TransferUpload` 第一帧必须是 `open`，服务端以 `ready.committed_offset` 返回权威断点。
4. 后续 `chunk` 必须顺序、连续，携带起始 offset、数据和块 SHA-256。
5. 服务端按配置的字节或时间间隔持久化 checkpoint；只有临时文件同步、session 元数据原子更新后，
   才发送新的 `committed_offset`。
6. `finish` 只在已接收声明大小时合法。服务端从临时文件头重新计算完整 SHA-256、设置权限并同步，
   最后以同目录 rename/link 原子发布。
7. `GetUploadSession` 用于断线恢复和确认丢失的最终响应；`AbortUploadSession` 幂等取消未完成会话。

`committed_offset` 是下一个应发送的字节位置，区间 `[0, committed_offset)` 在 controller 重启后仍然
有效。临时文件中超过该位置的尾部在恢复时截断。

上传稳定状态为 `OPEN`、`FINALIZING`、`COMPLETE`、`FAILED`、`ABORTED` 和 `EXPIRED`。
`COMPLETE` 等终态保留短期 tombstone，使 finish 或响应丢失仍可安全查询。上传目标在完整校验前保持
不变，活动 session 的临时文件不会通过 `Stat`、`List` 或 `Tree` 暴露。

## 3. 下载协议

下载不保存服务端 session。`DownloadRangeRequest` 包含路径、offset、可选 revision 和前缀 SHA-256：

- 首次请求 offset 为零，revision 和前缀摘要必须为空。
- 续传请求 offset 大于零，必须携带首次 metadata 返回的 32 字节 revision，以及本地 part 文件
  `[0, offset)` 的 SHA-256。
- revision 绑定 workspace、规范化路径、文件大小、mode、mtime 和平台文件身份，并由状态目录中的
  持久随机密钥做 HMAC。
- 服务端先验证 revision，再顺序重读远端前缀并比较摘要；通过后才发送 metadata 和剩余数据。
- 每个响应 chunk 携带 offset、数据和块 SHA-256。summary 携带总大小、完整 SHA-256 和 revision。
- 发送 summary 前再次检查文件 revision；发生正常的替换、截断、增长或原地写入时终止传输。

本地 CLI 在最终目标同目录保留权限为 `0600` 的随机 `.part` 文件，并在本地 transfer state 目录保存
revision 和 durable offset。写入 part、同步文件、原子更新状态后才能推进本地 checkpoint。完整大小和
SHA-256 校验成功后设置 mode、同步并 rename 到最终路径。

该一致性模型检测正常并发修改，但不是文件系统快照。具有工作区直接写权限的恶意进程若能同时修改
内容并伪造全部文件身份元数据，不在本协议的隔离保证内；严格时间点快照需要 reflink/copy staging。

## 4. 持久化与清理

服务端状态位于：

```text
<runtime_directory>/file-transfers/
  revision.key
  uploads/<upload-id>.json
```

session 状态只保存工作区 ID、相对目标/临时路径、大小、摘要、mode、offset、状态和时间戳；不保存认证
token 或文件内容。目录权限为 `0700`，状态与 key 为 `0600`。临时上传文件仍在目标同目录，避免发布时
跨文件系统。

GC 在启动时和运行期间清理过期 session。活动流不会被 GC；TTL 只因实际上传进展刷新。清理临时文件
必须有有效 session 记录且对象仍是普通文件，不能仅凭文件名前缀删除。

默认 `/run` 通常只能保证 controller 进程重启，不保证机器重启后保留。需要跨机器重启续传时，部署方
必须把 `runtime_directory` 放到持久磁盘。

## 5. 资源与错误

controller 限制单文件大小、活动 session 数、所有活动 session 的声明大小总和、chunk 大小和并发下载
数。staging quota 按声明大小预留，防止只创建 session 而不上传的磁盘耗尽攻击。

协议使用结构化 `FileTransferError` 返回 `OFFSET_MISMATCH`、`FILE_CHANGED`、`PREFIX_MISMATCH`、
`SESSION_STATE` 和 `ACTIVE_TRANSFER`。客户端不得解析错误文本。`Unavailable`、非用户取消导致的
`Canceled`/`DeadlineExceeded` 和短暂 `Aborted` 可重试；格式、权限、quota 与数据完整性错误不可自动
重试。

## 6. 安全约束

- 所有服务端路径仍通过现有工作区相对路径和 `os.Root` 校验。
- upload ID 使用随机 192-bit 值且没有枚举 RPC；它按 bearer capability 对待，不写日志。
- 不记录 session ID、SHA-256、认证 token 或上传/下载内容。
- 同一目标最多一个活动 resumable upload；对目标、临时文件或祖先目录的破坏性文件操作返回
  `FailedPrecondition`。
- 客户端只删除状态记录中、最终目标同目录且符合工具生成命名格式的 part 文件。

## 7. 验证要求

测试覆盖幂等创建、offset/chunk 摘要、断线、controller 重启、最终响应丢失、session 查询、临时文件
隐藏、远端文件变化、前缀校验和本地持久状态恢复。并发状态还必须通过 `go test -race ./...`。
