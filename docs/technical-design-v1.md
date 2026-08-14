# Remote Code 文件与进程控制技术方案

## 1. 方案概览

首版采用单进程 controller、长连接交互式 CLI 和版本化 gRPC API：

```mermaid
flowchart LR
    T[本地终端/PTY] --> R[remote-code REPL]
    R --> C[pkg/client]
    C <-->|gRPC streaming + optional TLS/token| G[FileService]
    G --> F[internal/files.Service]
    F --> O[os.Root]
    O --> W[(remote workspace)]
    C <-->|gRPC unary| P[ProcessService]
    P --> PR[process registry]
    PR --> X[PTY/pipe process groups]
```

CLI 的“当前目录”只是本地 REPL 状态。发送 RPC 前，CLI 将当前目录与用户参数合成为工作区相对路径；controller 不信任客户端，仍独立做路径和操作校验。

## 2. 语言、版本与依赖

- Go `1.26`：使用标准库 `os.Root`，以打开的目录句柄限制文件访问范围。
- Protocol Buffers `proto3`。
- `google.golang.org/grpc` `v1.76.0`：gRPC transport、状态码、TLS credentials 和 health service。
- `google.golang.org/protobuf` `v1.36.10`：protobuf runtime 与代码生成器。
- `github.com/chzyer/readline` `v1.5.1`：交互提示符、历史、Ctrl-C/EOF 处理。
- `github.com/google/shlex` `v0.0.0-20191202100458-e7afc7fbc510`：按 shell 引号规则拆分命令，但不执行 shell。
- `github.com/creack/pty` `v1.1.24`：在 Linux 上创建 PTY、独立 session 和控制终端。
- `github.com/pelletier/go-toml/v2` `v2.4.3`：严格解析 controller TOML 配置并提供带源码位置的错误。
- `github.com/bufbuild/buf` `v1.57.2`：无需系统 `protoc` 的可复现 protobuf 生成入口。
- `protoc-gen-go` `v1.36.10`、`protoc-gen-go-grpc` `v1.5.1`：生成 Go message 与 service stub。

版本固定在 `go.mod`、`Makefile` 和 Buf 配置中。生成代码提交到仓库；`make generate` 将工具安装到仓库忽略的 `.tools/bin`，不依赖全局安装。

## 3. 目录与职责

```text
api/remote/code/v1/
  remote_code.proto       API 源定义
  remote_code.pb.go       message 生成代码
  remote_code_grpc.pb.go  gRPC 生成代码
cmd/controller/           controller 入口
cmd/remote-code/          REPL 入口
internal/auth/            bearer token 服务端拦截器和客户端凭据
internal/cli/             命令解析、虚拟 cwd、展示与命令执行
internal/files/           工作区文件操作和 gRPC FileService
internal/process/         进程注册表、PTY/pipe、进程组、信号与回收
internal/server/          gRPC server 装配、TLS 与生命周期
pkg/client/               可复用 typed client、流式上传下载
docs/                     需求和技术方案
configs/                  不含凭据的安全示例配置
```

`internal/files` 不依赖 CLI，核心行为可用单元测试直接验证。`pkg/client` 隐藏流帧协议、摘要校验和本地原子下载细节。

## 4. gRPC API

protobuf package 为 `remote.code.v1`，Go package 为 `codev1`。

### 4.1 ControllerService

```protobuf
service ControllerService {
  rpc GetInfo(GetInfoRequest) returns (GetInfoResponse);
}
```

`GetInfoResponse` 返回 controller 版本、API 版本、工作区显示名、最大上传字节数和活动进程上限。它既是能力查询，也是 CLI 进入 REPL 前的连接探测。绝不返回工作区绝对路径。

### 4.2 FileService

```protobuf
service FileService {
  rpc Stat(StatRequest) returns (StatResponse);
  rpc List(ListRequest) returns (ListResponse);
  rpc Tree(TreeRequest) returns (TreeResponse);
  rpc Upload(stream UploadRequest) returns (UploadResponse);
  rpc Download(DownloadRequest) returns (stream DownloadResponse);
  rpc Remove(RemoveRequest) returns (RemoveResponse);
  rpc Move(MoveRequest) returns (MoveResponse);
  rpc Chmod(ChmodRequest) returns (ChmodResponse);
  rpc Mkdir(MkdirRequest) returns (MkdirResponse);
}
```

`FileInfo` 包含相对路径、名称、类型、大小、Unix 权限位、修改时间和可选符号链接目标。时间使用 `google.protobuf.Timestamp`。`TreeResponse.root` 是递归的 `TreeNode`，每个节点包含一个 `FileInfo` 和有序的 `children`。

### 4.3 ProcessService

```protobuf
service ProcessService {
  rpc StartProcess(StartProcessRequest) returns (StartProcessResponse);
  rpc ListProcesses(ListProcessesRequest) returns (ListProcessesResponse);
  rpc SignalProcess(SignalProcessRequest) returns (SignalProcessResponse);
}
```

`ProcessInfo` 返回 UUID、逻辑名称、OS PID、PTY/pipe 模式、具体命令、参数、虚拟工作目录、环境覆盖 key、状态、时间戳以及可选退出码/退出信号。`StartProcessRequest.environment` 是覆盖 map，value 不会出现在响应或磁盘元数据。`ListProcessesRequest.all` 区分活动进程与完整历史。`ProcessReference` 用 oneof 明确区分 UUID、名称与 PID。`SignalProcess.wait=true` 使用 RPC context 作为等待上限。完整设计见[通用进程管理详细设计 v1](process-management-design-v1.md)。

### 4.4 上传流

`UploadRequest` 是 oneof 帧：第一帧必须是 `UploadMetadata`，后续帧只能是 `bytes chunk`。

元数据包括：目标路径、总大小、期望 SHA-256、权限、是否覆盖。块大小由客户端固定为 64 KiB，服务端还会限制单帧和累计大小。服务端响应最终 `FileInfo`、实际字节数和 SHA-256。

状态约束：

```text
START -> METADATA -> CHUNK* -> CLIENT_EOF -> VERIFY -> PUBLISH -> RESPONSE
                    |              |            |
                    +------ any error ----------> CLEANUP TEMP
```

元数据缺失、重复或位于 chunk 之后返回 `InvalidArgument`；超过限制返回 `ResourceExhausted`；大小或摘要不一致返回 `DataLoss`。

### 4.5 下载流

`DownloadResponse` 是 oneof 帧：先发送 `DownloadMetadata`，再发送多个 chunk，最后发送 `DownloadSummary`。summary 包含实际总字节数和 SHA-256。客户端必须看到 metadata 与 summary，且必须核对计数和摘要后才能发布本地文件。

服务端从安全打开的文件描述符顺序读取 64 KiB 块，避免将整个文件载入内存。目录、设备或其他非普通文件返回 `FailedPrecondition`。

## 5. 路径安全与文件实现

### 5.1 路径规范

进入 `os.Root` 前执行协议级校验：

1. 拒绝空字节与绝对路径。
2. 将反斜杠视为普通 Linux 文件名字符，不替用户转换平台语义。
3. 使用 slash/path 规则清理路径；如果清理前存在 `..` 分量则直接拒绝，不允许“先出界再回来”。
4. 协议根统一表示为 `.`；响应路径统一转为 `/` 分隔。
5. destructive RPC 显式拒绝 `.`。

所有真实操作通过 `os.OpenRoot(workspace)` 返回的 `*os.Root` 完成。该 API 在 Linux 上以目录句柄逐分量解析，并拒绝指向根外的绝对或相对符号链接，因此安全性不依赖 `filepath.Join` 后的字符串前缀判断。

### 5.2 上传

1. 验证目标与元数据，按需验证父目录已经存在。
2. 在目标同一目录创建随机、排他的 `.remote-code-upload-*` 临时文件。
3. 流式写入，同时计算 SHA-256 并执行累计大小限制。
4. 校验声明大小和摘要，`fsync` 并通过文件描述符设置 `0777` 内权限。
5. 覆盖模式使用根目录内 rename 原子替换；非覆盖模式使用同文件系统 hard-link 发布，从而原子地获得 `AlreadyExists` 行为，然后删除临时链接。
6. 任意错误或 context 取消都关闭并删除临时文件。

### 5.3 下载

通过 `Root.Open` 获取文件描述符，再对描述符 `Stat` 并确认是普通文件。流式读取时计算摘要；文件在下载期间变化会表现为最终大小与起始 metadata 不一致，summary 携带真实结果，客户端拒绝发布不一致的本地文件。

### 5.4 列表、删除、移动与权限

- 列表通过根目录安全打开目录，并对直接子项做 `Lstat` 语义展示；名称排序保证输出稳定。
- `Tree` 返回递归的 `TreeNode { file, children[] }`，而不是服务端拼装的展示字符串。目录按名称排序，符号链接作为叶子节点且不跟随；遍历限制为 3000 个节点和 128 层。
- 删除禁止根路径；递归删除使用 `Root.RemoveAll`，非递归使用 `Root.Remove`。
- 移动的源和目标都经相同校验并由 `Root.Rename` 完成。非覆盖模式先判断目标不存在；普通文件上传另有 hard-link 保证原子 no-replace。通用目录 move 的 no-replace 在首版存在同一受信 workspace 内的并发竞态，记录为已知限制。
- `chmod` 先通过 `Root.Open` 获得安全文件描述符，再调用 `File.Chmod`，避免对可被替换的路径直接 chmod；只保留 `0777`。

## 6. CLI 设计

`readline` 负责终端模式和历史输入，`shlex` 只负责词法拆分。命令表为显式 allowlist，未知命令不会发送给 controller。

`readline.AutoCompleter` 根据光标左侧内容补全命令、选项、权限参数以及本地/远端路径。远端候选使用最长 2 秒的 `List` RPC，错误只会使本次候选为空，不影响 REPL；带空格或引号的路径按当前引用状态转义。

每条命令创建独立 context。RPC 错误按 gRPC status 展示为 `error: <message> (<Code>)`，随后回到提示符。`ls` 使用 `tabwriter`；时间以本地时区 RFC3339 显示；权限采用 `os.FileMode` 风格。

虚拟 cwd 使用 `/` 开头仅供展示，wire path 始终是相对路径。`cd ..` 可以回到父目录但不能高于 `/`。远端路径不做本地 glob 展开。

`cat` 将下载流写到终端限制 writer；超过 `--cat-max-bytes` 取消命令并提示使用 `download`。`download` 在本地目标同目录创建临时文件，校验完成后 chmod、sync、rename。

## 7. 连接、TLS 与认证

- controller 默认只监听 `127.0.0.1:9443`。
- 同时提供证书与私钥时创建 TLS gRPC server；只提供其中之一是配置错误。
- CLI 提供 CA 时使用 TLS transport credentials，否则使用 insecure credentials。
- token 文件去除首尾空白后必须非空。客户端以 `authorization: Bearer <token>` metadata 发送；服务端 unary 与 stream interceptor 都验证。
- 非 loopback 明文监听必须增加 `--allow-insecure-remote`，以防配置失误。该选项不改变风险，只表示操作者明确接受风险。

controller 可通过 `--config` 显式加载 schema v1 TOML。入口先把配置文件应用到内置默认值，
再把同一组 flag 绑定到合并后的字段，因此只有实际出现的命令行参数会覆盖文件值。decoder
拒绝未知字段；`--check-config` 完成解析、范围、workspace、TLS 配对和 token 文件校验后
退出。TOML 层只存在于 `cmd/controller`，`internal/server` 继续接收与来源无关的 typed
configuration。

## 8. 错误映射

| 场景 | gRPC code |
| --- | --- |
| 路径、mode、帧顺序不合法 | `InvalidArgument` |
| 未认证或 token 错误 | `Unauthenticated` |
| 文件不存在 | `NotFound` |
| 目标已存在 | `AlreadyExists` |
| 符号链接逃逸、系统权限拒绝 | `PermissionDenied` |
| 对目录下载、非递归删非空目录等状态冲突 | `FailedPrecondition` |
| 上传超过限制 | `ResourceExhausted` |
| 上传/下载大小或摘要不符 | `DataLoss` |
| context 取消/超时 | `Canceled` / `DeadlineExceeded` |
| 其他 I/O 故障 | `Internal` |

底层错误在 transport 边界统一转换，不把工作区绝对路径或敏感内容回传给客户端。

## 9. 并发和生命周期

`os.Root` 可供并发 goroutine 使用，service 本身不保存每个请求的可变共享状态。每次上传有独立临时文件和 hash。controller 设置 gRPC 最大消息大小，应用层再限制 chunk 与总上传大小。

进程服务以 mutex 保护 UUID/name/PID 索引、活动计数和有界历史。启动时先预留名称与活动名额，再创建持久化记录并调用 `exec.Start`；每个成功启动的命令立即建立唯一 reaper goroutine 调用 `Wait` 并原子记录退出结果。pipe 的 stdout/stderr 写入独立 frame log，PTY master 的合并输出写入 stdout frame log，防止子进程因输出缓冲写满而停住。

工作目录先通过 `os.Root` 安全打开并验证为目录；Linux 启动器使用 `/proc/self/fd/<fd>` 对已打开目录执行 child chdir，避免校验后符号链接替换。pipe 模式使用 `Setpgid`，PTY 模式使用 `setsid + controlling tty`，两者均以 PID 为进程组 ID。`SignalProcess` 只解析已注册且仍运行的记录，并对负 PGID 调用 `kill`。controller 关闭时注册表先拒绝新启动并发送 `TERM`，context 到期后发送 `KILL`；gRPC 随后停止，最后释放工作区句柄。

客户端传具体 executable、参数和环境覆盖，server 不调用 shell。该接口本身是通用远程代码执行能力，必须使用 TLS、认证、受限系统用户或额外隔离来保护。

## 10. 测试方案

- `internal/files` 单元测试：正常 CRUD、排序、权限、绝对路径、所有位置的 `..`、根删除、符号链接逃逸、内部符号链接、大小限制、摘要不一致、no-overwrite 和临时文件清理。
- `internal/process` 单元测试：真实 pipe/PTY、UUID/PID/name、工作目录边界、活动上限、多种信号、自动回收、强制关闭和并发注册/列表/终止。
- `pkg/client`/transport 集成测试：使用 loopback listener 和真实 gRPC server 完成文件与进程闭环，验证摘要、状态码和退出结果。
- `internal/cli` 单元测试：引号解析、虚拟 cwd 边界、mode、进程参数/引用/信号和补全。
- 全量执行 `go test ./...`、`go test -race ./...`、`go vet ./...` 与 `go build ./...`。

测试使用临时目录、确定性内容和内存/loopback server，不需要外部服务或 Agent 凭据。

## 11. 已知限制与后续演进

- `os.Root` 防止路径和符号链接逃逸，但不隔离 bind mount、设备文件或工作区内恶意文件系统；部署仍需 OS 级隔离。
- 通用 `mv` 的“检查后 rename”无法提供跨所有对象类型的强原子 no-replace；Linux 后续可用 `renameat2(RENAME_NOREPLACE)` 增强。
- 首版传输单文件且不续传；后续可加入 upload session、offset 与分块摘要。
- 首版 CLI 同步执行一条命令；并发任务、进度条和机器可读模式留待后续版本。
- PTY/pipe attach、日志回放 RPC、窗口 resize、Agent 语义和断线续传保持为后续 milestone；当前进程 API 负责启动、持久化元数据/输出、列表、信号和回收。
