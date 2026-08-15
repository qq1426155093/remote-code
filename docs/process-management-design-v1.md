# 通用进程管理详细设计 v1

本文实现 [通用进程管理需求 v1](process-management-requirements-v1.md)。

## 1. 组件与职责

```text
remote-code REPL
    │ typed gRPC
    ▼
ProcessService
    ├── validator       请求大小、name/env/cwd 校验
    ├── workspace root  使用 os.Root 固定工作区并阻止 symlink 逃逸
    ├── registry        UUID/name/PID 索引、状态机、并发限额
    ├── runner          Linux pipe/PTY、独立进程组、signal、Wait
    └── record store    JSON 状态、v2 tagged segment/index、启动恢复
```

registry 的所有索引和 `ProcessInfo` 状态由同一 mutex 保护。磁盘 I/O 不长时间持锁；
状态转换先在锁内确定，再用不可变快照写盘。每个进程只有一个 reaper goroutine 调用
`Wait`，只有 reaper 关闭 `done` channel。

## 2. protobuf 设计

- `StartProcessRequest.environment` 使用 `map<string,string>`；
- `ListProcessesRequest.all` 控制是否包含终态；
- `DeleteProcessRequest.process` 复用 `ProcessReference`，响应返回删除前的终态快照；
- `ProcessInfo` 增加 `created_at`、`environment_keys`；
- `ProcessState` 增加 `FAILED` 和 `LOST`；
- `GetInfoResponse.process_commands` 被保留字段号并移除，因为 server 不再维护命令
  allowlist。

字段使用追加编号，既有编号和枚举值不重用。现有 Start/List/Signal RPC 名称不变。

## 3. 请求校验与命令解析

server 对 `command` 和每个参数只做结构及大小校验，不使用 shell。bare command 通过
Go `exec.Command` 按 controller PATH 查找；含 `/` 的相对命令在已固定的工作目录中执行；
绝对命令按原路径执行。

工作目录先进行纯词法校验，再通过 `os.Root.Open` 打开目录句柄。runner 将 child cwd
设置为 `/proc/self/fd/<fd>`，从而固定已经校验的目录对象，避免校验后替换目录或符号
链接的竞态。

环境变量先从 `os.Environ()` 建立 map，再应用请求覆盖，最后按 key 排序生成 child env。
key 必须匹配 `[A-Za-z_][A-Za-z0-9_]*`。只把排序后的覆盖 key 放入 ProcessInfo 和
metadata；value 只存在于启动请求和 child env 内存中。

## 4. 创建与启动事务

启动按以下顺序执行：

1. 校验请求并打开工作目录；
2. 分配 UUID，派生缺省名称和创建时间；
3. 在锁内检查关闭状态、活动上限和活动名称冲突，预留 registry 条目与活动名额；
4. 创建 `<uuid>` 目录、metadata/status 和 v2 日志目录；
5. 启动 runner；
6. 启动成功后在锁内登记 PID、转换为 RUNNING，并写 RUNNING status；
7. 启动 reaper，返回 ProcessInfo。

步骤 4 失败时撤销内存预留并删除尚未公开的 UUID 目录。步骤 5 失败时保留记录，转换为
FAILED、释放活动名额和名称，关闭日志，并持久化失败状态。错误响应包含 UUID，但不包含
命令参数、环境 value 或操作系统错误细节。

名称和 PID 索引都指向当前可见的最新匹配记录；新启动的活动进程覆盖同名或 PID 历史
映射。删除最新记录时，从创建顺序逆向恢复下一个匹配项，以支持名称与 PID 复用。

## 5. runner 与进程组

PIPE：创建 stdin/stdout/stderr pipe，设置 `Setpgid`，启动后分别用 goroutine 将 stdout
和 stderr 复制到对应 frame writer。stdin 当前立即关闭，明确表示 v1 不支持交互输入。

PTY：使用 `creack/pty` 创建 session/controlling terminal；PTY master 的合并输出复制到
stdout frame writer，stderr 文件保持空。缺失时补充 `TERM=xterm-256color`。

runner 提供 `wait()`：先调用 `cmd.Wait()`。PIPE 等两个 copier 完成；PTY 在 leader 退出
后关闭 master 使 copier 结束，再等待 copier。然后关闭日志文件。这样 status 的 EXITED
原子更新发生在最后输出帧已经完成之后。

signal 使用 `kill(-pid, signal)` 作用于 process group。controller 不允许通过 API 发送
任意数字信号，以维持跨客户端的稳定枚举契约。

## 6. 日志实现

stdout/stderr 共用进程级 v2 append log 和 mutex，每个成功记录获得跨双流递增的逻辑 offset。
写入按换行和 64 KiB 上限切分，同时记录逻辑行首 offset；PTY 合并数据全部标记为 stdout。
segment 轮转后使用稀疏 offset 索引定位回放，使用 stdout/stderr 行索引反向选择 tail。

`ObserveProcessLogs` 在锁内捕获快照末尾和通知 channel，从磁盘回放后以同一游标进入 follow，
避免交接丢失。观察期间 segment 不会被保留任务删除。CRC、footer、索引重建、v1 双文件迁移、
尺寸/周期配置和错误语义见
[进程日志观测详细设计](process-log-observation-design-v1.md)。原 12-byte framed writer 仅保留用于
读取并迁移历史 v1 记录。

## 7. JSON 存储与崩溃恢复

存储层使用私有 versioned structs，不直接 JSON marshal protobuf，以避免 protobuf JSON
名称或 presence 变化影响磁盘兼容性。

- metadata 创建时使用 `O_CREATE|O_EXCL`，写完 `Sync`；
- status 使用 UUID 目录中的随机临时文件，写完 `Sync`、关闭、chmod 后 rename；
- 创建 metadata 和首次 status 后 sync UUID 目录；
- 目录及文件打开拒绝符号链接，runtime 根必须为真实目录且权限被收紧为 `0700`。

启动时枚举名称严格为 UUID 的目录，读取并校验 metadata/status。单条损坏记录被跳过并
写 controller 诊断日志，不阻止其它记录恢复。磁盘中的 STARTING/RUNNING 状态立刻原子
更新为 LOST，清除可控制 PID，记录恢复时间和原因。EXITED/FAILED/LOST 直接加载。

加载记录按创建时间、UUID 排序，超过 4096 条时只把最新 4096 条放入内存；不会删除
磁盘目录。恢复记录会重建查询索引，但没有 `runningCommand`，因此永远不会进入 signal 路径。

`DeleteProcess` 在 registry 锁内解析引用并确认进程处于终态，再与日志保留任务互斥地删除
严格 UUID 命名的目录并同步 runtime 根目录，最后清除 UUID/name/PID/order 索引。活动进程
或仍有日志 observer 的记录返回 `FailedPrecondition`；符号链接或非目录记录不会被递归删除。

## 8. CLI 与客户端

public client 的 `StartProcess` 接收一个选项结构，避免继续增长位置参数；
`ListProcesses(ctx, all)` 显式传递过滤条件，`DeleteProcess` 删除终态历史。REPL parser 支持重复 `-e`/`--env`，遇到
`--` 后停止解析选项。命令启动成功后打印 name、UUID、PID、mode 和 cwd。

`ObserveProcessLogs` 的 client 选项以互斥指针表达 offset/tail，返回原始 gRPC server stream。
REPL `logs` 支持 `-f`、`-n`、`--offset`、`--stdout` 和 `--stderr`，并保持二进制 chunk 原样输出。

`exec` 未设置 cwd 时使用 REPL 保存的工作区相对 cwd；显式 `--cwd` 也通过与文件命令
相同的远端路径解析器相对于当前 cwd 解析。`ps` 仅接受可选 `-a`/`--all`，`forget`
接受一个 UUID、名称或带前缀的 PID 引用。

Tab completion 增加 `exec` 的选项提示、`--cwd` 目录补全和 kill/ps 参数提示。具体命令
及其参数由目标程序定义，v1 不尝试从 controller 主机 shell 动态生成补全。

## 9. 并发与错误映射

- invalid fields/path -> `InvalidArgument`；
- cwd 不存在 -> `NotFound`，非目录 -> `FailedPrecondition`，逃逸/无权限 ->
  `PermissionDenied`；
- 活动名称冲突 -> `AlreadyExists`；并发或历史索引饱和 -> `ResourceExhausted`；
- 已退出/丢失进程 signal、活动进程 delete、删除正在观察的日志 -> `FailedPrecondition`；
  引用不存在 -> `NotFound`；
- 不支持的平台 -> `Unimplemented`；无法创建安全持久化记录 -> `Internal`。

所有返回给客户端和写入 status 的错误都经过清理，不包含 env value、完整 controller
路径或命令输出。

## 10. 测试策略

单元测试覆盖 validator、frame codec、原子 JSON store、恢复、状态转换、过滤、名称复用、
普通退出、启动失败、PIPE 双流、PTY 合并流、进程组信号以及工作区逃逸。日志测试覆盖 offset、
tail 行、回放/follow 交接、轮转保留、残缺尾恢复、索引重建、符号链接和 v1 迁移。gRPC 集成测试从
client 启动携带 cwd/env 的命令，检查 PID/UUID、ps 过滤、kill 和日志流。CLI 测试覆盖
`cd` 后 `exec`、`ps -a`、env/`--` 解析和输出。registry、copier/frame writer 使用 race
test 验证。
