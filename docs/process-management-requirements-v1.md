# 通用进程管理需求 v1

## 1. 目标与范围

controller 提供与 Claude Code 无关的通用进程管理能力。任何通过认证并获准调用
`ProcessService` 的客户端都可以在 controller 工作区内启动命令、查询进程、发送信号，
并在 controller 重启后查询已经落盘的历史记录。

本版本包含：

- 以普通 pipe 或 PTY 两种 I/O 模式启动进程；
- 为进程指定逻辑名称、工作目录、命令、参数和环境变量；
- 返回稳定 UUID 和操作系统 PID；
- 持久化元数据、状态以及带时间戳的 stdout/stderr 输出块；
- 按逻辑 offset 或最后 N 行回放日志，并可无缝跟随到进程退出；
- 列出当前活动进程，或列出活动及历史进程；
- 按 UUID、名称或 PID 向整个进程组发送受支持的 POSIX 信号；
- controller 始终 `Wait` 直接子进程，避免僵尸进程；
- `remote-code` REPL 提供 `exec`、`ps`、`ps -a` 和 `kill` 命令，并让
  `cd` 改变的远端当前目录成为后续 `exec` 的默认工作目录。

本版本不包含实时 attach、交互输入、终端尺寸变更、CPU/内存限额和
跨 controller 重启重新接管仍存活的操作系统进程。落盘日志格式为后续日志回放接口提供
稳定基础。

## 2. gRPC 契约

### 2.1 启动进程

`StartProcess` 请求包含：

- `name`：可选逻辑名称。必须匹配 `[A-Za-z0-9][A-Za-z0-9._-]{0,63}`；为空时由
  server 根据命令名和 UUID 自动生成。活动进程名称不可重复；已结束的历史记录不阻止
  名称复用。
- `command`：必填的具体命令，不再是 server allowlist 别名。可为 PATH 中的命令、
  绝对可执行路径或相对于工作目录的路径。绝对命令路径只用于选择可执行文件，不改变
  工作目录边界规则。
- `arguments`：原样传给子进程，不经 shell 拼接或解释。
- `working_directory`：工作区内的相对路径；空值表示工作区根目录。拒绝绝对路径、
  `..` 路径分量和符号链接逃逸。
- `io_mode`：`PIPE` 或 `PTY`；未指定时默认 `PIPE`。
- `environment`：环境变量覆盖表。子进程继承 controller 环境后应用覆盖。key/value
  均不得含 NUL，key 必须是合法环境变量名。API 和持久化元数据只暴露 key，绝不记录
  value，避免将 token 等秘密写入磁盘。

成功响应至少包含 UUID、逻辑名称、PID、I/O 模式、状态、命令、参数、工作目录、
环境变量 key 和创建/启动时间。RPC 返回成功时状态必须为 `RUNNING`。

启动名额在 `STARTING` 阶段即计入并发上限。创建持久化记录后启动失败的进程保留为
`FAILED` 历史记录，RPC 返回错误并在错误消息中给出记录 UUID，方便审计。

### 2.2 列出进程

`ListProcesses(all=false)` 只返回 `STARTING`/`RUNNING` 进程；
`ListProcesses(all=true)` 还返回 `EXITED`、`FAILED`、`LOST` 历史记录。结果按创建时间
稳定排序。历史记录受内存索引上限约束，但磁盘记录不因索引淘汰而删除。

### 2.3 发送信号

`SignalProcess` 支持 UUID、名称和 PID 三种引用，信号支持 HUP、INT、QUIT、TERM、
KILL、USR1、USR2、STOP、CONT。未指定信号默认 TERM。信号发送给进程组而不只发送给
leader，以关闭由命令创建的子进程。`wait=true` 时 RPC 等待 leader 被回收，等待时间受
RPC context 约束。

只有本次 controller 运行期间处于 `RUNNING` 的进程可以被信号控制。`EXITED`、
`FAILED` 和 `LOST` 返回 failed-precondition，避免 PID 重用导致误杀。

## 3. 生命周期

允许的状态转换为：

```text
STARTING -> RUNNING -> EXITED
STARTING -> FAILED
STARTING/RUNNING --controller restart--> LOST
```

- `EXITED` 同时记录正常 exit code 或终止 signal，两者至多一个；
- `FAILED` 表示命令没有成功启动，并记录不含敏感输入的简短错误；
- `LOST` 表示 controller 从磁盘发现上次运行留下的活动状态。controller 不依据旧 PID
  重新接管或发送信号。

关闭 controller 时先向所有活动进程组发送 TERM；超时后发送 KILL，并等待直接子进程
完成 `Wait`。

## 4. 持久化与输出格式

runtime 根目录由 `--runtime-dir` 配置，默认
`/var/run/remote-code-controller`。每个进程使用：

```text
<runtime-dir>/<uuid>/
├── metadata.json
├── status.json
└── logs/
    ├── state.json
    ├── <first-offset>.log
    ├── <first-offset>.oidx
    ├── <first-offset>.stdout.lidx
    └── <first-offset>.stderr.lidx
```

根目录和 UUID 目录权限为 `0700`；文件权限为 `0600`。拒绝将符号链接作为 runtime 根
目录或进程目录。`metadata.json` 保存 schema 版本、UUID、逻辑名称、I/O 模式、命令、
参数、工作区内工作目录、环境变量 key 和创建时间。`status.json` 保存状态、PID、启动/
结束时间、exit code/signal 及非敏感错误。状态文件采用同目录临时文件、`fsync`、rename
的原子替换方式更新。

`logs/` 使用 v2 tagged segment：每条记录具有跨双流递增的逻辑 offset、stdout/stderr tag、
时间戳、逻辑行 offset、行边界、CRC 和最大 64 KiB 的原始 payload。稀疏 offset 索引用于
定位回放，分 stream 行索引用于 tail。PIPE 保留双流标记；PTY 合并输出标记为 stdout。
完整格式、轮转、迁移和崩溃恢复规则见
[进程日志观测详细设计](process-log-observation-design-v1.md)。

环境变量 value、认证 token、上传文件内容和 prompt 不得写入 metadata/status 或普通
controller 日志。

## 5. CLI 行为

- `exec [--name NAME] [--pipe|--pty] [--cwd REMOTE_DIR] [-e KEY=VALUE ...] [--] CMD [ARG ...]`
  启动命令；未给 `--cwd` 时使用 REPL 当前远端目录，未给 `--name` 时由 server 生成。
- `cd test` 后执行 `exec ls -a`，请求中的工作目录必须是 `test`。
- `ps` 只显示活动进程。
- `ps -a` 显示活动和历史进程。
- `kill [-s SIGNAL] [-w] PROCESS` 保持 UUID、名称和 PID 引用能力。
- `logs [-f] [-n LINES|--offset OFFSET] [--stdout|--stderr] PROCESS_ID` 回放或持续跟随输出。

命令解析不调用本地 shell。需要 shell 语法时必须显式执行，例如
`exec sh -lc 'printf "%s\\n" "$HOME"'`。

## 6. 限制与校验

- 默认最多 16 个并发活动进程，server 可配置；
- 最多 256 个参数，单参数最多 4096 字节，参数总计最多 64 KiB；
- 最多 256 个环境变量覆盖，单 key/value 最多 4096 字节，总计最多 64 KiB；
- 工作目录和命令字段最长 4096 字节；
- 进程历史内存索引最多 4096 条；磁盘记录不会自动删除；
- 日志默认每进程保留 64 MiB、全局 4 GiB、退出后 7 天，可通过 controller 配置覆盖；
- 当前进程执行实现仅支持 Linux；其它平台返回 unimplemented。

## 7. 验收标准

- gRPC 能用 `name/cwd/io_mode/command/arguments/environment` 启动普通命令并返回 UUID/PID；
- PIPE 和 PTY 都能完成执行、回收和状态落盘；
- PIPE stdout/stderr、PTY 合并输出可按逻辑 offset 或最后 N 行无损回放，并能跟随到退出；
- kill 对进程组生效，`wait=true` 返回时 leader 已被回收；
- `ps` 与 `ps -a` 过滤正确，controller 重启后历史可见，遗留活动状态转换为 `LOST`；
- 工作目录逃逸、symlink 逃逸、无效环境变量和超限请求被拒绝；
- `go test ./...`、`go test -race ./...`、`go vet ./...`、`go build ./...` 全部通过。
