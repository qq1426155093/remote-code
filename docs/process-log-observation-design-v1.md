# 进程日志观测详细设计 v1

本文定义 `ProcessService.ObserveProcessLogs` 的契约、磁盘格式、恢复和保留策略。该接口同时支持
已退出进程的静态回放，以及运行中进程从磁盘历史无缝切换到实时跟随。

## 1. 对外语义

请求只接受稳定的进程 UUID。`streams` 为空表示 stdout 和 stderr；PTY 的合并输出只标记为
stdout。起点使用 oneof：

- `offset`：包含该位置的全局逻辑记录 offset；省略起点时默认 0；
- `tail_lines`：选中 stream 合并时间线中的最后 N 个逻辑行，最大 100000；未以换行结束的
  最后一行也计一行；
- `tail_lines = 0`：不回放历史，从当前末尾开始，适合只观察新输出；
- `follow = false`：回放请求时刻的稳定快照后结束；
- `follow = true`：回放快照后继续发送新记录，直到日志完成、客户端取消或发生错误。

offset 是进程级、跨 stdout/stderr 单调递增的记录序号，不是文件字节位置。即使请求只选择
stdout，checkpoint 仍跨过未返回的 stderr 记录，因此它可以直接作为下一次请求的恢复游标。
日志轮转不会改变 offset。

服务端响应顺序固定为：

```text
Header -> Chunk* -> Checkpoint(replay_complete=true)
       -> [Chunk* -> Checkpoint(replay_complete=false)]* -> End
```

`Header` 给出本次原子捕获的 `[earliest_offset, snapshot_end_offset)`、实际解析出的起点和历史/
tail 截断标记。每个 `Chunk` 包含 `[offset,next_offset)`、stream、时间戳、原始二进制数据、
所属逻辑行及行首/行尾标记。`End` 给出最终进程状态、退出码或信号，以及日志是否完整。

请求的 offset 小于 `earliest_offset` 或大于 `next_offset` 时返回 `OutOfRange`，并在
`google.rpc.ErrorInfo` 中返回当前边界。无效 UUID/stream/tail 返回 `InvalidArgument`，进程不存在
返回 `NotFound`，单进程观察者达到上限返回 `ResourceExhausted`，磁盘损坏或已知写入不完整返回
`DataLoss`。

## 2. 回放与 follow 交接

开始观察时，在单个日志 mutex 内解析 offset/tail、捕获 `snapshot_end_offset` 和当前通知 channel，
同时登记观察者。登记期间和整个 RPC 生命周期内，保留任务不会删除该进程的 segment。

服务随后从索引定位磁盘位置，只读取到捕获的 snapshot end。发送 replay checkpoint 后进入循环：

1. 在锁内读取新的 end、最早 offset、完成状态和通知 channel；
2. `end > cursor` 时读取 `[cursor,end)` 并推进 checkpoint；
3. 没有新数据时等待通知、RPC context 或日志完成；
4. 日志完成后等待进程状态落为终态，再发送 `End`。

写入方在追加完整记录后关闭并替换通知 channel。快照边界和通知对象在同一把锁下读取，因此不会
出现“读完磁盘后、开始等待前”丢失唤醒的窗口，也不需要从内存 ring buffer 拼接历史。

## 3. v2 磁盘布局

```text
<runtime-dir>/<uuid>/
├── metadata.json
├── status.json
└── logs/
    ├── state.json
    ├── 00000000000000000000.log
    ├── 00000000000000000000.oidx
    ├── 00000000000000000000.stdout.lidx
    └── 00000000000000000000.stderr.lidx
```

目录权限为 `0700`，文件为 `0600`。文件名的 20 位十进制数是 segment 的首个逻辑 offset。
`.log` 是权威数据；`.oidx` 和 `.lidx` 都是可重建派生索引。

segment 具有 32-byte header：8-byte magic、format version、header size、first offset 和创建时间。
其后是连续记录：

| offset | size | 内容 |
| --- | ---: | --- |
| 0 | 4 | record magic |
| 4 | 4 | 包含 footer 的总长度 |
| 8 | 8 | 全局 record offset |
| 16 | 8 | 本记录所属行的首 record offset |
| 24 | 8 | Unix 纳秒时间戳 |
| 32 | 4 | payload 长度，最大 64 KiB |
| 36 | 1 | stdout/stderr tag |
| 37 | 1 | line-start/line-end flags |
| 38 | 2 | 保留字段 |
| 40 | 4 | CRC32C |
| 44 | N | 原始 payload |
| 44+N | 4 | 重复的总长度 footer |

CRC 覆盖 offset 至 flags/reserved 的 header 部分和 payload。footer 用于确认记录边界。
`.oidx` 每 256 条记录保存 `(logical offset, file position)`；每个 stream 的 `.lidx` 保存已完成行的
`(line start, last record, timestamp)`，用于从 segment 末尾反向解析 tail。正在写入的未结束行保存在
内存状态中，进程退出时也会写入行索引。

## 4. 轮转、保留和配置

写入达到 `segment_bytes` 后封存当前 segment，再创建下一个；单条记录不会被拆到两个 segment。
超过单进程或全局尺寸时按最旧封存 segment 删除，活动 segment 不删除。已退出进程超过保留周期后
删除所有日志 segment，但保留进程 metadata/status 和 offset 边界。存在观察者时延迟删除，观察者退出
后重新执行单进程裁剪。

| 配置 | 默认值 | 命令行参数 |
| --- | ---: | --- |
| `max_bytes_per_process` | 64 MiB | `--process-log-max-bytes` |
| `max_total_bytes` | 4 GiB | `--process-log-max-total-bytes` |
| `segment_bytes` | 4 MiB | `--process-log-segment-bytes` |
| `retention_after_exit` | 168h | `--process-log-retention` |
| `max_observers_per_process` | 8 | `--process-log-max-observers` |

尺寸包含 segment 和索引。segment 和单进程上限最小为 256 KiB，总上限必须不小于单进程上限。
全局清理器每分钟运行；因为不会删除活动 segment 或观察中的数据，短时间实际占用可以高于配置值。

## 5. 崩溃恢复与旧格式迁移

启动时逐 segment 校验 header、连续 offset、长度、footer 和 CRC，并从权威日志重建全部索引。仅有
不完整尾记录时截断到最后完整边界，并将日志标记为不完整；中间损坏或 offset 缺口拒绝加载，避免
静默返回错误数据。状态、segment、索引和旧日志都必须是普通文件，符号链接不会被跟随。

旧记录若仍含 v1 `stdout.log`/`stderr.log`，首次加载时按帧时间戳稳定合并，写入 v2 tagged log，完成
并校验后删除旧文件。同时间戳以 stdout 在前作为确定性规则。由于 v1 双文件无法恢复真实的跨 stream
写入顺序，迁移顺序是尽力而为；迁移后的逻辑 offset 保持稳定。

## 6. 客户端与 CLI

`pkg/client.Client.ObserveProcessLogs` 接受 `ProcessLogOptions`；`Offset` 和 `TailLines` 是互斥指针，
均为空表示 offset 0。调用方应持久化 checkpoint 的 `next_offset`，而不是根据收到的 stream 片段自行
加一。

REPL 命令为：

```text
logs [-f] [-n LINES|--offset OFFSET] [--stdout|--stderr] PROCESS_ID
```

未指定 stream 时输出两者；stdout chunk 写本地 stdout，stderr chunk 写本地 stderr，数据不做 UTF-8
转换。`-f` 受 CLI 的 RPC timeout 配置约束；需要长期跟随时可用 `--timeout 0` 启动 CLI。跟随期间按
Ctrl-C 只取消本次日志观察并返回 REPL，不向远端进程发送信号。
