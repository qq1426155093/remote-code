# 进程标准输入详细设计 v1

## 1. 目标与边界

进程可在启动时选择 `MANAGED` 输入。controller 保留对应 PIPE writer 或 PTY master，使客户端
能够在进程已经进入 RUNNING 后连接、写入、detach，并在稍后重新连接。输出继续由
`ObserveProcessLogs` 提供，本接口不重复传输 stdout/stderr。PTY 窗口 resize 作为同一输入流中的
控制操作发送，因此与按键输入具有确定顺序，并复用独占 writer 权限。

`DISABLED` 是兼容默认值。PIPE writer 一旦关闭便无法为已经运行的进程重新创建，因此输入模式
不能从 `DISABLED` 动态升级为 `MANAGED`。

## 2. 状态

`ProcessInfo.input_mode` 表示启动时确定的能力；`input_state` 表示运行时状态：

```text
UNAVAILABLE
OPEN -> ATTACHED -> OPEN
OPEN/ATTACHED -> CLOSED
```

只有 RUNNING、MANAGED、OPEN 的进程可以获得 attachment。每个进程同时只有一个 writer。
detach、客户端半关闭发送方向和网络断开执行 `ATTACHED -> OPEN`。PIPE close_input 和进程退出
执行到 `CLOSED`，不可逆。

## 3. 流协议

`StreamProcessInput` 的第一帧必须是 open，服务端解析 `ProcessReference` 后固定具体进程记录，
并返回包含稳定 UUID 的 opened。每个 data 帧最多 64 KiB；data 和 resize 共用从 1 开始严格
递增的 sequence。服务端将整帧交给输入 pump，只有完整写入后才返回 ack；resize 在调用 PTY
ioctl 成功后返回 resize_ack，因此两种确认共同承担顺序确认和背压。终端行列数范围为 1..65535。

detach 帧或客户端发送 EOF 不关闭子进程输入。close_input 只对 PIPE 有效；PTY master 同时承载
输入与输出，关闭它会破坏输出并可能触发 SIGHUP，所以 PTY 请求返回 failed-precondition。

RPC context 在写入确认前取消时，客户端无法判断已经提交给操作系统的最后一帧是否完成；stdin
不是事务接口，客户端不得自动重试未确认帧。重新 attachment 的数据仍排在已经提交的写操作之后。

## 4. 服务端并发与关闭

registry mutex 只用于解析进程、获取独占 attachment 和更新 `input_state`，不会在操作系统写入
期间持有。每个 MANAGED 进程有一个 writer pump；调用方一次只向 pump 提交一帧，pump 串行执行
完整写入。目标进程不读取时最多阻塞一个 pump goroutine，不会积累输入数据。

进程 reaper 或 controller shutdown 会关闭底层端点，使阻塞写入返回并结束 pump。reaper 仍是
唯一调用 Wait、持久化 EXITED 和关闭进程 done channel 的组件。attachment 观察 done 后发送
PROCESS_EXITED end 帧。

## 5. 持久化与安全

metadata 记录 `input_mode`，status 记录最终 `input_state`；ATTACHED 是瞬时状态，不因每次连接写盘。
controller 重启时原活动记录转换为 LOST，输入状态转换为 CLOSED，因为当前版本不会接管旧进程。

输入 payload、sequence 对应的内容、prompt 和 token 均不写 metadata、status、controller 日志或
审计日志。PTY 行规或目标程序可能把输入回显到 stdout，现有输出日志会把它视为进程输出；敏感
交互需要后续的禁用持久化输出策略，不能依赖回显过滤。

## 6. 错误映射

- 帧顺序、sequence、空或超限 data：`InvalidArgument`；
- 缺失或超限终端尺寸：`InvalidArgument`；
- 进程不存在：`NotFound`；
- 非 RUNNING、输入未启用、输入已关闭、PTY close_input、PIPE resize：`FailedPrecondition`；
- 已有 writer：`AlreadyExists`；
- controller 正在关闭：`Unavailable`；
- 无法归类的底层写入失败：`Internal`。

## 7. 交互式 attach

public client 的 `ProcessAttachment` 在本地组合两条既有流：先获取输入 writer 和稳定 UUID，再以
`tail_lines=0, follow=true` 从当前日志边界观察 PTY stdout。最多保留 32 个未确认输入/resize
操作，使逐键交互无需逐次等待网络 RTT，同时保持有限背压。任一流失败会取消另一条流；进程退出时
以日志 end 为准，确保退出前的尾部输出已经发送。

CLI 进入 raw terminal mode，原样传送控制键和 ANSI 序列，监听 `SIGWINCH` 并发送 resize。
`Ctrl-] d` 释放 writer 且停止日志观察，远端进程继续运行；所有完成和错误路径都先恢复本地终端。
每次 attach 使用独立的本地 alternate screen，退出时恢复 REPL 原屏幕；连接建立后先发送一个临时
尺寸再恢复真实尺寸，保证已运行的 Vim 等 TUI 即使窗口大小未变化也会收到 `SIGWINCH` 并完整重绘。
