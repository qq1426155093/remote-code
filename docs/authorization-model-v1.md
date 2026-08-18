# 授权模型现状与演进 v1

## 1. 范围

本文只描述 controller 两个远程入口（gRPC 与 MCP Streamable HTTP）的**授权**现状：调用方通过认证
之后能做什么、由什么强制、边界在哪里。认证本身（bearer token、TLS、Origin 校验）、workspace 路径
安全和进程执行的信任模型不在本文重复，分别见 [Controller 使用指南](controller-guide.md)、
[MCP Server 详细设计](mcp-server-design-v1.md)和 README 的安全边界一节。

本文不引入新 RPC，也不改变现有行为。它记录一个已知的结构性缺口，并给出演进选项与取舍，供
Agent 语义层落地前决策。

## 2. 现状

### 2.1 两套不对称的授权机制

MCP 入口有一套完整的能力模型：`internal/mcp/host.go` 的 host function catalog 为每个 host function
标注 capability（`files.read`、`files.write`、`files.delete`、`processes.start`、`processes.signal` 等）
和 effect 等级（read/mutate/destructive）。tool 必须显式声明所需 capability，声明多余 capability 是
启动错误；tool capability 不能越过 controller 的全局 `allowed_host_capabilities` 上界；脚本不能调用
未声明的 host function。除授权外还有 per-tool 超时与并发、全局请求速率与突发、请求/响应字节上限和
固定字段的调用审计日志。

gRPC 入口没有任何授权层。`internal/auth/auth.go` 的 unary/stream 拦截器只做一件事：比对 bearer
token。通过之后，全部 `FileService`、`ProcessService` 与 `ControllerService` 方法一律可调用，没有
per-RPC 授权、没有速率限制、没有调用审计。

### 2.2 两个入口共用同一个 token

`internal/server/server.go` 在 `Prepare` 中执行 `config.MCP.Token = config.Token`。配置层没有独立的
MCP token 项，TLS 材料同样复用。因此不存在"只能访问 MCP、不能访问 gRPC"的凭据。

### 2.3 Principal 不是调用方身份

`internal/auth/http.go` 的 `Principal.ID` 由**已配置的** token 摘要在中间件构造时计算一次，是一个
常量标签，所有调用方共享同一个值。它用于审计日志关联，不参与任何授权判断。gRPC 侧没有对应概念。

## 3. 实际边界取决于部署拓扑

两个 listener 独立寻址，各自有 loopback/TLS 校验（`internal/server/server.go` 的 `ValidateConfig`），
所以 capability 模型是否构成真实边界，完全由 operator 的拓扑决定：

| 拓扑 | capability 是否构成边界 | 说明 |
| --- | --- | --- |
| 调用方可同时到达两个 listener | 否 | 用同一 token 直接调用 gRPC 即可绕过全部 capability 限制，包括 `StartProcess` |
| 只暴露 MCP，gRPC 留在 loopback | 是 | 但同一 token 已分发给 MCP 客户端；任何到达 loopback 的途径（controller 主机上的其它进程、SSH 隧道、端口转发）都等同于完整权限 |

结论：**capability 模型目前是一项纵深防御措施，不是可依赖的权限边界。** 它能限制一个行为正常的
MCP 客户端的误操作面，不能约束一个持有 token 的主动攻击者。

这与"命令执行有意敞开"的决策是一致的：`StartProcess` 接受任意可执行文件，因此**一个 gRPC token
在定义上就是完整权限**。真正的问题不是 gRPC 太松，而是系统只有一个权限等级，而 MCP 侧发明了第二
个等级，却没有任何全局机制强制它。

## 4. 对文档措辞的影响

[MCP Server 详细设计](mcp-server-design-v1.md)的威胁模型表中，"未认证远程 RCE"一行原本把
"process capability 默认不允许"与 token、TLS 并列为控制措施。对**未认证**调用方成立的控制是强制
token 与远程强制 TLS；capability 默认值降低的是默认暴露面，对**已认证**调用方不构成 RCE 控制，
因为存在 2.2 描述的 gRPC 旁路。该行已相应修正并链接本文。

## 5. 演进选项

### 5.1 选项 A：独立的 MCP token

给 MCP 增加独立的 `token_file` 配置项，不再复用 gRPC token。改动范围限于配置结构、`Prepare` 的
连线与测试，不触碰 RPC 契约。

收益：让 3. 中"只暴露 MCP"的拓扑第一次真正可用——MCP 客户端持有的凭据不再等同于 gRPC 完整权限，
两个秘密可独立轮换和吊销。代价：operator 多管理一个 token；仍然没有 per-caller 授权，同一 MCP
token 的所有持有者权限相同。

### 5.2 选项 B：capability 下沉到 service 层

把 capability 判定从 MCP 层移到 `internal/files` 与 `internal/process` 的调用边界，两个入口共享同
一套授权判定；token 不再是布尔开关，而是携带一组 capability。需要多 token 配置（token → capability
集合的映射）、gRPC 侧的 per-RPC capability 标注和拦截器改造，以及审计日志的统一。

这不是投机性的设计。README 的 Milestone 4 已经列出"agent 角色、只读/可写策略和并发控制"——一个
只读 review agent 若能调用 `FileService.Remove`，角色约束就不成立。**该里程碑实质上要求的正是这一
层**，因此 B 迟早要做，问题只是时机。

代价：改动面最大，触及全部 RPC 与配置契约；引入多 token 后，token 的分发、轮换和吊销成为新的运维
负担；需要决定 capability 粒度是否与 MCP 现有 catalog 对齐（建议对齐，避免两套词汇）。

### 5.3 选项 C：保持现状，写明拓扑要求

不改代码，只在文档中明确 capability 的边界条件与两种拓扑的实际含义。

单独采用不足以关闭缺口，但其中的文档部分无论选哪条路都应先落地——本文与第 4 节的修正即是。

## 6. 建议

1. 先落地本文与威胁模型表的修正（C 的文档部分），使现状可被准确评估；
2. 短期做 A，成本低且解锁唯一一种 capability 有意义的拓扑；
3. 把 B 作为 Agent 语义层的前置项，与 Milestone 4 的角色/读写策略一并设计，避免 agent 角色建立在
   无强制的约定之上。

在 B 落地之前，任何依赖 capability 做权限隔离的部署都必须满足：gRPC listener 不可被 MCP 客户端到
达，且 controller 主机上不存在可被 MCP 客户端间接利用的本地访问途径。

## 7. 验证要求

选项 A 落地时至少覆盖：MCP token 与 gRPC token 不同时两个入口各自认证正确；只配置其一时的行为；
两个 token 相同值不产生特殊处理；token 值不出现在任何日志或错误消息中。

选项 B 落地时至少覆盖：每个 RPC 的 capability 标注完整（缺失标注应是启动期错误而非默认放行）；
capability 不足时返回 `PermissionDenied` 且不泄露资源存在性；同一 capability 集合经 gRPC 与 MCP
两条路径得到一致判定；多 token 配置的解析、去重与冲突检测；审计日志包含 principal 与 capability
判定结果但不含 token。
