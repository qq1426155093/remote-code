# Controller 工作流与 Expr 节点需求 v1

## 1. 文档状态

本文定义 `remote-code-controller` 工作流模块的首版需求，用于在后续 Agent 语义层之下提供可恢复、
可审计的多节点编排能力。当前仓库已经实现 controller 内部 workflow core、事务存储、Expr/图校验、
Activity 执行器接口和 controller 生命周期接线；尚未实现 Workflow gRPC/CLI/MCP 接口或真实
Agent/Process 执行适配器。实现契约见[工作流模块详细设计 v1](workflow-design-v1.md)。

工作流模块建立在现有文件、通用进程、进程模板和 controller 日志能力之上，但不替代这些模块：

- [通用进程管理需求 v1](process-management-requirements-v1.md)继续拥有进程启动、信号、输入、日志和回收；
- [可配置 MCP Server 需求 v1](mcp-server-requirements-v1.md)中的 Expr tool 仍是有界的单次调用，
  不承担持久化 workflow；
- [授权模型现状与演进 v1](authorization-model-v1.md)记录的 capability 下沉仍是只读/可写 Agent
  角色成为强制安全边界的前置条件。

本文冻结行为和安全契约；存储格式、Go 类型和具体文件布局由详细设计冻结。protobuf 仍属于后续公共
API 设计范围。

## 2. 目标

首版工作流模块应支持：

- 在 controller 启动期加载并校验不可变工作流定义；
- 使用静态、有向无环流程图表达节点和允许的转移；
- 让每个可执行节点包含一段受限 Expr 脚本；
- 在构建流程图时静态验证脚本所有正常返回路径只能选择已声明的下一跳；
- 使用 Expr 的 `if/else`、三元表达式、`let` 和顺序表达式完成条件与参数计算；
- 将长时间运行的 host operation 建模为持久化 Activity，而不是长期阻塞 Expr VM；
- 提供有界、持久化的字符串键值 Workflow Context，在成功节点之间传递小型业务状态；
- 在 Activity 等待、失败和人工干预期间持久化状态，并在 controller 重启后恢复；
- 对预期业务失败提供结构化结果，使脚本可以选择重试、降级或失败路线；
- 为 Run、NodeRun、Activity、Attempt 和人工操作生成递增、可回放的领域事件；
- 支持节点级并行限制和共享/独占资源声明，为多 Agent 工作区隔离提供调度基础；
- 使用确定性 fake/manual executor 完成测试，不要求 Claude 凭据或真实 Agent 进程。

## 3. 非目标

首版不包含：

- Claude Code 或其它 Agent 进程的真实启动适配器；
- Workflow gRPC、CLI、MCP tool 或 Web UI；
- Expr 语言本身不具备的 `yield`、`await`、`try/catch` 或 `return` 语法扩展；
- 保存和恢复 Expr VM 的指令位置、Go 栈、局部变量或 goroutine；
- 运行中增加、删除或修改节点和边；
- 任意有向环、无界循环、动态 `foreach` 或递归子工作流；
- 多 controller 高可用、分布式调度或分布式锁；
- 外部副作用的通用 exactly-once 保证；
- 自动 Git merge、冲突解决或把 Agent 输出文本解释为成功状态；
- 把 `designer`、`implementer`、`reviewer` 等角色名称直接当作授权边界；
- 让工作流定义、参数或事件充当 token、prompt、进程输出和文件内容的秘密存储。

后续可以在静态 DAG 之上增加有明确最大次数的 iteration 复合节点，但不得直接放开任意环。

## 4. 核心概念与职责边界

### 4.1 核心对象

- **WorkflowDefinition**：不可变的工作流定义，包含名称、revision、参数 Schema、入口节点、节点及转移。
- **WorkflowRun**：某一 definition revision 和一组输入参数的一次运行。
- **WorkflowContext**：Run 内持久化的 `string -> string` 小型业务状态；与 Go `context.Context` 无关。
- **NodeDefinition**：静态节点定义，包含 Expr source、出口、超时、重试、资源和执行策略。
- **NodeRun**：某个节点在一次 Run 中的运行实例。
- **Activity**：脚本发出的一个可持久化外部操作，例如未来的 `call_process`。
- **Attempt**：Activity 的一次执行尝试；重试必须产生新的 attempt，而不是覆盖旧记录。
- **Route**：节点脚本正常完成时返回的出口名称。
- **ArtifactRef**：节点间传递的大型产物引用，只保存稳定路径、摘要和有界元数据，不保存内容。
- **ResourceClaim**：节点对 workspace、worktree、Git index 或其它资源的共享/独占声明。
- **WorkflowEvent**：带 Run 内单调递增序号的状态变化和审计事件。

### 4.2 模块边界

工作流模块拥有：

- 定义加载、Expr 编译、静态图校验和 revision；
- Run、NodeRun、Activity、Attempt 和资源锁状态机；
- 调度、重试、超时、暂停、取消、恢复和事件流；
- 执行器 claim/lease、结果提交和幂等性检查；
- 工作流状态及结构化结果的持久化。

工作流模块不拥有：

- PID、PTY、stdin、signal、stdout/stderr 或进程组；
- Agent 会话、模型协议和终端 attach；
- workspace 文件内容和 Git 仓库写入；
- 真实 process/agent executor 的实现。

后续 Agent executor adapter 可以同时依赖 workflow 和 process 模块；workflow core 不得导入或调用
`internal/process` 的具体实现。adapter 只向 workflow 返回 opaque `external_ref`，workflow 不根据 PID
或终端输出推断 Activity 状态。

## 5. 工作流定义与版本

### 5.1 定义来源

首版工作流定义由 controller operator 通过 workspace 外的显式文件列表提供，建议使用
`.workflow.yaml` 复合后缀。定义文件必须：

- 是有界大小的 UTF-8 普通文件，拒绝符号链接和其它文件类型；
- 使用严格 YAML JSON-compatible 子集；
- 拒绝未知字段、重复 key、anchor、alias、merge key、自定义 tag 和多个 document；
- 不支持 include、远程 URL、glob、环境变量插值或运行期上传；
- 不得位于 Agent 可写的 workspace 内；
- 在 controller `Prepare` 和 `--check-config` 阶段完成全部 Schema、Expr 和图校验；
- 任一错误都使完整 registry 构建失败，不允许跳过坏定义后部分启动。

首版不热加载。修改定义后需要重启 controller，并构建一个新的不可变 registry。

### 5.2 Revision

每个 WorkflowDefinition 必须对规范化后的完整语义内容计算 SHA-256 revision，至少覆盖：

- 名称、说明、参数 Schema 和入口节点；
- 全部节点、Expr source、route 映射、超时、重试和资源声明；
- 影响 Expr environment 或 host catalog 的 profile revision。

StartRun 必须固定到明确 revision。Run 持久化足以重新编译的规范化定义快照；operator 后续删除或修改
原文件不得改变已创建 Run 的行为和历史解释。

### 5.3 概念格式

以下示例只表达需求语义，不冻结最终 YAML 字段名称：

```yaml
version: 1
name: feature-change
language: expr
entry: dispatch

nodes:
  - id: dispatch
    script: |-
      if parameters.mode == "design" {
        "design"
      } else if parameters.mode == "review" {
        "review"
      } else {
        "implement"
      }
    routes:
      design: designer
      implement: [implementer-a, implementer-b]
      review: reviewer

  - id: designer
    # 后续详细设计定义 terminal、join 和 Activity 字段。
```

Route 名称与目标 node ID 分离。脚本返回稳定 route 名称，定义负责将 route 映射到目标节点；重命名目标
节点时不要求修改 Expr source。

## 6. 图模型与构建期校验

### 6.1 静态图

首版图在一次 Run 内不可变。构建 registry 时至少校验：

- Workflow 名称、node ID 和 route 名称合法且唯一；
- 恰好存在一个入口节点；
- 每个 route 的目标节点存在；
- 不存在自环和有向环；
- 不存在从入口不可达的普通节点；
- 至少存在一个可达的成功或失败终点；
- 节点、边、入度、出度、Expr 大小和全图总大小不超过配置上限；
- 并行分支、join policy 和资源声明没有结构冲突；
- 每个非终点节点的 Expr route 集合与其声明出口一致。
- 所有 Workflow Context 写入 key 都是合法字符串字面量；可能缺少确定先后关系的节点不得修改同一 key。

同一 route 可以映射一个或多个目标节点；多个目标形成静态 fan-out。节点有多个入边时必须显式声明
join policy：`all` 等待每条声明入边的 activation token，`any` 在首个 token 到达时激活且节点在一次
Run 中至多执行一次。定义构建器必须拒绝可能使 `all` join 永久等待的拓扑；具体 token 传播和
未选择分支的关闭算法由详细设计冻结。join 不得依赖 Agent 完成顺序或隐式共享上下文。

### 6.2 Expr route 静态校验

每个非终点节点声明允许出口，例如 `{design, implement, review}`。定义构建器必须分析**优化前 AST**，
保证脚本每一条正常完成路径的最终值都是这些出口之一的直接字符串字面量。

校验器只沿可能成为最终结果的位置递归：

- `StringNode`：值必须属于当前节点声明的 route 集合；
- `ConditionalNode`：分别校验 true/false 分支，`else if` 按嵌套条件处理；
- `SequenceNode`：只有最后一个表达式作为 route，前面的表达式仍接受普通脚本策略校验；
- `VariableDeclaratorNode`：校验声明之后的最终表达式；
- 其它节点作为最终值时全部拒绝。

因此以下脚本合法：

```expr
if result.status == "succeeded" {
    "review"
} else if result.status == "needs_intervention" {
    "wait_human"
} else {
    "fallback"
}
```

以下返回方式均不满足首版的字面量约束：

```expr
parameters.next
choose_next()
"review" + suffix
let next = "review"; next
```

如果声明出口只有 `A`、`B`、`C`，任一语法分支返回 `"D"` 都必须在构建流程图时失败，即使该分支的
条件在当前参数下看似恒为 false。校验不得依赖 optimizer 删除不可达分支。

构建错误至少包含 workflow、node、非法 route、允许集合以及 Expr 行列位置。脚本引用了未声明 route
是错误；声明但未在任何返回分支出现的 route 也应作为错误，避免保留无法选择的死配置。

Expr 编译还必须要求最终结果为 string 类型，但类型检查不能替代 AST 字面量检查。运行时必须再次验证
返回值类型和 route allowlist，以防御 Expr 升级、patcher/optimizer 行为变化和实现缺陷。

静态检查只能保证“脚本正常完成时返回合法 route”，不能保证脚本一定完成；host error、取消、超时和
Activity 挂起均可以在产生 route 之前结束本次求值。

## 7. Workflow Expr profile

### 7.1 支持的语言能力

仓库当前固定 `expr-lang/expr v1.17.8`。Workflow Expr profile 可以使用：

- `if { ... } else { ... }` 和 `else if`；
- 三元表达式 `condition ? a : b`；
- `let`；
- 以 `;` 分隔的顺序表达式；
- array、map 和经过 allowlist 的纯 builtin；
- workflow 显式注册并授权的 host function。

Expr 没有 `return` 语法。整个脚本或分支最后一个表达式就是结果；分支中的提前返回必须改写成完整
`if/else`，或者拆分为更小节点。

Expr 没有脚本可见的 `try/catch`。Host function 返回 Go error 时，整个本次 `expr.Run` 失败；`??`
只处理 `nil`，不能捕获 error。普通 `try(call(), fallback)` host function 也不能实现 catch，因为参数
会在 `try` 被调用之前求值。

Expr 没有 coroutine、Future、`yield` 或 `await`。`WithContext` 只把取消和 deadline 传给支持 context
的 host function，不保存 VM 指令位置。

### 7.2 确定性限制

为了允许 controller 重启恢复和脚本重放，Workflow Expr profile 必须：

- 禁用时间、随机数、环境遍历和其它未记录的非确定性来源；
- 不允许脚本访问网络、任意文件、shell 或 controller 进程内可变全局状态；
- 对 AST node/depth、脚本 byte、collection、host call 数量及输入输出设置上限；
- 只向脚本暴露不可变 Run parameters、只读 Workflow Context、已持久化节点结果和 operation journal；
- 对每个 host function 声明 effect、capability、是否可挂起以及输入输出 Schema；
- 禁止在 `map`、`filter`、`all`、`any` 等动态次数 predicate 中调用有副作用或可挂起 host function；
- 对所有可挂起或具有外部副作用的调用要求显式、稳定、节点内唯一的 `operation_id` 字符串字面量；
- 在 definition revision 改变时产生新定义，不能让活动 Run 改用新脚本。

### 7.3 持久化 Workflow Context

每个 Run 包含一个初始为空的 `map[string]string` 和单调递增的 `context_version`。它只用于跨节点传递
小型、非秘密的业务状态；启动输入继续放在不可变 `parameters`，大型或结构化产物继续使用 Activity
result 或 ArtifactRef。该对象与传递取消和 deadline 的 Go `context.Context` 必须使用不同的内部类型和名称。

Expr 环境提供：

- `context`：当前已提交 Context 的只读快照；
- `context_set(key, value)`：暂存一个字符串写入；
- `context_delete(key)`：暂存一个删除。

key 必须是匹配 `[A-Za-z][A-Za-z0-9_.-]{0,127}` 的直接字符串字面量。mutation 不允许出现在条件、
短路表达式或 collection predicate 中；在普通顺序表达式和 `if/else` 分支主体中允许。同一节点内对同一
key 的最后一次暂存操作生效。

Context mutation 具有节点事务语义：

1. 每次 Expr 求值从已提交 Context 的副本开始，脚本不能直接修改该 map；
2. `context_set`/`context_delete` 只写入本次求值的临时 change set；
3. Activity 挂起、脚本错误、取消或非法 route 会丢弃整个 change set；
4. 节点成功时，change set、合法 route、节点状态、`context_version` 和事件在同一事务中提交；
5. 下一节点只能读取已经成功提交的 Context；重放从持久化快照重新计算未提交的 change set。

Context 最多 256 个 key，单个 value 最多 16 KiB，规范化 JSON 总大小最多 256 KiB；value 必须是无
NUL 的合法 UTF-8。`context_updated` 事件只记录版本和排序后的 set/delete key，不记录 value。Context
本身会进入 Run 快照和 `GetRun` 结果，因此不得保存 token、凭据、prompt、文件内容或未脱敏进程输出。

为了避免 fan-out 完成顺序影响结果，构建器从 AST 收集每个节点的 mutation key，并进行保守的先后关系
校验。只有定义图能证明一个 writer 必须在另一个 writer 前完成时，两个节点才允许修改同一个 key；
否则定义构建失败。operator 应优先使用节点命名空间（例如 `review.status`），需要汇总时由确定有序的
merge 节点写共享 key。

## 8. 长时间 Activity 与逻辑挂起

### 8.1 外部行为

`call_process`、`call_agent` 等长时间 operation 不得让 Expr VM 和 workflow 调度槽阻塞到任务结束。
当脚本首次执行到这类调用时，workflow 必须：

1. 以 Run、NodeRun、operation ID 和 attempt 序号建立稳定调用身份；
2. 在持久化状态中记录 operation spec、输入摘要和调度意图；
3. 将 NodeRun 转为 `WAITING_ACTIVITY`；
4. 释放 Expr VM、goroutine 和普通脚本执行名额；
5. 由 executor 在 workflow 事务之外执行实际副作用；
6. 在 Activity 状态变化时持久化结果并唤醒 NodeRun；
7. 重新求值脚本，使已经完成的 operation 返回 journal 中的原结果；
8. 继续执行直到下一个待处理 operation、节点失败或产生合法 route。

从脚本作者视角，该调用在逻辑上等待结果；从实现和持久化视角，它是 workflow engine 提供的挂起与重放，
不是 Expr VM 原生 yield。controller 不得尝试序列化 VM 栈、instruction pointer 或局部变量。

Expr 将 host error 包装为运行错误时仍保留 Go error chain；workflow 内部的挂起控制信号只能由
coordinator 识别，不能作为普通业务错误暴露给脚本或被降级逻辑吞掉。

### 8.2 Operation journal 与重放

每个 operation journal 至少保存：

- definition、Run、NodeRun、operation ID 和 attempt；
- host function 名称及规范化输入摘要；
- `SCHEDULED`、`CLAIMED`、`RUNNING`、`NEEDS_INTERVENTION`、`SUCCEEDED`、`FAILED`、
  `CANCELLED` 或 `LOST` 状态；
- executor lease、opaque external reference、时间戳和有界结构化结果；
- 人工处理、重试、取消和最终解决事件。

重放时：

- 已成功的 operation 必须返回原持久化结果，绝不重复副作用；
- 仍等待的 operation 必须再次使节点挂起，不重复调度；
- operation ID 相同但函数名或输入摘要不同表示非确定性，Run 必须进入失败或人工处理状态；
- 旧 attempt 的迟到结果不得覆盖新 attempt；
- dispatch 与 journal 之间无法原子提交时，executor adapter 必须使用幂等 request ID 或明确进入结果未知状态，
  不得盲目重复启动进程。

Workflow core 首版只需用 deterministic fake executor 证明上述语义。真实 ProcessService 目前没有通用
幂等启动 request ID，该缺口必须在后续 process/agent adapter 设计中关闭，不能由 workflow 假装已经
提供 exactly-once。

## 9. 错误、降级与人工干预

### 9.1 错误分类

脚本可处理的预期 Activity 结果必须作为结构化值返回，而不是 Go error：

```text
status:
  succeeded
  retryable_failure
  permanent_failure
  needs_intervention
  cancelled

code: 稳定、机器可读的错误码
message: 有界、脱敏的人类说明
output: 成功或允许降级时的结构化结果
external_ref: 可选 opaque handle
```

脚本可以通过 `if/else` 选择 fallback route。以下错误不得转换为普通结果供脚本吞掉：

- Expr 解析、类型、策略或静态约束错误；
- capability/权限不足；
- workflow context 取消或 deadline；
- operation journal 不一致、状态损坏或重放非确定性；
- host 返回非法或超限结果；
- controller 内部错误。

系统错误、workflow 挂起信号和人工审批信号必须与业务失败使用不同的内部类型。

### 9.2 人工干预

Activity 遇到可人工恢复的问题时进入 `NEEDS_INTERVENTION`，不得自动执行 fallback，也不得占用 Expr VM。
授权操作者可以：

- 对仍运行的 Agent/Process attach 或发送输入，然后继续等待同一 attempt；
- 重试并创建新 attempt；
- 带结构化结果和原因人工确认成功；
- 将当前问题确认为业务失败，使脚本得到失败 Result 并选择 fallback；
- 明确终止 NodeRun 或整个 Run。

每次人工操作必须记录 principal、时间、Run/Node/Activity/Attempt、动作和有界原因，不记录 token、输入
内容或终端内容。人工确认不得改写或删除旧 attempt 历史。

如果原 Activity 仍可能产生副作用，workflow 不得在没有取消、隔离或明确 operator 授权的情况下同时
启动 fallback writer，避免两个 Agent 并发修改同一工作区。

## 10. 状态与生命周期

### 10.1 WorkflowRun

```text
CREATED -> RUNNING <-> PAUSED
              |
              +-> SUCCEEDED
              +-> FAILED
              +-> CANCELLING -> CANCELLED
```

`PAUSED` 是软暂停：不再调度新 Activity，已经运行的 Activity 是否继续由其取消策略决定。

### 10.2 NodeRun

```text
PENDING -> READY -> EVALUATING -> SUCCEEDED
                       |
                       +-> WAITING_ACTIVITY -> EVALUATING
                       +-> NEEDS_INTERVENTION -> EVALUATING
                       +-> WAITING_RETRY -> READY
                       +-> FAILED
                       +-> CANCELLED

PENDING/READY -> SKIPPED
```

`SUCCEEDED` 必须记录脚本选择的合法 route，并把本次 Workflow Context change set 原子合入 Run。
ActivityResult 和 ArtifactRef 作为 NodeRun 的独立持久化
产物供后续节点读取，不与 route 字符串混为一个动态返回对象。节点在挂起、失败或取消时没有 route。

### 10.3 Attempt 与 lease

Executor 通过 claim 获得有期限 lease。claim、Attempt 创建、NodeRun/Activity 状态和资源获取必须在一个
持久化事务中完成。heartbeat 只延长当前 attempt 的 lease；lease 到期、executor 丢失或 controller
重启后，根据恢复策略将 attempt 标记为 `LOST` 并决定重试或人工处理。

Complete/Fail 必须携带 attempt ID、lease token 和幂等键。重复提交返回首次结果；过期 attempt 的迟到
提交返回 failed-precondition，且不得改变 Run。

## 11. 并发、资源与工作区

节点必须可以声明有界并行度以及多个 `SHARED`/`EXCLUSIVE` ResourceClaim。资源 key 可以表示：

- controller 根 workspace；
- 某个 Git worktree；
- 某个 worktree 的 Git index；
- operator 定义的其它项目资源。

获取多个资源必须原子完成；实现应使用稳定排序避免锁顺序死锁。默认建议同一 workspace writer 使用
`EXCLUSIVE`，避免其它 Agent 读取或覆盖半成品。多个并行 writer 必须使用不同 worktree/resource key，
再通过显式产物、提交或补丁交接。

角色、capability selector 和 ResourceClaim 是不同概念：资源锁防并发冲突，capability 控制允许调用的
服务能力，`reviewer` 等角色只是调度元数据。capability 未下沉到 file/process service 之前，不得把角色
或 workflow 声明宣传为安全沙箱。

## 12. 持久化、恢复和事件

工作流存储必须以事务方式原子提交一次状态转换涉及的：

- Run、NodeRun、Activity 和 Attempt；
- ready/timer/lease 索引；
- 资源锁；
- 幂等键；
- 一个或多个带连续 sequence 的 WorkflowEvent。

不得使用多个彼此独立的 JSON 状态文件模拟跨对象事务。具体采用 SQLite、bbolt 或其它嵌入式事务存储
由详细设计决定；正式 controller 实现必须支持 fsync、schema version、迁移和崩溃恢复。

controller 重启后必须：

- 从 Run 保存的 definition snapshot 重新编译相同 revision；
- 校验 operation journal 的确定性；
- 恢复 ready、retry、timeout、resource 和 event cursor；
- 将不能确认所有权的旧 lease/attempt 标记为 `LOST`，不根据旧 PID 猜测控制权；
- 唤醒等待已完成 journal result 的 NodeRun；
- 不重复已经有成功 journal result 的副作用。

WorkflowEvent 与 controller 诊断日志分离。领域事件供 Run 回放、follow 和审计；controller log 只记录
workflow service 启停、恢复计数、存储错误等诊断。两者均不得包含 prompt、token、环境变量 value、
文件内容、进程输入或完整输出。

## 13. 安全与限制

首版至少设置以下可配置硬上限，并在详细设计中给出默认值：

- definition 文件数、单文件和总 byte；
- workflow 数、每图节点数、边数、入度和出度；
- Run 总数、活动 Run、每 Run 并行 NodeRun 和全局活动 Attempt；
- Expr source byte、AST node/depth、collection 和 host call 数；
- parameters、节点输出、Activity input/result 和 event payload byte；
- Workflow Context key 数、单 value byte 和总 JSON byte；
- retry 次数、退避上限、Activity deadline、lease 和 heartbeat；
- 每 Run event 数量、observer 数和历史保留期；
- 人工干预等待时间和未解决 Run 数量。

工作流定义是 operator trusted code，但 Run parameters、host result、Agent 输出和人工提交内容均是不可信
输入。所有错误必须脱敏，不回显定义文件绝对路径之外的敏感部署信息，不把脚本源码、参数值或 Activity
payload 写入普通 controller 日志。

## 14. 首版内部接口

首版只要求 controller 内部模块接口，最终命名由详细设计决定。能力至少包括：

- 加载、列出和查询不可变 WorkflowDefinition；
- 创建、查询、列出、暂停、恢复、取消和删除终态 WorkflowRun；
- 按 sequence 回放/跟随 WorkflowEvent；
- executor claim ready Activity；
- 标记 Attempt started、heartbeat、complete、fail 和 acknowledge cancellation；
- operator 对 `NEEDS_INTERVENTION` 执行 continue、retry、resolve、fail 或 cancel。

Worker claim/complete 接口不得直接暴露给持有普通 controller token 的客户端，否则调用方可以伪造 Activity
完成。未来如果支持远程 executor，必须设计独立 worker 身份、capability 和审计边界。

## 15. 验收标准

首版 workflow core 至少通过以下验证：

- 合法静态 DAG、revision 和 definition snapshot 可以构建和恢复；
- 重复 node/route、未知目标、环、不可达节点和超限定义在准备期失败；
- route 为 `A/B/C` 时，任一最终分支出现 `"D"` 在构图期失败并报告源码位置；
- 动态 route、函数 route、拼接 route 和变量 route 被拒绝；
- `if/else`、`else if`、三元、顺序表达式和分支内 `let` 的最终字面量分析正确；
- optimizer 不能隐藏非法但语法上存在的 route 分支；
- 运行时再次拒绝非 string 或未声明 route；
- Host Go error 终止本次求值，业务失败 Result 可以通过 `if/else` 选择 fallback；
- 可挂起 Activity 不长期占用 Expr VM/goroutine/普通执行槽；
- 完成 Activity 后重放只返回 journal result，不重复副作用；
- operation ID/输入摘要不一致被识别为非确定性；
- 双重 claim、重复 complete、过期 lease 和迟到旧 attempt 结果不能破坏状态；
- `NEEDS_INTERVENTION` 可以继续同一 attempt、重试新 attempt、人工解决或失败，并保留审计历史；
- controller 在调度前、调度后、完成前、完成后各崩溃点恢复时不丢失已提交状态；
- 并行节点遵守 per-run/global 限额和共享/独占 ResourceClaim；
- workflow event replay/follow 在快照与实时交接处不丢失、不重复 sequence；
- 日志、错误和事件不泄露 token、prompt、环境 value、文件内容或进程输入；
- 测试使用 deterministic fake clock、ID generator、store 和 executor，不依赖 Agent 凭据；
- `go test ./...`、`go test -race ./...`、`go vet ./...` 和 `go build ./...` 全部通过。

## 16. 后续演进

在首版契约稳定后可以分别设计：

- WorkflowService protobuf、公共 Go client 和 CLI；
- Agent/Process executor adapter 及幂等启动契约；
- 有上限的 iteration、动态 `foreach` 和子工作流；
- definition 热更新与 revision 并存；
- capability 下沉、每 Run/Agent principal 和更细粒度审计；
- artifact registry、Git worktree 生命周期和提交/补丁交接；
- 在不破坏确定性与挂起控制信号的前提下，提供更高层的错误处理 DSL。

即使未来增加错误处理 DSL，也不得让普通 catch 吞掉 workflow suspend、cancel、deadline、权限拒绝或
重放不一致等控制/系统错误。
