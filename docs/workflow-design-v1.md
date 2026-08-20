# Controller 工作流模块详细设计 v1

## 1. 文档状态

- 状态：实现基线。
- 需求来源：[工作流模块需求 v1](workflow-requirements-v1.md)。
- 实现位置：`internal/workflow`，由 `internal/server` 创建和关闭。
- 首版不增加 gRPC、CLI 或 MCP 公共接口，也不负责启动 Claude 等真实 Agent 进程。

本文只描述首版实际实现的契约。后续增加远程执行器或公共 API 时，不得破坏已经持久化的定义快照、运行记录和事件语义。

## 2. 目标与边界

工作流模块负责：

1. 从 controller 配置指定的 YAML 文件加载静态有向无环图（DAG）。
2. 在 controller 启动前编译 Expr 脚本，并验证脚本所有正常出口都是已声明路由的字符串字面量。
3. 创建可恢复的 Run、NodeRun、Activity 和 Attempt 状态。
4. 通过 Activity journal 把长时间 host 调用表示为持久化挂起，而不是阻塞 Expr VM。
5. 为外部执行器提供认领、开始、心跳、正常完成、系统失败和请求人工介入的内部 Go API。
6. 通过租约、幂等键、资源声明和事务事件保证重试与恢复安全。
7. 在 controller 重启后从数据库恢复等待中的运行，并处理已过期租约。

首版明确不做：

- 启动或管理真实 Agent 进程；
- 动态建图、循环、动态 fan-out、子工作流；
- 在 Expr 内提供原生 `yield`、`await`、`try/catch` 或 `return`；
- 将工作流暴露为网络 API；
- 分布式 controller 多主写入。

## 3. 包与依赖方向

```text
cmd/controller
      |
      v
internal/server ---------> internal/workflow
      |                          |
      +--> internal/process      +--> Expr
      +--> internal/files        +--> JSON Schema
      +--> internal/mcp          +--> bbolt
```

`internal/workflow` 不导入 `internal/process`。未来的进程或 Agent 适配器只通过执行器 API 认领 Activity，因此核心状态机不依赖进程实现。

包内文件职责：

- `definition.go`：YAML 数据模型、规范化和 Registry。
- `loader.go`、`definition_file_*.go`：严格、安全地读取定义文件。
- `script.go`：Expr 编译、预优化 AST 检查和 Activity host 函数。
- `model.go`：公开快照、状态和命令类型。
- `record.go`：仅供持久化的版本化内部记录。
- `store.go`：bbolt 事务存储和事件游标。
- `service.go`：内部 Go API、状态机、调度与恢复。

## 4. Controller 配置

controller TOML schema 升级到版本 8，并增加可选表：

```toml
[workflows]
enabled = false
definition_files = []
max_active_runs = 64
max_active_attempts = 16
lease_duration = "30s"
retry_initial_backoff = "1s"
retry_max_backoff = "1m"
reconcile_interval = "1s"
```

规则：

- 只有 schema v8 可以使用 `workflows` 表。
- `enabled = true` 时至少配置一个定义文件。
- 定义文件必须在 workspace 之外，必须是普通文件，且不能通过符号链接逃逸。
- 数据库存放在 `<runtime_directory>/workflows/workflows.db`；目录权限为 `0700`，新文件权限为 `0600`。
- `--check-config` 会完成定义加载、Schema 编译、Expr 编译和图校验，但不会打开数据库或启动后台协程。

## 5. 定义文件格式

一个文件可以包含多个工作流：

```yaml
version: 1
language: expr
workflows:
  - name: review-change
    description: Review and optionally repair a change
    revision: 1
    entry: review
    max_parallelism: 2
    parameters_schema:
      $schema: https://json-schema.org/draft/2020-12/schema
      type: object
      properties:
        repair:
          type: boolean
      required: [repair]
      additionalProperties: false
    nodes:
      - id: review
        timeout: 2h
        script: |
          let result = activity("review-agent", "manual", {task: "review"});
          if result.status == "ok" {
            "accepted"
          } else if parameters.repair {
            "repair"
          } else {
            "rejected"
          }
        routes:
          accepted: [done]
          repair: [repair]
          rejected: [failed]
        retry:
          max_attempts: 3
        resources:
          - key: workspace
            mode: exclusive
      - id: repair
        timeout: 4h
        script: |
          let result = activity("repair-agent", "manual", {task: "repair"});
          if result.status == "ok" { "done" } else { "failed" }
        routes:
          done: [done]
          failed: [failed]
        resources:
          - key: workspace
            mode: exclusive
      - id: done
        join: any
        terminal: succeeded
      - id: failed
        join: any
        terminal: failed
```

### 5.1 顶层字段

- `version`：固定为 `1`。
- `language`：固定为 `expr`。
- `workflows`：非空，名称在所有文件中唯一。

### 5.2 工作流字段

- `name`：`[a-z][a-z0-9._-]{0,62}`。
- `revision`：operator 提供的正整数版本；构建器另对规范化定义计算 SHA-256 `definition_digest`。
  Run 同时保存 digest 和完整定义快照，不依赖重启后的当前版本。
- `entry`：唯一入口节点，且入度必须为 0。
- `parameters_schema`：JSON Schema 2020-12 对象 Schema；拒绝外部引用。
- `max_parallelism`：单个 Run 同时占有资源的节点数，默认 1，上限 64。
- `nodes`：1 到 256 个节点。

### 5.3 节点字段

节点分为执行节点和终止节点：

- 执行节点必须有 `script` 和非空 `routes`，不能有 `terminal`。
- 终止节点必须有 `terminal: succeeded|failed`，不能有脚本或路由。
- `join` 为 `all` 或 `any`，默认 `all`。
- `timeout` 为节点从首次 EVALUATING 到终止的墙钟上限，默认 `24h`，范围 `1s` 到 `720h`。
- `retry.max_attempts` 默认 3，包含首次尝试；退避使用 controller 全局配置。
- `resources` 是稳定排序的资源声明；`mode` 为 `shared` 或 `exclusive`。

`routes.<name>` 的值是一个非空目标节点数组。多个目标表示静态 fan-out；目标的入边根据其 `join` 汇合。源节点选择一条路由后，它到所有可能目标的边都会被标记为“已解析”：选中目标为 true，其他目标为 false。因此：

- `join: all` 仅在所有前驱都选中它时运行，否则在全部前驱解析后跳过；
- `join: any` 在第一个前驱选中它时运行；如果所有前驱均未选中则跳过；
- 被跳过节点的所有出边继续以 false 传播，不会永久等待。

构建 Registry 时执行：名称与限制校验、目标存在性、唯一入口、可达性、DAG 拓扑排序、参数 Schema 编译和脚本编译。任一步失败都会阻止 controller 启动。

## 6. Expr 语言契约

### 6.1 可见环境

每次执行脚本都会重建只读环境：

- `parameters`：创建 Run 时通过 Schema 校验的参数。
- `nodes`：已经终止的节点结果摘要。
- `activities`：当前节点 Activity journal 的只读摘要。
- `activity(operation_id, executor_kind, input)`：持久 Activity host 函数。

禁用产生非确定性或无界工作的内建函数，包括时间、时区、随机效果、`range`、`repeat` 和 `reduce`。脚本有源码长度、AST 节点数和 AST 深度限制。

### 6.2 路由字面量检查

静态检查直接针对 parser 生成的预优化 AST，不能针对优化后的 Program AST。一个节点的正常出口只允许：

1. `StringNode`；
2. `ConditionalNode` 的两个分支递归满足规则；
3. `SequenceNode` 的最后一个表达式满足规则；
4. `VariableDeclaratorNode` 的主体表达式满足规则。

变量、字符串拼接、函数返回值、数组下标或对象属性不能作为最终路由。收集到的字符串集合必须与 `routes` key 集合完全一致；声明但永远不返回和返回未声明路由都属于构建错误。Expr 执行完成后仍再次验证返回值，作为纵深防御。

### 6.3 Activity 调用检查

`activity` 的前两个参数必须是非空字符串字面量。`operation_id` 在单个节点内唯一，并和 `executor_kind`、规范化 input 的 SHA-256 一起构成重放一致性条件。Activity 调用不能出现在 `if` 条件或逻辑短路条件中，避免 journal 次序随运行中数据变化。

### 6.4 挂起与重放

Expr VM 不支持保存调用栈。`activity` 的执行规则是：

1. journal 中没有该 `operation_id`：host 函数返回内部挂起信号；事务创建 Activity，并把节点置为 `WAITING_ACTIVITY`。
2. Activity 尚未完成：再次执行仍挂起，不重复创建外部操作。
3. Activity 正常完成：host 函数返回结构化 `ActivityResult`，脚本从头重放并继续。
4. operation id 相同但 kind 或 input hash 不同：Run 以非确定性系统错误失败。

挂起信号和系统错误不会作为脚本数据暴露，所以脚本不能用业务分支吞掉租约丢失、存储失败、取消或非确定性错误。

## 7. 状态模型

### 7.1 Run

`PENDING -> RUNNING <-> PAUSED -> SUCCEEDED|FAILED|CANCELLED`

Run 保存：ID、幂等键、定义名称和完整快照、参数、状态、节点记录、资源锁、事件序号、时间戳和失败摘要。

### 7.2 NodeRun

`PENDING -> READY -> EVALUATING`

从 `EVALUATING` 可以进入：

- `WAITING_ACTIVITY`：等待 Activity 被认领或完成；
- `WAITING_RETRY`：Activity 尝试失败且退避尚未到期；
- `NEEDS_INTERVENTION`：执行器请求人工处理；
- `SUCCEEDED`：得到合法路由；
- `FAILED`、`CANCELLED`；
- `SKIPPED`：入边解析后不满足 join。

节点从开始 EVALUATING 到终止一直持有资源声明，重放 Activity 时不会让同一 workspace 出现并发写入。

### 7.3 Activity 与 Attempt

Activity 状态：

`SCHEDULED -> CLAIMED -> RUNNING -> SUCCEEDED`

异常分支包括 `WAITING_RETRY`、`NEEDS_INTERVENTION`、`FAILED`、`CANCELLED`。每次认领创建一个 Attempt，Attempt 状态为 `CLAIMED|RUNNING|SUCCEEDED|FAILED|LOST|CANCELLED`。

`CompleteActivity` 表示外部操作本身已经正常结束。业务失败也应作为 `ActivityResult{status: ..., output: ...}` 正常完成并交给 Expr 选择降级路由。`FailAttempt` 只表示执行器崩溃、协议错误等系统失败，按 retry policy 重试，耗尽后使节点与 Run 失败。

## 8. 内部 Go API

`Service` 提供以下能力：

- `StartRun(ctx, workflowName, idempotencyKey, parameters)`：验证参数并创建或返回幂等 Run。
- `GetRun`、`ListRuns`：返回不可变快照。
- `PauseRun`、`ResumeRun`：软暂停/恢复新调度；已有 attempt 仍接受心跳和完成。
- `CancelRun`：终止未完成节点与 Activity，并释放资源。
- `DeleteRun`：只删除终态 Run，并在同一事务中删除事件与幂等索引。
- `ClaimActivity(executorID, kinds)`：认领一个到期的 SCHEDULED Activity并返回 lease token。
- `StartActivity`、`HeartbeatActivity`：校验 token 并推进/续租。
- `CompleteActivity`：事务写入结果、完成 Attempt，并触发节点重放。
- `FailAttempt`：记录系统失败并退避重试或终止 Run。
- `RequestIntervention`：记录原因并冻结 Activity。
- `ResolveIntervention`：支持 `continue`、`retry`、`resolve`、`fail`、`cancel`。
- `Reconcile(now)`：恢复到期重试并把过期 lease 标记为 LOST；controller 后台定时调用，测试可显式调用。
- `ListEvents`、`ObserveEvents`：按单调序号回放和跟随事件。

所有带 lease token 的命令都可重复提交：如果数据库中已记录相同 command id，则返回第一次结果；token 不匹配或旧 attempt 的迟到结果不能修改当前状态。

## 9. 持久化与事务

bbolt 数据库包含：

- `meta`：store schema version；
- `runs`：key 为 Run ID，value 为版本化 JSON record；
- `idempotency`：`workflow-name + NUL + key -> run-id`；
- `events`：`run-id + NUL + big-endian sequence -> event JSON`。

一次状态迁移、事件追加和幂等记录在同一个 bbolt 写事务中提交。事件先分配 Run 内连续序号，再和 Run record 一起写入；因此崩溃后不会出现“状态已变但事件缺失”。数据库只保存 JSON 兼容数据，Expr Program 和 JSON Schema Validator 在加载 Run 快照时重新编译。

单 controller 进程内由 `Service.mu` 串行化状态迁移；bbolt 文件锁阻止第二个 controller 同时打开同一运行目录。这是首版的单主约束。

## 10. 调度、资源和容量

每次有 Run 创建、Activity 完成、人工处理或定时恢复时，调度器执行到稳定点：

1. 传播已经解析的边和 SKIPPED 节点；
2. 将满足 join 的节点置为 READY；
3. 在 Run `max_parallelism`、controller `max_active_attempts` 和资源锁允许时执行 READY 节点；
4. 自动完成被选中的终止节点；
5. 遇到新 Activity 后持久化挂起，遇到已完成 Activity 则继续重放。

资源 key 在全 controller 范围生效。exclusive 与任何相同 key 的锁冲突；shared 只与 exclusive 冲突。资源按 key 排序后一次性检查和获取，避免部分持有导致死锁。

## 11. 人工介入

执行器通过 `RequestIntervention` 提交有界的原因和详情。解析命令：

- `continue`：原 attempt 回到 RUNNING，并生成新 lease token；适合人工修复外部环境后继续同一操作。
- `retry`：结束旧 attempt，按新 attempt 重新调度。
- `resolve`：人工直接提供 `ActivityResult`，脚本重放。
- `fail`：以系统失败结束节点与 Run。
- `cancel`：取消整个 Run。

所有命令写审计事件，但事件和错误摘要不得包含 token、prompt、上传文件内容或未截断的执行器输出。

## 12. 恢复与关闭

服务启动时：

1. 打开并校验 store schema；
2. 逐个读取非终止 Run，用保存的定义快照重新编译；
3. 将已过期 CLAIMED/RUNNING attempt 标记 LOST；
4. 推进到可安全到达的稳定状态；
5. 启动 reconcile ticker。

关闭时先停止接受新的 StartRun/ClaimActivity，再停止 ticker，等待当前短事务结束并关闭数据库。不会等待外部 Activity 完成；其 lease 在下次启动后按规则恢复。

## 13. 错误与安全

- 定义错误带文件、workflow、node 以及 Expr 行列信息。
- 未找到、冲突、无效状态、lease 失效、容量耗尽使用可 `errors.Is`/`errors.As` 判断的包级错误或类型。
- 参数、Activity input/output 和事件都有字节上限；Run、节点、Activity、AST 和观察者都有数量上限。
- 定义文件按 operator 配置处理，但仍拒绝符号链接、workspace 内文件、外部 JSON Schema 引用和未知字段。
- 日志只记录稳定 ID、状态、错误分类和有界摘要；lease token 只存 hash，原 token 仅返回给认领者。

## 14. 验证策略

单元与集成测试必须覆盖：

- if/else、else-if、序列和声明变量后的路由字面量分析；
- 动态路由、未声明路由、不可达节点、环和非法 join；
- 首次 Activity 调用挂起、完成后确定性重放以及 journal 不一致；
- 业务失败通过 Expr 降级，系统失败不可被脚本吞掉；
- lease 心跳、过期、迟到完成、重试耗尽；
- 人工 continue/retry/resolve/fail/cancel；
- fan-out、all/any join、skip 传播和资源互斥；
- 幂等 StartRun、事务事件序号、重启恢复与事件跟随；
- 定义文件路径、符号链接、workspace 边界和各项限制；
- controller schema v8、`--check-config` 和服务关闭。

提交前运行：

```bash
gofmt -w .
go test ./...
go test -race ./...
go vet ./...
go build ./...
```
