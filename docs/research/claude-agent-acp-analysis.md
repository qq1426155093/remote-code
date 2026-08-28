# claude-agent-acp 源码分析

> 分析对象：本地克隆 `/tmp/claude-agent-acp`（即 github.com/agentclientprotocol/claude-agent-acp，TypeScript，Node >= 22，ESM）。
> 依赖版本：`@agentclientprotocol/sdk` 1.3.0、`@anthropic-ai/claude-agent-sdk` 0.3.238、`zod`（见 `package.json`；`node_modules` 未安装，SDK 行为全部依据调用点反推）。
> 所有 `file:line` 引用均相对仓库根目录，行号以当前工作区代码为准。

---

## 1. 项目定位与总体架构

claude-agent-acp 是 Anthropic 官方维护的 **ACP（Agent Client Protocol）适配器**：
把 Claude Agent SDK 的 `query()` 异步迭代器 API 适配为 ACP 的 JSON-RPC-over-stdio 协议，
使 Zed / JetBrains 等编辑器可以把 Claude Code 当作一个标准 agent 子进程驱动（`README.md`）。

核心不是"新 agent"，而是**协议翻译层 + 大量边界情况处理**：

- 入口 `src/index.ts:12` 支持 `--cli` 透传给原生 Claude CLI；
- `src/index.ts:47-50` 在启动 SDK 前用 `resolveSettings({ settingSources: [] })` 应用策略级环境变量；
- `src/index.ts:55-56` 把 console 重定向到 stderr，保证 stdout 只承载 ACP 帧；
- `src/index.ts:66` 支持 `CLAUDE_AGENT_LOGS` 文件日志；
- `src/index.ts:88` 调 `runAcp()`；`:100-106` 以 `connection.closed`、SIGTERM/SIGINT 触发 shutdown，`stdin.resume()` 维持事件循环。

结构要点：

- 单一长连接模型：`runAcp()`（`src/acp-agent.ts:9311`）创建 ACP 连接和 `ClaudeAcpAgent`，一个进程可同时承载多个 ACP session。
- `ClaudeAcpAgent`（`src/acp-agent.ts:1513`）实现 ACP agent 侧全部方法，构造函数里组装两个策略对象：
  `ExitPlanCoordinator`（`:1536` 起，拥有"接受计划后清上下文重启"的生命周期）与 `SessionModeManager`（`:1573` 起，拥有模式策略）。
- `src/lib.ts` 导出可复用 API（`ClaudeAcpAgent` / `runAcp` / `toAcpNotifications` / `toolInfoFromToolUse` 等），
  说明作者明确把它当作"可嵌入的适配库"而非仅是二进制。
- 其余 `src/*.ts` 基本都是从 `acp-agent.ts`（9573 行）里抽出的、可独立测试的策略模块：

| 模块 | 职责 |
|---|---|
| `src/permissions/` | 权限选项构造、建议快照、响应解码、效果应用、呈现 |
| `src/exit-plan.ts` + `src/clear-context-coordinator.ts` | ExitPlanMode 双车道与清上下文重启编排 |
| `src/session-mode.ts` | 权限模式策略与 auto 模式回退 |
| `src/native-subagents.ts` | 原生子代理注册表与生命周期排序 |
| `src/async-tasks.ts` | 后台任务面板生命周期 |
| `src/elicitation.ts` | MCP elicitation / AskUserQuestion / 拒答回退三条桥 |
| `src/file-change-audit.ts` | 隐藏 MCP + 钩子的文件变更审计 |
| `src/session-titles.ts` / `src/session-failure-extension.ts` | 标题生成与 AIR 失败上报 |
| `src/settings.ts` | 设置合并与热重载 |
| `src/tools.ts` | 工具调用呈现与 tool_result 渲染 |
| `src/goal-extension.ts` / `src/air-extension.ts` / `src/acp-subagents.ts` | `_meta` 扩展与能力协商的临时类型面 |
| `src/tool-result-meta.ts` / `src/utils.ts` / `src/session-config-ids.ts` | 小工具 |

对 remote-code 的类比：这就是一个"以 SDK 为内核、以翻译为职责"的适配器进程——
与我们"Claude Code 集成将构建在通用 process 能力之上"的路线同构。

## 2. 会话生命周期

| ACP 方法 | 实现 | 说明 |
|---|---|---|
| `initialize` | `src/acp-agent.ts:1583` | 声明能力（见下）；处理 gateway / terminal 认证方法（`:1619-1690`，terminal 方式带 `_meta["terminal-auth"]` 的 command/args，`:1636-1643`） |
| `session/new` | `:1759` → `getOrCreateSession:6573` / `createSession:6645` | 校验 cwd（`validateCwd:6622`）、组装 SDK `Options`、构建初始 configOptions |
| `session/new` (fork) | `unstable_forkSession:1772` | `createSession` 带 `forkSession: true`（`:6666`、`:6959`） |
| `session/resume` | `:1793` | `resume` + `forkSession` 组合 |
| `session/load` | `:1804` | 之后调用 `replaySessionHistory:5502` 回放历史 |
| `session/list` | `:1818` | 基于 `getSessionInfo` |
| `authenticate` / `logout` | `:1836` / `:1959` | logout 执行 `claude auth logout` 并清空 contextWindowCache |
| `prompt` | `:1991` | 见 §4 |
| `cancel` | `:5034` | 见 §5 |
| `setMode` / `setConfigOption` | `:5401` / `:5405` | 见 §7 |
| `closeSession` / `deleteSession` / `dispose` | `:5383` / `:5391` / `:5379` | `teardownSession:5346` 统一释放 |

`initialize` 声明的能力（`src/acp-agent.ts:1703-1722`）：

- `agentCapabilities._meta.claudeCode.promptQueueing`（`:1706-1710`）；
- `promptCapabilities{image, embeddedContext}`、`mcpCapabilities{http, sse}`、`auth.logout`、`providers`、`loadSession`；
- `sessionCapabilities{additionalDirectories, close, delete, fork, list, resume, subagents}`（`:1699-1701`）；
- 顶层 `_meta`（`:1730-1748`）发布 AIR 能力（sessionFailure / agentFileChangeReport / nativeSubagentSessions / asyncTasks）、
  `steering.supported`、`goal` 能力对象（version + controlMethod + actions）。

**new vs load 的关键差异**：

- `session/new` 的 `_meta.claudeCode.options` 会原样透传给 SDK `Options`（`NewSessionMeta`，`src/acp-agent.ts:977-1005`），
  包括 `emitRawSDKMessages`（`:1002`，经 `_claude/sdkMessage` 扩展通知把原始 SDKMessage 转发给客户端，消费侧 `:3043-3048`）
  与旧式 `additionalRoots`（`:1004`，消费侧 `:6778`、`:6964`）。
- `session/load` 不做透传，只回放历史。

`replaySessionHistory`（`:5502`）体现"重放即二次翻译"：

- 跳过合成登录消息（`isSyntheticLoginMessage:1396`，issue #863）与合成 usage-limit 消息；
- 按 `parent_tool_use_id` 重建子代理 transcript；
- 抑制文件变更审计通道（回放不触发审计）；
- 把活跃的 usage-limit 失败恢复为 AIR session failure（`activeUsageLimitMessage`，`src/session-failure-extension.ts:198`）。

## 3. Turn 模型：核心状态机

这是全仓库最重要的设计。ACP 的 `session/prompt` 是一个**请求-响应** RPC，
而 SDK 的 `query()` 是**一条永不停的异步迭代器 + push 输入流**。适配器用 `Turn`（`src/acp-agent.ts:411-525`）弥合：

1. `prompt()` 把每个 prompt 包装成 `Turn{promptUuid, deferred, ...}` 入队 `session.turnQueue`（FIFO）；
2. 把 `SDKUserMessage` push 进 `session.input`（`Pushable`，`src/utils.ts:8`）；
3. `ensureConsumer`（`:2266`）保证常驻 consumer 在跑；
4. **await turn 的 deferred**（`src/acp-agent.ts:2053-2068`）。`prompt()` 本身不写任何循环。

结算（settle）信号不是 `result` 一种，而是"user 消息回显 + 终态 result + `session_state_changed: idle`"的组合，
并有专门机制处理乱序：

- `command_lifecycle` 帧（`:3062`）在 exhaustive switch 之前拦截：2.1.206+ 的 CLI 用它报告每条 uuid 标记命令的
  queued/started/completed/cancelled/discarded/refused，喂给孤儿记账（`Session.orphanCommands`），
  turn 结算仍以回显/result/idle 为准。该帧是 `@internal` 且不在 SDKMessage 联合类型里，所以不能写成 case（`:3054-3061` 注释）。
- `msg_lifecycle_v1` 能力决定孤儿记账走计数通道还是按 uuid 的 map 通道（`Session.msgLifecycleV1`）；
  `cancel()` 在 `interrupt()` 前先锁存通道避免竞态（`:5078-5082` 注释）。
- `deferredSettle`：后台子代理在场时挂起结算，等子代理终态；
- `steeredEchoes / steeredSettle`：steering 注入的新回显接管结算（`:2216-2226`）；
- `owedTrailingIdles`：多付的 idle 记账，防止下个 turn 提前结算；
- **强杀兜底**：`interrupt()` 后若宽限期内没有出现 trailing idle 则强制结算
  （`DEFAULT_FORCE_CANCEL_GRACE_MS = 30_000`，`:281`；定时器 `:5251-5259`；解除函数 `:944-952`）。

流已死（`queryClosed`）时：

- `prompt()` 直接抛 `SESSION_ENDED_MESSAGE`（`:1998-2002`）；
- consumer 观察到流结束会 settle 全部队列并 `closeQueryStream`（`:3020-3038`），之后的 prompt 立刻失败而非悬挂。

`Session` 类型（`:527-900`）字段极多（cwd、sessionFingerprint、settingsManager、titles、usage 累加器、
modes/models/modelInfos/configOptions、abort/cancel controller、toolUseCache/emittedToolCalls、liveBackgroundTasks、
nativeSubagents\* 映射、asyncTaskRuntime、sessionFailureState 等），但每个字段都有注释解释其在结算/去重/回放中的作用——
**注释本身就是协议不变式文档**。

## 4. 消息流：SDK 消息 → ACP 通知

`runConsumer`（`src/acp-agent.ts:2285`）`for await` 整条 SDK 消息流，按 `message.type` 分派。主要 case：

| SDK 消息 | 行号 | 处理 |
|---|---|---|
| `system/init` | `:3159` | 锁存能力（含 `msg_lifecycle_v1`） |
| `system/status` | `:3195` | 诊断信息转发 |
| `system/compact_boundary` | `:3234` | 发布 usage 更新（上下文压缩边界） |
| `system/local_command_output` | `:3270` | 本地命令输出 |
| `system/session_state_changed: idle` | `:3280` | turn 结算的权威时刻之一 |
| `system/memory_recall` | `:3400` | 记忆召回 |
| `system/commands_changed` | `:3440` | 重发可用斜杠命令（`sendAvailableCommandsUpdate:6295`，过滤 `UNSUPPORTED_COMMANDS`） |
| `system/permission_denied` / `informational` | `:3469` / `:3538` | 权限拒绝与提示性消息 |
| `system/task_started / task_notification / task_updated` | `:3575` / `:3622` / `:3640` | 喂给 NativeSubagentRuntime 与 AsyncTaskRuntime |
| `system/worker_shutting_down` | `:3656` | 会话失败上报 |
| `system/api_retry` | `:3686` | 重试提示 |
| `system/model_refusal_fallback` | `:3702` | 触发 `syncModelAfterRefusalFallback:6442`，经 elicitation 征求同意 |
| `system/model_refusal_no_fallback` | `:3772` | 直接失败 |
| `system/background_tasks_changed` | `:3795` | replace 语义的后台任务级别 |
| `result` | `:3843` | 终结 turn（§5） |
| `stream_event` | `:4365` | 原始 Anthropic 流事件增量渲染 |
| `user` / `assistant` | `:4498-4499` | 内容块映射（见下） |
| `tool_progress` | `:4837` | 工具进度 |
| `rate_limit_event` | `:4896` | 限流提示 |
| `conversation_reset` | `:4910` | 清 usage、titles、goal |
| `tool_use_summary / prompt_suggestion / auth_status` | `:4928-4931` | 记录或转发 |

两个关键映射函数：

- `toAcpNotifications`（`:8672`）：内容块映射核心。text → `agent_message_chunk`；
  `tool_use` → `tool_call`（title/kind/content/locations 来自 `toolInfoFromToolUse`，见 §10）；
  thinking → 带 `_meta` 的块；image → `image` 块；`tool_result` → `tool_call_update`
  （经 `toolUpdateFromToolResult`，并复用 `messageIdForGrouping:8419` 做工具调用归属）。
  `parent_tool_use_id` 非空的消息路由到子代理 session（§8）。
- `streamEventToAcpNotifications`（`:9135`）：把 `stream_event` 的增量 delta 映射为 ACP 分块，
  并支持流式 tool input 精化（`streamedInputRefinement:8640`、`scanStreamedToolInput:1189`、
  `recoveredToolInput:1227`——一个手写增量 JSON lexer，从部分 JSON 里恢复已完整字段）。

**prompt 输入方向**：`promptToClaude`（`:8323`）把 ACP `PromptRequest.prompt[]`（text/image/resource_link 块）
转成 `SDKUserMessage`，`origin: {kind:"human"}`；`prompt()` 再盖上 `uuid = promptUuid`（`:2014-2015`），
这个 uuid 就是后续回显匹配 turn 的键。

## 5. stopReason 映射与取消

`result` case 中 `stopReason` 赋值点（`src/acp-agent.ts`）：

| 值 | 行号 | 来源 |
|---|---|---|
| `cancelled` | `:4052`、`:4146` | 强杀路径 / 取消结算 |
| `refusal` | `:4174` | 模型拒答 |
| `max_tokens` | `:4218`、`:4267` | success / error_during_execution 中 `stop_reason === "max_tokens"` |
| `end_turn` | `:4281` | `subtype === "success"` 的默认 |
| `max_turn_requests` | `:4296` / `:4310` / `:4324` | `error_max_budget_usd` / `error_max_turns` / `error_max_structured_output_retries` |

错误 result 走 `failActiveWithSessionFailure`，把 SDK 错误分类
（`errorKindData:7439` + `providerFailureCategory`，`src/session-failure-extension.ts:409`）
映射为 AIR session failure 并以 `RequestError` 拒绝 turn。
`result.result` 文本仅在"本地命令"或"未流式输出（缓存重放，issue #453）"时补发（`:4240-4259`），
由 `emittedAssistantText` 去重，防止同一段回答发两次。

`cancel()`（`:5034`）顺序：

1. 先 `exitPlan.cancel()` 中止进行中的清上下文重启；
2. 标记 `session.cancelled`、清两个 exit-plan 挂起状态；
3. 流已死则只清钩子返回；
4. `finishAll("cancelled")` 关闭所有 native 子代理，清 eager tool call 与钩子注册表；
5. 把活动 turn 的 steering 回显登记为孤儿（避免其 result 被提升到下一个 prompt）；
6. 锁存孤儿记账通道，**立即结算所有未启动的排队 turn**（`stopReason:"cancelled"`、无 usage，`:5084-5100`）；
7. 对活动 turn 调 `query.interrupt()`，由 consumer 观察其 trailing idle 结算，30s 兜底强杀。

排队 turn 的 result 之后到达时按孤儿处理丢弃。

## 6. 权限系统（最值得抄的部分）

入口 `canUseTool`（`src/acp-agent.ts:5971`）是 SDK 的 `CanUseTool` 回调（在 `createSession` 里接线，`:6877`）。
管线如下（`docs/permission-extension.md` 为规范文本）：

1. **abort 检查**：`signal` 已中止直接拒绝（"Tool use aborted"）。
2. **保证 tool_call 已发出**：`ensureToolCallEmitted`（`:5924`）——权限请求可能先于流式 `tool_use` 到达，
   适配器必须先把 `tool_call`（status pending）发给客户端，否则客户端无法渲染审批 UI；
   失败时回滚 `emittedToolCalls` 去重集合（`:5960-5968`），让流式路径仍可发布。
3. **快照 SDK 建议**：`normalizeDurablePermissionChangeSet`（`src/permissions/normalization.ts`）
   对 SDK 给出的 `suggestions`（`PermissionUpdate[]`）做严格校验 + `structuredClone` 快照：
   上限 100 个 update / 每 update 100 条规则；未知类型、非法 destination/behavior/mode、含控制字符的字符串一律整体丢弃；
   `matchedAskRule` 命中（SDK 判定该调用已被既有规则覆盖但仍需确认）时强制禁用持久选项。
4. **构造选项**：`buildClaudePermissionOptions`（`src/permissions/options.ts`）按工具分派（见下表）。
   排序固定 allow_once(0) / allow_always(1) / default(2) / reject(3)。
5. **构造呈现**：`buildClaudePermissionPresentation`（`src/permissions/presentation.ts:52`）
   复用 `toolInfoFromToolUse` 生成 toolCall 字段（**避免为同一操作维护第二套名称**，`:76-78` 注释），
   `blockedPath` 追加进 locations，`_meta.permission{version:1,title,description:"Reason: <decisionReason>"}` 扩展承载自定义标题。
6. **发起请求**：`requestPermissionFromClient`（`src/acp-agent.ts:5872`）在 `withPendingUserInput`（`:5855`）
   内调用 `ctx.request`，并携带取消 signal（`raceWithAbort:1489`），保证 `cancel` 能中断挂起的权限请求。
7. **校验响应**：`decodeClaudePermissionResponse`（`src/permissions/response.ts`）只解析一次 envelope，
   校验 optionId 确实被提供过；未选择 → `throw "Tool use aborted"`。
8. **应用效果**：`applyClaudePermissionSelection`（`src/permissions/effects.ts:190`）分派到
   `PermissionResult{behavior, updatedInput, updatedPermissions, interrupt, decisionClassification}`：
   `allowWithUpdates` 应用**第 3 步快照的** updates（防 TOCTOU）；`reject` → `deny`；
   `decisionClassification` 区分 `user_temporary / user_permanent / user_reject`。

选项构造的分派表：

| 工具 | 文件 | 要点 |
|---|---|---|
| Bash / PowerShell | `src/permissions/options/shell.ts:48` | `shellSuggestionsLabel` 是 Claude Code `generateShellSuggestionsLabel` 的纯文本移植（命令前缀 / Read 路径 / addDirectories），`displayPaths:18` 只缩短到能区分为止 |
| Read / Glob / Grep / Edit / Write / NotebookEdit | `src/permissions/options/filesystem.ts:101` | 区分 read/write、session 级 acceptEdits / addDirectories、`.claude` 目录特判 "allow Claude to edit its own settings" |
| WebFetch | `src/permissions/options/tools.ts:14` | 域名规则 `domain:hostname` |
| Skill | `src/permissions/options/tools.ts:31` | 精确 + `prefix:*` 两个持久选项 |
| EnterPlanMode | `src/permissions/options/tools.ts:54` | 固定 "Yes, enter plan mode" / "No, start implementing now" |
| ExitPlanMode | `src/permissions/options/tools.ts:58-117` | 清上下文变体（带 `(N% used)` 提示）+ 提升模式变体 auto > bypassPermissions > acceptEdits + 手动 + 拒绝 |
| SandboxNetworkAccess / computer-use MCP | `src/permissions/options/tools.ts:119/157` | host 精确规则 / MCP 整工具规则 |
| `mcp__*` 与兜底 | `src/permissions/options/tools.ts:130` | 生成整工具本地 localSettings 规则 |

模式解析（`src/permissions/modes.ts`）：

- `ALLOW_BYPASS` 由 root/sandbox 判定，决定是否暴露 bypassPermissions；
- `PERMISSION_MODE_ALIASES` 接受 manual→default、acceptedits→acceptEdits、dontask→dontAsk、bypass→bypassPermissions；
- `resolvePermissionMode` 从 settings 的 `permissions.defaultMode` 读取并兜底回 default。

**对 remote-code 的直接启示**：我们的 gRPC 没有 ACP 的 `session/requestPermission` 那样的反向 RPC，
但"审批前先发 pending 的 tool_call 事件 + 选项与效果分离 + 快照持久化建议 + 强制校验回传 optionId"
这套结构可以整体平移成 controller↔CLI 之间的审批流（用我们已有的 `rpcerror` reason 表达 abort/timeout）。

## 7. Plan 模式与"清上下文重启"

**EnterPlanMode**：`tools.ts` 把它映射为 `kind:"switch_mode"` 的 tool call；
PostToolUse 钩子 `createPostToolUseHook`（`src/tools.ts:1397`）在成功执行后通知客户端模式变化。
权限选项固定两个（`options/tools.ts:54`）。

**ExitPlanMode 的双车道**（`src/exit-plan.ts`）：

- `pendingExitPlanModeInterruption`：用户拒绝或选"keep planning"时 `deny(..., interrupt:true)`
  （`effects.ts:152-157`）结束当前 ACP turn；适配器把 Claude 内部 `[ede_diagnostic]` 诊断
  （`executionDiagnostic:107`）映射回 cancelled。
- `pendingExitPlanContextReset`：用户选了 "Yes, clear context and ..." → `deny + interrupt`
  （`effects.ts:142-147`，注释明确"此时 allow 会在旧上下文里执行 ExitPlanMode"），并记录
  `ClearContextReset{toolUseId, plan, mode}`。

**重启编排**：`ExitPlanCoordinator`（`exit-plan.ts:140-217`）持有每 session 的 AbortController；
`continuePlanInFreshContext`（`src/clear-context-coordinator.ts:107-166`）按严格顺序：

1. 检查仍有活动 turn 且未被替换（`assertRestartActive`）；
2. `closeQueryStream` 旧 session；
3. `restartSession`（公开 sessionId 不变）；
4. **replacement 成功后**才搬移 `carriedUsage/carriedModelUsage`、清 `pendingExitPlanContextReset`；
5. 恢复 Fast mode；
6. 迁移 turnQueue（`freshSession.turnQueue = [turn]`）、清零 contextUsedTokens；
7. 发布 mode/configOption；
8. push `Implement the following plan:\n\n<plan>` 续接消息；
9. `ensureConsumer`。

每个 await 后 `assertRestartActive`；失败时区分 abort（取消结算）与 provider 失败（失败结算），
并销毁半成品 replacement、从两个队列里移除 turn（`exit-plan.ts:188-210`）。
`observeExitPlanToolResults`（`exit-plan.ts:75`）用 `tool_result_meta.non_execution_kind === "user-rejected"`
+ 流内容双重对账，因为 resume 后 canUseTool 装的短命标记会丢失。

**模式管理**：`SessionModeManager`（`src/session-mode.ts:55`）拥有模式策略：

- 可用模式 default / acceptEdits / plan / auto（+ 条件 bypassPermissions，`:284-320`）；
- `auto` 模式受 `ModelInfo.supportsAutoMode` 约束，不可用时回退 acceptEdits 并只警告一次（`:20`、`:225-242`）；
- 模型切换后 `reconcileForModel`（`:163`）重新校验；
- 权限结果里的 `setMode:auto` 也会被 `applyPermissionFallback`（`:134`）改写为 acceptEdits。

## 8. 子代理：ACP 级与原生两条路径

**能力协商**：`clientSupportsSubagents`（`src/acp-subagents.ts:102`）接受 ACP 草案的
`clientCapabilities.subagents` 或 AIR 的 `nativeSubagentSessions`（`src/air-extension.ts:57`）。
`acp-subagents.ts` 整个文件是给 SDK 尚未发布的 PR #1992 类型打的临时补丁面
（`subagent_spawned` / `subagent_state_update` / `async_task_*` 通知类型，`:21-92`），
并集中了唯一一处 cast（`asSdkSessionNotification:114`）——SDK 发布后可整体替换。

`NativeSubagentRuntime`（`src/native-subagents.ts:49`）拥有连接本地的注册表与生命周期排序：

- Agent/Task 控制帧（`isNativeSubagentControlUpdate:381`）从根 session 吸收、不转发；
- 子代理的输出按 `_meta.claudeCode.parentToolUseId` 重定向到子 session（`route:81-151`）；
- 未 announce 前的更新进 pending 缓冲（上限 64 父 / 256 总 / 每父 32，超限丢弃并记日志，`:283-298`）；
- 同一 taskId 复用时生成 `taskId:generation:N` 新 session id（`:306`）；
- `finishAll` 以逆序回收；
- **权限请求已落地的 tool_call 不迁移 session**（`:129-133`），避免两侧 transcript 出现孤儿。

旧路径：`forwardSubagentText` / `_meta["subagent-transcript"]`（README"subagent sessions"章节），
无能力协商时退化为把子代理文本并入主 transcript。

## 9. 异步任务与 Steering

`AsyncTaskRuntime`（`src/async-tasks.ts:87`）把 SDK 的非 agent 后台工作（后台 Bash、workflow、monitor）
发布为 `async_task_spawned / progress / state_update`：

- 注册表同时保留**终态墓碑**（Bash 结果里的 backgrounded 证据可能晚于终态事件，`:88-93`）；
- `background_tasks_changed` 是 replace 语义的权威存活边界，缺失者立即以 `stopped` 收尾、
  后续 `task_notification` 可纠正为 completed/failed（`:308-317`、`finish:483-487`）；
- `panelOnlyRecovery` 保证 level-only 恢复不会创建 transcript 卡片且决策单调（`:296-305`、`mergeStarted:414-421`）；
- `taskStopped`（`:350`）在 transcript 发一条 "**Task stopped by user:** ..." 作为唯一回执
  （SDK 不会为停止的 shell 任务注入任何模型上下文），且不受终态门控（`stopAnnounced` 保证只发一次）；
- 后台 Bash 的生命周期只能从 tool result 文本里恢复（`backgroundBashTaskFromToolResult:560`，
  解析 "Command running in background with ID: ..." 与 "Output is being written to: ..." 标记）。

停止入口：`_session/async_task/stop`（`acp-agent.ts:299`、`:2236`），`claimStop/releaseStop` 单飞，
`query.stopTask` + `taskStopped`。

Steering：`_session/steering`（`:296`、`:2172`）：

- 无运行中 turn 时：`idleBehavior:"promptRequired"` 则返回 `{outcome:"promptRequired"}`；
  否则 fire-and-forget 地发起普通 prompt 并返回 `startedNewTurn`（`:2191-2205`）；
- 有运行中 turn 时：构造带 `priority`（now/later，由 `pendingUserInputCount` 决定）的用户消息直接 push 进运行中的 turn，
  并预先登记 `steeredEchoes`、把 `deferredSettle` 挪进 `steeredSettle`（`:2207-2226`），返回 `injected`。

`goal()`（`:2071`）在无运行 turn 时退化为同样逻辑。可运行示例见 `examples/steering.ts`、`examples/simple-client.ts`
（后者演示缓冲 prompt / `!steer` / `!queue` / `!cancel` 四个输入通道与关闭顺序）。

## 10. 工具呈现与 tool_result_meta

`toolInfoFromToolUse`（`src/tools.ts:131-493`）按工具名生成 ACP `ToolCallInfo{title, kind, content, locations}`：

- Bash → `execute`（客户端支持终端时 `content:[{type:"terminal", terminalId: toolUse.id}]`）；
- Read → `read` + 行号 locations；Write/Edit → `edit` + diff 内容；
- Glob/Grep → `search`（Grep 重建出 grep 命令行样式标题）；
- WebFetch/WebSearch → `fetch`；TodoWrite/Task\*/ExitPlanMode → `think` / `switch_mode`；
- 未知 → `other` + JSON 代码块。`toDisplayPath`（`:121`）把绝对路径转为项目相对。

`toolUpdateFromToolResult`（`:577-919`）优先用消息级 `tool_use_result` 的**结构化输出**重建：

- Read 重新生成带行号视图并恢复截断横幅（`:611-660`）；
- Bash 用 `BashOutput.stdout/stderr` 并重建 "aborted" / "persisted output" 提示（`:690-757`）；
- Agent/Task 用 `AgentOutput.content` 并剥掉模型定向尾部（`:828-865`）；
- WebSearch 列出 "Title (url)"（`:880-913`）；
- 结构化缺失时回退原始 tool_result 文本；
- Bash 终端输出走 `_meta.terminal_info / terminal_output / terminal_exit`（`:792-810`），
  无 terminal id 时降级为代码块以免客户端悬挂（`:785-791` 对 Zed `pending_terminal_output` 的说明）。

其他要点：

- Edit/Write 的 diff 来自 PostToolUse 钩子的 `structuredPatch`（`toolUpdateFromDiffToolResponse:1298`），
  钩子回调注册表是**进程级全局 Map**，按 ownerId（session）清理、30s 宽限（`:1341-1394`）；
- 任务清单 `TaskState`（Map）由 TaskCreate/TaskUpdate 工具调用 + `TaskCreated/TaskCompleted` 钩子
  （`createTaskHook:1430`）共同维护，输出为 ACP `plan` 条目（`taskStateToPlanEntries:1260`）；
- `src/tool-result-meta.ts` 解析 SDK 未类型化的 `tool_result_meta` 边车
  （`{id, non_execution_kind, user_feedback}`），未知 kind 保留以兼容新 CLI——
  这是判断"工具结果是否真的执行过"（如 user-rejected）的关键信号。

## 11. Elicitation 桥接（`src/elicitation.ts`）

1. **MCP elicitation**（`mcpElicitationToCreateRequest:42`）：SDK `onElicitation` → ACP form/url 模式；
   url 模式需要稳定 `elicitationId`（缺省生成）；响应经 `createElicitationResponseToElicitResult:90`
   映射回 MCP `ElicitResult`（accept/decline/cancel）。
2. **AskUserQuestion 工具**（`askUserQuestionsToCreateRequest:175`）：注册 `canUseTool` 后 SDK 会把该工具的
   权限检查也路由过来，适配器改用 `handleAskUserQuestion`（`acp-agent.ts:6213`）渲染成表单：
   - 字段键 `question_<n>`（避免题目文本重复出现）；
   - 单选用 `oneOf` 枚举、多选用 `array+anyOf`；
   - 每题附加自由文本 `question_<n>_custom`（带跨 agent 通用 `_meta` 标记 `_askUserQuestionCustomAnswer:157`）；
   - 选项 `preview` 走 `_claude/askUserQuestionOption` `_meta`（`:149`）；
   - 回填时 custom 优先于选择（`applyAskElicitationResponse:256`），decline → 空 answers
     （模型被告知用户跳过），cancel → 取消工具调用。
3. **模型拒答回退**（`REFUSAL_FALLBACK_DIALOG_KIND:318`）：`request_user_dialog` 的 `refusal_fallback_prompt`，
   表单枚举直接用 CLI 的 wire 值 `retry_fallback / cancelled`（`:363-364`），非显式 retry 一律保持拒答（`:415`）——
   **默认值安全**：半填的表单绝不能触发用户没要求的模型切换。

## 12. 文件变更审计（`src/file-change-audit.ts`）

JetBrains AIR 扩展 `agentFileChangeReport`：客户端在 prompt `_meta` 里带 `requestId`，
适配器为该 turn 建立 `FileChangeAuditTurnState{requestId, phase: requested|collecting|finished}`（`:23-26`）。

- 一个隐藏 MCP server `claude_agent_acp`（`:282-287`）暴露 `report_changed_files` 工具
  （zod schema：paths / complete / uncertainty，`:77-93`），`_meta` 标记 `claude/endTurn:true`；
- Stop 钩子（`:250-279`）在 requested 阶段用 `additionalContext` 注入"停止前必须调用一次该工具"的指令
  并把 phase 翻到 collecting；有后台任务时挂起等待最终 Stop（`:257`）；
- **PreToolUse 钩子是强制边界**（`:229-248`）：`canUseTool` 在 bypassPermissions 等模式不会触发，
  只有钩子能保证审计续跑期间只读——除报表工具外一律 deny；
- 路径规范化（`:314-368`）：解析到 workspace 根（cwd + additionalDirectories，去重、处理 macOS
  `/tmp→/private/tmp` 别名与不存在路径的最近存在祖先 `:416-434`）、拒绝越界/超长（4096）/含控制字符、
  总量上限 1024 条 / 256KB（`:18-20`），超限置 `truncated:true, complete:false`；
- 发布 fail-open：终端结果先占位再发布，任何失败都不阻塞用户 turn（`:148-168`）。

## 13. 设置、模型解析、上下文窗口与 token 计量

**设置**：`SettingsManager`（`src/settings.ts:49`）用 SDK 的 `resolveSettings` 合并
user/project/local/managed 设置并应用 `filterEscalatingDefaultMode`（剔除仓库提交的提权
`permissions.defaultMode`，匹配 CLI 信任策略）；对 4 个设置文件所在**目录**做 `fs.watch`
（捕捉新建，`:118-141`），100ms 去抖后重解析并回调 `onChange`。注意它读的是 `CLAUDE_CONFIG_DIR`
而非默认 `~/.claude`（`:94`）。

**模型解析**：

- `tokenizeModelPreference:7894` 把用户输入拆 token（支持 `sonnet` 这类别名 + 上下文提示）；
- `resolveModelPreference:7926` 打分匹配；`matchResumedModel:8011` 为 resume 场景匹配存活模型；
- `applyAvailableModelsAllowlist:8078` 应用 `CLAUDE_MODEL_CONFIG` 的 `availableModels`
  （`parseModelConfig:9538`，`docs/model-configuration.md`）。优先级：`_meta.claudeCode.options.settings` > 环境变量；
- configOption 切换模型走 `applyConfigOptionValue:6327`（含无 IPC 的上下文窗口种子、effort 同步、
  `applyFlagSettings` 的 agent 切换）。

**上下文窗口**：多级解析——`getContextUsage`（CLI 报告）> 模型名推断
`inferContextWindowFromModel:9392`（内置映射表）> 默认 200k（`:270`）。结果按 `(providerCacheKey, modelId)`
进程级缓存（`contextWindowCache:9437`、`providerCacheKeyFor:9472`、`immediateContextWindow:9498`）；
provider 切换会清缓存（`logout:1959`）。`contextWindowAuthoritative` 区分测量值与猜测。

**token 计量**：会话级累加 `accumulatedUsage / accumulatedModelUsage`（`normalizeModelUsage:7350`），
turn 结算时快照；`sessionUsage:7273` / `turnQuotaMeta:7311` / `quotaTokenCount:7329` 产出与
**codex-acp 形状兼容**的 `_meta.quota{token_count, model_usage[]}`（`reasoningOutputTokens` 恒 0、
cache 读记为 `cachedInputTokens`，测试 `src/tests/acp-agent.test.ts:130-164` 固定了该契约）；
`usage_update` 通知携带 used/size/cost。

## 14. Providers / 网关路由

`providers/list | set | disable`（`src/acp-agent.ts:1844-1927`）把 ACP provider 请求（含 Bedrock 变体）
转成 `ProviderConfig`（`gatewayRequestToProviderConfig:7473`），`enqueueProviderUpdate:7223` 串行化应用；
`createEnvForProvider:7490` 把配置翻译成环境变量路由（`ANTHROPIC_BASE_URL` / `ANTHROPIC_AUTH_TOKEN` /
Bedrock 一组，`PROVIDER_ROUTING_ENV_VARS:9448-9461`），未设置的占位符用 `"acp-proxy"` 之类哨兵避免遗留值干扰。
`authenticate` 支持 gateway 流程。

## 15. 会话标题与会话失败扩展

**标题**（`src/session-titles.ts`）：SDK 路径下 Claude Code 的自动标题生成不会运行（headless 预置了闩），
`SDKSessionInfo.summary` 退化为原始首条 prompt。适配器：

- 收集本 session 的 user+assistant 文本尾部 1000 字符（`:59`）；
- turn 结束（idle）时优先采纳 `customTitle`（用户 `/rename` 与生成标题共用该字段，
  因此**每 session 至多生成一次**，`:81-85`）；
- 否则后台调用未公开的 `query.generateSessionTitle(description, {persist:true})`（`:34`、`:219`），
  失败释放闩并回退 summary；`conversation_reset` 时 `reset()`。

**AIR sessionFailure**（`src/session-failure-extension.ts:247`）：`SessionFailureController` 把
`ClaudeFailureKind`（12 种，`:19-31`）映射到 category/actions/fallbackTitle（`:83-151`）与
recoveryPolicy（`:225-242`）：

- id 语义区分 turn 级（`<turnId>:error`）与会话级（`<sessionId>:session-error:<epoch>:<n>`），revision 递增；
- **发布成功才记 active**（`:371-378`）；恢复只在内部清除（transcript 记录是历史）；
- advisory 类按标题去重（`:325-333`）。

## 16. 值得借鉴的设计点（面向 remote-code 的 Go 控制平面）

按可移植性排序：

1. **Turn/deferred 状态机本身**（§3）。我们的 controller 已经有"进程长期存活 + 会话内多次请求"的形态，
   把"Claude 对话轮"建模为入队 + 常驻 reader 结算的 turn，是把流式 agent 内核套进 RPC 语义的正确姿势。
   Go 里用 channel + 每 turn 一个 done channel/errgroup 即可等价实现；孤儿记账与 30s 强杀兜底是必须保留的健壮性。
2. **权限管线结构**（§6）：先发 pending 的 tool_call 事件、选项与效果分离、快照持久化建议、
   校验回传 optionId、`decisionClassification`。可直接映射为 gRPC 双向流上的审批子协议。
3. **"呈现复用"原则**：审批 UI 与 transcript 用同一个 `toolInfoFromToolUse`，避免双份名称漂移
   （`src/permissions/presentation.ts:76-78` 注释）。remote-code 的 CLI 渲染也应复用同一份工具元数据构造。
4. **能力协商 + `_meta` 扩展**：所有非标准能力（steering、goal、AIR 系列）都经 initialize 协商后才启用
   （`acp-agent.ts:1730-1748`、`air-extension.ts:57`）。我们做 Claude 集成扩展时应照此模式，
   而不是无条件推送自定义字段。
5. **settings 的信任模型**：`filterEscalatingDefaultMode` 剔除仓库内提权配置 + 热重载（`src/settings.ts:105-113`）。
   controller 侧如果读取工作区内的 Claude 配置，必须同样过滤提权项——
   这与我们"模板/MCP 定义必须在工作区外"的不变量同源。
6. **结构化优先、原始文本兜底**的 tool_result 渲染（§10）：先读结构化 `tool_use_result`，缺失再解析文本，
   且解析失败仅"停止匹配"而不破坏数据（`stripAgentTrailer` 系）。Go 侧做日志/输出渲染时同样要双路径。
7. **文件变更审计的钩子组合**（§12）：MCP 工具 + Stop 钩子（注入指令）+ PreToolUse 钩子（强制只读）+
   fail-open 发布。remote-code 若要提供"本轮改了哪些文件"的审计，这是现成方案，
   且它的路径规范化（别名消解、越界剔除、256KB 上限）可整体移植到 Go。
8. **context window / token 计量的形状兼容**（§13）：`_meta.quota` 与 codex-acp 对齐、按 (provider, model) 缓存。
   我们的 token 上报若想同时服务多个 agent 内核，先定一个中立 schema 再各自适配是正确方向。
9. **`Pushable` + `nodeToWeb*`**（`src/utils.ts`）：把 push 型输入桥接到 async 迭代器，
   等价于 Go 的 channel，思路一致。
10. **集中式 mock 基桩的教训**（§18 的 `makeMockQuery`）：新 SDK 方法出现时只改一处，
    避免"漏改的拷贝静默走错误分支"。

## 17. 坑与教训（同样重要）

1. **SDK 是强耦合点，且在快速漂移**：代码里大量防御新 CLI 版本——
   `command_lifecycle` 是 `@internal` 且不在 SDKMessage 联合类型里，只能在 exhaustive switch 前手动拦截
   （`acp-agent.ts:3054-3061`）；`generateSessionTitle` 存在于运行时但 `sdk.d.ts` 未声明
   （`session-titles.ts:31-39`）；`tool_result_meta` 完全未类型化（`tool-result-meta.ts:7`）。
   **remote-code 若走"包装官方 SDK"路线，会继承同样的版本追逐成本**；
   替代方案是直接以 `claude -p --output-format stream-json` 等 CLI 约定驱动（我们已有通用进程能力），
   代价是自己处理这些边角。
2. **cancel 语义远比"杀进程"复杂**：排队 turn 立即结算、活动 turn 等 trailing idle、
   steering 回显变孤儿、权限请求要可中断、30s 兜底强杀（§5）。
   任何"取消=abort"的简化都会产生幽灵 result 或悬挂请求。
3. **缓存重放路径会产生零 token、零流式事件的 turn**（issue #453，`acp-agent.ts:4240-4259`），
   必须保留"result 文本兜底 + 已发文本去重"。我们做用量统计时不能假设 output_tokens > 0。
4. **replace 语义的后台任务级别**（§9）：`background_tasks_changed` 是权威存活边界，但缺身份信息；
   适配器用 `panelOnlyRecovery` 保证"先恢复面板、后到的事实不回溯创建 transcript"的单调性。
   Go 侧做任务面板会遇到同款问题。
5. **进程级全局钩子注册表**（`tools.ts:1341`）：SDK 钩子是进程范围，多 session 必须带 ownerId 清理 +
   定时器兜底，否则泄漏。
6. **stdout 即协议**：一切日志走 stderr（`src/index.ts:55-56`）。remote-code 的进程封装已遵循此道
   （分段日志），集成 Claude 时同样不能往子进程 stdout 写任何东西。
7. **权限选项必须回传校验**：不校验 optionId 的客户端回包会直接把任意 `PermissionUpdate` 注入 SDK
   （`permissions/response.ts` 的存在理由）。
8. **模型能力约束模式**：auto 模式并非普适（`supportsAutoMode`），需要在模型切换后重校验并只警告一次
   （`session-mode.ts:163-207`）。

## 18. 测试策略

**框架**：vitest，`vitest.config.ts`（node 环境、`src/**/*.{test,spec}`、v8 覆盖率、`@→/src` 别名）；
`package.json` 的 `test:integration` 用 `RUN_INTEGRATION_TESTS=true` 门控。

**双层结构**：

- 单元层：不 mock SDK 模块，而是 **mock `Session` 与 query 对象**——
  - `src/tests/helpers.ts:24` 的 `makeMockQuery` 集中桩掉 agent 无条件触碰的 SDK 表面
    （`initializationResult / setModel / setPermissionMode / supportedCommands / getContextUsage / close / interrupt / asyncIterator`），
    注释明确记录了"新方法没加进基桩会静默走错误分支"的教训；
  - `src/tests/session-doubles.ts:54` 的 `mockSessionState` 集中 Session 字面量；
    `wrapQuery:38` 给裸 async generator 补控制方法；`userEcho:24` 构造 SDK 会回显的 user 消息；
    `successfulResultMessage:100` 是"一条成功 turn"的标准消息序列；
  - `injectGeneratorSession`（`acp-agent.test.ts:173`）把生成器接进 session 的 Pushable 输入，
    使测试可以"半路再推一条消息"。
- 集成层：`describe.skipIf(!process.env.RUN_INTEGRATION_TESTS)`（`acp-agent.test.ts:331` 起）先 `tsc`
  编译再 `spawn("npm", "run", "dev")` 起真实子进程，用 `ndJsonStream` + `acpClient` 走完整 ACP 握手；
  `TestClient`（`:356-442`）实现 fs 读写存根、权限自动选 allow_once、elicitation 自动选首项，
  并对 `available_commands_update` 用 Promise 解锁异步断言。`getSessionMessages` mock 为透传真实实现
  （`:76-87`）以便读真实 transcript 的集成用例仍可用。

**断言风格**：把**线上契约固化为构造器**——`expectedTokenCount / expectedQuotaMeta`
（`acp-agent.test.ts:130-164`）、`lifecycleFrame`（`:109`）每测试复用，防 wire 形状漂移。

**对 remote-code 的启示**：Go 侧等价物是"定义一个最小的 Claude 消息流接口 + fakes"，
让单元测试完全不依赖 Claude 凭证/网络（与我们 CLAUDE.md 的既有要求一致），
再用 build tag 或环境变量门控真正拉起 `claude` 二进制的集成测试；
契约断言集中在少数 builder 函数里。

## 19. 结论

claude-agent-acp 的价值不在算法而在**工程化程度**：它把一个语义丰富的流式 agent 内核（Claude Agent SDK）
翻译成请求-响应协议（ACP），并用显式的 turn 状态机、孤儿记账、结算多信号、能力协商和分层权限管线
消化了真实世界的不确定时序。

对 remote-code 而言：Turn 状态机、权限审批管线、能力协商式扩展、settings 信任过滤、
token 计量 schema 与测试分层都可以直接借鉴；而它对 SDK 内部未文档化表面的依赖
（command_lifecycle、tool_result_meta、generateSessionTitle）提醒我们：
**选择包装 SDK 还是驱动 CLI，本质是在"功能完整度"与"版本追逐成本"之间做取舍**，
两条路线都需要 §5 / §17 列出的那套取消与去重防御。
