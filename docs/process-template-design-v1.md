# 进程模板详细设计 v1

本文定义服务端进程模板（process template）的首版契约。模板用于把稳定、复杂的 Code Agent
启动配置保存在 controller 侧，由客户端只提交模板名称和一组动态参数。模板渲染后的结果仍然进入
现有通用进程服务；模板层不直接创建子进程，也不引入 shell。

## 1. 目标与非目标

首版目标：

- operator 可以配置多个具名模板；
- 每个模板固定 executable、PIPE/PTY 模式和输入生命周期模式；
- 客户端使用任意 JSON object 作为动态参数；
- 每个模板使用 JSON Schema 约束参数，并用受限 Expr 表达式生成 arguments、工作目录和环境覆盖；
- 模板在 controller 启动期完成安全读取、严格解码、Schema 编译和 Expr 编译；
- gRPC 和 CLI 可以发现、描述并启动模板；
- 模板启动复用 `StartProcess` 的工作区、大小、并发、持久化、日志和进程组语义；
- 动态参数值以及由其生成的 argv 不进入进程元数据、审计日志或错误日志。

首版不包括：

- shell command line、引号解析或字符串按空白切分；
- 模板热更新、include、继承、模板嵌套或远程模板；
- 在 Expr 中调用文件、进程、网络、时间、随机数或 MCP host function；
- 通过模板启动 RPC 发送初始 stdin 内容；
- 自动重启进程或根据模板重建 LOST 进程；
- 为 `StartProcess` 增加通用幂等键；
- 把模板当成独立操作系统沙箱。

## 2. 核心流程与职责

```text
StartProcessFromTemplate
    │
    ├── immutable template registry：查找名称和可选 expected revision
    ├── JSON Schema validator：校验 parameters
    ├── pure Expr renderer：生成动态启动字段
    ├── rendered result normalizer：严格类型、字段和资源上限
    ▼
existing process start core
    ├── name/cwd/argv/env/PTY/input 最终校验
    ├── workspace 安全打开
    ├── registry 并发与名称约束
    ├── record/log 创建
    └── runner、进程组和 reaper
```

进程服务是唯一的启动实现。直接 `StartProcess` 和模板启动只在请求构造及公开元数据策略上不同，
从工作目录校验开始共享同一路径。

模板定义属于 operator trusted code。RPC parameters、模板渲染结果以及 Expr 运行错误都按不可信输入
处理。模板文件不能位于 Agent 可写的 workspace 中。

## 3. Controller 配置

Controller TOML schema 增加 version 3。version 1、2 继续保持原语义；只有 version 3 可以包含：

```toml
[process_templates]
definition_files = [
  "/etc/remote-code/process-templates/code-agents.process-template.yaml",
]
```

模板首版不提供命令行覆盖，避免列表字段的追加/替换歧义。省略 table 或使用空列表表示没有模板。

每个 definition file：

- 后缀必须为 `.process-template.yaml`；
- 必须是 1 byte–1 MiB 的普通文件，全部文件总计不超过 8 MiB；
- Linux 使用 `O_NOFOLLOW` 打开并以 device/inode 去重；
- 物理路径必须在 workspace 外；
- 只接受单个 YAML document；
- 拒绝 alias、anchor、merge key、显式 tag、directive、重复 key 和非 JSON scalar；
- 使用严格字段解码，未知字段使 controller 启动失败。

模板 document 的结构如下：

```yaml
version: 1
language: expr
templates:
  - name: agent
    description: Start an interactive code agent.
    parameters_schema:
      $schema: https://json-schema.org/draft/2020-12/schema
      type: object
      required: [model, prompt, working_directory]
      additionalProperties: false
      properties:
        model:
          type: string
          description: Model name passed to the agent.
        prompt:
          type: string
          description: Initial task prompt; never persisted by controller.
        working_directory:
          type: string
          description: Workspace-relative working directory.
    command: agent-command
    io_mode: pty
    input_mode: managed
    render: |-
      {
        "arguments": ["--model", parameters.model, "--prompt", parameters.prompt],
        "working_directory": parameters.working_directory,
        "environment": {}
      }
```

`command`、`io_mode` 和 `input_mode` 是静态字段。把 executable 固定在 operator 配置中，避免一个模板
退化为任意命令代理。`render` 必须返回 object，只允许以下字段：

| 字段 | 类型 | 缺省值 |
| --- | --- | --- |
| `arguments` | array of string | `[]` |
| `working_directory` | string | `.` |
| `environment` | object，value 必须为 string | `{}` |

渲染结果不能设置 command、进程实例名称、I/O 模式、输入模式或 terminal size。环境变量名称如果由参数
间接控制，代表 operator 明确授予调用者该自由度；安全模板应在 Expr map literal 中固定环境变量名称。

## 4. JSON Schema 参数契约

`parameters_schema` 必须：

- 使用 JSON Schema Draft 2020-12；
- root `type` 为 `object`；
- 显式设置 `additionalProperties: false`；
- 每个公开 property 提供非空 description；
- `$ref` 和 `$dynamicRef` 只能引用本 document 的 fragment；
- 禁止加载 file、HTTP 或其它 external resource；
- 满足 Schema 深度、节点数量和正则表达式长度上限。

JSON Schema 的 `default` 只是说明信息，首版不自动改写 parameters。可选值的缺省行为应在 Expr 中显式
表达。校验错误只返回最多若干个 JSON Pointer 路径，不回显实际参数值。

gRPC 使用 `google.protobuf.Struct` 承载 parameters，因此参数采用 JSON 数值语义。需要精确表示超过
IEEE-754 安全整数范围的模板不属于首版能力；命令行数值应由参数提供为 string。

## 5. Expr 执行模型

模板使用仓库已经固定版本的 `expr-lang/expr`，但使用独立的 pure render profile：

- 唯一环境变量为 `parameters: map<string, any>`；
- 不提供 context、call metadata、自定义函数或 host dispatcher；
- 禁用 `now`；禁止 range operator 和 `repeat`；
- 限制脚本 byte、AST depth/node、collection call site；
- 参数和输出限制总 byte、节点数、深度及 collection 大小；
- definition 加载时编译，调用时只运行 immutable program；
- 运行前后检查 RPC context；Expr 没有通用强制中断，因此资源上限是主要的运行时间边界。

类型转换必须严格。布尔、数字、null、object 不能隐式格式化为 argv 或 env value。模板作者需要在 Expr
中显式转换为 string。一个数组元素始终是一个 argv 元素，即使包含空格、引号或换行，也绝不二次切分。

渲染完成后仍调用现有进程请求 validator，因此当前 256 个参数、单参数 4 KiB、总 argv 64 KiB、环境
变量数量/大小、NUL、PTY terminal size 和 cwd 边界全部继续生效。

## 6. gRPC API

`ProcessService` 追加三个 unary RPC：

```protobuf
rpc ListProcessTemplates(ListProcessTemplatesRequest) returns (ListProcessTemplatesResponse);
rpc GetProcessTemplate(GetProcessTemplateRequest) returns (GetProcessTemplateResponse);
rpc StartProcessFromTemplate(StartProcessFromTemplateRequest) returns (StartProcessFromTemplateResponse);
```

主要 message 语义：

- `ProcessTemplateSummary`：name、description、revision、io_mode、input_mode；
- `ProcessTemplate`：summary 加 `google.protobuf.Struct parameters_schema`；
- `StartProcessFromTemplateRequest.template_name`：精确模板名称；
- `parameters`：模板参数 object；未设置等价于空 object；
- `process_name`：可选实例名称，不属于 parameters；
- `terminal_size`：仅模板为 PTY 时有效；
- `expected_template_revision`：可选的完整 SHA-256 revision；不匹配返回 `FAILED_PRECONDITION`。

revision 是模板规范化内容的 SHA-256 小写十六进制值，至少覆盖名称、说明、Schema、command、I/O 模式、
输入模式和 Expr source。即使首版不热更新，revision 仍用于进程历史、部署比较和未来兼容。

错误码：

| 场景 | gRPC code |
| --- | --- |
| request 缺失、名称格式错误、Schema 校验失败 | `INVALID_ARGUMENT` |
| 模板不存在 | `NOT_FOUND` |
| expected revision 不匹配、Expr 或结果因模板定义无法生成有效 spec | `FAILED_PRECONDITION` |
| 渲染后命中现有 cwd、限额、名称或启动错误 | 保持现有进程 API 语义 |
| shutdown 已开始 | `UNAVAILABLE` |

新增 RPC 和 message 都是 `remote.code.v1` 内的追加式变更，不修改或复用现有字段编号。

## 7. 进程元数据与脱敏

`ProcessInfo` 追加：

- `template_name`；
- `template_revision`；
- `arguments_redacted`。

直接启动保持当前行为。模板启动时：

- runner 只在启动事务内存中获得完整渲染 argv；
- `ProcessInfo.arguments` 和 `metadata.json.arguments` 保存空数组；
- `arguments_redacted` 为 true；
- command、工作目录、环境 key、模板名称和 revision 正常保存；
- parameters、环境 value、Expr 输出和完整 argv 不写日志或磁盘；
- 启动失败所保留的 FAILED record 同样只包含脱敏信息。

进程 record schema 升级为 2，并继续读取 schema 1。schema 1 记录按直接启动历史解释。controller 不会
根据模板信息自动重启进程，因此无需为了恢复保存参数值。

模板本身可能把 prompt 输出到子进程 stdout/stderr；这属于受管进程输出数据，遵循已有进程日志权限和
保留策略，而不是 controller 审计日志。

## 8. 权限与安全边界

首版模板功能首先解决可用性，不自动成为授权边界。若同一 bearer token 同时可以调用原始
`StartProcess`，调用者仍然可以绕开模板。未来若需要模板白名单语义，应增加独立 RPC capability 或允许
关闭直接启动，不能仅依靠 UI 隐藏原始 RPC。

即使没有 shell，目标 executable 仍可能把某些 argv 当作代码、配置文件或子命令执行。模板 operator
必须限制高风险参数，避免无约束 `extra_args`、`sh -c`、动态 executable、动态 loader 环境变量等入口。

工作区限制仍不是 sandbox。面向不可信 parameters 或 Agent 时，继续要求独立系统用户、容器/VM、文件
权限、网络策略和 CPU/内存限制。

## 9. CLI 与客户端

公共 Go client 增加 list、get 和 start-from-template typed wrapper。

REPL 首版命令：

```text
templates [TEMPLATE]
exec-template [--name NAME] [--attach] [--params JSON|--params-file LOCAL_FILE] TEMPLATE
```

`templates` 无参数时显示摘要，有名称时显示 Schema。`exec-template` 缺省 parameters 为 `{}`。
`--attach` 先读取模板摘要并确认 PTY + MANAGED，再获取本地 terminal size、启动并复用已有 attach。
CLI 不打印 parameters 或渲染后的 argv。

## 10. 启动、并发与配置更新

`server.Prepare` 在绑定 listener 或创建进程 runtime 前构造 immutable template registry。
`--check-config` 执行相同准备过程。任一文件、模板、Schema 或 Expr 错误都拒绝整个 registry，不做部分发布。

多个 RPC 可以并发运行同一个 immutable Expr program；每次调用拥有独立 parameters 和结果对象。最终启动
仍受现有 process service mutex、活动进程上限和活动名称唯一性保护。

首版修改模板需要重启 controller。未来热更新必须构造完整新 registry 后原子替换；已启动进程保留原
revision，不随 registry 改变。

## 11. 测试与验收

单元测试至少覆盖：

- YAML unknown/duplicate/alias/tag/multi-document、文件大小、后缀、symlink、workspace 内文件和重复 inode；
- 模板名称、重复名称、静态 command、模式和 description；
- Schema dialect、root type、additionalProperties、external ref、深度和非法参数；
- Expr 编译、未定义值、`now`、range/repeat、AST/输入/输出限制；
- arguments/cwd/environment 正常渲染、条件参数和严格类型错误；
- unknown template、revision mismatch、context cancellation；
- 模板启动复用 cwd symlink 防护、进程限额、PTY/input 和 shutdown 语义；
- ProcessInfo、FAILED/EXITED record 和重启恢复不包含动态 argv/参数值；
- gRPC client、CLI list/describe/start/attach 与错误输出不泄露 parameters；
- 并发渲染及启动通过 race test。

完整验收命令：

```bash
make generate
make format
make test
make test-race
make lint
make build
```
