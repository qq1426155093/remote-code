# Remote Code 文件控制首版需求

## 1. 目标

首版交付两个可执行程序：

- `remote-code-controller`：运行在远端机器，限制在一个启动时指定的工作区中，通过 gRPC 提供文件服务。
- `remote-code`：运行在本地，连接 controller 后进入类似 MySQL client 的长期交互式命令行。

本版本打通“连接、浏览、上传、下载、查看、删除、移动、创建目录、修改权限”的完整闭环。CLI 运行在用户当前终端或 PTY 中，读取命令并展示结果；终端退出前保持 gRPC 连接。文件命令不是远程 shell，controller 不执行任意命令。

## 2. 用户流程

启动 controller：

```bash
remote-code-controller --workspace /srv/project --listen-addr 127.0.0.1:9443
```

连接并进入交互环境：

```bash
remote-code --controller-addr 127.0.0.1:9443
```

连接成功后显示 controller 版本与提示符：

```text
Connected to remote-code-controller v0.1.0
remote-code:/> ls
remote-code:/> cd docs
remote-code:/docs> upload ./requirements.md requirements.md
remote-code:/docs> exit
```

当单条命令失败时，CLI 打印结构化、可理解的错误并继续运行。输入 `exit`、`quit` 或终端 EOF 才结束会话；`Ctrl-C` 清空当前输入，不关闭 controller。

## 3. 交互命令

远端路径一律相对于 controller 工作区。提示符中的 `/` 只是虚拟根，不代表远端系统根目录。

| 命令 | 行为 |
| --- | --- |
| `help [command]` | 显示命令帮助 |
| `pwd` | 显示当前远端虚拟目录 |
| `cd [REMOTE_DIR]` | 切换 CLI 维护的远端当前目录，默认回到 `/` |
| `ls [-l] [REMOTE_PATH]` | 列出目录；若参数是文件则显示该文件 |
| `stat REMOTE_PATH` | 显示类型、大小、权限和修改时间 |
| `cat REMOTE_FILE` | 将小型远端文件内容输出到当前终端 |
| `upload LOCAL_FILE [REMOTE_FILE]` | 分块上传本地普通文件；省略目标时使用本地文件名 |
| `download REMOTE_FILE [LOCAL_FILE]` | 分块下载；省略目标时使用远端文件名 |
| `mkdir [-p] REMOTE_DIR` | 创建目录，`-p` 同时创建父目录 |
| `rm [-r] REMOTE_PATH` | 删除文件或空目录；`-r` 递归删除目录 |
| `mv [-f] SOURCE DESTINATION` | 移动或重命名；`-f` 允许覆盖目标 |
| `chmod OCTAL_MODE REMOTE_PATH` | 修改权限，例如 `chmod 640 configs/app.yaml` |
| `clear` | 清理本地终端显示 |
| `exit` / `quit` | 关闭连接并退出 CLI |

命令参数支持单引号、双引号和反斜杠转义，以便处理带空格的文件名。首版不支持通配符展开、管道、重定向、远程 shell 命令或交互式远端文件编辑。

## 4. 启动参数

### 4.1 controller

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--workspace` | 无 | 必填，允许访问的工作区目录 |
| `--listen-addr` | `127.0.0.1:9443` | gRPC 监听地址 |
| `--max-upload-bytes` | `1073741824` | 单个上传文件最大字节数 |
| `--tls-cert` / `--tls-key` | 空 | 同时提供时启用 TLS |
| `--token-file` | 空 | 可选 bearer token 文件，内容不会写日志 |
| `--allow-insecure-remote` | `false` | 显式允许在非 loopback 地址上使用明文 gRPC |

`--workspace` 必须是已存在的目录。TLS 证书与私钥必须成对提供。为防止误暴露，非 loopback 监听在未启用 TLS 时默认拒绝启动。

### 4.2 CLI

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--controller-addr` | `127.0.0.1:9443` | controller 地址 |
| `--tls-ca` | 空 | controller CA/证书文件；为空时使用明文 gRPC |
| `--tls-server-name` | 空 | 可选 TLS ServerName 覆盖值 |
| `--token-file` | 空 | 可选 bearer token 文件 |
| `--timeout` | `30s` | 单条交互命令的 RPC 超时，`0` 表示不设超时 |
| `--cat-max-bytes` | `1048576` | `cat` 最多向终端输出的字节数 |

CLI 只有在 `GetInfo` 调用成功后才进入提示符，因此“连接成功”代表网络、TLS、认证和 API 都已验证。

## 5. 文件行为

- `ls` 返回直接子项，按名称排序，不递归遍历。
- `stat` 与 `ls` 能识别普通文件、目录和符号链接；符号链接信息不泄漏工作区外部目标。
- `cat` 和 `download` 只接受普通文件。下载内容携带 SHA-256 摘要，CLI 在完成时校验。
- `upload` 只接受本地普通文件。controller 在目标同目录写临时文件，校验声明大小和 SHA-256 后原子发布；失败时清理临时文件。
- 上传默认覆盖现有普通文件；不能用文件覆盖目录。上传保留本地的 `0777` 权限位，不传输 owner、group、ACL、扩展属性或特殊权限位。
- `download` 在本地同目录写临时文件，校验摘要后重命名到最终路径，避免失败留下半文件。默认覆盖已有普通文件。
- `rm` 禁止删除工作区虚拟根。没有 `-r` 时不删除非空目录。
- `mv` 默认不覆盖已存在的目标；`-f` 才允许覆盖。禁止把任何对象移动到工作区之外或移动工作区虚拟根。
- `chmod` 只接受 `0000` 到 `0777`，不允许设置 setuid、setgid 或 sticky 位。
- 空路径按当前目录处理，但所有需要明确目标的破坏性命令在 CLI 与 controller 两端都校验。

## 6. 安全与可靠性要求

- 拒绝绝对路径、NUL 字节、`..` 路径穿越以及通过符号链接逃出工作区的访问。
- controller 使用基于目录句柄的根目录 API 执行文件操作，路径校验不能只依赖字符串前缀。
- RPC 错误使用合适的 gRPC 状态码，例如 `InvalidArgument`、`NotFound`、`AlreadyExists`、`PermissionDenied`、`ResourceExhausted` 和 `DataLoss`。
- 上传大小由服务端强制限制；网络取消、客户端中断、哈希不一致和写盘失败均不得发布不完整目标。
- 下载与上传以固定上限的块传输，不把整个文件读入内存。
- controller 收到 `SIGINT` 或 `SIGTERM` 时停止接受新请求并优雅关闭。
- 日志不得记录 token、文件内容或上传内容。
- token 比较采用常量时间比较；启用 token 后，所有业务 RPC 均需认证。

工作区边界不是完整操作系统沙箱：首版不阻止工作区内的 bind mount、设备文件或其他文件系统挂载。生产环境仍应使用受限系统用户、容器或虚拟机，并启用 TLS 与认证。

## 7. 验收标准

1. `go build ./...`、`go test ./...`、`go test -race ./...` 和 `go vet ./...` 全部通过。
2. CLI 能建立连接并持续处理多条命令，单条错误不会结束会话。
3. 通过真实 gRPC 连接完成目录创建、上传、列表、查看、下载、移动、修改权限和删除闭环。
4. 上传与下载对内容执行 SHA-256 校验，并使用临时文件避免暴露半成品。
5. 自动测试覆盖绝对路径、`..`、符号链接逃逸、根目录删除、上传大小限制、哈希不一致和覆盖规则。
6. `.proto`、生成代码和可复现的生成命令一并提交；测试不依赖任何 Agent 凭据。

## 8. 首版不包含

- Claude Code/其他 Agent 的启动、PTY attach 和生命周期管理。
- 多租户、细粒度权限、配额持久化和审计数据库。
- 断点续传、增量同步、目录整体上传下载和文件监控。
- Windows controller 支持承诺；首版验证目标为 Linux。
- Web UI、非交互批处理输出格式和 shell 补全。

