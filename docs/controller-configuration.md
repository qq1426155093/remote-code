# Controller 配置文件

`remote-code-controller` 同时支持 TOML 配置文件和原有命令行参数。配置文件必须显式通过
`--config` 指定，不会从当前目录或系统目录隐式加载。

## 覆盖顺序

最终配置按以下顺序合并：

```text
内置默认值 < TOML 配置文件 < 显式命令行参数
```

例如：

```bash
remote-code-controller \
  --config /etc/remote-code/controller.toml \
  --max-processes 32
```

除 `max_processes` 被命令行覆盖外，其它值仍来自 TOML。布尔值可以显式反向覆盖，例如
`--allow-insecure-remote=false`。

## TOML schema v1

```toml
version = 1
workspace = "/srv/remote-code/workspace"
listen_address = "127.0.0.1:9443"
runtime_directory = "/var/run/remote-code-controller"
max_upload_bytes = 1073741824
max_processes = 16
allow_insecure_remote = false

[tls]
certificate_file = "/etc/remote-code/tls/server.crt"
key_file = "/etc/remote-code/tls/server.key"

[auth]
token_file = "/etc/remote-code/controller.token"
```

`version = 1` 必须存在。TLS certificate/key 必须同时配置。认证配置只接受 token 文件路径，
不允许直接把 token 放进 TOML。所有相对路径按 controller 进程的当前工作目录解释；生产
配置建议使用绝对路径。

解析采用严格模式：未知字段、错误类型、重复 key、缺失/不支持的 schema version 都会使
controller 拒绝启动。配置文件最大 1 MiB。

## 字段映射

| TOML | 命令行覆盖参数 | 默认值 |
| --- | --- | --- |
| `workspace` | `--workspace` | 必填 |
| `listen_address` | `--listen-addr` | `127.0.0.1:9443` |
| `runtime_directory` | `--runtime-dir` | `/var/run/remote-code-controller` |
| `max_upload_bytes` | `--max-upload-bytes` | `1073741824` |
| `max_processes` | `--max-processes` | `16` |
| `allow_insecure_remote` | `--allow-insecure-remote` | `false` |
| `tls.certificate_file` | `--tls-cert` | 空 |
| `tls.key_file` | `--tls-key` | 空 |
| `auth.token_file` | `--token-file` | 空 |

## 校验

以下命令解析、合并并校验配置，不启动 listener：

```bash
remote-code-controller --config /etc/remote-code/controller.toml --check-config
```

成功时输出 `configuration OK`。校验包含 schema、字段类型、范围、TLS 配对、明文远端
监听策略、workspace 目录和 token 文件；listener 是否可绑定、TLS 证书内容以及 runtime
目录创建仍在实际启动时验证。
