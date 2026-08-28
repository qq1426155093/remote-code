# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Remote Code is a control plane for remote development Code Agents, written in Go (requires Go 1.26). A long-running `controller` on a remote machine exposes a versioned gRPC API; a local interactive CLI (`remote-code`) manages workspace files and generic managed processes. Claude Code integration will be built on the generic process capability later. The README and `docs/` are written in Chinese; design docs in `docs/` are the authoritative feature specifications.

## Commands

```bash
go build ./...                              # compile everything
go test ./...                               # full test suite
go test ./internal/process/ -run TestName   # single test
make test-race                              # race tests (needs CGO + cc; see Makefile RACE_CC)
go vet ./...                                # static checks
make format                                 # gofmt over all .go files
make build                                  # builds bin/remote-code-controller and bin/remote-code
make generate                               # protobuf codegen via pinned buf/protoc plugins in .tools/bin
make lint                                   # buf lint + go vet
```

Generated protobuf code (`api/remote/code/v1/*.pb.go`) is committed; regenerate with `make generate` after editing `remote_code.proto`.

Run a local controller + CLI:

```bash
./bin/remote-code-controller --config ./bin/remote-code-controller.toml
./bin/remote-code --controller-addr 127.0.0.1:9443   # REPL; type `help`
```

`--check-config` validates configuration (strict YAML/JSON Schema/Expr compilation) without binding ports.

## Architecture

Two binaries over one gRPC API (`api/remote/code/v1/remote_code.proto`):

- `cmd/controller` — parses TOML config (schema-versioned, currently v8; see `cmd/controller/config.go`) plus CLI flags that override TOML. Hands off to `internal/server`.
- `cmd/remote-code` — interactive REPL; commands are registered centrally as `commandSpec` entries in `internal/cli/command.go` (add new commands there, not ad hoc).

`internal/server` is the wiring point: `Prepare()` validates config and compiles process templates, MCP tools, and workflows *without* binding listeners (this powers `--check-config`); `NewPrepared*()` binds sockets. It assembles:

- `internal/files` — workspace-confined file service built on `os.Root`; atomic uploads (temp file + size/SHA-256 verification + rename), resumable transfer sessions with durable offsets/checkpoints.
- `internal/process` — persistent process registry. Process groups (PTY also gets own session); per-process records under `--runtime-dir/<uuid>/` (`metadata.json`, `status.json`, v2 segmented logs with tail index); on restart, exited history is recovered and records left in an active state are marked LOST (live processes are not re-adopted). Process templates: operator-defined executables validated by JSON Schema with pure-Expr parameter expansion. One input writer per process; attach = client composes `StreamProcessInput` + `ObserveProcessLogs(follow=true)` — there is deliberately no separate Attach RPC.
- `internal/agent` — ACP agent bridge (enabled by default, lazy): one shared `claude-agent-acp` child over the registry's raw-pipe entry; `AgentService.Query` streams turn events; crash → sessions marked lost, next query restarts.
- `internal/mcp` — optional MCP Streamable HTTP server (disabled by default). Tools are defined in `.mcp.yaml` files that must live *outside* the workspace; may have a bearer token separate from gRPC.
- `internal/workflow` — durable workflow engine (bbolt event store, disabled by default, no public RPC/CLI yet).
- `internal/controllerlog` — bounded JSON event log of controller lifecycle/process/MCP diagnostics, replayed via `ControllerService.ObserveControllerLogs` / CLI `controller-logs`.
- `internal/auth` — bearer-token auth for both gRPC and MCP HTTP.
- `internal/rpcerror` — machine-readable `google.rpc.ErrorInfo` reasons attached to gRPC errors. Reason strings are stable API surface: add new ones, never rename or reuse. Clients switch on reasons, not messages.
- `pkg/client` — reusable public Go client used by the CLI; negotiates resumable transfers via `GetInfo.file_transfers`.

Platform handling uses build tags: Linux and macOS are full server targets (`runner_command_linux.go` / `runner_command_darwin.go`); other platforms get explicit "unsupported" stubs (`*_unsupported.go`, tag `!linux && !darwin`). macOS-specific safe path/dirfd logic lives in `internal/darwinfd`; file locking per platform in `internal/controllerlog/lock_*.go`.

## Conventions

- Follow idiomatic Go and `gofmt`. Wrap errors with context (`fmt.Errorf("start agent %q: %w", id, err)`); use typed gRPC status errors (with `rpcerror` reasons) at transport boundaries.
- Table-driven tests named after behavior (e.g. `TestRegistry_StopExitedAgent`). Cover lifecycle transitions, cancellation, concurrent attach/detach, and failure paths. Run race tests for anything touching registries, streams, or buffers. Tests must not require Claude credentials — use interfaces and deterministic fakes. Integration tests (`*_integration_test.go`) boot a real in-process server.
- protobuf API stays backward compatible; breaking changes ship as a new version package.
- Commit messages: short imperative subjects (`Add agent lifecycle registry`), often with conventional prefixes (`feat(scope): ...`).

## Security Invariants

The workspace boundary and credential handling are security-sensitive. When touching path or process code:

- Resolve all relative paths to normalized paths inside the workspace root; reject `..`, absolute paths, and symlink escapes (see AGENTS.md).
- Uploads write to a same-directory temp file, verify size and hash, then atomically replace.
- Never log tokens, prompts, or uploaded file contents; persisted environment stores keys only, never values. Log the *existence* of credentials, never their values.
- Template/MCP/workflow definition files must be compiled at startup and must live outside the workspace; template-started argv is sanitized in `ProcessInfo` (template name + SHA-256 revision only).
- Command execution trust model is intentionally open (`StartProcess` runs arbitrary caller-specified executables, no allowlist) — a token equals remote code execution as the controller's user. Don't add the illusion of app-layer restriction; isolation is OS-level (containers/VMs), and process templates are the constrained path.
- See `docs/authorization-model-v1.md` for the gRPC vs MCP token asymmetry.
