# Repository Guidelines

## Project Structure & Module Organization

The repository is in the design stage; `README.md` defines the intended architecture. Keep Go code aligned with this layout:

- `cmd/controller/`: remote controller executable.
- `cmd/remote-code/`: local CLI executable.
- `api/remote/code/v1/`: versioned protobuf definitions.
- `internal/`: private agent, file, process, authentication, event, and gRPC packages.
- `pkg/client/`: reusable public Go client.
- `configs/`: safe example configuration; never commit credentials.

Place unit tests beside packages as `*_test.go`. Put end-to-end fixtures under `testdata/` or top-level `tests/`.

## Build, Test, and Development Commands

No build scripts or `go.mod` exist yet. The initial implementation should establish these commands:

```bash
go build ./...          # compile all commands and packages
go test ./...           # run the complete test suite
go test -race ./...     # detect concurrency errors
go vet ./...            # run Go static checks
gofmt -w .              # format Go source before review
```

If protobuf tooling or a `Makefile` is added, pin versions and provide reproducible `generate`, `lint`, `test`, and `build` targets. Commit generated output with source definitions unless documented otherwise.

## Coding Style & Naming Conventions

Follow idiomatic Go and `gofmt`. Package names are short, lowercase nouns. Exported identifiers use `CamelCase`; locals use `camelCase`. Wrap errors with context (`fmt.Errorf("start agent %q: %w", id, err)`) and use typed gRPC status errors at transport boundaries. Keep platform-specific code in files such as `pty_linux.go`.

## Testing Guidelines

Use Go's `testing` package and table-driven tests. Name tests after behavior, for example `TestRegistry_StopExitedAgent`. Cover lifecycle transitions, cancellation, concurrent attach/detach, path traversal, symlink escape, limits, and process-group shutdown. Run race tests for registries, streams, or buffers. Tests must not require Claude credentials; use interfaces and deterministic fakes.

## Commit & Pull Request Guidelines

History currently contains only `Initial commit`, so no established convention exists. Use short, imperative subjects such as `Add agent lifecycle registry`; keep unrelated changes separate. Pull requests should explain behavior, design tradeoffs, security impact, and verification commands, and should link relevant issues. Include CLI output or protobuf compatibility notes when user-facing contracts change.

## Security & Agent Instructions

Treat the workspace boundary as security-sensitive: reject absolute paths, traversal, and symlink escapes. Never log tokens, prompts, or uploaded file contents. Preserve unrelated working-tree changes, and clearly distinguish planned interfaces from implemented functionality in documentation.
