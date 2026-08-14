GO ?= go
RACE_CC := $(shell command -v cc 2>/dev/null || command -v gcc 2>/dev/null || command -v clang 2>/dev/null || command -v clang-22 2>/dev/null)
TOOLS_DIR := $(CURDIR)/.tools/bin
BUF := $(TOOLS_DIR)/buf
PROTOC_GEN_GO := $(TOOLS_DIR)/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(TOOLS_DIR)/protoc-gen-go-grpc
GO_FILES := $(shell find . -type f -name '*.go' -not -path './.tools/*')

.PHONY: tools generate format lint test test-race build clean

tools: $(BUF) $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC)

$(TOOLS_DIR):
	mkdir -p $@

$(BUF): | $(TOOLS_DIR)
	GOBIN=$(TOOLS_DIR) $(GO) install github.com/bufbuild/buf/cmd/buf@v1.57.2

$(PROTOC_GEN_GO): | $(TOOLS_DIR)
	GOBIN=$(TOOLS_DIR) $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10

$(PROTOC_GEN_GO_GRPC): | $(TOOLS_DIR)
	GOBIN=$(TOOLS_DIR) $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

generate: tools
	PATH=$(TOOLS_DIR):$$PATH $(BUF) generate

format:
	gofmt -w $(GO_FILES)

lint: tools
	PATH=$(TOOLS_DIR):$$PATH $(BUF) lint
	$(GO) vet ./...

test:
	$(GO) test ./...

test-race:
	test -n "$(RACE_CC)"
	CGO_ENABLED=1 CC=$(RACE_CC) $(GO) test -race ./...

build:
	mkdir -p bin
	$(GO) build -o bin/remote-code-controller ./cmd/controller
	$(GO) build -o bin/remote-code ./cmd/remote-code

clean:
	$(GO) clean ./...
