package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/qq1426155093/remote-code/internal/controllerlog"
	"github.com/qq1426155093/remote-code/internal/logging"
	"github.com/qq1426155093/remote-code/internal/server"
	"github.com/qq1426155093/remote-code/internal/version"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "controller error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	return runController(os.Args[1:], os.Stdout, os.Stderr)
}

func runController(args []string, stdout, stderr io.Writer) error {
	options, err := parseControllerOptions(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if options.showVersion {
		fmt.Fprintln(stdout, version.Version)
		return nil
	}
	config, err := options.validatedServerConfig()
	if err != nil {
		return err
	}
	var controllerLogger *controllerlog.Logger
	if !options.checkConfig {
		controllerLogger, err = controllerlog.Open(config.RuntimeDirectory, config.ControllerLogs, stderr)
		if err != nil {
			controllerLogger = controllerlog.NewFallback(stderr)
			fmt.Fprintf(stderr, "controller runtime log unavailable: %v\n", err)
		}
	}
	prepared, err := server.Prepare(config)
	if err != nil {
		if controllerLogger != nil {
			controllerLogger.Emit(logging.Event{Level: logging.LevelError, Component: "controller", Name: "prepare_failed", Message: "controller preparation failed", Fields: map[string]string{
				"error_kind": fmt.Sprintf("%T", err),
			}})
			_ = controllerLogger.Close()
		}
		return err
	}
	if options.checkConfig {
		fmt.Fprintln(stdout, "configuration OK")
		return nil
	}

	if controllerLogger == nil {
		controllerLogger = controllerlog.NewFallback(stderr)
	}
	controller, err := server.NewPreparedWithLogger(prepared, controllerLogger)
	if err != nil {
		return err
	}
	controller.Emit(logging.Event{Level: logging.LevelInfo, Component: "controller", Name: "started", Message: "controller is listening", Fields: map[string]string{
		"version": version.Version, "address": controller.Address(),
	}})
	if address := controller.MCPAddress(); address != "" {
		// Report which credential the endpoint authenticates with, never its
		// value, so an operator can confirm the deployed topology from the log.
		credential := "shared_with_grpc"
		if options.mcpTokenFile != "" {
			credential = "separate"
		}
		controller.Emit(logging.Event{Level: logging.LevelInfo, Component: "mcp", Name: "listening", Message: "MCP Streamable HTTP is listening", Fields: map[string]string{
			"address": address, "path": prepared.Config.MCP.EndpointPath, "credential": credential,
		}})
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- controller.Serve()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	select {
	case received := <-signals:
		controller.Emit(logging.Event{Level: logging.LevelInfo, Component: "controller", Name: "signal_received", Message: "shutdown signal received", Fields: map[string]string{
			"signal": received.String(),
		}})
	case err := <-serveErrors:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			controller.Emit(logging.Event{Level: logging.LevelError, Component: "controller", Name: "serve_failed", Message: "controller serve loop failed", Fields: map[string]string{
				"error_kind": fmt.Sprintf("%T", err),
			}})
		}
		// Serve can return because a listener failed or because an embedding
		// caller stopped the gRPC server. In either case own the full cleanup
		// path here; otherwise the runtime-log lock and MCP listener can leak
		// when no signal branch performs the shutdown.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		shutdownErr := controller.Shutdown(ctx)
		cancel()
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			if shutdownErr != nil && !errors.Is(shutdownErr, context.DeadlineExceeded) {
				return fmt.Errorf("serve controller: %w (shutdown: %v)", err, shutdownErr)
			}
			return fmt.Errorf("serve controller: %w", err)
		}
		if shutdownErr != nil && !errors.Is(shutdownErr, context.DeadlineExceeded) {
			return fmt.Errorf("shutdown controller: %w", shutdownErr)
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := controller.Shutdown(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("shutdown controller: %w", err)
	}
	return nil
}
