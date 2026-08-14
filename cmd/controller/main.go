package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/qq1426155093/remote-code/internal/server"
	"github.com/qq1426155093/remote-code/internal/version"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		log.Printf("controller error: %v", err)
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
	if options.checkConfig {
		fmt.Fprintln(stdout, "configuration OK")
		return nil
	}

	controller, err := server.New(config)
	if err != nil {
		return err
	}
	log.Printf("remote-code-controller v%s listening on %s", version.Version, controller.Address())
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- controller.Serve()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	select {
	case received := <-signals:
		log.Printf("received %s, shutting down", received)
	case err := <-serveErrors:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("serve gRPC: %w", err)
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
