package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/qq1426155093/remote-code/internal/auth"
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
	var config server.Config
	var tokenFile string
	var showVersion bool
	flag.StringVar(&config.Workspace, "workspace", "", "workspace directory (required)")
	flag.StringVar(&config.ListenAddress, "listen-addr", "127.0.0.1:9443", "gRPC listen address")
	flag.Int64Var(&config.MaxUploadBytes, "max-upload-bytes", 1<<30, "maximum bytes per uploaded file")
	flag.StringVar(&config.RuntimeDirectory, "runtime-dir", "/var/run/remote-code-controller", "persistent process runtime directory")
	flag.IntVar(&config.MaxProcesses, "max-processes", 16, "maximum concurrently active managed processes")
	flag.StringVar(&config.TLSCertificateFile, "tls-cert", "", "TLS certificate PEM file")
	flag.StringVar(&config.TLSKeyFile, "tls-key", "", "TLS private key PEM file")
	flag.StringVar(&tokenFile, "token-file", "", "optional bearer token file")
	flag.BoolVar(&config.AllowInsecureRemote, "allow-insecure-remote", false, "allow plaintext gRPC on a non-loopback listener")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Parse()
	if showVersion {
		fmt.Println(version.Version)
		return nil
	}
	if config.Workspace == "" {
		return errors.New("--workspace is required")
	}
	if tokenFile != "" {
		token, err := auth.ReadTokenFile(tokenFile)
		if err != nil {
			return err
		}
		config.Token = token
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
