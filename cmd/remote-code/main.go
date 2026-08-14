package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chzyer/readline"
	"github.com/qq1426155093/remote-code/internal/auth"
	"github.com/qq1426155093/remote-code/internal/cli"
	"github.com/qq1426155093/remote-code/internal/version"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
)

func main() {
	log.SetFlags(0)
	if err := run(); err != nil {
		log.Printf("remote-code: %v", err)
		os.Exit(1)
	}
}

func run() error {
	var config remoteclient.Config
	var tokenFile string
	var commandTimeout time.Duration
	var catMaxBytes int64
	var showVersion bool
	flag.StringVar(&config.Address, "controller-addr", "127.0.0.1:9443", "controller host:port")
	flag.StringVar(&config.TLSCAFile, "tls-ca", "", "controller CA or certificate PEM file")
	flag.StringVar(&config.TLSServerName, "tls-server-name", "", "TLS server name override")
	flag.StringVar(&tokenFile, "token-file", "", "optional bearer token file")
	flag.DurationVar(&commandTimeout, "timeout", 30*time.Second, "timeout for each RPC; 0 disables timeouts")
	flag.Int64Var(&catMaxBytes, "cat-max-bytes", 1<<20, "maximum bytes printed by cat")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Parse()
	if showVersion {
		fmt.Println(version.Version)
		return nil
	}
	if catMaxBytes <= 0 {
		return fmt.Errorf("--cat-max-bytes must be positive")
	}
	if tokenFile != "" {
		token, err := auth.ReadTokenFile(tokenFile)
		if err != nil {
			return err
		}
		config.Token = token
	}
	connectTimeout := commandTimeout
	if connectTimeout <= 0 {
		connectTimeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	client, err := remoteclient.New(ctx, config)
	cancel()
	if err != nil {
		return err
	}
	defer client.Close()

	line, err := readline.NewEx(&readline.Config{
		Prompt:          "remote-code:/> ",
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
		HistoryLimit:    500,
	})
	if err != nil {
		return fmt.Errorf("initialize terminal: %w", err)
	}
	defer line.Close()
	repl := cli.New(client, line, cli.Config{
		Timeout: commandTimeout, CatMaxBytes: catMaxBytes, Stdout: os.Stdout, Stderr: os.Stderr,
	})
	return repl.Run()
}
