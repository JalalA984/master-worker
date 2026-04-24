// main.go is the single binary entry point for both master and worker roles.
//
// Usage:
//   go run main.go master    # Start as master (gRPC :50051 + HTTP :9092)
//   go run main.go worker    # Start as worker (connects to master)
//
// Configuration comes from environment variables (12-Factor App pattern).
// See internal/config/config.go for all configurable values.
package main

import (
	"embed"
	"fmt"
	"os"

	"github.com/jalala984/master-worker/internal/config"
	"github.com/jalala984/master-worker/internal/logging"
	"github.com/jalala984/master-worker/internal/master"
	"github.com/jalala984/master-worker/internal/worker"
)

// Embed the web/ directory for the dashboard.
// embed.FS requires the directive to be in the same directory as the files.
//
//go:embed web/*
var webContent embed.FS

// Embed the docs/ directory for serving documentation from the dashboard.
//
//go:embed docs/*
var docsContent embed.FS

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: main [master|worker]")
		os.Exit(1)
	}

	cfg := config.Load()
	cfg.Role = os.Args[1]

	logger := logging.NewLogger(cfg.Role, cfg.LogLevel, cfg.LogFormat)

	switch cfg.Role {
	case "master":
		logger.Info("starting master node")
		// Pass embedded content to the master package.
		master.WebContent = webContent
		master.DocsContent = docsContent
		m := master.NewMaster(logger, cfg)
		if err := m.Start(); err != nil {
			logger.Error("master crashed", "error", err)
			os.Exit(1)
		}

	case "worker":
		logger.Info("starting worker node")
		w, err := worker.NewWorker(cfg.MasterAddr, logger, cfg)
		if err != nil {
			logger.Error("worker failed to connect", "error", err)
			os.Exit(1)
		}
		defer w.Close()

		if err := w.Start(); err != nil {
			logger.Error("worker failed", "error", err)
			os.Exit(1)
		}

	default:
		fmt.Printf("Unknown role: %s. Use 'master' or 'worker'.\n", cfg.Role)
		os.Exit(1)
	}
}
