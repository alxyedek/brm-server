package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"brm/internal/server"
	"brm/pkg/config"
	"brm/utils"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger with configured log level
	logLevel := cfg.GetLogLevel(slog.LevelInfo)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))

	// Get server sub-configuration
	serverCfg := cfg.GetSubConfig("server")

	// Create server
	srv := server.New(serverCfg, cfg, logger)

	// Log runtime information
	logger.Info("Runtime information", "maxOSThreads", runtime.NumCPU())
	goroutineInfo := utils.GetGoroutineInfo()
	logger.Info("Goroutine info", "details", goroutineInfo)
	logger.Info("Available endpoints", "endpoints", []string{
		"GET /status - Server status",
		"GET /rest/blocking - Blocking operation simulator",
	})
	logger.Info("Press Ctrl+C to shutdown gracefully")

	// Start server in a goroutine
	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error("Server failed to start", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown error", "error", err)
	}

	logger.Info("Server shutdown complete")
}
