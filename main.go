package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"brm-server/utils"
)

// Status handler to show runtime information
func statusHandler(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Fprintf(w, "S3 Server Status\n")
	fmt.Fprintf(w, "================\n")
	fmt.Fprintf(w, "Goroutines: %d\n", runtime.NumGoroutine())
	fmt.Fprintf(w, "OS Threads: %d\n", runtime.NumCPU())
	fmt.Fprintf(w, "Memory Allocated: %d KB\n", m.Alloc/1024)
	fmt.Fprintf(w, "Memory Total: %d KB\n", m.TotalAlloc/1024)
	fmt.Fprintf(w, "GC Cycles: %d\n", m.NumGC)
	utils.PrintGoroutineInfo(w)
	// fmt.Fprintf(w, "Buffer Size: %d KB\n", InternalChunkSize/1024)
}

func main() {

	// Set up HTTP server with timeouts
	server := &http.Server{
		Addr:         ":8080",
		Handler:      nil,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Set up routes
	http.HandleFunc("/status", statusHandler)

	fmt.Printf("Starting BRM server on :8080\n")
	// fmt.Printf("Internal buffer size: %s\n", strconv.FormatInt(InternalChunkSize, 10))
	fmt.Printf("Max OS threads: %d\n", runtime.NumCPU())
	fmt.Printf("Available endpoints:\n")
	fmt.Printf("  GET /status - Server status\n")
	fmt.Printf("Press Ctrl+C to shutdown gracefully\n")
	utils.PrintGoroutineInfo(os.Stdout)

	// Start server in a goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Server failed to start: %v\n", err)
			os.Exit(1) // Exit the application when server fails to start
		}
	}()

	// Wait for interrupt signal (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("\nShutting down server gracefully...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown server (this waits for active connections to finish)
	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("Server shutdown error: %v\n", err)
	}

	fmt.Println("Server shutdown complete")
}
