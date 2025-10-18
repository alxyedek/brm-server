package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"

	"brm/utils"
)

// statusHandler shows runtime information
func (s *Server) statusHandler(w http.ResponseWriter, r *http.Request) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Fprintf(w, "S3 Server Status\n")
	fmt.Fprintf(w, "================\n")
	fmt.Fprintf(w, "Goroutines: %d\n", runtime.NumGoroutine())
	fmt.Fprintf(w, "OS Threads: %d\n", runtime.NumCPU())
	fmt.Fprintf(w, "Memory Allocated: %d KB\n", m.Alloc/1024)
	fmt.Fprintf(w, "Memory Total: %d KB\n", m.TotalAlloc/1024)
	fmt.Fprintf(w, "GC Cycles: %d\n", m.NumGC)
	fmt.Fprintf(w, "Goroutine Info: %+v\n", utils.GetGoroutineInfo())

	// Example: Access server state (logger, port, etc.)
	s.logger.Info("Status endpoint accessed", "goroutines", runtime.NumGoroutine(), "port", s.port)
}

// blockingHandler simulates blocking operations
func (s *Server) blockingHandler(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	operationType := r.URL.Query().Get("operation-type")
	minBlockPeriodMs := parseIntParam(r, "min-block-period-ms")
	maxBlockPeriodMs := parseIntParam(r, "max-block-period-ms")

	// Perform blocking operation
	s.blockingSimulator.PerformBlockingOperation(
		nullableString(operationType),
		minBlockPeriodMs,
		maxBlockPeriodMs,
	)

	// Build response
	response := utils.NewSimpleResponse("/rest/blocking")

	// Check response format from config
	if s.responseFormat == "json" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	} else {
		// Text format (minimal)
		fmt.Fprintf(w, "Blocking operation completed\n")
		fmt.Fprintf(w, "Path: %s\n", response.PathString)
		fmt.Fprintf(w, "Time: %s\n", response.TimeString)
	}

	s.logger.Info("Blocking endpoint accessed", "operationType", operationType, "responseFormat", s.responseFormat)
}

// parseIntParam parses an integer query parameter
func parseIntParam(r *http.Request, param string) *int {
	value := r.URL.Query().Get(param)
	if value == "" {
		return nil
	}
	if intVal, err := strconv.Atoi(value); err == nil {
		return &intVal
	}
	return nil
}

// nullableString returns a pointer to string if not empty, nil otherwise
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
