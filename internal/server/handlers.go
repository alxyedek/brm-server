package server

import (
	"fmt"
	"net/http"
	"runtime"

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
