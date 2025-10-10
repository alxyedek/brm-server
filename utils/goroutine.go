package utils

import (
	"bytes"
	"runtime"
	"strconv"
	"syscall"
)

var (
	goroutinePrefix = []byte("goroutine ")
)

// GoroutineInfo contains identification information about the current goroutine and thread
type GoroutineInfo struct {
	GoroutineID  int64
	ThreadID     int64
	FunctionName string
}

// GetCurrentGoroutineID returns the current goroutine ID using a reliable method
func GetCurrentGoroutineID() int64 {
	buf := make([]byte, 32)
	n := runtime.Stack(buf, false)
	buf = buf[:n]
	// Format: "goroutine 123 [running]: ..."

	buf, ok := bytes.CutPrefix(buf, goroutinePrefix)
	if !ok {
		return 0
	}

	i := bytes.IndexByte(buf, ' ')
	if i < 0 {
		return 0
	}

	id, err := strconv.ParseInt(string(buf[:i]), 10, 64)
	if err != nil {
		return 0
	}

	return id
}

// GetCurrentThreadID returns the current OS thread ID
func GetCurrentThreadID() int64 {
	return int64(syscall.Gettid())
}

// GetCurrentFunctionName returns the name of the function that called this function
func GetCurrentFunctionName() string {
	pc := make([]uintptr, 1)
	runtime.Callers(2, pc) // Skip 2 frames: this function and the caller
	f := runtime.FuncForPC(pc[0])
	if f != nil {
		return f.Name()
	}
	return "unknown"
}

// GetCallerFunctionName returns the name of the function that called the caller of this function
// This skips the utility functions to show the actual caller
func GetCallerFunctionName() string {
	pc := make([]uintptr, 1)
	runtime.Callers(3, pc) // Skip 3 frames: this function, caller, and utility function
	f := runtime.FuncForPC(pc[0])
	if f != nil {
		return f.Name()
	}
	return "unknown"
}

// GetGoroutineInfo returns comprehensive information about the current goroutine
func GetGoroutineInfo() GoroutineInfo {
	return GoroutineInfo{
		GoroutineID:  GetCurrentGoroutineID(),
		ThreadID:     GetCurrentThreadID(),
		FunctionName: GetCallerFunctionName(),
	}
}
