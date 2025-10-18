package utils

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"brm/pkg/models"
)

var (
	hostname     string
	hostnameOnce sync.Once
)

// GetHostname returns the cached hostname
func GetHostname() string {
	hostnameOnce.Do(func() {
		hostname = findHostname()
	})
	return hostname
}

// findHostname attempts to get the hostname
func findHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}

	// Try to get FQDN
	addrs, err := net.LookupHost(hostname)
	if err == nil && len(addrs) > 0 {
		return fmt.Sprintf("%s/%s", hostname, addrs[0])
	}

	return hostname
}

// GetCurrentTimeMillis returns current time as milliseconds string
func GetCurrentTimeMillis() string {
	return fmt.Sprintf("%d", time.Now().UnixMilli())
}

// NewRandomInt returns a random integer
func NewRandomInt() int {
	return rand.Int()
}

// NewSimpleResponse creates a complete SimpleResponse
func NewSimpleResponse(path string) *models.SimpleResponse {
	goroutineInfo := GetGoroutineInfo()
	return &models.SimpleResponse{
		HostString:    GetHostname(),
		PathString:    path,
		TimeString:    GetCurrentTimeMillis(),
		RandomInteger: NewRandomInt(),
		GoroutineInfo: fmt.Sprintf("G%d:%s", goroutineInfo.GoroutineID, goroutineInfo.FunctionName),
	}
}
