package blocking

import (
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"brm/pkg/config"
)

// OperationType represents the type of blocking operation
type OperationType string

const (
	Sleep     OperationType = "SLEEP"
	FileIO    OperationType = "FILE_IO"
	NetworkIO OperationType = "NETWORK_IO"
	Mixed     OperationType = "MIXED"
)

// Simulator handles blocking operations
type Simulator struct {
	cfg           *config.Config
	logger        *slog.Logger
	operationType OperationType
	minMs         int
	maxMs         int
}

// New creates a new blocking simulator
func New(cfg *config.Config, logger *slog.Logger) *Simulator {
	operationTypeStr := cfg.GetStringWithDefault("operation-type", "SLEEP")
	operationType := OperationType(strings.ToUpper(operationTypeStr))

	// Validate operation type
	switch operationType {
	case Sleep, FileIO, NetworkIO, Mixed:
		// Valid
	default:
		logger.Warn("Invalid operation type, defaulting to SLEEP", "type", operationTypeStr)
		operationType = Sleep
	}

	return &Simulator{
		cfg:           cfg,
		logger:        logger,
		operationType: operationType,
		minMs:         cfg.GetIntWithDefault("min-block-period-ms", 1000),
		maxMs:         cfg.GetIntWithDefault("max-block-period-ms", 5000),
	}
}

// PerformBlockingOperation executes a blocking operation with optional parameter overrides
func (s *Simulator) PerformBlockingOperation(operationType *string, minMs *int, maxMs *int) {
	// Determine operation type
	resolvedOperationType := s.determineOperationType(operationType)

	// Determine duration parameters
	resolvedMinMs := s.minMs
	resolvedMaxMs := s.maxMs
	if minMs != nil {
		resolvedMinMs = *minMs
	}
	if maxMs != nil {
		resolvedMaxMs = *maxMs
	}

	// Generate random duration
	durationMs := s.generateRandomDuration(resolvedMinMs, resolvedMaxMs)

	s.logger.Info("Performing blocking operation",
		"type", resolvedOperationType,
		"durationMs", durationMs,
		"minMs", resolvedMinMs,
		"maxMs", resolvedMaxMs)

	// Execute the blocking operation
	switch resolvedOperationType {
	case Sleep:
		s.performSleepBlocking(durationMs)
	case FileIO:
		s.performFileIoBlocking(durationMs)
	case NetworkIO:
		s.performNetworkIoBlocking(durationMs)
	case Mixed:
		s.performMixedBlocking(durationMs, resolvedMinMs, resolvedMaxMs)
	default:
		s.logger.Warn("Unknown operation type, defaulting to sleep", "type", resolvedOperationType)
		s.performSleepBlocking(durationMs)
	}
}

// determineOperationType resolves the operation type from parameter or config
func (s *Simulator) determineOperationType(operationType *string) OperationType {
	if operationType != nil && *operationType != "" {
		opType := OperationType(strings.ToUpper(*operationType))
		switch opType {
		case Sleep, FileIO, NetworkIO, Mixed:
			return opType
		default:
			s.logger.Warn("Invalid operation type parameter, using config default", "type", *operationType)
		}
	}
	return s.operationType
}

// generateRandomDuration creates a random duration within the specified range
func (s *Simulator) generateRandomDuration(minMs, maxMs int) int {
	if minMs >= maxMs {
		return minMs
	}
	return rand.Intn(maxMs-minMs+1) + minMs
}

// performSleepBlocking executes a sleep operation
func (s *Simulator) performSleepBlocking(durationMs int) {
	time.Sleep(time.Duration(durationMs) * time.Millisecond)
}

// performFileIoBlocking executes file I/O operations with sleeps
func (s *Simulator) performFileIoBlocking(durationMs int) {
	tempFile, err := os.CreateTemp("", "brm_blocking_test_*.tmp")
	if err != nil {
		s.logger.Warn("Failed to create temp file", "error", err)
		// Fallback to sleep
		s.performSleepBlocking(durationMs)
		return
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	// Write some data to simulate I/O
	data := make([]byte, 1024)
	rand.Read(data)

	if _, err := tempFile.Write(data); err != nil {
		s.logger.Warn("Failed to write to temp file", "error", err)
		s.performSleepBlocking(durationMs)
		return
	}
	tempFile.Sync()

	// Sleep for a portion of the duration
	time.Sleep(time.Duration(durationMs/2) * time.Millisecond)

	// Read the data back
	tempFile.Seek(0, 0)
	buffer := make([]byte, 1024)
	for {
		n, err := tempFile.Read(buffer)
		if n == 0 || err == io.EOF {
			break
		}
		if err != nil {
			s.logger.Warn("Failed to read from temp file", "error", err)
			break
		}
		// Simulate processing
		time.Sleep(10 * time.Millisecond)
	}

	// Sleep for remaining duration
	time.Sleep(time.Duration(durationMs/2) * time.Millisecond)
}

// performNetworkIoBlocking executes network I/O operations with timeout
func (s *Simulator) performNetworkIoBlocking(durationMs int) {
	// Create a realistic network I/O blocking scenario
	// Use a combination of connection attempts and actual network operations

	// Try multiple approaches to create realistic network blocking

	// 1. Try to connect to a non-routable IP (will timeout)
	conn1, err1 := net.DialTimeout("tcp", "192.0.2.1:80", time.Duration(durationMs/3)*time.Millisecond)
	if err1 != nil {
		s.logger.Debug("Network I/O attempt 1 completed", "error", err1, "durationMs", durationMs/3)
	} else {
		conn1.Close()
	}

	// 2. Try to connect to another non-routable IP
	conn2, err2 := net.DialTimeout("tcp", "198.51.100.1:80", time.Duration(durationMs/3)*time.Millisecond)
	if err2 != nil {
		s.logger.Debug("Network I/O attempt 2 completed", "error", err2, "durationMs", durationMs/3)
	} else {
		conn2.Close()
	}

	// 3. Try UDP to a non-routable address (will timeout)
	conn3, err3 := net.DialTimeout("udp", "203.0.113.1:53", time.Duration(durationMs/3)*time.Millisecond)
	if err3 != nil {
		s.logger.Debug("Network I/O attempt 3 completed", "error", err3, "durationMs", durationMs/3)
	} else {
		conn3.Close()
	}

	s.logger.Debug("Network I/O blocking completed", "totalDurationMs", durationMs)
}

// performMixedBlocking randomly selects one of the three operation types
func (s *Simulator) performMixedBlocking(durationMs int, minMs int, maxMs int) {
	types := []OperationType{Sleep, FileIO, NetworkIO}
	selectedType := types[rand.Intn(len(types))]

	s.logger.Info("Performing mixed blocking operation", "selectedType", selectedType)

	switch selectedType {
	case Sleep:
		s.performSleepBlocking(durationMs)
	case FileIO:
		s.performFileIoBlocking(durationMs)
	case NetworkIO:
		s.performNetworkIoBlocking(durationMs)
	default:
		s.logger.Warn("Unknown type in mixed mode, defaulting to sleep")
		s.performSleepBlocking(durationMs)
	}
}
