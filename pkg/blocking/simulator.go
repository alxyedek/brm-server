package blocking

import (
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
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
	testDataDir   string // Path to test data files
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

	s := &Simulator{
		cfg:           cfg,
		logger:        logger,
		operationType: operationType,
		minMs:         cfg.GetIntWithDefault("min-block-period-ms", 1000),
		maxMs:         cfg.GetIntWithDefault("max-block-period-ms", 5000),
		testDataDir:   "testdata/blocking",
	}

	// Ensure test data files exist
	s.ensureTestDataFiles()

	return s
}

// ensureTestDataFiles creates test data files if they don't exist
func (s *Simulator) ensureTestDataFiles() {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(s.testDataDir, 0755); err != nil {
		s.logger.Warn("Failed to create test data directory", "error", err)
		return
	}

	// Define test file sizes
	testFiles := map[string]int{
		"file_1kb.dat":   1 * 1024,         // 1KB
		"file_100kb.dat": 100 * 1024,       // 100KB
		"file_1mb.dat":   1 * 1024 * 1024,  // 1MB
		"file_10mb.dat":  10 * 1024 * 1024, // 10MB
	}

	for filename, size := range testFiles {
		filepath := filepath.Join(s.testDataDir, filename)

		// Check if file already exists
		if _, err := os.Stat(filepath); err == nil {
			continue // File exists, skip
		}

		// Create file with random data
		data := make([]byte, size)
		rand.Read(data)

		if err := os.WriteFile(filepath, data, 0644); err != nil {
			s.logger.Warn("Failed to create test data file", "file", filename, "error", err)
		} else {
			s.logger.Info("Created test data file", "file", filename, "size", size)
		}
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

// performFileIoBlocking executes file I/O operations with controlled timing
func (s *Simulator) performFileIoBlocking(durationMs int) {
	// Select appropriate test file based on duration
	// Rough estimate: 10ms-100ms -> 1KB, 100ms-500ms -> 100KB, 500ms-2000ms -> 1MB, 2000ms+ -> 10MB
	var testFile string
	var readIterations int

	if durationMs < 100 {
		testFile = filepath.Join(s.testDataDir, "file_1kb.dat")
		readIterations = durationMs / 2 // ~2ms per read (cached files are fast)
	} else if durationMs < 500 {
		testFile = filepath.Join(s.testDataDir, "file_100kb.dat")
		readIterations = durationMs / 5 // ~5ms per read
	} else if durationMs < 2000 {
		testFile = filepath.Join(s.testDataDir, "file_1mb.dat")
		readIterations = durationMs / 10 // ~10ms per read
	} else {
		testFile = filepath.Join(s.testDataDir, "file_10mb.dat")
		readIterations = durationMs / 20 // ~20ms per read
	}

	if readIterations < 1 {
		readIterations = 1
	}

	// Read the file multiple times to achieve desired duration
	totalBytesRead := 0
	startTime := time.Now()

	for i := 0; i < readIterations; i++ {
		bytesRead, err := s.readTestFile(testFile)
		if err != nil {
			s.logger.Warn("Failed to read test file, falling back to sleep", "file", testFile, "error", err)
			s.performSleepBlocking(durationMs)
			return
		}
		totalBytesRead += bytesRead

		// Add small delay between reads to simulate I/O processing time
		// This ensures we achieve the desired blocking duration even with cached files
		elapsed := time.Since(startTime)
		remainingTime := time.Duration(durationMs)*time.Millisecond - elapsed

		if remainingTime > 0 && i < readIterations-1 { // Don't delay after last iteration
			// Distribute remaining time across remaining iterations
			delayPerIteration := remainingTime / time.Duration(readIterations-i-1)
			if delayPerIteration > 0 {
				time.Sleep(delayPerIteration)
			}
		}
	}

	s.logger.Debug("File I/O blocking completed", "durationMs", durationMs, "iterations", readIterations, "totalBytesRead", totalBytesRead)
}

// readTestFile reads a test file completely with small buffer to force I/O syscalls
func (s *Simulator) readTestFile(filepath string) (int, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Use small buffer (4KB) to force multiple syscalls
	buffer := make([]byte, 4096)
	totalRead := 0

	for {
		n, err := file.Read(buffer)
		totalRead += n

		if err == io.EOF {
			break
		}
		if err != nil {
			return totalRead, err
		}
	}

	return totalRead, nil
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
