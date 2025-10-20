# BRM Go Server Performance Testing

Performance testing infrastructure for the BRM Go server.

## Quick Start

```bash
# Fast verification test (completes in seconds)
./perf-test-client/run-performance-test.sh --env-file environments/fast-test.env

# Single-core goroutine efficiency test
./perf-test-client/run-performance-test.sh --env-file environments/single-core-perf-test.env

# Multi-core performance test
./perf-test-client/run-performance-test.sh --env-file environments/multi-core-perf-test.env
```

## Components

- **load-test.py** - HTTP load testing tool
- **run-test-client.sh** - Test client wrapper
- **run-performance-test.sh** - Full test orchestrator
- **monitor-performance.sh** - System performance monitoring (always enabled)

## Environment Variables

### Go Server Resources
- `CPU_CORES` - Number of CPU cores (default: 2)
- `GOMAXPROCS` - Go max processors (default: CPU_CORES)
- `GOMEMLIMIT` - Go memory limit (default: 256MiB)

### Test Configuration
- `OPERATION_TYPE` - SLEEP, FILE_IO, NETWORK_IO, MIXED
- `MIN_BLOCK_PERIOD_MS` - Min blocking duration
- `MAX_BLOCK_PERIOD_MS` - Max blocking duration
- `CONCURRENT_REQUESTS` - Concurrent request count
- `TOTAL_REQUESTS` - Total request count
- `TIMEOUT_SECONDS` - Request timeout

## Log Files

All logs are stored in `logs/` directory with timestamps:
- `server-TIMESTAMP.log` - Server output
- `pidstat-TIMESTAMP.log` - Performance metrics (CPU, memory, I/O)
- `monitor-TIMESTAMP.log` - Monitoring script output

### Performance Monitoring

The system automatically monitors server performance during tests using `pidstat`:
- **CPU Usage**: User and system CPU percentages
- **Memory**: Memory usage patterns
- **I/O**: Disk and network I/O statistics
- **Process Info**: PID, CPU affinity, command details

Monitoring duration is automatically calculated based on test parameters with a safety buffer.

## Manual Testing

You can also run individual components:

```bash
# Start server with resource limits (logs to terminal)
CPU_CORES=1 GOMAXPROCS=1 GOMEMLIMIT=128MiB ./build/start-limited.sh

# Start server with log redirection to file
LOG_FILE="logs/server-$(date +%Y%m%d-%H%M%S).log" CPU_CORES=1 GOMAXPROCS=1 GOMEMLIMIT=128MiB ./build/start-limited.sh &

# Run load test directly
python3 perf-test-client/load-test.py \
    --url "http://localhost:8080/rest/blocking?operation-type=SLEEP&min-block-period-ms=100&max-block-period-ms=200" \
    --concurrent 50 \
    --total 1000 \
    --timeout 30

# Stop server
./build/stop-limited.sh

# Manual performance monitoring
./perf-test-client/monitor-performance.sh -d 120 -o logs/manual-monitor.log
```

## Key Differences from Java Version

### Resource Limiting

**Java (JVM):**
- `-XX:ActiveProcessorCount` for CPU cores
- `-Xms/-Xmx` for heap memory
- `-XX:MaxMetaspaceSize` for metaspace
- Platform thread limits via Tomcat config

**Go:**
- `GOMAXPROCS` for CPU/goroutine scheduling
- `GOMEMLIMIT` for memory soft limit
- `taskset` for CPU affinity
- No explicit goroutine limits (scales automatically)

### Process Management

**Java:** Look for `java.*brm-apiserver.*jar` process
**Go:** Look for `brm-server` process

### Startup Time

**Java:** Longer startup, needs more warmup
**Go:** Fast startup, minimal warmup needed
