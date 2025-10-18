#!/bin/bash
# BRM Go Server Performance Test Orchestrator
# Manages server lifecycle and delegates testing to run-test-client.sh

set -e

# Go server resource limits (can be overridden via environment variables)
CPU_CORES=${CPU_CORES:-2}
GOMAXPROCS=${GOMAXPROCS:-$CPU_CORES}
GOMEMLIMIT=${GOMEMLIMIT:-256MiB}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "BRM Go Server Performance Test Orchestrator"
    echo "Manages server lifecycle and delegates testing to run-test-client.sh"
    echo ""
    echo "Options:"
    echo "  -h, --help                   Show this help message"
    echo "  -e, --env-file FILE          Load environment from file (default: .env)"
    echo ""
    echo "Environment variables (can be set via shell or .env file):"
    echo "  CPU_CORES                    Number of CPU cores for server (default: 2)"
    echo "  GOMAXPROCS                   Go max processors (default: CPU_CORES)"
    echo "  GOMEMLIMIT                   Go memory limit (default: 256MiB)"
    echo ""
    echo "  # Test client variables (passed through to run-test-client.sh):"
    echo "  OPERATION_TYPE               Operation type: SLEEP, FILE_IO, NETWORK_IO, MIXED (default: MIXED)"
    echo "  MIN_BLOCK_PERIOD_MS          Minimum block period in milliseconds (default: 500)"
    echo "  MAX_BLOCK_PERIOD_MS          Maximum block period in milliseconds (default: 2000)"
    echo "  CONCURRENT_REQUESTS          Number of concurrent requests (default: 50)"
    echo "  TOTAL_REQUESTS               Total number of requests (default: 1000)"
    echo "  TIMEOUT_SECONDS              Request timeout in seconds (default: 30)"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Use defaults or .env file"
    echo "  $0 --env-file environments/fast-test.env"
    echo "  CPU_CORES=1 GOMEMLIMIT=128MiB $0"
}

# Function to cleanup processes
cleanup() {
    echo ""
    echo "🧹 Cleaning up..."
    
    # Stop monitoring script if still running
    local monitor_pid=$(pgrep -f "monitor-performance.sh" | head -1)
    if [ -n "$monitor_pid" ]; then
        echo "Stopping performance monitoring (PID: $monitor_pid)..."
        kill "$monitor_pid" 2>/dev/null || true
        sleep 1
        # Force kill if still running
        kill -9 "$monitor_pid" 2>/dev/null || true
    fi
    
    # Stop pidstat processes if still running
    local pidstat_pids=$(pgrep -f "pidstat.*brm-server" || true)
    if [ -n "$pidstat_pids" ]; then
        echo "Stopping pidstat processes..."
        echo "$pidstat_pids" | xargs kill 2>/dev/null || true
        sleep 1
        echo "$pidstat_pids" | xargs kill -9 2>/dev/null || true
    fi
    
    # Stop the server if we started it
    if [ "$SERVER_STARTED_BY_SCRIPT" = "true" ]; then
        echo "Stopping BRM Go server..."
        ./build/stop-limited.sh
    fi
    
    echo "Cleanup completed."
}

# Set up signal handlers for cleanup
trap cleanup EXIT INT TERM

# Default .env file location
ENV_FILE=".env"

# Parse minimal command line arguments (only --help and --env-file)
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -e|--env-file)
            ENV_FILE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Load .env file if it exists
if [ -f "$ENV_FILE" ]; then
    echo "Loading environment from: $ENV_FILE"
    # Export variables from .env file (ignoring comments and empty lines)
    export $(grep -v '^#' "$ENV_FILE" | grep -v '^[[:space:]]*$' | xargs)
fi

# Create logs directory if it doesn't exist
mkdir -p logs

# Generate timestamp for this test run
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

echo "🚀 Running BRM Go Performance Test Orchestrator"
echo "=============================================="
echo "Server Resources: ${CPU_CORES} cores, GOMAXPROCS=${GOMAXPROCS}, GOMEMLIMIT=${GOMEMLIMIT}"
echo "Timestamp: $TIMESTAMP"
echo ""

# Check if server is running, start if not
SERVER_STARTED_BY_SCRIPT="false"
if ! pgrep -f "brm-server" > /dev/null; then
    echo "📋 Starting BRM Go server..."
    CPU_CORES="$CPU_CORES" GOMAXPROCS="$GOMAXPROCS" GOMEMLIMIT="$GOMEMLIMIT" \
        ./build/start-limited.sh > "logs/server-$TIMESTAMP.log" 2>&1 &
    SERVER_PID=$!
    SERVER_STARTED_BY_SCRIPT="true"
    echo "Server started with PID: $SERVER_PID"
    
    # Wait for server to start
    echo "Waiting for server to be ready..."
    for i in {1..30}; do
        if curl -s http://localhost:8080/status > /dev/null 2>&1; then
            echo "✅ Server is ready!"
            break
        fi
        if [ $i -eq 30 ]; then
            echo "❌ Server failed to start within 30 seconds"
            exit 1
        fi
        sleep 1
    done
else
    echo "✅ Server is already running"
fi

# Start performance monitoring in background
echo "📊 Starting performance monitoring..."
# Calculate monitoring duration based on test parameters (add buffer for warmup and completion)
MONITOR_DURATION=$((TOTAL_REQUESTS / CONCURRENT_REQUESTS + 30))  # Estimate + 30s buffer
if [ "$MONITOR_DURATION" -lt 60 ]; then
    MONITOR_DURATION=60  # Minimum 1 minute
fi

./perf-test-client/monitor-performance.sh -d "$MONITOR_DURATION" -o "logs/pidstat-$TIMESTAMP.log" > "logs/monitor-$TIMESTAMP.log" 2>&1 &
MONITOR_PID=$!

# Run the test client
echo "🔥 Running test client..."
BASE_URL="http://localhost:8080" TIMESTAMP="$TIMESTAMP" ./perf-test-client/run-test-client.sh --env-file "$ENV_FILE"

TEST_CLIENT_EXIT_CODE=$?

echo ""
echo "⏳ Stopping monitoring..."
# Stop monitoring if still running
if [ -n "$MONITOR_PID" ] && kill -0 "$MONITOR_PID" 2>/dev/null; then
    kill "$MONITOR_PID" 2>/dev/null || true
    sleep 1
    kill -9 "$MONITOR_PID" 2>/dev/null || true
fi

# Stop any remaining pidstat processes
pkill -f "pidstat.*brm-server" 2>/dev/null || true

echo ""
if [ $TEST_CLIENT_EXIT_CODE -eq 0 ]; then
    echo "✅ Performance test completed successfully!"
else
    echo "❌ Performance test completed with errors (exit code: $TEST_CLIENT_EXIT_CODE)"
fi

echo ""
echo "📁 Log files:"
if [ "$SERVER_STARTED_BY_SCRIPT" = "true" ]; then
    echo "  Server:      logs/server-$TIMESTAMP.log"
fi
echo "  Performance: logs/pidstat-$TIMESTAMP.log"
echo "  Monitor:     logs/monitor-$TIMESTAMP.log"

exit $TEST_CLIENT_EXIT_CODE
