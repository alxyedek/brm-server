#!/bin/bash
# Resource-Limited Go Server Startup Script
# This script runs the BRM Go server with constrained resources to test goroutine efficiency

set -e

# Default resource limits (can be overridden via environment variables)
CPU_CORES=${CPU_CORES:-2}
GOMAXPROCS=${GOMAXPROCS:-$CPU_CORES}
GOMEMLIMIT=${GOMEMLIMIT:-256MiB}
GODEBUG=${GODEBUG:-""}

echo "=== BRM Go Server - Resource Limited Mode ==="
echo "CPU Cores: $CPU_CORES"
echo "GOMAXPROCS: $GOMAXPROCS"
echo "GOMEMLIMIT: $GOMEMLIMIT"
echo "=============================================="

# Build if binary doesn't exist
if [ ! -f "target/brm-server" ]; then
    echo "Building application..."
    go build -o target/brm-server
fi

# Set Go runtime environment
export GOMAXPROCS="$GOMAXPROCS"
export GOMEMLIMIT="$GOMEMLIMIT"
if [ -n "$GODEBUG" ]; then
    export GODEBUG="$GODEBUG"
fi

# Determine CPU affinity mask for taskset
if [ "$CPU_CORES" -eq 1 ]; then
    CPU_MASK="0"
elif [ "$CPU_CORES" -eq 2 ]; then
    CPU_MASK="0,1"
elif [ "$CPU_CORES" -eq 4 ]; then
    CPU_MASK="0,1,2,3"
else
    # For other values, use first N cores
    CPU_MASK="0"
    for ((i=1; i<CPU_CORES; i++)); do
        CPU_MASK="$CPU_MASK,$i"
    done
fi

# Check if LOG_FILE environment variable is set for log redirection
if [ -n "$LOG_FILE" ]; then
    echo "Logging to: $LOG_FILE"
    # Create logs directory if it doesn't exist
    mkdir -p "$(dirname "$LOG_FILE")"
fi

# Check if taskset is available
if command -v taskset >/dev/null 2>&1; then
    echo "Using taskset to limit CPU cores to: $CPU_MASK"
    if [ -n "$LOG_FILE" ]; then
        echo "Server logs will be written to: $LOG_FILE"
        echo "Press Ctrl+C to stop the server"
        echo ""
        taskset -c "$CPU_MASK" ./target/brm-server > "$LOG_FILE" 2>&1
    else
        echo "Press Ctrl+C to stop the server"
        echo ""
        taskset -c "$CPU_MASK" ./target/brm-server
    fi
else
    echo "Warning: taskset not available, using GOMAXPROCS only"
    if [ -n "$LOG_FILE" ]; then
        echo "Server logs will be written to: $LOG_FILE"
        echo "Press Ctrl+C to stop the server"
        echo ""
        ./target/brm-server > "$LOG_FILE" 2>&1
    else
        echo "Press Ctrl+C to stop the server"
        echo ""
        ./target/brm-server
    fi
fi
