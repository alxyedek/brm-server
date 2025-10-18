#!/bin/bash
# Performance Monitoring Script for BRM Go Server
# This script uses pidstat to monitor the Go process running the BRM server

set -e

# Default values
INTERVAL=${INTERVAL:-1}
DURATION=${DURATION:-60}
OUTPUT_FILE=${OUTPUT_FILE:-"logs/pidstat-$(date +%Y%m%d-%H%M%S).log"}

# Function to find the BRM Go server process
find_server_pid() {
    # Look for the specific Go process running brm-server
    local pids=$(pgrep -f "brm-server" | grep -v grep)
    
    if [ -z "$pids" ]; then
        echo "Error: No BRM Go server process found. Make sure the server is running." >&2
        echo "Start the server with: ./build/start-limited.sh > logs/server.log 2>&1 &" >&2
        exit 1
    fi
    
    # If multiple PIDs found, use the first one
    local pid=$(echo "$pids" | head -1)
    echo "Found BRM Go server process: PID $pid" >&2
    echo "$pid"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Monitor BRM Go server performance using pidstat"
    echo ""
    echo "Options:"
    echo "  -i, --interval SECONDS    Sampling interval in seconds (default: 1)"
    echo "  -d, --duration SECONDS    Total monitoring duration in seconds (default: 60)"
    echo "  -o, --output FILE         Output file (default: logs/pidstat-TIMESTAMP.log)"
    echo "  -h, --help               Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  INTERVAL                  Sampling interval in seconds"
    echo "  DURATION                  Total monitoring duration in seconds"
    echo "  OUTPUT_FILE               Output file path"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Monitor for 60 seconds with 1s interval"
    echo "  $0 -i 2 -d 120                       # Monitor for 120 seconds with 2s interval"
    echo "  $0 -o logs/my-test.log               # Custom output file"
    echo "  INTERVAL=5 DURATION=300 $0           # Using environment variables"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--interval)
            INTERVAL="$2"
            shift 2
            ;;
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate arguments
if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]] || [ "$INTERVAL" -lt 1 ]; then
    echo "Error: Interval must be a positive integer"
    exit 1
fi

if ! [[ "$DURATION" =~ ^[0-9]+$ ]] || [ "$DURATION" -lt 1 ]; then
    echo "Error: Duration must be a positive integer"
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p "$(dirname "$OUTPUT_FILE")"

# Find the server process
SERVER_PID=$(find_server_pid)

echo "Starting performance monitoring..."
echo "Server PID: $SERVER_PID"
echo "Interval: ${INTERVAL}s"
echo "Duration: ${DURATION}s"
echo "Output: $OUTPUT_FILE"
echo ""

# Calculate number of samples
SAMPLES=$((DURATION / INTERVAL))

# Start pidstat monitoring
echo "Collecting $SAMPLES samples..."
pidstat -p "$SERVER_PID" $INTERVAL $SAMPLES > "$OUTPUT_FILE" 2>&1

echo "Performance monitoring completed!"
echo "Results saved to: $OUTPUT_FILE"
echo ""
echo "To view the results:"
echo "  cat $OUTPUT_FILE"
echo "  tail -f $OUTPUT_FILE  # For real-time viewing during monitoring"
