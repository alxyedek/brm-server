#!/bin/bash

# Generic Performance Test Client
# Tests any server endpoint with configurable parameters

set -e

# Default values
BASE_URL=${BASE_URL:-"http://localhost:8080"}
OPERATION_TYPE=${OPERATION_TYPE:-"MIXED"}
MIN_BLOCK_PERIOD_MS=${MIN_BLOCK_PERIOD_MS:-"500"}
MAX_BLOCK_PERIOD_MS=${MAX_BLOCK_PERIOD_MS:-"2000"}

# Load test parameters
CONCURRENT_REQUESTS=${CONCURRENT_REQUESTS:-50}
TOTAL_REQUESTS=${TOTAL_REQUESTS:-1000}
TIMEOUT_SECONDS=${TIMEOUT_SECONDS:-30}
TIMESTAMP=${TIMESTAMP:-$(date +%Y%m%d-%H%M%S)}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Generic performance test client for any server endpoint"
    echo ""
    echo "Options:"
    echo "  -h, --help                   Show this help message"
    echo "  -e, --env-file FILE          Load environment from file (default: .env)"
    echo ""
    echo "Environment variables (can be set via shell or .env file):"
    echo "  BASE_URL                     Server base URL (default: http://localhost:8080)"
    echo "  OPERATION_TYPE               Operation type: SLEEP, FILE_IO, NETWORK_IO, MIXED (default: MIXED)"
    echo "  MIN_BLOCK_PERIOD_MS          Minimum block period in milliseconds (default: 500)"
    echo "  MAX_BLOCK_PERIOD_MS          Maximum block period in milliseconds (default: 2000)"
    echo "  CONCURRENT_REQUESTS          Number of concurrent requests (default: 50)"
    echo "  TOTAL_REQUESTS               Total number of requests (default: 1000)"
    echo "  TIMEOUT_SECONDS              Request timeout in seconds (default: 30)"
    echo "  TIMESTAMP                    Timestamp for log files (default: generated)"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Test localhost:8080 with defaults"
    echo "  BASE_URL=http://remote:8080 $0        # Test remote server"
    echo "  $0 --env-file environments/fast-test.env"
    echo "  OPERATION_TYPE=SLEEP CONCURRENT_REQUESTS=200 $0"
}

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

# Validate arguments
if ! [[ "$MIN_BLOCK_PERIOD_MS" =~ ^[0-9]+$ ]] || [ "$MIN_BLOCK_PERIOD_MS" -lt 0 ]; then
    echo "Error: MIN_BLOCK_PERIOD_MS must be a non-negative integer"
    exit 1
fi

if ! [[ "$MAX_BLOCK_PERIOD_MS" =~ ^[0-9]+$ ]] || [ "$MAX_BLOCK_PERIOD_MS" -lt 0 ]; then
    echo "Error: MAX_BLOCK_PERIOD_MS must be a non-negative integer"
    exit 1
fi

if [ "$MIN_BLOCK_PERIOD_MS" -gt "$MAX_BLOCK_PERIOD_MS" ]; then
    echo "Error: MIN_BLOCK_PERIOD_MS cannot be greater than MAX_BLOCK_PERIOD_MS"
    exit 1
fi

# Validate load test parameters
if ! [[ "$CONCURRENT_REQUESTS" =~ ^[0-9]+$ ]] || [ "$CONCURRENT_REQUESTS" -lt 1 ]; then
    echo "Error: CONCURRENT_REQUESTS must be a positive integer"
    exit 1
fi

if ! [[ "$TOTAL_REQUESTS" =~ ^[0-9]+$ ]] || [ "$TOTAL_REQUESTS" -lt 1 ]; then
    echo "Error: TOTAL_REQUESTS must be a positive integer"
    exit 1
fi

if ! [[ "$TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || [ "$TIMEOUT_SECONDS" -lt 1 ]; then
    echo "Error: TIMEOUT_SECONDS must be a positive integer"
    exit 1
fi

if [ "$CONCURRENT_REQUESTS" -gt "$TOTAL_REQUESTS" ]; then
    echo "Error: CONCURRENT_REQUESTS cannot be greater than TOTAL_REQUESTS"
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p logs

echo "🔥 Running Performance Test Client"
echo "=================================="
echo "Base URL: $BASE_URL"
echo "Operation Type: $OPERATION_TYPE"
echo "Block Period: ${MIN_BLOCK_PERIOD_MS}ms - ${MAX_BLOCK_PERIOD_MS}ms"
echo "Load Test: ${CONCURRENT_REQUESTS} concurrent, ${TOTAL_REQUESTS} total requests, ${TIMEOUT_SECONDS}s timeout"
echo "Timestamp: $TIMESTAMP"
echo ""

# Build test URL
LOAD_TEST_URL="${BASE_URL}/rest/blocking?operation-type=${OPERATION_TYPE}&min-block-period-ms=${MIN_BLOCK_PERIOD_MS}&max-block-period-ms=${MAX_BLOCK_PERIOD_MS}"

echo "Test URL: $LOAD_TEST_URL"
echo ""

# Run the load test
python3 perf-test-client/load-test.py \
    --url "$LOAD_TEST_URL" \
    --concurrent "$CONCURRENT_REQUESTS" \
    --total "$TOTAL_REQUESTS" \
    --timeout "$TIMEOUT_SECONDS"

LOAD_TEST_EXIT_CODE=$?

echo ""
if [ $LOAD_TEST_EXIT_CODE -eq 0 ]; then
    echo "✅ Performance test completed successfully!"
else
    echo "❌ Performance test completed with errors (exit code: $LOAD_TEST_EXIT_CODE)"
fi

exit $LOAD_TEST_EXIT_CODE
