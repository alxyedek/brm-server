#!/bin/bash
# Stop BRM Go Server Script
# This script stops the BRM Go server running with limited resources

echo "🛑 Stopping BRM Go Server..."

# Find and kill the Go process
SERVER_PIDS=$(pgrep -f "brm-server" | grep -v grep || true)

if [ -z "$SERVER_PIDS" ]; then
    echo "No BRM Go server process found."
    exit 0
fi

echo "Found server processes: $SERVER_PIDS"

# Kill gracefully first (SIGTERM)
for pid in $SERVER_PIDS; do
    echo "Stopping server process (PID: $pid)..."
    kill "$pid" 2>/dev/null || true
done

# Wait for graceful shutdown
sleep 3

# Force kill if still running (SIGKILL)
REMAINING_PIDS=$(pgrep -f "brm-server" | grep -v grep || true)
if [ -n "$REMAINING_PIDS" ]; then
    echo "Force stopping remaining processes..."
    for pid in $REMAINING_PIDS; do
        echo "Force killing server process (PID: $pid)..."
        kill -9 "$pid" 2>/dev/null || true
    done
fi

# Verify stopped
FINAL_CHECK=$(pgrep -f "brm-server" | grep -v grep || true)
if [ -z "$FINAL_CHECK" ]; then
    echo "✅ BRM Go server stopped successfully."
else
    echo "⚠️  Some processes may still be running: $FINAL_CHECK"
    exit 1
fi
