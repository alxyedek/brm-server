#!/bin/bash
set -e

echo "Running BRM Server Configuration Tests..."
echo "=========================================="

# Run tests with coverage
go test -v -cover -coverprofile=coverage.out ./internal/config/...

# Display coverage summary
echo ""
echo "Coverage Summary:"
go tool cover -func=coverage.out | grep total

# Optional: Generate HTML coverage report
if [ "$1" == "--html" ]; then
    go tool cover -html=coverage.out -o coverage.html
    echo "HTML coverage report generated: coverage.html"
fi

echo ""
echo "All tests passed!"
