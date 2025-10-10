#!/bin/bash
set -e

echo "Building and running BRM Server..."
echo "=========================================="

# Build the application
go build -o ./target/brm-server main.go

# Run the application
./target/brm-server

echo "=========================================="
echo "BRM Server built and run successfully!"