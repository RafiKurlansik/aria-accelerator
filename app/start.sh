#!/bin/bash
set -e

echo "=== ARIA Startup Script ==="
echo "Working directory: $(pwd)"
echo "Python version: $(python --version)"
echo "Node version: $(node --version)"
echo "Files in current directory:"
ls -la

# Set Python path for local development
export PYTHONPATH=$(pwd)/src
echo "PYTHONPATH set to: $PYTHONPATH"

echo "=== Testing Python imports ==="
if python test_imports.py; then
    echo "✅ Python imports successful"
else
    echo "❌ Python imports failed - continuing anyway"
fi

echo "=== Starting Python FastAPI backend ==="
echo "Starting: python app.py"
python app.py &
BACKEND_PID=$!

echo "Backend started with PID: $BACKEND_PID"
echo "Waiting 5 seconds for backend to start..."
sleep 5

# Test if backend is responding
echo "=== Testing backend connectivity ==="
if curl -f http://localhost:8080/healthz 2>/dev/null; then
    echo "✅ Backend is responding"
else
    echo "❌ Backend not responding yet"
fi

echo "=== Starting Node.js frontend ==="
echo "Starting: node app.js"
node app.js &
FRONTEND_PID=$!

echo "Frontend started with PID: $FRONTEND_PID"
echo "Both services started!"

# Function to cleanup on exit
cleanup() {
    echo "Cleaning up processes..."
    kill $BACKEND_PID 2>/dev/null || true
    kill $FRONTEND_PID 2>/dev/null || true
    exit
}

# Trap signals
trap cleanup SIGTERM SIGINT

# Wait for either process to exit
wait
