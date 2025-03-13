#!/bin/bash

# Function to check if process is running
check_process() {
    pgrep -f "$1" >/dev/null
    return $?
}

# Kill existing processes
pkill -9 -f "python3 main.py"
pkill -9 -f "aria2c"
sleep 2

# Activate virtual environment
source venv/bin/activate

# Set process priorities
renice -n -10 -p $$
ionice -c 1 -n 0 -p $$

# Start bot with auto-restart
while true; do
    echo "Starting bot..."
    python3 main.py
    
    # If bot crashes, wait before restart
    echo "Bot stopped. Restarting in 5 seconds..."
    sleep 5
done
