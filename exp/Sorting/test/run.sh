#!/bin/bash

# Build the server and client
make

# Run the server in the background
./server &
SERVER_PID=$!

# Wait a bit to ensure the server is up
sleep 1

# Run the client
./client

# Kill the server process
kill $SERVER_PID
