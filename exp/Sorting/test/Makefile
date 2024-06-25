# Variables
CXX = g++
CXXFLAGS = -Wall -std=c++11
TARGETS = server client

# Build all targets
all: $(TARGETS)

# Build server
server: server.cpp
	$(CXX) $(CXXFLAGS) -o server server.cpp

# Build client
client: client.cpp
	$(CXX) $(CXXFLAGS) -o client client.cpp

# Clean the build files
clean:
	rm -f $(TARGETS) *.o

# PHONY targets
.PHONY: all clean
