# Makefile for HoverCraft++ prototype

CXX = mpic++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra
TARGET = hovercraft_demo

# Component source files (separate compilation)
COMP_SOURCES = switch_main.cpp leader_main.cpp follower_main.cpp netagg_main.cpp client_main.cpp main_driver.cpp
HEADERS = common.hpp switch.hpp leader.hpp follower.hpp netagg.hpp client.hpp

# Default target
all: $(TARGET)

# Build the main executable
$(TARGET): main.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) -o $@ main.cpp

# Alternative: Build separate executables for each component
separate: switch_exe leader_exe follower_exe netagg_exe client_exe

switch_exe: switch_main.cpp common.hpp
	$(CXX) $(CXXFLAGS) -DSWITCH_MAIN -o switch switch_main.cpp

leader_exe: leader_main.cpp common.hpp
	$(CXX) $(CXXFLAGS) -DLEADER_MAIN -o leader leader_main.cpp

follower_exe: follower_main.cpp common.hpp
	$(CXX) $(CXXFLAGS) -DFOLLOWER_MAIN -o follower follower_main.cpp

netagg_exe: netagg_main.cpp common.hpp
	$(CXX) $(CXXFLAGS) -DNETAGG_MAIN -o netagg netagg_main.cpp

client_exe: client_main.cpp common.hpp
	$(CXX) $(CXXFLAGS) -DCLIENT_MAIN -o client client_main.cpp

# Run with default 100 requests
run: $(TARGET)
	mpirun -np 6 ./$(TARGET) 100

# Run with custom number of requests
run-custom: $(TARGET)
	@echo "Usage: make run-custom REQUESTS=<number>"
	mpirun -np 6 ./$(TARGET) $(REQUESTS)

# Run with verbose output
run-verbose: $(TARGET)
	mpirun -np 6 -v ./$(TARGET) 100

# Clean build artifacts
clean:
	rm -f $(TARGET) switch_exe leader_exe follower_exe netagg_exe client_exe *.o

# Help target
help:
	@echo "Available targets:"
	@echo "  all         - Build the main HoverCraft++ demo executable"
	@echo "  run         - Run with 100 requests"
	@echo "  run-custom  - Run with custom number of requests (REQUESTS=n)"
	@echo "  run-verbose - Run with verbose MPI output"
	@echo "  clean       - Remove all build artifacts"
	@echo "  help        - Show this help message"

# Phony targets
.PHONY: all run run-custom run-verbose clean help separate