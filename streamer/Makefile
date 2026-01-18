# ===== Compiler =====
CXX := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread

# ===== Boost / System libs =====
LIBS := -lboost_system

# ===== Paths =====
SRC_DIR := src
TARGET := streamer

SRCS := $(SRC_DIR)/streamer.cpp

# ===== Default =====
all: $(TARGET)

$(TARGET): $(SRCS)
	$(CXX) $(CXXFLAGS) $(SRCS) $(LIBS) -o $@

# ===== Dev helpers =====
run: $(TARGET)
	./$(TARGET) data/CLX5_mbo.csv 9000 1000 1

clean:
	rm -f $(TARGET)

.PHONY: all clean run
