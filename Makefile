# ===== Compiler & flags =====
CXX := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread
LIBS := -lboost_system -lpq

# ===== Paths =====
SRC_DIR := mbo-stream/src
INCLUDES := -I $(SRC_DIR)/../include

# ===== Sources =====
SRCS := \
	$(SRC_DIR)/tcp_main_ws.cpp \
	$(SRC_DIR)/ws_server.cpp \
	$(SRC_DIR)/snapshot_store.cpp \
	$(SRC_DIR)/mbo_order_book.cpp \
	$(SRC_DIR)/pg_writer.cpp \
	$(SRC_DIR)/csv_parser.cpp \
	$(SRC_DIR)/app_config.cpp \
	$(SRC_DIR)/file_output.cpp \
	$(SRC_DIR)/jsonl_writer.cpp

# ===== Targets =====
TARGET := tcp_main_ws

# ===== Default rule =====
all: $(TARGET)

$(TARGET): $(SRCS)
	$(CXX) $(CXXFLAGS) $(SRCS) $(INCLUDES) $(LIBS) -o $@

# ===== Defaults (override-able) =====
HOST ?= 127.0.0.1
FEED_PORT ?= 9000
WS_PORT ?= 8080

DEPTH ?= 50
SNAPSHOT_EVERY ?= 1000
MAX_MSGS ?= -1
PUSH_MS ?= 50

FEED_ENABLED ?= 1
FEED_FILE ?= frontend/public/snapshots_feed.jsonl
BENCH_FILE ?= frontend/public/benchmarks.jsonl

PG_CONNINFO ?= host=127.0.0.1 port=5432 dbname=batonic user=postgres password=postgres

# ===== Run (local dev) =====
run: $(TARGET)
	@mkdir -p frontend/public
	rm -f "$(FEED_FILE)" "$(BENCH_FILE)"
	PG_CONNINFO="$(PG_CONNINFO)" \
	FEED_ENABLED="$(FEED_ENABLED)" \
	FEED_PATH="$(FEED_FILE)" \
	BENCH_LOG_PATH="$(BENCH_FILE)" \
	./$(TARGET) $(HOST) $(FEED_PORT) $(WS_PORT) $(DEPTH) $(SNAPSHOT_EVERY) $(MAX_MSGS) $(PUSH_MS)

# ===== Clean =====
clean:
	rm -f $(TARGET) tools/bench/bench_apply

.PHONY: all clean bench_apply run
