#pragma once
#include <cstdint>
#include <memory>
#include <fstream>
#include <string>

struct AppConfig {
    // CLI
    std::string host;
    int port = 0;
    int ws_port = 0;
    int depth = 5;
    int64_t snapshot_every = 200;
    int64_t max_msgs = -1;
    int push_ms = 50;

    // env
    bool feed_enabled = false;
    std::string feed_path;

    std::string bench_log_path;
    std::string pg_conninfo; // empty => disabled
};

// prints usage
void usage(const char* prog);

// parse CLI + env + compute defaults (paths)
AppConfig parse_config(int argc, char** argv);

// open bench log append (optional). returns nullptr if cannot open.
std::unique_ptr<std::ofstream> open_bench_log_append(const AppConfig& cfg);
