#include "mbo/app_config.hpp"

#include <cstdlib>
#include <cctype>
#include <iostream>
#include <filesystem>

static inline bool env_truthy(const char* v) {
    if (!v || !*v) return false;
    std::string s(v);
    for (auto& c : s) c = (char)std::tolower((unsigned char)c);
    return (s == "1" || s == "true" || s == "yes" || s == "y" || s == "on");
}

// stable "repo root" from current working dir (best-effort)
static std::filesystem::path guess_repo_root() {
    std::filesystem::path p = std::filesystem::current_path();
    for (int i = 0; i < 6; i++) {
        if (std::filesystem::exists(p / "frontend") &&
            std::filesystem::is_directory(p / "frontend")) {
            return p;
        }
        p = p.parent_path();
        if (p.empty()) break;
    }
    return std::filesystem::current_path();
}

static std::string default_public_dir() {
    const auto repo = guess_repo_root();
    const auto outdir = repo / "frontend" / "public";
    std::error_code ec;
    std::filesystem::create_directories(outdir, ec);
    return outdir.string();
}

void usage(const char* prog) {
    std::cerr
        << "Usage: " << prog
        << " <feed_host> <feed_port> <ws_port> [depth=5] [snapshot_every=200] [max_msgs=-1] [push_ms=50]\n"
        << "Example: " << prog << " 127.0.0.1 9000 8080 50 200 -1 50\n"
        << "Env: PG_CONNINFO=\"host=127.0.0.1 port=5432 dbname=batonic user=postgres password=postgres\"\n"
        << "Env: FEED_ENABLED=1 (optional)\n"
        << "Env: FEED_PATH=frontend/public/snapshots_feed.jsonl (optional)\n"
        << "Env: BENCH_LOG_PATH=frontend/public/benchmarks.jsonl (optional)\n";
}

AppConfig parse_config(int argc, char** argv) {
    AppConfig cfg;

    if (argc < 4) {
        usage(argv[0]);
        // keep cfg defaults; caller can decide to exit
        return cfg;
    }

    cfg.host = argv[1];
    cfg.port = std::atoi(argv[2]);
    cfg.ws_port = std::atoi(argv[3]);
    cfg.depth = (argc >= 5) ? std::atoi(argv[4]) : 5;
    cfg.snapshot_every = (argc >= 6) ? std::atoll(argv[5]) : 200;
    cfg.max_msgs = (argc >= 7) ? std::atoll(argv[6]) : -1;
    cfg.push_ms = (argc >= 8) ? std::atoi(argv[7]) : 50;

    // feed env
    cfg.feed_enabled = env_truthy(std::getenv("FEED_ENABLED"));
    if (const char* fp = std::getenv("FEED_PATH"); fp && *fp) {
        cfg.feed_path = fp;
    } else {
        cfg.feed_path = (std::filesystem::path(default_public_dir()) / "snapshots_feed.jsonl").string();
    }

    // bench env
    if (const char* bp = std::getenv("BENCH_LOG_PATH"); bp && *bp) {
        cfg.bench_log_path = bp;
    } else {
        cfg.bench_log_path = (std::filesystem::path(default_public_dir()) / "benchmarks.jsonl").string();
    }

    // pg env
    if (const char* conn = std::getenv("PG_CONNINFO"); conn && *conn) {
        cfg.pg_conninfo = conn;
    } else {
        cfg.pg_conninfo.clear();
    }

    return cfg;
}

std::unique_ptr<std::ofstream> open_bench_log_append(const AppConfig& cfg) {
    if (cfg.bench_log_path.empty()) return nullptr;

    auto ofs = std::make_unique<std::ofstream>(cfg.bench_log_path, std::ios::binary | std::ios::app);
    if (!(*ofs)) {
        std::cerr << "[bench] failed to open log: " << cfg.bench_log_path << "\n";
        return nullptr;
    }
    std::cerr << "[bench] logging to: " << cfg.bench_log_path << "\n";
    return ofs;
}
