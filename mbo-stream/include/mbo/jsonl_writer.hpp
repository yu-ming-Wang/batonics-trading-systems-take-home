#pragma once
#include <cstdint>
#include <fstream>
#include <string>

namespace mbo {

struct FeedLine {
    int64_t ts_us = 0;
    std::string symbol;
    int64_t processed = 0;
    int depth = 0;
    std::string book_json; // already a JSON object string
};

struct BenchLine {
    int64_t ts_wall_us = 0;
    std::string host;
    int port = 0;
    int depth = 0;
    int64_t snapshot_every = 0;
    bool feed_enabled = false;
    bool pg_enabled = false;
    int64_t processed = 0;
    double elapsed_s = 0.0;
    double throughput_msgs_per_s = 0.0;

    double apply_p50_us = 0.0;
    double apply_p95_us = 0.0;
    double apply_p99_us = 0.0;

    double snap_p50_ms = 0.0;
    double snap_p95_ms = 0.0;
    double snap_p99_ms = 0.0;
};

class JsonlWriter {
public:
    JsonlWriter() = default;
    explicit JsonlWriter(const std::string& path, bool append = true);
    ~JsonlWriter();

    JsonlWriter(const JsonlWriter&) = delete;
    JsonlWriter& operator=(const JsonlWriter&) = delete;

    JsonlWriter(JsonlWriter&&) noexcept = default;
    JsonlWriter& operator=(JsonlWriter&&) noexcept = default;

    bool open(const std::string& path, bool append = true);
    bool is_open() const { return ofs_.is_open() && ofs_.good(); }
    const std::string& path() const { return path_; }

    void write_feed(const FeedLine& line);
    void write_bench(const BenchLine& line);

    void flush();

private:
    std::string path_;
    std::ofstream ofs_;
};

} // namespace mbo
