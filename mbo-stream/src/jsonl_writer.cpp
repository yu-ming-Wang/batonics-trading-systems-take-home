#include "mbo/jsonl_writer.hpp"
#include <filesystem>
#include <iostream>

namespace mbo {

JsonlWriter::JsonlWriter(const std::string& path, bool append) {
    open(path, append);
}

JsonlWriter::~JsonlWriter() {
    flush();
}

bool JsonlWriter::open(const std::string& path, bool append) {
    path_ = path;

    std::filesystem::path fp(path);
    std::error_code ec;
    if (fp.has_parent_path()) {
        std::filesystem::create_directories(fp.parent_path(), ec);
    }

    auto mode = std::ios::binary | (append ? std::ios::app : std::ios::trunc);
    ofs_.open(path, mode);

    if (!ofs_) {
        std::cerr << "[jsonl] failed to open: " << path << "\n";
        return false;
    }
    return true;
}

void JsonlWriter::flush() {
    if (ofs_.is_open()) ofs_.flush();
}

void JsonlWriter::write_feed(const FeedLine& line) {
    if (!is_open()) return;
    if (line.ts_us <= 0) return;
    if (line.symbol.empty()) return;
    if (line.book_json.empty()) return;

    // NOTE: symbol assumed safe (CLX5 etc). If arbitrary symbols appear, escape quotes/backslashes.
    ofs_
        << "{\"ts_us\":" << line.ts_us
        << ",\"symbol\":\"" << line.symbol
        << "\",\"processed\":" << line.processed
        << ",\"depth\":" << line.depth
        << ",\"book\":" << line.book_json
        << "}\n";
}

void JsonlWriter::write_bench(const BenchLine& b) {
    if (!is_open()) return;

    ofs_
        << "{"
        << "\"ts_wall_us\":" << b.ts_wall_us
        << ",\"host\":\"" << b.host << "\""
        << ",\"port\":" << b.port
        << ",\"depth\":" << b.depth
        << ",\"snapshot_every\":" << b.snapshot_every
        << ",\"feed_enabled\":" << (b.feed_enabled ? "true" : "false")
        << ",\"pg_enabled\":" << (b.pg_enabled ? "true" : "false")
        << ",\"processed\":" << b.processed
        << ",\"elapsed_s\":" << b.elapsed_s
        << ",\"throughput_msgs_per_s\":" << b.throughput_msgs_per_s
        << ",\"apply_p50_us\":" << b.apply_p50_us
        << ",\"apply_p95_us\":" << b.apply_p95_us
        << ",\"apply_p99_us\":" << b.apply_p99_us
        << ",\"snap_p50_ms\":" << b.snap_p50_ms
        << ",\"snap_p95_ms\":" << b.snap_p95_ms
        << ",\"snap_p99_ms\":" << b.snap_p99_ms
        << "}\n";
}

} // namespace mbo
