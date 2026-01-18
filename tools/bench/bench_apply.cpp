#include "mbo/csv_parser.hpp"
#include "mbo/mbo_order_book.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

using Clock = std::chrono::steady_clock;

static uint64_t percentile(std::vector<uint64_t>& v, double p) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    size_t idx = (size_t)((p / 100.0) * (v.size() - 1));
    return v[idx];
}

int main(int argc, char** argv) {
    std::string path = "CLX5_mbo.csv";
    int warmup = 50'000;
    long long max_msgs = -1;        // -1 = all
    int sample_every = 10;          // 每 N 筆記一次 latency，降低量測 overhead
    std::string symbol = "";        // optional: set book symbol

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--path" && i + 1 < argc) path = argv[++i];
        else if (a == "--warmup" && i + 1 < argc) warmup = std::stoi(argv[++i]);
        else if (a == "--max" && i + 1 < argc) max_msgs = std::stoll(argv[++i]);
        else if (a == "--sample_every" && i + 1 < argc) sample_every = std::stoi(argv[++i]);
        else if (a == "--symbol" && i + 1 < argc) symbol = argv[++i];
        else if (a == "--help") {
            std::cout
                << "Usage: bench_apply [--path CLX5_mbo.csv] [--warmup N] [--max N]\n"
                << "                  [--sample_every K] [--symbol SYM]\n";
            return 0;
        }
    }

    std::ifstream fin(path);
    if (!fin) {
        std::cerr << "[bench_apply] Failed to open: " << path << "\n";
        return 1;
    }

    // 你的 streamer 會 skip header；這裡也一樣做
    std::string line;
    if (!std::getline(fin, line)) {
        std::cerr << "[bench_apply] Empty file\n";
        return 1;
    }

    MboOrderBook book(symbol);

    // --- warmup ---
    int warmed = 0;
    MboEvent e{};
    while (warmed < warmup && std::getline(fin, line)) {
        if (parse_mbo_csv_line(line, e)) {
            book.apply(e);
            ++warmed;
        }
        if (max_msgs >= 0 && warmed >= max_msgs) break;
    }

    // --- measure ---
    std::vector<uint64_t> lat_ns;
    lat_ns.reserve(200000);

    uint64_t processed = 0;
    auto t0 = Clock::now();

    while (std::getline(fin, line)) {
        if (max_msgs >= 0 && (long long)(processed + warmed) >= max_msgs) break;

        if (!parse_mbo_csv_line(line, e)) continue;

        bool sample = (sample_every <= 1) || ((processed % (uint64_t)sample_every) == 0);

        Clock::time_point s;
        if (sample) s = Clock::now();

        book.apply(e);

        if (sample) {
            uint64_t dt = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - s).count();
            lat_ns.push_back(dt);
        }

        ++processed;
    }

    uint64_t total_ns = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
    double secs = (double)total_ns / 1e9;
    double mps = (secs > 0) ? (processed / secs) : 0.0;

    uint64_t p50 = percentile(lat_ns, 50);
    uint64_t p95 = percentile(lat_ns, 95);
    uint64_t p99 = percentile(lat_ns, 99);

    std::cout << "Warmup applied: " << warmed << "\n";
    std::cout << "Measured applied: " << processed << "\n";
    std::cout << "Throughput: " << (uint64_t)mps << " msg/s\n";
    std::cout << "Apply latency (ns): p50=" << p50 << " p95=" << p95 << " p99=" << p99 << "\n";
    std::cout << "Apply latency (us): p50=" << (p50/1000.0)
              << " p95=" << (p95/1000.0)
              << " p99=" << (p99/1000.0) << "\n";

    // optional: print one BBO JSON at end (sanity check)
    // std::cout << book.to_json_bbo() << "\n";

    return 0;
}
