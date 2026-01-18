#include "mbo/mbo_order_book.hpp"
#include "mbo/pow2_histogram.hpp"
#include "mbo/csv_parser.hpp"
#include "mbo/snapshot_store.hpp"
#include "mbo/ws_server.hpp"
#include "mbo/pg_writer.hpp"

#include <memory>
#include <boost/asio.hpp>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <cstdio>
#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <vector>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <filesystem>
#include <sstream>
#include <cctype>

using boost::asio::ip::tcp;
using SteadyClock = std::chrono::steady_clock;

// ----------------------- DB Writer Queue -----------------------
struct SnapshotWrite {
    int64_t ts_us = 0;
    std::string symbol;
    TopOfBook tob;
};

static void usage(const char* prog) {
    std::cerr
        << "Usage: " << prog
        << " <feed_host> <feed_port> <ws_port> [depth=5] [snapshot_every=200] [max_msgs=-1] [push_ms=50]\n"
        << "Example: " << prog << " 127.0.0.1 9000 8080 50 200 -1 50\n"
        << "Env: PG_CONNINFO=\"host=127.0.0.1 port=5432 dbname=batonic user=postgres password=postgres\"\n"
        << "Env: FEED_ENABLED=1 (optional)\n"
        << "Env: FEED_PATH=frontend/public/snapshots_feed.jsonl (optional)\n"
        << "Env: BENCH_LOG_PATH=frontend/public/benchmarks.jsonl (optional)\n";
}

// stable "repo root" from current working dir (best-effort)
static std::filesystem::path guess_repo_root() {
    std::filesystem::path p = std::filesystem::current_path();
    for (int i = 0; i < 6; i++) {
        if (std::filesystem::exists(p / "frontend") && std::filesystem::is_directory(p / "frontend")) {
            return p;
        }
        p = p.parent_path();
        if (p.empty()) break;
    }
    return std::filesystem::current_path();
}

static std::string default_bench_log_path() {
    const auto repo = guess_repo_root();
    const auto outdir = repo / "frontend" / "public";
    std::error_code ec;
    std::filesystem::create_directories(outdir, ec);
    return (outdir / "benchmarks.jsonl").string();
}

static std::unique_ptr<std::ofstream> open_bench_log_append() {
    const char* p = std::getenv("BENCH_LOG_PATH");
    std::string path = (p && *p) ? std::string(p) : default_bench_log_path();

    auto ofs = std::make_unique<std::ofstream>(path, std::ios::binary | std::ios::app);
    if (!(*ofs)) {
        std::cerr << "[bench] failed to open log: " << path << "\n";
        return nullptr;
    }
    std::cerr << "[bench] logging to: " << path << "\n";
    return ofs;
}

static inline int64_t now_wall_us() {
    using namespace std::chrono;
    return (int64_t)duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
}

static inline bool env_truthy(const char* v) {
    if (!v || !*v) return false;
    std::string s(v);
    for (auto& c : s) c = (char)std::tolower((unsigned char)c);
    return (s == "1" || s == "true" || s == "yes" || s == "y" || s == "on");
}

static inline int64_t ts_event_to_us(const std::string& ts) {
    int Y=0,M=0,D=0,h=0,m=0,s=0;
    long ns = 0;

    if (sscanf(ts.c_str(), "%d-%d-%dT%d:%d:%d", &Y,&M,&D,&h,&m,&s) != 6) return 0;

    auto dot = ts.find('.');
    if (dot != std::string::npos) {
        size_t z = ts.find('Z', dot);
        std::string frac = ts.substr(dot+1, (z==std::string::npos? ts.size(): z) - (dot+1));
        while (frac.size() < 9) frac.push_back('0');
        if (frac.size() > 9) frac.resize(9);
        ns = std::strtol(frac.c_str(), nullptr, 10);
    }

    std::tm t{};
    t.tm_year = Y - 1900;
    t.tm_mon  = M - 1;
    t.tm_mday = D;
    t.tm_hour = h;
    t.tm_min  = m;
    t.tm_sec  = s;

    time_t sec = timegm(&t);
    if (sec < 0) return 0;

    int64_t us = (int64_t)sec * 1000000LL + (int64_t)(ns / 1000);
    return us;
}

static void enqueue_snapshot_write(
    PgWriter* pg,
    std::mutex& q_mtx,
    std::condition_variable& q_cv,
    std::deque<SnapshotWrite>& q,
    size_t max_q,
    int64_t ts_us,
    const std::string& symbol,
    const TopOfBook& tob
) {
    if (!pg) return;

    SnapshotWrite item;
    item.ts_us = ts_us;
    item.symbol = symbol;
    item.tob = tob;

    {
        std::lock_guard<std::mutex> lk(q_mtx);
        while (q.size() >= max_q) q.pop_front();
        q.push_back(std::move(item));
    }
    q_cv.notify_one();
}

// JSONL feed writer: write ONE line per snapshot
static inline void write_feed_line(
    std::ofstream* feed,
    int64_t ts_us,
    const std::string& symbol,
    int64_t processed,
    int depth,
    const std::string& book_json
) {
    if (!feed || !(*feed) || ts_us <= 0 || symbol.empty()) return;

    // NOTE: symbol assumed safe (CLX5 etc). If you later allow arbitrary symbols, escape quotes/backslashes.
    (*feed) << "{\"ts_us\":" << ts_us
            << ",\"symbol\":\"" << symbol
            << "\",\"processed\":" << processed
            << ",\"depth\":" << depth
            << ",\"book\":" << book_json
            << "}\n";
}

static void dump_final_books(
    const MboOrderBook& book,
    const std::string& book_symbol,
    int depth_full
) {
    const auto repo = guess_repo_root();
    const auto outdir = repo / "frontend" / "public";
    std::error_code ec;
    std::filesystem::create_directories(outdir, ec);

    std::string full_json = book.to_json(depth_full);

    {
        auto out1 = outdir / "final_book.json";
        std::ofstream ofs(out1, std::ios::binary);
        if (!ofs) {
            std::cerr << "[final] failed to open: " << out1.string() << "\n";
        } else {
            ofs << full_json;
            ofs.close();
            std::cerr << "[final] wrote " << out1.string()
                      << " (" << full_json.size() << " bytes)\n";
        }
    }

    if (!book_symbol.empty()) {
        auto out2 = outdir / ("final_book_" + book_symbol + ".json");
        std::ofstream ofs(out2, std::ios::binary);
        if (!ofs) {
            std::cerr << "[final] failed to open: " << out2.string() << "\n";
        } else {
            ofs << full_json;
            ofs.close();
            std::cerr << "[final] wrote " << out2.string()
                      << " (" << full_json.size() << " bytes)\n";
        }
    }
}

static bool handle_line(
    std::string& line,
    MboOrderBook& book,
    std::string& book_symbol,
    bool& has_symbol,
    Pow2Histogram& apply_hist,        // Benchmark 1
    Pow2Histogram& snap_hist,         // Benchmark 2
    int depth,
    int64_t snapshot_every,
    int64_t& processed,
    int64_t& parsed_ok,
    uint64_t& lines_total,
    int64_t& last_ts_us,
    PgWriter* pg,
    std::mutex& q_mtx,
    std::condition_variable& q_cv,
    std::deque<SnapshotWrite>& q,
    size_t max_q,
    std::ofstream* feed_ofs           // optional
) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (line.empty()) return false;

    static bool printed_hdr = false;
    if (!printed_hdr) {
        std::cerr << "[hdr] " << line << "\n";
        printed_hdr = true;
    }

    // skip CSV header lines
    if (line.rfind("ts_event", 0) == 0 ||
        line.rfind("publisher_id", 0) == 0 ||
        line.rfind("instrument_id", 0) == 0) {
        return false;
    }

    lines_total++;

    MboEvent e;
    if (!parse_mbo_csv_line(line, e)) return false;
    parsed_ok++;

    if (!e.ts_event.empty()) {
        last_ts_us = ts_event_to_us(e.ts_event);
    }

    if (!has_symbol && !e.symbol.empty()) {
        book_symbol = e.symbol;
        book = MboOrderBook(e.symbol);
        has_symbol = true;
    }

    // Benchmark 1: apply latency
    auto s = SteadyClock::now();
    book.apply(e);
    auto f = SteadyClock::now();
    uint64_t apply_ns = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(f - s).count();
    apply_hist.add(apply_ns);

    processed++;

    // Snapshot path
    if (snapshot_every > 0 && (processed % snapshot_every == 0)) {
        const std::string& sym = (!book_symbol.empty() ? book_symbol : std::string(""));

        // Benchmark 2: snapshot output latency = to_json + publish + feed write + enqueue
        auto t0 = SteadyClock::now();

        // IMPORTANT: build JSON ONCE
        std::string book_json = book.to_json(depth);

        // 1) WS publish (latest snapshot)
        if (!sym.empty()) publish_snapshot(sym, book_json);
        else publish_snapshot(book_json);

        // 2) DB enqueue (Top-of-Book only)
        if (!sym.empty() && last_ts_us > 0) {
            TopOfBook tob = book.top_of_book();
            enqueue_snapshot_write(pg, q_mtx, q_cv, q, max_q, last_ts_us, sym, tob);
        }

        // 3) JSONL feed (full snapshot)
        if (!sym.empty() && last_ts_us > 0) {
            write_feed_line(feed_ofs, last_ts_us, sym, processed, depth, book_json);
        }

        auto t1 = SteadyClock::now();
        uint64_t snap_ns = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        snap_hist.add(snap_ns);

        std::cerr << book.to_pretty_bbo() << "\n";
    }

    return true;
}

static void run_one_replay_session(
    const std::string& host,
    int port,
    int depth,
    int64_t snapshot_every,
    int64_t max_msgs,
    PgWriter* pg,
    std::mutex& q_mtx,
    std::condition_variable& q_cv,
    std::deque<SnapshotWrite>& q,
    size_t max_q,
    bool feed_enabled,
    const std::string& feed_path,
    std::ofstream* bench_log  // optional JSONL log
) {
    boost::asio::io_context io;
    tcp::resolver resolver(io);
    tcp::socket socket(io);

    // connect
    auto endpoints = resolver.resolve(host, std::to_string(port));
    boost::asio::connect(socket, endpoints);
    socket.set_option(tcp::no_delay(true));
    std::cerr << "[tcp_main] connected to " << host << ":" << port << "\n";

    // per-session JSONL feed open (append)
    std::ofstream feed_ofs;
    std::ofstream* feed_ptr = nullptr;
    if (feed_enabled && !feed_path.empty()) {
        std::filesystem::path fp(feed_path);
        std::error_code ec;
        if (fp.has_parent_path()) {
            std::filesystem::create_directories(fp.parent_path(), ec);
        }
        feed_ofs.open(feed_path, std::ios::binary | std::ios::app);
        if (!feed_ofs) {
            std::cerr << "[feed] failed to open: " << feed_path << "\n";
        } else {
            feed_ptr = &feed_ofs;
            std::cerr << "[feed] appending snapshots to: " << feed_path << "\n";
        }
    }

    // reset per-session state
    MboOrderBook book("");
    bool has_symbol = false;
    std::string book_symbol;
    book_symbol.reserve(16);

    Pow2Histogram apply_hist; // Benchmark 1
    Pow2Histogram snap_hist;  // Benchmark 2

    int64_t processed = 0, parsed_ok = 0;
    uint64_t bytes_total = 0;
    uint64_t lines_total = 0;
    int64_t last_ts_us = 0;

    std::string carry;
    carry.reserve(1 << 20);
    std::vector<char> buf(1 << 20);

    auto t0 = SteadyClock::now();
    boost::system::error_code ec;

    while (true) {
        std::size_t n = socket.read_some(boost::asio::buffer(buf), ec);

        if (ec && ec != boost::asio::error::eof) {
            std::cerr << "[tcp_main] read error: " << ec.message() << "\n";
            break;
        }

        if (n > 0) {
            bytes_total += n;
            carry.append(buf.data(), n);

            std::size_t pos = 0;
            while (true) {
                std::size_t nl = carry.find('\n', pos);
                if (nl == std::string::npos) {
                    carry.erase(0, pos);
                    break;
                }

                std::string line = carry.substr(pos, nl - pos);
                pos = nl + 1;

                if (max_msgs < 0 || processed < max_msgs) {
                    handle_line(line, book, book_symbol, has_symbol,
                                apply_hist, snap_hist,
                                depth, snapshot_every,
                                processed, parsed_ok, lines_total,
                                last_ts_us,
                                pg, q_mtx, q_cv, q, max_q,
                                feed_ptr);
                } else {
                    lines_total++;
                }
            }
        }

        if (ec == boost::asio::error::eof) {
            break;
        }
    }

    // trailing partial line
    if (!carry.empty() && (max_msgs < 0 || processed < max_msgs)) {
        std::string tail = carry;
        carry.clear();
        handle_line(tail, book, book_symbol, has_symbol,
                    apply_hist, snap_hist,
                    depth, snapshot_every,
                    processed, parsed_ok, lines_total,
                    last_ts_us,
                    pg, q_mtx, q_cv, q, max_q,
                    feed_ptr);
    }

    // final flush if remainder exists (also measure snapshot latency once)
    if (processed > 0 && (snapshot_every <= 0 || (processed % snapshot_every != 0))) {
        auto t0s = SteadyClock::now();

        std::string json = book.to_json(depth);

        if (!book_symbol.empty()) publish_snapshot(book_symbol, json);
        else publish_snapshot(json);

        if (pg && !book_symbol.empty() && last_ts_us > 0) {
            TopOfBook tob = book.top_of_book();
            enqueue_snapshot_write(pg, q_mtx, q_cv, q, max_q, last_ts_us, book_symbol, tob);
        }

        if (!book_symbol.empty() && last_ts_us > 0) {
            write_feed_line(feed_ptr, last_ts_us, book_symbol, processed, depth, json);
        }

        auto t1s = SteadyClock::now();
        uint64_t snap_ns = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1s - t0s).count();
        snap_hist.add(snap_ns);

        std::cerr << "[final] forced snapshot flush (remainder)\n";
    }

    // final BBO
    std::cerr << book.to_pretty_bbo() << "\n";

    // dump full book json for frontend
    dump_final_books(book, book_symbol, /*depth_full=*/1'000'000);

    if (feed_ofs.is_open()) {
        feed_ofs.flush();
        std::cerr << "[feed] flushed\n";
    }

    auto t1 = SteadyClock::now();
    double secs = std::chrono::duration<double>(t1 - t0).count();
    double mps = (secs > 0) ? (processed / secs) : 0.0;

    auto ns_to_us = [](uint64_t ns) -> double { return (double)ns / 1000.0; };
    auto ns_to_ms = [](uint64_t ns) -> double { return (double)ns / 1e6; };

    auto apply_p50 = apply_hist.percentile(0.50);
    auto apply_p95 = apply_hist.percentile(0.95);
    auto apply_p99 = apply_hist.percentile(0.99);

    auto snap_p50 = snap_hist.percentile(0.50);
    auto snap_p95 = snap_hist.percentile(0.95);
    auto snap_p99 = snap_hist.percentile(0.99);

    std::cerr << "=== TCP Main Stats (session) ===\n";
    std::cerr << "bytes_total: " << bytes_total << "\n";
    std::cerr << "lines_total: " << lines_total << "\n";
    std::cerr << "processed: " << processed << " (parsed_ok=" << parsed_ok << ")\n";
    std::cerr << "elapsed_s: " << secs << "\n";
    std::cerr << "throughput_msgs_per_s: " << mps << "\n";
    std::cerr << "apply_latency_est_p50: " << ns_to_us(apply_p50) << " us\n";
    std::cerr << "apply_latency_est_p95: " << ns_to_us(apply_p95) << " us\n";
    std::cerr << "apply_latency_est_p99: " << ns_to_us(apply_p99) << " us\n";

    if (snapshot_every > 0) {
        std::cerr << "snapshot_latency_est_p50: " << ns_to_ms(snap_p50) << " ms\n";
        std::cerr << "snapshot_latency_est_p95: " << ns_to_ms(snap_p95) << " ms\n";
        std::cerr << "snapshot_latency_est_p99: " << ns_to_ms(snap_p99) << " ms\n";
    }

    // JSONL bench summary
    if (bench_log && *bench_log) {
        (*bench_log)
            << "{"
            << "\"ts_wall_us\":" << now_wall_us()
            << ",\"host\":\"" << host << "\""
            << ",\"port\":" << port
            << ",\"depth\":" << depth
            << ",\"snapshot_every\":" << snapshot_every
            << ",\"feed_enabled\":" << (feed_enabled ? "true" : "false")
            << ",\"pg_enabled\":" << (pg ? "true" : "false")
            << ",\"processed\":" << processed
            << ",\"elapsed_s\":" << secs
            << ",\"throughput_msgs_per_s\":" << mps
            << ",\"apply_p50_us\":" << ns_to_us(apply_p50)
            << ",\"apply_p95_us\":" << ns_to_us(apply_p95)
            << ",\"apply_p99_us\":" << ns_to_us(apply_p99)
            << ",\"snap_p50_ms\":" << ns_to_ms(snap_p50)
            << ",\"snap_p95_ms\":" << ns_to_ms(snap_p95)
            << ",\"snap_p99_ms\":" << ns_to_ms(snap_p99)
            << "}\n";
        bench_log->flush();
    }

    std::cerr << "[tcp_main] session done, back to waiting...\n";
}

int main(int argc, char** argv) {
    if (argc < 4) { usage(argv[0]); return 1; }

    const std::string host = argv[1];
    const int port = std::atoi(argv[2]);
    const int ws_port = std::atoi(argv[3]);
    const int depth = (argc >= 5) ? std::atoi(argv[4]) : 5;
    const int64_t snapshot_every = (argc >= 6) ? std::atoll(argv[5]) : 200;
    const int64_t max_msgs = (argc >= 7) ? std::atoll(argv[6]) : -1;
    const int push_ms = (argc >= 8) ? std::atoi(argv[7]) : 50;

    // Feed config via env (B)
    bool feed_enabled = env_truthy(std::getenv("FEED_ENABLED"));

    std::string feed_path;
    if (const char* fp = std::getenv("FEED_PATH"); fp && *fp) {
        feed_path = fp;
    } else {
        auto repo = guess_repo_root();
        auto outdir = repo / "frontend" / "public";
        std::error_code ec;
        std::filesystem::create_directories(outdir, ec);
        feed_path = (outdir / "snapshots_feed.jsonl").string();
    }

    if (feed_enabled) {
        std::cerr << "[feed] enabled, path=" << feed_path << "\n";
    } else {
        std::cerr << "[feed] disabled (set FEED_ENABLED=1)\n";
    }

    // Bench log (optional, default to frontend/public/benchmarks.jsonl)
    auto bench_log = open_bench_log_append();

    // ---- Start WebSocket server (separate io_context/thread) ----
    boost::asio::io_context ws_ioc;
    try {
        start_ws_server(ws_ioc, ws_port, push_ms);
    } catch (const std::exception& e) {
        std::cerr << "[ws] failed to start: " << e.what() << "\n";
        return 1;
    }

    std::thread ws_thread([&]{
        std::cerr << "[ws] listening on port " << ws_port
                  << " (push every " << push_ms << " ms)\n";
        ws_ioc.run();
    });

    // ---- PG Writer init (optional) ----
    std::unique_ptr<PgWriter> pg;
    {
        const char* conn = std::getenv("PG_CONNINFO");
        if (conn && *conn) {
            pg = std::make_unique<PgWriter>(std::string(conn));
            std::cerr << "[pg] enabled\n";
        } else {
            std::cerr << "[pg] disabled (set PG_CONNINFO)\n";
        }
    }

    // ---- Async DB writer thread ----
    std::mutex q_mtx;
    std::condition_variable q_cv;
    std::deque<SnapshotWrite> q;
    std::atomic<bool> stop{false};
    const size_t max_q = 20000;

    std::thread pg_thread;
    if (pg) {
        pg_thread = std::thread([&]{
            while (true) {
                SnapshotWrite item;
                {
                    std::unique_lock<std::mutex> lk(q_mtx);
                    q_cv.wait(lk, [&]{ return stop.load() || !q.empty(); });

                    if (q.empty()) {
                        if (stop.load()) break;
                        continue;
                    }

                    item = std::move(q.front());
                    q.pop_front();
                }

                pg->write_snapshot(item.ts_us, item.symbol, item.tob);
            }
            std::cerr << "[pg] writer thread exit\n";
        });
    }

    // Main loop: wait for streamer forever (retry connect)
    while (true) {
        try {
            std::cerr << "[tcp_main] waiting for feed " << host << ":" << port << " ...\n";
            run_one_replay_session(
                host, port, depth, snapshot_every, max_msgs,
                pg.get(), q_mtx, q_cv, q, max_q,
                feed_enabled, feed_path,
                bench_log ? bench_log.get() : nullptr
            );
        } catch (const std::exception& e) {
            std::cerr << "[tcp_main] connect/session failed: " << e.what()
                      << " (retry in 2000ms)\n";
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }

    // unreachable normally
    stop.store(true);
    q_cv.notify_all();
    if (pg_thread.joinable()) pg_thread.join();
    ws_ioc.stop();
    if (ws_thread.joinable()) ws_thread.join();
    return 0;
}
