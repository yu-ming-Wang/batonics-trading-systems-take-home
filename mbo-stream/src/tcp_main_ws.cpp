#include "mbo/mbo_order_book.hpp"
#include "mbo/pow2_histogram.hpp"
#include "mbo/csv_parser.hpp"
#include "mbo/snapshot_store.hpp"
#include "mbo/ws_server.hpp"
#include "mbo/pg_writer.hpp"
#include "mbo/app_config.hpp"
#include "mbo/jsonl_writer.hpp"
#include "mbo/file_output.hpp"

#include <boost/asio.hpp>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <string>
#include <vector>
#include <memory>

using boost::asio::ip::tcp;
using SteadyClock = std::chrono::steady_clock;

// ----------------------- DB Writer Queue -----------------------
struct SnapshotWrite {
    int64_t ts_us = 0;
    std::string symbol;
    TopOfBook tob;
};

static inline int64_t now_wall_us() {
    using namespace std::chrono;
    return (int64_t)duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
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
    mbo::JsonlWriter* feed_writer     // optional
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
    uint64_t apply_ns =
        (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(f - s).count();
    apply_hist.add(apply_ns);

    processed++;

    if (snapshot_every > 0 && (processed % snapshot_every == 0)) {
        const std::string& sym = (!book_symbol.empty() ? book_symbol : std::string(""));

        // Benchmark 2: snapshot latency = to_json + publish + db enqueue + feed write
        auto t0 = SteadyClock::now();

        std::string book_json = book.to_json(depth);

        // 1) WS publish
        if (!sym.empty()) publish_snapshot(sym, book_json);
        else publish_snapshot(book_json);

        // 2) DB enqueue (Top-of-Book only)
        if (!sym.empty() && last_ts_us > 0) {
            TopOfBook tob = book.top_of_book();
            enqueue_snapshot_write(pg, q_mtx, q_cv, q, max_q, last_ts_us, sym, tob);
        }

        // 3) JSONL feed
        if (feed_writer && !sym.empty() && last_ts_us > 0) {
            mbo::FeedLine fl;
            fl.ts_us = last_ts_us;
            fl.symbol = sym;
            fl.processed = processed;
            fl.depth = depth;
            fl.book_json = book_json;
            feed_writer->write_feed(fl);
        }

        auto t1 = SteadyClock::now();
        uint64_t snap_ns =
            (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        snap_hist.add(snap_ns);

        std::cerr << book.to_pretty_bbo() << "\n";
    }

    return true;
}

static void run_one_replay_session(
    const AppConfig& cfg,
    PgWriter* pg,
    std::mutex& q_mtx,
    std::condition_variable& q_cv,
    std::deque<SnapshotWrite>& q,
    size_t max_q,
    mbo::JsonlWriter* bench_writer // optional
) {
    boost::asio::io_context io;
    tcp::resolver resolver(io);
    tcp::socket socket(io);

    // connect
    auto endpoints = resolver.resolve(cfg.host, std::to_string(cfg.port));
    boost::asio::connect(socket, endpoints);
    socket.set_option(tcp::no_delay(true));
    std::cerr << "[tcp_main] connected to " << cfg.host << ":" << cfg.port << "\n";

    // per-session feed writer (append)
    mbo::JsonlWriter feed_writer;
    mbo::JsonlWriter* feed_ptr = nullptr;
    if (cfg.feed_enabled && !cfg.feed_path.empty()) {
        if (feed_writer.open(cfg.feed_path, /*append=*/true)) {
            feed_ptr = &feed_writer;
            std::cerr << "[feed] appending snapshots to: " << feed_writer.path() << "\n";
        } else {
            std::cerr << "[feed] disabled (open failed)\n";
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

                if (cfg.max_msgs < 0 || processed < cfg.max_msgs) {
                    handle_line(line, book, book_symbol, has_symbol,
                                apply_hist, snap_hist,
                                cfg.depth, cfg.snapshot_every,
                                processed, parsed_ok, lines_total,
                                last_ts_us,
                                pg, q_mtx, q_cv, q, max_q,
                                feed_ptr);
                } else {
                    lines_total++;
                }
            }
        }

        if (ec == boost::asio::error::eof) break;
    }

    // trailing partial line
    if (!carry.empty() && (cfg.max_msgs < 0 || processed < cfg.max_msgs)) {
        std::string tail = carry;
        carry.clear();
        handle_line(tail, book, book_symbol, has_symbol,
                    apply_hist, snap_hist,
                    cfg.depth, cfg.snapshot_every,
                    processed, parsed_ok, lines_total,
                    last_ts_us,
                    pg, q_mtx, q_cv, q, max_q,
                    feed_ptr);
    }

    // final flush if remainder exists (also measure snapshot latency once)
    if (processed > 0 && (cfg.snapshot_every <= 0 || (processed % cfg.snapshot_every != 0))) {
        auto t0s = SteadyClock::now();

        std::string json = book.to_json(cfg.depth);

        if (!book_symbol.empty()) publish_snapshot(book_symbol, json);
        else publish_snapshot(json);

        if (pg && !book_symbol.empty() && last_ts_us > 0) {
            TopOfBook tob = book.top_of_book();
            enqueue_snapshot_write(pg, q_mtx, q_cv, q, max_q, last_ts_us, book_symbol, tob);
        }

        if (feed_ptr && !book_symbol.empty() && last_ts_us > 0) {
            mbo::FeedLine fl;
            fl.ts_us = last_ts_us;
            fl.symbol = book_symbol;
            fl.processed = processed;
            fl.depth = cfg.depth;
            fl.book_json = json;
            feed_ptr->write_feed(fl);
        }

        auto t1s = SteadyClock::now();
        uint64_t snap_ns =
            (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t1s - t0s).count();
        snap_hist.add(snap_ns);

        std::cerr << "[final] forced snapshot flush (remainder)\n";
    }

    // final BBO
    std::cerr << book.to_pretty_bbo() << "\n";

    // ✅ NEW: dump full book json via file_output module
    {
        std::string full_json = book.to_json(1'000'000);
        mbo::write_final_books_json(full_json, book_symbol);
    }

    if (feed_ptr) {
        feed_ptr->flush();
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

    if (cfg.snapshot_every > 0) {
        std::cerr << "snapshot_latency_est_p50: " << ns_to_ms(snap_p50) << " ms\n";
        std::cerr << "snapshot_latency_est_p95: " << ns_to_ms(snap_p95) << " ms\n";
        std::cerr << "snapshot_latency_est_p99: " << ns_to_ms(snap_p99) << " ms\n";
    }

    // JSONL bench summary (one line per session)
    if (bench_writer && bench_writer->is_open()) {
        mbo::BenchLine bl;
        bl.ts_wall_us = now_wall_us();
        bl.host = cfg.host;
        bl.port = cfg.port;
        bl.depth = cfg.depth;
        bl.snapshot_every = cfg.snapshot_every;
        bl.feed_enabled = cfg.feed_enabled;
        bl.pg_enabled = (pg != nullptr);

        bl.processed = processed;
        bl.elapsed_s = secs;
        bl.throughput_msgs_per_s = mps;

        bl.apply_p50_us = ns_to_us(apply_p50);
        bl.apply_p95_us = ns_to_us(apply_p95);
        bl.apply_p99_us = ns_to_us(apply_p99);

        bl.snap_p50_ms = ns_to_ms(snap_p50);
        bl.snap_p95_ms = ns_to_ms(snap_p95);
        bl.snap_p99_ms = ns_to_ms(snap_p99);

        bench_writer->write_bench(bl);
        bench_writer->flush();
    }

    std::cerr << "[tcp_main] session done, back to waiting...\n";
}

int main(int argc, char** argv) {
    AppConfig cfg = parse_config(argc, argv);
    if (argc < 4) return 1;

    if (cfg.feed_enabled) {
        std::cerr << "[feed] enabled, path=" << cfg.feed_path << "\n";
    } else {
        std::cerr << "[feed] disabled (set FEED_ENABLED=1)\n";
    }

    // ---- Start WebSocket server ----
    boost::asio::io_context ws_ioc;
    try {
        start_ws_server(ws_ioc, cfg.ws_port, cfg.push_ms);
    } catch (const std::exception& e) {
        std::cerr << "[ws] failed to start: " << e.what() << "\n";
        return 1;
    }

    std::thread ws_thread([&]{
        std::cerr << "[ws] listening on port " << cfg.ws_port
                  << " (push every " << cfg.push_ms << " ms)\n";
        ws_ioc.run();
    });

    // ---- PG Writer init (optional) ----
    std::unique_ptr<PgWriter> pg;
    if (!cfg.pg_conninfo.empty()) {
        pg = std::make_unique<PgWriter>(cfg.pg_conninfo);
        std::cerr << "[pg] enabled\n";
    } else {
        std::cerr << "[pg] disabled (set PG_CONNINFO)\n";
    }

    // ---- Bench writer (append) ----
    mbo::JsonlWriter bench_writer;
    mbo::JsonlWriter* bench_ptr = nullptr;
    if (!cfg.bench_log_path.empty()) {
        if (bench_writer.open(cfg.bench_log_path, /*append=*/true)) {
            bench_ptr = &bench_writer;
            std::cerr << "[bench] logging to: " << bench_writer.path() << "\n";
        } else {
            std::cerr << "[bench] disabled (open failed)\n";
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
            std::cerr << "[tcp_main] waiting for feed " << cfg.host << ":" << cfg.port << " ...\n";
            run_one_replay_session(
                cfg,
                pg.get(),
                q_mtx, q_cv, q, max_q,
                bench_ptr
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
