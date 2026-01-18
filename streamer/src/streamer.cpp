#include <boost/asio.hpp>
#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <thread>
#include <vector>

using boost::asio::ip::tcp;
using SteadyClock = std::chrono::steady_clock;

int main(int argc, char* argv[]) {
    // 1. Parameter check
    if (argc < 5) {
        std::cerr
            << "Usage: streamer <csv_path> <port> <rate_msgs_per_sec> <loop:0|1> [max_msgs]\n"
            << "Example: streamer CLX5_mbo.csv 9000 500000 1\n";
        return 1;
    }

    const std::string csv_path = argv[1];
    const int port = std::stoi(argv[2]);
    const int rate = std::stoi(argv[3]);
    const bool loop = std::stoi(argv[4]) != 0;
    const long long max_msgs = (argc >= 6) ? std::stoll(argv[5]) : -1;

    // 2. Open file
    std::ifstream fin(csv_path);
    if (!fin) {
        std::cerr << "[streamer] Failed to open: " << csv_path << "\n";
        return 1;
    }

    // 3. Skip header
    std::string header;
    if (!std::getline(fin, header)) {
        std::cerr << "[streamer] Empty CSV\n";
        return 1;
    }

    // 4. Start TCP server and wait for a client connection
    boost::asio::io_context io;
    tcp::acceptor acceptor(io, tcp::endpoint(tcp::v4(), port));

    std::cout << "[streamer] Listening on port " << port << "...\n";
    tcp::socket sock(io);
    acceptor.accept(sock);

    // Enable TCP_NODELAY (disable Nagle) for lower-latency replay
    sock.set_option(tcp::no_delay(true));
    std::cout << "[streamer] Client connected.\n";

    // Pre-allocate send buffer (8MB)
    std::string out;
    out.reserve(8 * 1024 * 1024);

    std::string line;
    long long sent_total = 0;
    auto last_log = SteadyClock::now();

    // 5. Main loop
    try {
        while (true) {
            auto sec_start = SteadyClock::now();
            out.clear();

            int sent_this_sec = 0;

            // Best effort: fill the buffer with up to `rate` messages within one second
            while (sent_this_sec < rate) {
                if (max_msgs >= 0 && sent_total >= max_msgs) goto done;

                if (!std::getline(fin, line)) {
                    if (!loop) {
                        std::cout << "[streamer] EOF reached.\n";
                        goto done;
                    }
                    // Replay mode: rewind file and skip header again
                    fin.clear();
                    fin.seekg(0);
                    std::getline(fin, header);
                    if (!std::getline(fin, line)) {
                        std::cerr << "[streamer] Replay failed (empty after rewind)\n";
                        goto done;
                    }
                }

                // Append to buffer
                out.append(line);
                out.push_back('\n');

                ++sent_this_sec;
                ++sent_total;

                // If the buffer grows too large (6MB), flush early to avoid excessive memory usage
                if (out.size() >= 6 * 1024 * 1024) {
                    boost::asio::write(sock, boost::asio::buffer(out));
                    out.clear();
                }
            }

            // End of the 1-second window: send any remaining data
            if (!out.empty()) {
                boost::asio::write(sock, boost::asio::buffer(out));
            }

            if (max_msgs >= 0 && sent_total >= max_msgs) goto done;

            // Rate control: if we sent too fast, sleep to complete the 1-second cycle
            auto sec_end = SteadyClock::now();
            auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(sec_end - sec_start).count();

            if (elapsed_ms < 1000) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000 - elapsed_ms));
            }

            // Log progress
            auto now = SteadyClock::now();
            if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_log).count() >= 1000) {
                std::cout << "[streamer] sent_total=" << sent_total
                          << " (target " << rate << " msg/s)\n";
                last_log = now;
            }
        }
    } catch (std::exception& e) {
        std::cerr << "[streamer] Exception: " << e.what() << "\n";
    }

done:
    // ==========================================
    // Key fix: graceful shutdown
    // ==========================================

    // 1) Make sure any remaining data in the string buffer is flushed
    if (!out.empty()) {
        try {
            boost::asio::write(sock, boost::asio::buffer(out));
        } catch (...) {
            std::cerr << "[streamer] Failed to flush final buffer (client disconnected?)\n";
        }
    }

    std::cout << "[streamer] All messages sent. Total=" << sent_total << "\n";
    std::cout << "[streamer] Shutting down socket...\n";

    boost::system::error_code ec;

    // 2) Send FIN: tell the client "no more data will be sent"
    // This lets the client observe EOF instead of a connection reset.
    sock.shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);
    if (ec) {
        std::cerr << "[streamer] Shutdown error: " << ec.message() << "\n";
    }

    // 3) Linger delay:
    // Give the OS kernel a few seconds to actually drain the TCP send buffer.
    // Under high throughput, the kernel buffer may still have MBs queued.
    std::cout << "[streamer] Waiting 3s for buffer drain...\n";
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // 4) Close socket
    sock.close(ec);

    std::cout << "[streamer] Exiting.\n";
    return 0;
}
