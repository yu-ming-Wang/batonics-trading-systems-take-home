#include "mbo/ws_server.hpp"
#include "mbo/snapshot_store.hpp"

#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <cctype>

using boost::asio::ip::tcp;
namespace beast = boost::beast;
namespace websocket = beast::websocket;

class WsSession : public std::enable_shared_from_this<WsSession> {
public:
    WsSession(tcp::socket socket, boost::asio::io_context& ioc, int default_push_ms)
        : ws_(std::move(socket))
        , timer_(ioc)
        , push_ms_(default_push_ms) {}

    void run() {
        ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::server));
        ws_.set_option(websocket::stream_base::decorator(
            [](websocket::response_type& res) {
                res.set(beast::http::field::server, std::string("tcp_main_ws"));
            }
        ));

        ws_.async_accept(
            beast::bind_front_handler(&WsSession::on_accept, shared_from_this())
        );
    }

private:
    websocket::stream<beast::tcp_stream> ws_;
    boost::asio::steady_timer timer_;

    // ---- Control plane (per-session config) ----
    std::string symbol_ = "CLX5";
    int depth_ = 10;
    int push_ms_;

    // ---- Data plane bookkeeping ----
    beast::flat_buffer read_buf_;
    std::shared_ptr<const std::string> last_sent_;
    bool write_in_flight_ = false;

    // ---------------- Minimal JSON-lite parsing ----------------
    // We only need: type (string), symbol (string), depth (int), push_ms (int)
    // Example payloads:
    // {"type":"subscribe","symbol":"CLX5","depth":10,"push_ms":50}
    // {"type":"update","depth":20}
    static void skip_ws(const std::string& s, size_t& i) {
        while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
    }

    static bool parse_string_value_after_key(const std::string& s, const std::string& key, std::string& out) {
        // find key occurrence (very lightweight)
        auto kpos = s.find("\"" + key + "\"");
        if (kpos == std::string::npos) return false;

        auto cpos = s.find(':', kpos);
        if (cpos == std::string::npos) return false;

        size_t i = cpos + 1;
        skip_ws(s, i);

        if (i >= s.size() || s[i] != '"') return false;
        ++i;

        // capture until next unescaped quote (we'll ignore escape handling for MVP)
        auto end = s.find('"', i);
        if (end == std::string::npos) return false;

        out = s.substr(i, end - i);
        return true;
    }

    static bool parse_int_value_after_key(const std::string& s, const std::string& key, int& out) {
        auto kpos = s.find("\"" + key + "\"");
        if (kpos == std::string::npos) return false;

        auto cpos = s.find(':', kpos);
        if (cpos == std::string::npos) return false;

        size_t i = cpos + 1;
        skip_ws(s, i);

        // parse optional sign + digits
        bool neg = false;
        if (i < s.size() && s[i] == '-') { neg = true; ++i; }

        if (i >= s.size() || !std::isdigit(static_cast<unsigned char>(s[i]))) return false;

        long long val = 0;
        while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) {
            val = val * 10 + (s[i] - '0');
            ++i;
            if (val > 1000000000LL) break;
        }

        if (neg) val = -val;
        out = static_cast<int>(val);
        return true;
    }

    bool parse_control_message(const std::string& msg, std::string& type_out) {
        type_out.clear();

        // type is required for control messages; if missing, ignore.
        if (!parse_string_value_after_key(msg, "type", type_out)) return false;

        if (type_out != "subscribe" && type_out != "update") {
            // ignore unknown message types in MVP
            return false;
        }

        // optional fields
        std::string sym;
        if (parse_string_value_after_key(msg, "symbol", sym)) {
            if (!sym.empty()) symbol_ = sym;
        }

        int d = 0;
        if (parse_int_value_after_key(msg, "depth", d)) {
            if (d > 0 && d <= 200) depth_ = d;
        }

        int pm = 0;
        if (parse_int_value_after_key(msg, "push_ms", pm)) {
            if (pm < 10) pm = 10;
            if (pm > 5000) pm = 5000;
            push_ms_ = pm;
        }

        return true;
    }

    static std::string make_ack_json(const std::string& symbol, int depth, int push_ms) {
        // Simple JSON build (symbol assumed safe, e.g. "CLX5")
        return std::string("{\"type\":\"ack\",\"symbol\":\"") + symbol +
               "\",\"depth\":" + std::to_string(depth) +
               ",\"push_ms\":" + std::to_string(push_ms) + "}";
    }

    // ---------------- WebSocket lifecycle ----------------
    void on_accept(beast::error_code ec) {
        if (ec) return;

        // Start reading control messages (subscribe/update)
        do_read();

        // Start data pushing loop
        schedule_send_now();
    }

    // ---------------- Control plane: read & parse ----------------
    void do_read() {
        ws_.async_read(
            read_buf_,
            beast::bind_front_handler(&WsSession::on_read, shared_from_this())
        );
    }

    void on_read(beast::error_code ec, std::size_t bytes) {
        (void)bytes;

        if (ec == websocket::error::closed) {
            return; // client disconnected
        }
        if (ec) {
            return; // ignore for MVP
        }

        std::string msg = beast::buffers_to_string(read_buf_.data());
        read_buf_.consume(read_buf_.size());

        std::string type;
        if (parse_control_message(msg, type)) {
            // Optional debug:
            // std::cerr << "[WS] " << type << " symbol=" << symbol_
            //           << " depth=" << depth_ << " push_ms=" << push_ms_ << "\n";

            // Send ack (fire-and-forget; does not block snapshot loop)
            auto ack_str = std::make_shared<std::string>(make_ack_json(symbol_, depth_, push_ms_));
            ws_.text(true);
            ws_.async_write(
                boost::asio::buffer(*ack_str),
                [self = shared_from_this(), ack_str](beast::error_code, std::size_t) {
                    // ignore errors for MVP
                }
            );
        }

        // keep reading
        do_read();
    }

    // ---------------- Data plane: push snapshots ----------------
    void schedule_send_now() {
        timer_.expires_after(std::chrono::milliseconds(0));
        timer_.async_wait(beast::bind_front_handler(&WsSession::on_tick, shared_from_this()));
    }

    void schedule_next() {
        timer_.expires_after(std::chrono::milliseconds(push_ms_));
        timer_.async_wait(beast::bind_front_handler(&WsSession::on_tick, shared_from_this()));
    }

    void on_tick(beast::error_code ec) {
        if (ec) return;

        // Backpressure: if last async_write not finished, skip this tick
        if (write_in_flight_) {
            schedule_next();
            return;
        }

        // Now supports per-symbol snapshots:
        auto cur = load_snapshot(symbol_);
        if (!cur) {
            schedule_next();
            return;
        }

        // Skip duplicates (pointer equality works because publisher swaps shared_ptr)
        if (last_sent_ && cur == last_sent_) {
            schedule_next();
            return;
        }

        last_sent_ = cur;
        write_in_flight_ = true;

        ws_.text(true);
        ws_.async_write(
            boost::asio::buffer(*cur),
            beast::bind_front_handler(&WsSession::on_write, shared_from_this())
        );
    }

    void on_write(beast::error_code ec, std::size_t) {
        write_in_flight_ = false;
        if (ec) return;
        schedule_next();
    }
};

class WsListener : public std::enable_shared_from_this<WsListener> {
public:
    WsListener(boost::asio::io_context& ioc, tcp::endpoint ep, int push_ms)
        : ioc_(ioc), acceptor_(ioc), push_ms_(push_ms) {
        beast::error_code ec;

        acceptor_.open(ep.protocol(), ec);
        if (ec) throw std::runtime_error("acceptor.open: " + ec.message());

        acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
        if (ec) throw std::runtime_error("acceptor.set_option: " + ec.message());

        acceptor_.bind(ep, ec);
        if (ec) throw std::runtime_error("acceptor.bind: " + ec.message());

        acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
        if (ec) throw std::runtime_error("acceptor.listen: " + ec.message());
    }

    void run() { do_accept(); }

private:
    boost::asio::io_context& ioc_;
    tcp::acceptor acceptor_;
    int push_ms_;

    void do_accept() {
        acceptor_.async_accept(
            boost::asio::make_strand(ioc_),
            beast::bind_front_handler(&WsListener::on_accept, shared_from_this())
        );
    }

    void on_accept(beast::error_code ec, tcp::socket socket) {
        if (!ec) {
            std::make_shared<WsSession>(std::move(socket), ioc_, push_ms_)->run();
        }
        do_accept();
    }
};

void start_ws_server(boost::asio::io_context& ioc, int port, int push_ms) {
    auto listener = std::make_shared<WsListener>(
        ioc, tcp::endpoint(tcp::v4(), static_cast<unsigned short>(port)), push_ms
    );
    listener->run();
}
