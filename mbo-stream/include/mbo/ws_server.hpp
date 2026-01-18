#pragma once
#include <boost/asio.hpp>

// Start a WebSocket server on given port.
// push_ms: how often to push latest snapshot (e.g., 50ms)
void start_ws_server(boost::asio::io_context& ioc, int port, int push_ms);
