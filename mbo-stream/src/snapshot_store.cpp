#include "mbo/snapshot_store.hpp"

#include <shared_mutex>
#include <mutex>
#include <unordered_map>
#include <string>
#include <memory>

// Thread-safe store: symbol -> latest snapshot string
static std::shared_mutex g_mtx;
static std::unordered_map<std::string, std::shared_ptr<const std::string>> g_latest_by_symbol;

// Fallback "global" snapshot (backward compatible)
static std::shared_ptr<const std::string> g_latest_global =
    std::make_shared<const std::string>(std::string{"{}"});

// ----------------------- Publish APIs -----------------------

// Backward compatible: publish global snapshot
void publish_snapshot(std::string s) {
    auto p = std::make_shared<const std::string>(std::move(s));
    std::unique_lock lock(g_mtx);
    g_latest_global = std::move(p);
}

// New: publish per-symbol snapshot
void publish_snapshot(const std::string& symbol, std::string s) {
    auto p = std::make_shared<const std::string>(std::move(s));
    std::unique_lock lock(g_mtx);
    g_latest_by_symbol[symbol] = std::move(p);
}

// ----------------------- Load APIs -----------------------

// Backward compatible: load global snapshot
std::shared_ptr<const std::string> load_snapshot() {
    std::shared_lock lock(g_mtx);
    return g_latest_global;
}

// New: load per-symbol snapshot; if missing, fall back to global or "{}"
std::shared_ptr<const std::string> load_snapshot(const std::string& symbol) {
    std::shared_lock lock(g_mtx);

    auto it = g_latest_by_symbol.find(symbol);
    if (it != g_latest_by_symbol.end()) {
        return it->second;
    }

    // fallback: global (so old behavior still works)
    return g_latest_global;
}
