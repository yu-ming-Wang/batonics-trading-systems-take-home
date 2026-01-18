#include "mbo/mbo_order_book.hpp"

#include <sstream>
#include <iomanip>
#include <algorithm>

MboOrderBook::MboOrderBook(std::string sym)
    : symbol_(std::move(sym)) {}

static inline bool is_buy_side(char side) {
    return side == 'B';
}

void MboOrderBook::apply(const MboEvent& e) {
    // Trade/Fill/None: typically no change to resting book state
    if (e.action == 'T' || e.action == 'F' || e.action == 'N') return;

    // Clear book
    if (e.action == 'R') {
        clear_();
        return;
    }

    // For A/C/M, we expect side to be 'A' or 'B'
    if (e.side != 'A' && e.side != 'B') return;

    switch (e.action) {
        case 'A': add_(e); break;
        case 'C': cancel_(e); break;
        case 'M': modify_(e); break;
        default:
            break; // ignore unknown
    }
}

void MboOrderBook::clear_() {
    bids_.clear();
    asks_.clear();
    index_.clear();
}

void MboOrderBook::add_(const MboEvent& e) {
    const bool is_buy = is_buy_side(e.side);

    // If duplicate order_id appears, remove old one first (defensive)
    auto existing = index_.find(e.order_id);
    if (existing != index_.end()) {
        auto& oldRef = existing->second;

        if (oldRef.is_buy) {
            auto lvlIt = bids_.find(oldRef.price);
            if (lvlIt != bids_.end()) {
                lvlIt->second.erase(oldRef.it);
                if (lvlIt->second.empty()) bids_.erase(lvlIt);
            }
        } else {
            auto lvlIt = asks_.find(oldRef.price);
            if (lvlIt != asks_.end()) {
                lvlIt->second.erase(oldRef.it);
                if (lvlIt->second.empty()) asks_.erase(lvlIt);
            }
        }

        index_.erase(existing);
    }

    // Insert at end of FIFO queue for this price level
    if (is_buy) {
        auto& q = bids_[e.price];
        q.push_back(Order{e.order_id, e.price, e.size});
        auto it = std::prev(q.end());
        index_.emplace(e.order_id, OrderRef{true, e.price, it});
    } else {
        auto& q = asks_[e.price];
        q.push_back(Order{e.order_id, e.price, e.size});
        auto it = std::prev(q.end());
        index_.emplace(e.order_id, OrderRef{false, e.price, it});
    }
}

void MboOrderBook::cancel_(const MboEvent& e) {
    auto itRef = index_.find(e.order_id);
    if (itRef == index_.end()) return; // unknown order_id

    auto& ref = itRef->second;

    if (ref.is_buy) {
        auto lvlIt = bids_.find(ref.price);
        if (lvlIt == bids_.end()) { index_.erase(itRef); return; } // inconsistent

        // Partial cancel
        if (e.size >= ref.it->qty) ref.it->qty = 0;
        else ref.it->qty -= e.size;

        // Remove if fully cancelled
        if (ref.it->qty == 0) {
            lvlIt->second.erase(ref.it);
            index_.erase(itRef);
            if (lvlIt->second.empty()) bids_.erase(lvlIt);
        }
    } else {
        auto lvlIt = asks_.find(ref.price);
        if (lvlIt == asks_.end()) { index_.erase(itRef); return; } // inconsistent

        if (e.size >= ref.it->qty) ref.it->qty = 0;
        else ref.it->qty -= e.size;

        if (ref.it->qty == 0) {
            lvlIt->second.erase(ref.it);
            index_.erase(itRef);
            if (lvlIt->second.empty()) asks_.erase(lvlIt);
        }
    }
}

void MboOrderBook::modify_(const MboEvent& e) {
    auto itRef = index_.find(e.order_id);
    if (itRef == index_.end()) {
        // If order not found, treat as an add (matches Databento example)
        add_(e);
        return;
    }

    auto& ref = itRef->second;

    // Defensive: side mismatch -> ignore (or assert)
    if (is_buy_side(e.side) != ref.is_buy) return;

    const int64_t old_px = ref.price;
    const int32_t old_qty = ref.it->qty;

    if (ref.is_buy) {
        // Price change => lose priority, move to new level tail
        if (e.price != old_px) {
            auto oldLvlIt = bids_.find(old_px);
            if (oldLvlIt != bids_.end()) {
                oldLvlIt->second.erase(ref.it);
                if (oldLvlIt->second.empty()) bids_.erase(oldLvlIt);
            }

            auto& newQ = bids_[e.price];
            newQ.push_back(Order{e.order_id, e.price, e.size});
            ref.price = e.price;
            ref.it = std::prev(newQ.end());
            return;
        }

        // Same price:
        // Increasing size => lose priority, move to tail
        if (e.size > old_qty) {
            auto lvlIt = bids_.find(old_px);
            if (lvlIt == bids_.end()) return;

            lvlIt->second.erase(ref.it);
            lvlIt->second.push_back(Order{e.order_id, old_px, e.size});
            ref.it = std::prev(lvlIt->second.end());
            return;
        }

        // Decrease or same => keep priority, update in place
        ref.it->qty = e.size;
    } else {
        if (e.price != old_px) {
            auto oldLvlIt = asks_.find(old_px);
            if (oldLvlIt != asks_.end()) {
                oldLvlIt->second.erase(ref.it);
                if (oldLvlIt->second.empty()) asks_.erase(oldLvlIt);
            }

            auto& newQ = asks_[e.price];
            newQ.push_back(Order{e.order_id, e.price, e.size});
            ref.price = e.price;
            ref.it = std::prev(newQ.end());
            return;
        }

        if (e.size > old_qty) {
            auto lvlIt = asks_.find(old_px);
            if (lvlIt == asks_.end()) return;

            lvlIt->second.erase(ref.it);
            lvlIt->second.push_back(Order{e.order_id, old_px, e.size});
            ref.it = std::prev(lvlIt->second.end());
            return;
        }

        ref.it->qty = e.size;
    }
}

std::string MboOrderBook::to_json(int depth, double price_scale) const {
    std::ostringstream oss;

    oss << "{";
    if (!symbol_.empty()) {
        oss << "\"symbol\":\"" << symbol_ << "\",";
    }

    // bids
    oss << "\"bids\":[";
    {
        int printed = 0;
        bool first = true;
        for (auto it = bids_.begin(); it != bids_.end() && printed < depth; ++it, ++printed) {
            const int64_t px = it->first;
            int64_t sum_qty = 0;
            int64_t ct = 0;
            for (const auto& o : it->second) { sum_qty += o.qty; ++ct; }

            if (!first) oss << ",";
            first = false;

            oss << "{"
                << "\"px\":" << px << ","
                << "\"px_f\":" << std::fixed << std::setprecision(4) << (px / price_scale) << ","
                << "\"sz\":" << sum_qty << ","
                << "\"ct\":" << ct
                << "}";
            oss.unsetf(std::ios::floatfield);
        }
    }
    oss << "],";

    // asks
    oss << "\"asks\":[";
    {
        int printed = 0;
        bool first = true;
        for (auto it = asks_.begin(); it != asks_.end() && printed < depth; ++it, ++printed) {
            const int64_t px = it->first;
            int64_t sum_qty = 0;
            int64_t ct = 0;
            for (const auto& o : it->second) { sum_qty += o.qty; ++ct; }

            if (!first) oss << ",";
            first = false;

            oss << "{"
                << "\"px\":" << px << ","
                << "\"px_f\":" << std::fixed << std::setprecision(4) << (px / price_scale) << ","
                << "\"sz\":" << sum_qty << ","
                << "\"ct\":" << ct
                << "}";
            oss.unsetf(std::ios::floatfield);
        }
    }
    oss << "]";

    oss << "}";
    return oss.str();
}

std::string MboOrderBook::to_json_bbo(double price_scale) const {
    std::ostringstream oss;
    oss << "{";
    if (!symbol_.empty()) oss << "\"symbol\":\"" << symbol_ << "\",";

    // best bid
    if (!bids_.empty()) {
        auto it = bids_.begin(); // best bid (desc)
        const int64_t px = it->first;
        int64_t sum_qty = 0, ct = 0;
        for (const auto& o : it->second) { sum_qty += o.qty; ++ct; }

        oss << "\"bid\":{"
            << "\"px\":" << px << ","
            << "\"px_f\":" << std::fixed << std::setprecision(4) << (px / price_scale) << ","
            << "\"sz\":" << sum_qty << ","
            << "\"ct\":" << ct
            << "},";
        oss.unsetf(std::ios::floatfield);
    } else {
        oss << "\"bid\":null,";
    }

    // best ask
    if (!asks_.empty()) {
        auto it = asks_.begin(); // best ask (asc)
        const int64_t px = it->first;
        int64_t sum_qty = 0, ct = 0;
        for (const auto& o : it->second) { sum_qty += o.qty; ++ct; }

        oss << "\"ask\":{"
            << "\"px\":" << px << ","
            << "\"px_f\":" << std::fixed << std::setprecision(4) << (px / price_scale) << ","
            << "\"sz\":" << sum_qty << ","
            << "\"ct\":" << ct
            << "}";
        oss.unsetf(std::ios::floatfield);
    } else {
        oss << "\"ask\":null";
    }

    oss << "}";
    return oss.str();
}

std::string MboOrderBook::to_pretty_bbo(double price_scale) const {
    std::ostringstream oss;
    oss << symbol_ << " Aggregated BBO\n";

    // ask first (上面 ask, 下面 bid)
    if (!asks_.empty()) {
        auto it = asks_.begin();
        const int64_t px = it->first;
        int64_t sum_qty = 0, ct = 0;
        for (const auto& o : it->second) { sum_qty += o.qty; ++ct; }

        oss << "     " << sum_qty << " @ " << std::fixed << std::setprecision(2)
            << (px / price_scale) << " |  " << ct << " order(s)\n";
        oss.unsetf(std::ios::floatfield);
    } else {
        oss << "     None\n";
    }

    if (!bids_.empty()) {
        auto it = bids_.begin();
        const int64_t px = it->first;
        int64_t sum_qty = 0, ct = 0;
        for (const auto& o : it->second) { sum_qty += o.qty; ++ct; }

        oss << "     " << sum_qty << " @ " << std::fixed << std::setprecision(2)
            << (px / price_scale) << " |  " << ct << " order(s)\n";
        oss.unsetf(std::ios::floatfield);
    } else {
        oss << "     None\n";
    }

    return oss.str();
}

TopOfBook MboOrderBook::top_of_book(double price_scale) const {
    TopOfBook t;

    // Best bid (highest price)
    if (!bids_.empty()) {
        auto it = bids_.begin();
        t.has_bid = true;
        t.bid_px = static_cast<double>(it->first) / price_scale;

        int64_t sz = 0;
        for (const auto& ord : it->second) {
            sz += ord.qty;
        }
        t.bid_sz = sz;
    }

    // Best ask (lowest price)
    if (!asks_.empty()) {
        auto it = asks_.begin();
        t.has_ask = true;
        t.ask_px = static_cast<double>(it->first) / price_scale;

        int64_t sz = 0;
        for (const auto& ord : it->second) {
            sz += ord.qty;
        }
        t.ask_sz = sz;
    }

    // Mid / spread
    if (t.has_bid && t.has_ask) {
        t.mid = 0.5 * (t.bid_px + t.ask_px);
        t.spread = t.ask_px - t.bid_px;
    }

    return t;
}