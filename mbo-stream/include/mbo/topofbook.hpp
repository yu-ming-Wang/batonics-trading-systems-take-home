#pragma once
#include <string>
#include <cstdint>

/**
 * Top-of-book snapshot (from reconstructed order book)
 * Prices are in DOUBLE (already divided by scale)
 */
struct TopOfBook {
    bool has_bid = false;
    bool has_ask = false;

    double bid_px = 0.0;
    int64_t bid_sz = 0;

    double ask_px = 0.0;
    int64_t ask_sz = 0;

    double mid = 0.0;
    double spread = 0.0;
};