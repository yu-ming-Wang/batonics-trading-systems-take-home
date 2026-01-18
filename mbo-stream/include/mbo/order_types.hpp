#pragma once
#include <cstdint>
#include <list>

// A resting order inside the book (state)
struct Order {
    int64_t order_id;
    int64_t price;   // fixed-point integer
    int32_t qty;
};

// Reference to an order's exact position inside the book
// Used for O(1) cancel / modify.
struct OrderRef {
    bool is_buy;     // true = bid, false = ask
    int64_t price;   // price level where the order resides
    std::list<Order>::iterator it;
};
