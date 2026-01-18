#pragma once
#include "mbo/mbo_event.hpp"
#include "mbo/order_types.hpp"
#include "mbo/topofbook.hpp"

#include <string>
#include <unordered_map>
#include <map>
#include <list>

class MboOrderBook {
public:
    explicit MboOrderBook(std::string sym = "");
    void apply(const MboEvent& e);
    std::string to_json(int depth = 5, double price_scale = 10000.0) const;
    std::string to_json_bbo(double price_scale = 10000.0) const;
    std::string to_pretty_bbo(double price_scale = 10000.0) const;

    TopOfBook top_of_book(double price_scale = 10000.0) const;


private:
    void clear_();
    void add_(const MboEvent& e);
    void cancel_(const MboEvent& e);
    void modify_(const MboEvent& e);

    std::string symbol_;
    std::map<int64_t, std::list<Order>, std::greater<int64_t>> bids_;
    std::map<int64_t, std::list<Order>, std::less<int64_t>> asks_;
    std::unordered_map<int64_t, OrderRef> index_;
};
