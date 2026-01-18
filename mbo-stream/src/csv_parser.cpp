#include "mbo/csv_parser.hpp"

#include <charconv>
#include <string_view>
#include <vector>
#include <cmath>   // llround

// Header:
// ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,price,size,channel_id,order_id,flags,ts_in_delta,sequence,symbol

static inline bool split_csv_simple(std::string_view s, std::vector<std::string_view>& out) {
    out.clear();
    out.reserve(16);

    size_t start = 0;
    for (size_t i = 0; i <= s.size(); ++i) {
        if (i == s.size() || s[i] == ',') {
            out.emplace_back(s.data() + start, i - start);
            start = i + 1;
        }
    }
    return true;
}

template <typename T>
static inline bool parse_int(std::string_view sv, T& out) {
    if (sv.empty()) return false;
    const char* begin = sv.data();
    const char* end = sv.data() + sv.size();
    auto res = std::from_chars(begin, end, out);
    return (res.ec == std::errc{});
}

// ✅ parse a floating number like "64.83"
static inline bool parse_double(std::string_view sv, double& out) {
    if (sv.empty()) return false;
    // easiest + good enough for MVP; if you want max perf later, write a custom decimal parser
    try {
        out = std::stod(std::string(sv));
        return true;
    } catch (...) {
        return false;
    }
}

bool parse_mbo_csv_line(const std::string& line, MboEvent& out) {
    std::string_view s(line);
    if (!s.empty() && s.back() == '\r') s.remove_suffix(1);
    if (s.empty()) return false;
    if (s.rfind("ts_recv,", 0) == 0) return false;

    static thread_local std::vector<std::string_view> f;
    split_csv_simple(s, f);

    if (f.size() < 15) return false;

    out.ts_recv  = std::string(f[0]);
    out.ts_event = std::string(f[1]);
    out.symbol   = std::string(f[14]);

    // ✅ IMPORTANT: check parse results; fail line if critical fields missing
    if (!parse_int<int32_t>(f[3], out.publisher_id)) return false;
    if (!parse_int<int32_t>(f[4], out.instrument_id)) return false;

    // ✅ price: CSV has dollars like 64.83 -> convert to ticks for internal book
    double px_d = 0.0;
    if (!parse_double(f[7], px_d)) return false;

    // tick size = 1e-4 => scale 10000
    constexpr double PRICE_SCALE = 10000.0;
    out.price = static_cast<int64_t>(llround(px_d * PRICE_SCALE));

    if (!parse_int<int32_t>(f[8], out.size)) return false;
    if (!parse_int<int64_t>(f[10], out.order_id)) return false;
    if (!parse_int<uint32_t>(f[11], out.flags)) return false;

    out.action = (!f[5].empty()) ? f[5][0] : 'N';
    out.side   = (!f[6].empty()) ? f[6][0] : 'N';

    return true;
}
