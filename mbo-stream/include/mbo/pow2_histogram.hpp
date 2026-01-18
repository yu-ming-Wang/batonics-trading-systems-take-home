#pragma once
#include <cstdint>

struct Pow2Histogram {
    static constexpr int K = 64;
    uint64_t c[K]{};
    uint64_t n = 0;

    static int bucket(uint64_t ns) {
        if (ns == 0) return 0;
#if defined(__GNUG__) || defined(__clang__)
        int b = 63 - __builtin_clzll(ns);
#else
        int b = 0;
        while ((1ull << (b + 1)) <= ns && b < 63) ++b;
#endif
        if (b < 0) b = 0;
        if (b > 63) b = 63;
        return b;
    }

    void add(uint64_t ns) { c[bucket(ns)]++; n++; }

    uint64_t percentile(double p) const {
        if (n == 0) return 0;
        if (p < 0) p = 0;
        if (p > 1) p = 1;

        uint64_t target = static_cast<uint64_t>(p * (double)n);
        if (target == 0) target = 1;

        uint64_t cum = 0;
        for (int b = 0; b < K; ++b) {
            cum += c[b];
            if (cum >= target) {
                if (b >= 63) return (1ull << 63);
                return (1ull << (b + 1));
            }
        }
        return (1ull << 63);
    }
};
