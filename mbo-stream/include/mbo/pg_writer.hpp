#pragma once
#include <string>
#include <cstdint>
#include "mbo/topofbook.hpp"

/**
 * Thin PostgreSQL writer for snapshots table
 * - owns DB connection
 * - provides idempotent insert
 */
class PgWriter {
public:
    // example conninfo:
    // "host=127.0.0.1 port=5432 dbname=batonic user=postgres password=postgres"
    explicit PgWriter(const std::string& conninfo);
    ~PgWriter();

    // non-copyable (DB connection ownership)
    PgWriter(const PgWriter&) = delete;
    PgWriter& operator=(const PgWriter&) = delete;

    // Write one snapshot (idempotent on symbol + ts)
    bool write_snapshot(
        int64_t ts_us,              // UNIX epoch microseconds
        const std::string& symbol,
        const TopOfBook& tob
    );

private:
    struct Impl;   // 👉 PIMPL：把 libpq 藏起來
    Impl* impl_;
};
