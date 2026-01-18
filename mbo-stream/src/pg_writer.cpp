#include "mbo/pg_writer.hpp"
#include <postgresql/libpq-fe.h>
#include <iostream>
#include <sstream>

struct PgWriter::Impl {
    PGconn* conn = nullptr;
    PGresult* prep = nullptr;
};

PgWriter::PgWriter(const std::string& conninfo) : impl_(new Impl) {
    impl_->conn = PQconnectdb(conninfo.c_str());

    if (PQstatus(impl_->conn) != CONNECTION_OK) {
        std::cerr << "[pg] connection failed: "
                  << PQerrorMessage(impl_->conn) << "\n";
        PQfinish(impl_->conn);
        impl_->conn = nullptr;
        return;
    }

    // Prepare idempotent insert
    const char* sql =
        "INSERT INTO snapshots "
        "(ts, symbol, best_bid_px, best_bid_sz, best_ask_px, best_ask_sz, mid, spread) "
        "VALUES (to_timestamp($1 / 1e6), $2, $3, $4, $5, $6, $7, $8) "
        "ON CONFLICT (symbol, ts) DO NOTHING";

    impl_->prep = PQprepare(
        impl_->conn,
        "insert_snapshot",
        sql,
        8,
        nullptr
    );

    if (PQresultStatus(impl_->prep) != PGRES_COMMAND_OK) {
        std::cerr << "[pg] prepare failed: "
                  << PQerrorMessage(impl_->conn) << "\n";
    }
}

PgWriter::~PgWriter() {
    if (impl_) {
        if (impl_->prep) PQclear(impl_->prep);
        if (impl_->conn) PQfinish(impl_->conn);
        delete impl_;
    }
}

bool PgWriter::write_snapshot(
    int64_t ts_us,
    const std::string& symbol,
    const TopOfBook& tob
) {
    if (!impl_ || !impl_->conn) return false;

    std::string ts = std::to_string(ts_us);
    std::string bid_px = tob.has_bid ? std::to_string(tob.bid_px) : "";
    std::string bid_sz = tob.has_bid ? std::to_string(tob.bid_sz) : "";
    std::string ask_px = tob.has_ask ? std::to_string(tob.ask_px) : "";
    std::string ask_sz = tob.has_ask ? std::to_string(tob.ask_sz) : "";
    std::string mid    = std::to_string(tob.mid);
    std::string spread = std::to_string(tob.spread);

    const char* values[] = {
        ts.c_str(),
        symbol.c_str(),
        bid_px.empty() ? nullptr : bid_px.c_str(),
        bid_sz.empty() ? nullptr : bid_sz.c_str(),
        ask_px.empty() ? nullptr : ask_px.c_str(),
        ask_sz.empty() ? nullptr : ask_sz.c_str(),
        mid.c_str(),
        spread.c_str()
    };

    PGresult* res = PQexecPrepared(
        impl_->conn,
        "insert_snapshot",
        8,
        values,
        nullptr,
        nullptr,
        0
    );

    bool ok = (PQresultStatus(res) == PGRES_COMMAND_OK);
    if (!ok) {
        std::cerr << "[pg] insert failed: "
                  << PQerrorMessage(impl_->conn) << "\n";
    }

    PQclear(res);
    return ok;
}
