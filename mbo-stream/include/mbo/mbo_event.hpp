#pragma once
#include <cstdint>
#include <string>


struct MboEvent {
    std::string ts_recv;
    std::string ts_event;
    int32_t publisher_id = 0;
    int32_t instrument_id = 0;
    char action = 'N';
    char side = 'N';
    int64_t price = 0;
    int32_t size = 0;
    int64_t order_id = 0;
    uint32_t flags = 0;
    std::string symbol;
};
