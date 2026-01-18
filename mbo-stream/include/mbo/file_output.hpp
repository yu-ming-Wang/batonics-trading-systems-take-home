#pragma once
#include <filesystem>
#include <string>

namespace mbo {

// Best-effort: search upwards for a repo root that contains "frontend/" directory.
std::filesystem::path guess_repo_root();

// Write final full depth book json into frontend/public:
//   - final_book.json
//   - final_book_<symbol>.json (if symbol non-empty)
void write_final_books_json(
    const std::string& book_json,
    const std::string& symbol,
    int depth_full
);

// Convenience: writes from already-built json. (Overload without depth_full if you want)
void write_final_books_json(
    const std::string& book_json,
    const std::string& symbol
);

} // namespace mbo
