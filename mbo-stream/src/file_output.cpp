#include "mbo/file_output.hpp"
#include <fstream>
#include <iostream>
#include <system_error>

namespace mbo {

std::filesystem::path guess_repo_root() {
    std::filesystem::path p = std::filesystem::current_path();
    for (int i = 0; i < 6; i++) {
        if (std::filesystem::exists(p / "frontend") &&
            std::filesystem::is_directory(p / "frontend")) {
            return p;
        }
        p = p.parent_path();
        if (p.empty()) break;
    }
    return std::filesystem::current_path();
}

static std::filesystem::path ensure_frontend_public_dir() {
    const auto repo = guess_repo_root();
    const auto outdir = repo / "frontend" / "public";
    std::error_code ec;
    std::filesystem::create_directories(outdir, ec);
    return outdir;
}

static void write_file_atomic_like(const std::filesystem::path& out, const std::string& data) {
    // Simple/portable approach:
    // write to temp file then rename (best-effort)
    auto tmp = out;
    tmp += ".tmp";

    {
        std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
        if (!ofs) {
            std::cerr << "[final] failed to open: " << tmp.string() << "\n";
            return;
        }
        ofs.write(data.data(), (std::streamsize)data.size());
        ofs.close();
    }

    std::error_code ec;
    std::filesystem::rename(tmp, out, ec);
    if (ec) {
        // fallback: if rename failed (e.g. across FS), try direct write
        std::ofstream ofs(out, std::ios::binary | std::ios::trunc);
        if (!ofs) {
            std::cerr << "[final] failed to open: " << out.string() << "\n";
            return;
        }
        ofs.write(data.data(), (std::streamsize)data.size());
        ofs.close();
        // best-effort remove tmp
        std::filesystem::remove(tmp, ec);
    }

    std::cerr << "[final] wrote " << out.string()
              << " (" << data.size() << " bytes)\n";
}

void write_final_books_json(const std::string& book_json, const std::string& symbol, int /*depth_full*/) {
    const auto outdir = ensure_frontend_public_dir();

    // always write final_book.json
    write_file_atomic_like(outdir / "final_book.json", book_json);

    // optionally write final_book_<symbol>.json
    if (!symbol.empty()) {
        write_file_atomic_like(outdir / ("final_book_" + symbol + ".json"), book_json);
    }
}

void write_final_books_json(const std::string& book_json, const std::string& symbol) {
    write_final_books_json(book_json, symbol, /*depth_full*/0);
}

} // namespace mbo
