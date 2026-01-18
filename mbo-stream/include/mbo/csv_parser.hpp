#pragma once
#include <string>
#include "mbo/mbo_event.hpp"

// Parse one CSV line (already framed as a full line) into MboEvent.
// Return true if parsing succeeded, false otherwise.
bool parse_mbo_csv_line(const std::string& line, MboEvent& out);
