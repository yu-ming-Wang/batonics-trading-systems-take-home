#pragma once
#include <memory>
#include <string>

void publish_snapshot(std::string s);
std::shared_ptr<const std::string> load_snapshot();

// NEW
void publish_snapshot(const std::string& symbol, std::string s);
std::shared_ptr<const std::string> load_snapshot(const std::string& symbol);
