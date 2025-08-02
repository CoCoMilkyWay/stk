#include "binary_parser.hpp"
#include "json_config.hpp"
#include <algorithm>
#include <filesystem>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#endif

void ProcessAsset(const std::string &asset_code,
                  const JsonConfig::StockInfo &stock_info,
                  const std::string &snapshot_dir,
                  const std::string &output_dir) {
  try {
    // Get month range for this asset
    auto month_range = JsonConfig::GetMonthRange(stock_info.ipo_date, stock_info.delist_date);

    // Convert to string format for folder names
    std::vector<std::string> month_folders;
    for (const auto &ym : month_range) {
      month_folders.push_back(JsonConfig::FormatYearMonth(ym));
    }

    // Create a parser instance for this asset (no output file needed for lifespan parsing)
    BinaryParser::Parser parser;

    // Process the asset across its entire lifespan
    parser.ParseAssetLifespan(asset_code, snapshot_dir, month_folders, output_dir);

  } catch (const std::exception &e) {
    std::cerr << "Error processing asset " << asset_code << ": " << e.what() << "\n";
  }
}

int main() {
  try {
#ifdef _WIN32
    // Set console output code page to UTF-8
    SetConsoleOutputCP(CP_UTF8);
    // Set console input code page to UTF-8 as well
    SetConsoleCP(CP_UTF8);
#endif

    // Configuration file paths
    std::string config_file = "../config/config.json";
    std::string stock_info_file = "../config/daily_holding/stock_info_test.json";
    std::string output_dir = "../output";

    std::cout << "=== Asset Parser ====================================================" << "\n";
    std::cout << "Loading configuration..." << "\n";

    // Parse configuration files
    JsonConfig::AppConfig app_config = JsonConfig::ParseAppConfig(config_file);
    auto stock_info_map = JsonConfig::ParseStockInfo(stock_info_file);

    // Override IPO date if earlier than start_month, and delist_date for active stocks using configured end_month
    for (auto &pair : stock_info_map) {
      // If IPO date is earlier than start_month, use start_month
      if (pair.second.ipo_date < app_config.start_month) {
        pair.second.ipo_date = app_config.start_month;
      }
      // Override delist_date for active stocks using configured end_month
      if (!pair.second.is_delisted) {
        pair.second.delist_date = app_config.end_month;
      }
    }

    std::cout << "Configuration loaded successfully:" << "\n";
    std::cout << "  Snapshot directory: " << app_config.snapshot_dir << "\n";
    std::cout << "  Data available through: " << JsonConfig::FormatYearMonth(app_config.end_month) << "\n";
    std::cout << "  Total assets found: " << stock_info_map.size() << "\n";
    std::cout << "  Output directory: " << output_dir << "\n\n";

    // Create output directory if it doesn't exist
    std::filesystem::create_directories(output_dir);

    // Determine optimal thread count
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0)
      num_threads = 4; // Fallback

    std::cout << "Using " << num_threads << " threads for parallel processing\n\n";

    // Process assets in batches using thread pool
    std::vector<std::future<void>> futures;
    auto stock_iter = stock_info_map.begin();

    while (stock_iter != stock_info_map.end()) {
      // Wait for any completed threads if we're at capacity
      if (futures.size() >= num_threads) {
        // Find and remove completed futures
        auto is_completed = [](std::future<void> &f) {
          return f.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
        };
        futures.erase(
            std::remove_if(futures.begin(), futures.end(), is_completed),
            futures.end());

        // If still at capacity, wait for one to complete
        if (futures.size() >= num_threads && !futures.empty()) {
          futures.front().wait();
          futures.erase(futures.begin());
        }
      }

      // Launch new task
      const std::string &asset_code = stock_iter->first;
      const JsonConfig::StockInfo &stock_info = stock_iter->second;

      std::cout << "Queuing asset: " << asset_code << " (" << stock_info.name << ")\n";

      futures.push_back(
          std::async(
              std::launch::async,
              ProcessAsset,
              asset_code,
              stock_info,
              app_config.snapshot_dir,
              output_dir));

      ++stock_iter;
    }

    // Wait for all remaining tasks to complete
    std::cout << "\nWaiting for all processing to complete...\n";
    for (auto &future : futures) {
      future.wait();
    }

    std::cout << "\n=== Processing completed successfully! ===" << "\n";
    std::cout << "All asset lifespans have been processed and saved to: " << output_dir << "\n";

    return 0;

  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << "\n";
    std::cerr
        << "Make sure all configuration files exist and contain valid data.\n";
    return 1;
  }
}