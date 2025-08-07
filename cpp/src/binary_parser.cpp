#include "binary_parser.hpp"
#include "misc/misc.hpp"
#include "technical_analysis.hpp"

#include <cassert>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

extern "C" {
#include "miniz.h"
}

// #define DEBUG_TIMER
// #define PRINT_BINARY

#ifdef DEBUG_TIMER
#include "misc/misc.hpp"
#endif

namespace BinaryParser {

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

// Note: CSV dumping functionality moved to technical_analysis.cpp

// ============================================================================
// CONSTRUCTOR AND DESTRUCTOR
// ============================================================================

Parser::Parser() {
  // Pre-allocate buffers for efficiency
  read_buffer_.reserve(BUFFER_SIZE);
  write_buffer_.resize(BUFFER_SIZE);
}

Parser::~Parser() {
  // Clean up completed
}

// ============================================================================
// CORE PARSING FUNCTIONS
// ============================================================================

std::vector<uint8_t> Parser::DecompressFile(const std::string &filepath, size_t record_count) {
#ifdef DEBUG_TIMER
  misc::Timer timer("DecompressFile");
#endif
  std::ifstream file(filepath, std::ios::binary);
  if (!file) {
    std::cerr << "Failed to open file: " << filepath << "\n";
    return {};
  }

  // Read compressed data
  file.seekg(0, std::ios::end);
  size_t compressed_size = file.tellg();
  file.seekg(0, std::ios::beg);

  std::vector<uint8_t> compressed_data(compressed_size);
  file.read(reinterpret_cast<char *>(compressed_data.data()), compressed_size);
  file.close();

  // Use exact buffer size from record count for fast single decompression
  mz_ulong exact_size = static_cast<mz_ulong>(record_count * sizeof(BinaryRecord));
  std::vector<uint8_t> decompressed_data(static_cast<size_t>(exact_size));

  int result = mz_uncompress(decompressed_data.data(), &exact_size,
                             compressed_data.data(),
                             static_cast<mz_ulong>(compressed_size));

  if (result != MZ_OK) {
    std::cerr << "Decompression failed for file: " << filepath << "\n";
    return {};
  }
  decompressed_data.resize(exact_size);
  return decompressed_data;
}

std::vector<BinaryRecord> Parser::ParseBinaryData(const std::vector<uint8_t> &binary_data) {
#ifdef DEBUG_TIMER
  misc::Timer timer("ParseBinaryData");
#endif
  size_t record_count = binary_data.size() / sizeof(BinaryRecord);
  if (record_count == 0 || binary_data.size() % sizeof(BinaryRecord) != 0) {
    std::cerr << "Invalid binary data size: " << binary_data.size() << "\n";
    return {};
  }

  std::vector<BinaryRecord> records(record_count);
  std::memcpy(records.data(), binary_data.data(), binary_data.size());
  return records;
}

void Parser::ReverseDifferentialEncoding(std::vector<BinaryRecord> &records) {
#ifdef DEBUG_TIMER
  misc::Timer timer("ReverseDifferentialEncoding");
#endif
  if (records.size() <= 1)
    return;

  // Process each record starting from index 1
  for (size_t i = 1; i < records.size(); ++i) {
    // Reverse differential encoding for day
    records[i].day += records[i - 1].day;

    // Reverse differential encoding for time_s
    records[i].time_s += records[i - 1].time_s;

    // Reverse differential encoding for latest_price_tick
    records[i].latest_price_tick += records[i - 1].latest_price_tick;

    // Reverse differential encoding for bid_price_ticks (array)
    for (int j = 0; j < 5; ++j) {
      records[i].bid_price_ticks[j] += records[i - 1].bid_price_ticks[j];
    }

    // Reverse differential encoding for ask_price_ticks (array)
    for (int j = 0; j < 5; ++j) {
      records[i].ask_price_ticks[j] += records[i - 1].ask_price_ticks[j];
    }
  }
}

// ============================================================================
// DATA CONVERSION FUNCTIONS
// ============================================================================

void Parser::ProcessBinaryRecords(const std::vector<BinaryRecord> &binary_records, uint16_t year, uint8_t month) {
#ifdef DEBUG_TIMER
  misc::Timer timer("ProcessBinaryRecords");
#endif
  if (binary_records.empty())
    return;

  Table::Snapshot_Record snapshot;

  for (const auto &record : binary_records) {
    ++records_count_;
    misc::print_progress(records_count_, total_records_);

    // Calculate time components (optimized)
    uint16_t time_s = record.time_s;
    uint8_t current_hour = static_cast<uint8_t>(time_s / 3600);
    uint16_t remaining_seconds = static_cast<uint16_t>(time_s % 3600);
    uint8_t current_minute = static_cast<uint8_t>(remaining_seconds / 60);
    uint8_t current_second = static_cast<uint8_t>(remaining_seconds % 60);
#ifdef PRINT_BINARY
    std::cout << "year: " << static_cast<int>(year)
              << ", month: " << static_cast<int>(month)
              << ", day: " << static_cast<int>(record.day)
              << ", hour: " << static_cast<int>(current_hour)
              << ", minute: " << static_cast<int>(current_minute)
              << ", second: " << static_cast<int>(current_second)
              << ", trade_count: " << static_cast<int>(record.trade_count)
              << ", volume: " << static_cast<int>(record.volume)
              << ", turnover: " << static_cast<int>(record.turnover)
              << ", latest_price_tick: " << static_cast<int>(record.latest_price_tick)
              << ", direction: " << static_cast<int>(record.direction)
              << "\n";
#endif

    // Convert to Snapshot_Record
    snapshot.year = year;
    snapshot.month = month;
    snapshot.day = record.day;
    snapshot.hour = current_hour;
    snapshot.minute = current_minute;
    snapshot.second = current_second;
    snapshot.seconds_in_day = record.time_s;
    // Convert price from tick to float
    snapshot.latest_price_tick = TickToPrice(record.latest_price_tick);
    snapshot.trade_count = record.trade_count;
    snapshot.volume = record.volume;
    snapshot.turnover = record.turnover;
    // Convert bid prices and volumes
    for (int i = 0; i < 5; ++i) {
      snapshot.bid_price_ticks[i] = TickToPrice(record.bid_price_ticks[i]);
      snapshot.bid_volumes[i] = record.bid_volumes[i];
    }
    // Convert ask prices and volumes
    for (int i = 0; i < 5; ++i) {
      snapshot.ask_price_ticks[i] = TickToPrice(record.ask_price_ticks[i]);
      snapshot.ask_volumes[i] = record.ask_volumes[i];
    }
    snapshot.direction = record.direction;
    // Send single snapshot to technical analysis for processing
    technical_analysis_->ProcessSingleSnapshot(snapshot);
  }
}
// Note: UpdateBar1mRecord function moved to technical_analysis.cpp

// ============================================================================
// FILE SYSTEM UTILITIES
// ============================================================================

std::string Parser::FindAssetFile(const std::string &month_folder,
                                  const std::string &asset_code) {
  try {
    for (const auto &entry : std::filesystem::directory_iterator(month_folder)) {
      if (entry.is_regular_file() && entry.path().extension() == ".bin") {
        std::string filename = entry.path().filename().string();

        // Extract asset code from filename: e.g. "sh600004_58381.bin" -> "600004"
        size_t underscore_pos = filename.find('_');
        if (underscore_pos != std::string::npos && underscore_pos > 2) {
          std::string file_asset_code = filename.substr(2, underscore_pos - 2);
          if (file_asset_code == asset_code) {
            return entry.path().string();
          }
        }
      }
    }
  } catch (const std::filesystem::filesystem_error &e) {
    std::cout << "Error reading directory " << month_folder << ": " << e.what() << "\n";
  }

  return ""; // Not found
}

size_t Parser::ExtractRecordCountFromFilename(const std::string &filename) {
  // Extract record count from filename like "sh600000_12345.bin"
  if (filename.length() >= 10 && filename.substr(filename.length() - 4) == ".bin") {
    std::string basename = filename.substr(0, filename.length() - 4);

    // Find last underscore to get record count
    size_t last_underscore = basename.find_last_of('_');
    if (last_underscore != std::string::npos && last_underscore < basename.length() - 1) {
      std::string count_str = basename.substr(last_underscore + 1);
      try {
        return std::stoull(count_str);
      } catch (const std::exception &) {
        return 0; // Invalid format
      }
    }
  }
  return 0; // No record count found
}

std::tuple<size_t, uint16_t, uint8_t> Parser::ExtractRecordCountAndDateFromPath(const std::string &filepath) {
#ifdef DEBUG_TIMER
  misc::Timer timer("ExtractRecordCountAndDateFromPath");
#endif
  std::filesystem::path path(filepath);
  std::string filename = path.filename().string();
  std::string folder_name = path.parent_path().filename().string();

  // Extract record count from filename using existing function
  size_t record_count = ExtractRecordCountFromFilename(filename);

  // Extract year and month from folder name (format: "YYYY_MM")
  uint16_t year = 0;
  uint8_t month = 0;
  size_t underscore_pos = folder_name.find('_');
  if (underscore_pos != std::string::npos && underscore_pos == 4) {
    std::string year_str = folder_name.substr(0, 4);
    std::string month_str = folder_name.substr(5, 2);

    try {
      year = static_cast<uint16_t>(std::stoi(year_str));
      month = static_cast<uint8_t>(std::stoi(month_str));
    } catch (const std::exception &) {
      // Invalid format, year and month remain 0
    }
  }

  return {record_count, year, month};
}

size_t Parser::CalculateTotalRecordsForAsset(const std::string &asset_code,
                                             const std::string &snapshot_dir,
                                             const std::vector<std::string> &month_folders) {
  size_t total_records = 0;

  for (const std::string &month_folder : month_folders) {
    std::string month_path = snapshot_dir + "/" + month_folder;
    std::string asset_file = FindAssetFile(month_path, asset_code);

    if (!asset_file.empty()) {
      // Extract record count from filename (e.g., "sh600004_59482.bin" -> 59482)
      std::string filename = std::filesystem::path(asset_file).filename().string();
      size_t month_records = ExtractRecordCountFromFilename(filename);
      total_records += month_records;
    }
  }

  return total_records;
}

// ============================================================================
// MAIN INTERFACE
// ============================================================================

void Parser::ParseAsset(const std::string &asset_code,
                        const std::string &snapshot_dir,
                        const std::vector<std::string> &month_folders,
                        const std::string &output_dir) {
  auto start_time = std::chrono::high_resolution_clock::now();

  // Pre-calculate total records for progress tracking
  total_records_ = CalculateTotalRecordsForAsset(asset_code, snapshot_dir, month_folders);
  records_count_ = 0;

  // Initialize technical analysis engine
  technical_analysis_ = std::make_unique<::TechnicalAnalysis>();

  for (const std::string &month_folder : month_folders) {
#ifdef DEBUG_TIMER
    std::cout << "\n========================================================";
#endif
    std::string month_path = snapshot_dir + "/" + month_folder;
    std::string asset_file = FindAssetFile(month_path, asset_code);

    if (asset_file.empty()) {
      std::cout << "  No file found for " << asset_code << " in " << month_folder << "\n";
      continue;
    }

    // Extract record count, year and month from file path at start of loop
    auto [record_count, year, month] = ExtractRecordCountAndDateFromPath(asset_file);

    try {
      // Decompress and parse binary data
      auto decompressed_data = DecompressFile(asset_file, record_count);
      if (decompressed_data.empty()) {
        std::cout << "  Warning: Empty file " << asset_file << "\n";
        continue;
      }

      auto records = ParseBinaryData(decompressed_data);
      ReverseDifferentialEncoding(records);

      // Process binary records through technical analysis
      ProcessBinaryRecords(records, year, month);

    } catch (const std::exception &e) {
      std::cout << "  Warning: Error processing " << asset_file << ": " << e.what() << "\n";
    }
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

  std::cout << "Processed asset " << asset_code << " across " << month_folders.size()
            << " months (estimated " << total_records_ << " total records, "
            << technical_analysis_->GetSnapshotCount() << " snapshot records, "
            << technical_analysis_->GetBarCount() << " bar records (" << duration.count() << "ms))\n";

  // Export results to CSV files via technical analysis
  technical_analysis_->DumpBarCSV(asset_code, output_dir, 10000);
  technical_analysis_->DumpSnapshotCSV(asset_code, output_dir, 10000);
}

// Note: CSV output utilities moved to technical_analysis.cpp

} // namespace BinaryParser