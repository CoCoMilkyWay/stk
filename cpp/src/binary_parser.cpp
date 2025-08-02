#include "binary_parser.hpp"

#include <cassert>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

extern "C" {
#include "miniz.h"
}

// #define DEBUG_TIMER

#ifdef DEBUG_TIMER
#include "misc/misc.hpp"
#endif

namespace BinaryParser {

// ============================================================================
// FORWARD DECLARATIONS
// ============================================================================

// CSV output utility functions
void DumpSnapshotCSV(const std::vector<Table::Snapshot_3s_Record> &records,
                     const std::string &asset_code,
                     const std::string &output_dir,
                     size_t last_n = 0);

void DumpBarCSV(const std::vector<Table::Bar_1m_Record> &records,
                const std::string &asset_code,
                const std::string &output_dir,
                size_t last_n = 0);

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

void Parser::ConvertToSnapshot3sAndBar1m(const std::vector<BinaryRecord> &binary_records, uint16_t year, uint8_t month) {
#ifdef DEBUG_TIMER
  misc::Timer timer("ConvertToSnapshot3sAndBar1m");
#endif
  if (binary_records.empty())
    return;

  uint32_t current_minute_index = 0;
  uint8_t last_minute = 0;
  uint8_t last_hour = 0;

  for (const auto &record : binary_records) {
    // Calculate minute index based on day and time (optimized to reduce costly division operations)
    uint8_t current_day = record.day;
    uint8_t current_hour = static_cast<uint8_t>(record.time_s / 3600);
    uint16_t remaining_seconds = static_cast<uint16_t>(record.time_s - static_cast<uint32_t>(current_hour) * 3600);
    uint8_t current_minute = static_cast<uint8_t>(remaining_seconds / 60);
    uint8_t current_second = static_cast<uint8_t>(remaining_seconds - static_cast<uint16_t>(current_minute) * 60);
    // std::cout << "year: " << static_cast<int>(year)
    //           << ", month: " << static_cast<int>(month)
    //           << ", day: " << static_cast<int>(current_day)
    //           << ", hour: " << static_cast<int>(current_hour)
    //           << ", minute: " << static_cast<int>(current_minute)
    //           << ", second: " << static_cast<int>(current_second)
    //           << ", trade_count: " << static_cast<int>(record.trade_count)
    //           << ", volume: " << static_cast<int>(record.volume)
    //           << ", turnover: " << static_cast<int>(record.turnover)
    //           << ", latest_price_tick: " << static_cast<int>(record.latest_price_tick)
    //           << ", direction: " << static_cast<int>(record.direction)
    //           << "\n";

    // Trigger a new bar when either minute or hour changes
    if (current_minute != last_minute || current_hour != last_hour) {
      current_minute_index = static_cast<uint32_t>(bar_1m_buffer_.size());

      Table::Bar_1m_Record new_bar;
      new_bar.year = year;
      new_bar.month = month;
      new_bar.day = current_day;
      new_bar.hour = current_hour;
      new_bar.minute = current_minute;
      new_bar.open = static_cast<float>(TickToPrice(record.latest_price_tick));
      new_bar.high = static_cast<float>(TickToPrice(record.latest_price_tick));
      new_bar.low = static_cast<float>(TickToPrice(record.latest_price_tick));
      new_bar.close = static_cast<float>(TickToPrice(record.latest_price_tick));
      new_bar.volume = static_cast<float>(record.volume);
      new_bar.turnover = static_cast<float>(record.turnover);

      bar_1m_buffer_.push_back(new_bar);

      last_minute = current_minute;
      last_hour = current_hour;
    } else {
      // Update existing Bar_1m_Record for the current minute
      UpdateBar1mRecord(bar_1m_buffer_.back(), record);
    }

    // Convert to Snapshot_3s_Record
    auto snapshot_record = ConvertToSnapshot3s(record, current_minute_index, current_second);
    snapshot_3s_buffer_.push_back(snapshot_record);

    technical_analysis
  }
}

Table::Snapshot_3s_Record Parser::ConvertToSnapshot3s(const BinaryRecord &record, uint32_t minute_index, uint8_t second) {
  Table::Snapshot_3s_Record snapshot;

  snapshot.index_1m = minute_index;
  snapshot.seconds = second;
  snapshot.latest_price_tick = record.latest_price_tick;
  snapshot.trade_count = record.trade_count;
  snapshot.turnover = record.turnover;
  snapshot.volume = record.volume;

  // Copy bid and ask prices and volumes
  for (int i = 0; i < 5; ++i) {
    snapshot.bid_price_ticks[i] = record.bid_price_ticks[i];
    snapshot.bid_volumes[i] = record.bid_volumes[i];
    snapshot.ask_price_ticks[i] = record.ask_price_ticks[i];
    snapshot.ask_volumes[i] = record.ask_volumes[i];
  }

  snapshot.direction = record.direction;
  return snapshot;
}

void Parser::UpdateBar1mRecord(Table::Bar_1m_Record &bar_record, const BinaryRecord &binary_record) {
  float current_price = static_cast<float>(TickToPrice(binary_record.latest_price_tick));

  // Update OHLC
  if (current_price > bar_record.high) {
    bar_record.high = current_price;
  }
  if (current_price < bar_record.low) {
    bar_record.low = current_price;
  }
  bar_record.close = current_price; // Latest price becomes close

  // Accumulate volume and turnover
  bar_record.volume += static_cast<float>(binary_record.volume);
  bar_record.turnover += static_cast<float>(binary_record.turnover);
}

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
// FORMATTING UTILITIES
// ============================================================================

std::string Parser::FormatTime(uint16_t time_s) const {
  int hours = time_s / 3600;
  int minutes = (time_s % 3600) / 60;
  int seconds = time_s % 60;

  std::ostringstream oss;
  oss << std::setfill('0') << std::setw(2) << hours << ":"
      << std::setfill('0') << std::setw(2) << minutes << ":"
      << std::setfill('0') << std::setw(2) << seconds;
  return oss.str();
}

const char *Parser::FormatDirection(uint8_t direction) const {
  switch (direction) {
  case 0:
    return "B"; // Buy
  case 1:
    return "S"; // Sell
  default:
    return "-"; // Unknown
  }
}

// ============================================================================
// MAIN INTERFACE
// ============================================================================

void Parser::ParseAsset(const std::string &asset_code,
                        const std::string &snapshot_dir,
                        const std::vector<std::string> &month_folders,
                        const std::string &output_dir) {
  auto start_time = std::chrono::high_resolution_clock::now();

  // Pre-calculate total records for efficient allocation
  estimated_total_records_ = CalculateTotalRecordsForAsset(asset_code, snapshot_dir, month_folders);

  // Pre-allocate buffers for entire asset lifespan - major optimization!
  snapshot_3s_buffer_.clear();
  snapshot_3s_buffer_.reserve(estimated_total_records_);
  bar_1m_buffer_.clear();
  bar_1m_buffer_.reserve(estimated_total_records_ / 20 / 24 * 5); // Rough estimate: 5 hours of 3s data per day

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

      // Convert to Snapshot_3s_Record and Bar_1m_Record tables
      ConvertToSnapshot3sAndBar1m(records, year, month);

    } catch (const std::exception &e) {
      std::cout << "  Warning: Error processing " << asset_file << ": " << e.what() << "\n";
    }
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

  std::cout << "Processed asset " << asset_code << " across " << month_folders.size()
            << " months (estimated " << estimated_total_records_ << " total records, "
            << snapshot_3s_buffer_.size() << " snapshot records, "
            << bar_1m_buffer_.size() << " bar records (" << duration.count() << "ms))\n";

  // Export results to CSV files
  DumpBarCSV(bar_1m_buffer_, asset_code, output_dir, 10000);
  DumpSnapshotCSV(snapshot_3s_buffer_, asset_code, output_dir, 10000);

  // Clear buffers to free memory
  snapshot_3s_buffer_.clear();
  snapshot_3s_buffer_.shrink_to_fit();
  bar_1m_buffer_.clear();
  bar_1m_buffer_.shrink_to_fit();
}

// ============================================================================
// CSV OUTPUT UTILITIES
// ============================================================================

namespace {

// Helper to convert price ticks to actual prices
inline double TickToPrice(int16_t tick) {
  return tick * 0.01;
}

// Helper to output a single field to CSV
template <typename T>
inline void OutputField(std::ostringstream &output, const T &field, bool &first) {
  if (!first)
    output << ",";
  first = false;

  if constexpr (std::is_same_v<T, int16_t>) {
    // Convert price ticks to prices for int16_t fields (likely price ticks)
    output << TickToPrice(field);
  } else if constexpr (std::is_integral_v<T>) {
    output << static_cast<int>(field);
  } else {
    output << field;
  }
}

// Helper to output array fields to CSV
template <typename T, size_t N>
inline void OutputArray(std::ostringstream &output, const T (&array)[N], bool &first) {
  for (size_t i = 0; i < N; ++i) {
    OutputField(output, array[i], first);
  }
}

// Generic CSV dump function that works with any record type using structured bindings
template <typename RecordType>
inline void DumpRecordsToCSV(const std::vector<RecordType> &records,
                             const std::string &asset_code,
                             const std::string &output_dir,
                             const std::string &suffix,
                             size_t last_n = 0) {
  if (records.empty())
    return;

  std::filesystem::create_directories(output_dir);
  std::string filename = output_dir + "/" + asset_code + "_" + suffix + ".csv";
  std::ofstream file(filename);

  if (!file.is_open()) {
    std::cerr << "Failed to create file: " << filename << "\n";
    return;
  }

  // Generate header based on record type
  if constexpr (std::is_same_v<RecordType, Table::Snapshot_3s_Record>) {
    file << "index_1m,seconds,latest_price,trade_count,turnover,volume,";
    file << "bid_price_1,bid_price_2,bid_price_3,bid_price_4,bid_price_5,";
    file << "bid_vol_1,bid_vol_2,bid_vol_3,bid_vol_4,bid_vol_5,";
    file << "ask_price_1,ask_price_2,ask_price_3,ask_price_4,ask_price_5,";
    file << "ask_vol_1,ask_vol_2,ask_vol_3,ask_vol_4,ask_vol_5,direction\n";
  } else if constexpr (std::is_same_v<RecordType, Table::Bar_1m_Record>) {
    file << "year,month,day,hour,minute,open,high,low,close,volume,turnover\n";
  }

  std::ostringstream batch_output;
  batch_output << std::fixed << std::setprecision(2);

  size_t start_index = (last_n == 0 || last_n >= records.size()) ? 0 : records.size() - last_n;
  for (size_t i = start_index; i < records.size(); ++i) {
    const auto &record = records[i];
    bool first = true;

    if constexpr (std::is_same_v<RecordType, Table::Snapshot_3s_Record>) {
      auto [index_1m, seconds, latest_price_tick, trade_count, turnover, volume,
            bid_price_ticks, bid_volumes, ask_price_ticks, ask_volumes, direction] = record;

      OutputField(batch_output, index_1m, first);
      OutputField(batch_output, seconds, first);
      OutputField(batch_output, latest_price_tick, first);
      OutputField(batch_output, trade_count, first);
      OutputField(batch_output, turnover, first);
      OutputField(batch_output, volume, first);
      OutputArray(batch_output, bid_price_ticks, first);
      OutputArray(batch_output, bid_volumes, first);
      OutputArray(batch_output, ask_price_ticks, first);
      OutputArray(batch_output, ask_volumes, first);
      OutputField(batch_output, direction, first);

    } else if constexpr (std::is_same_v<RecordType, Table::Bar_1m_Record>) {
      auto [year, month, day, hour, minute, open, high, low, close, volume, turnover] = record;

      OutputField(batch_output, year, first);
      OutputField(batch_output, month, first);
      OutputField(batch_output, day, first);
      OutputField(batch_output, hour, first);
      OutputField(batch_output, minute, first);
      OutputField(batch_output, open, first);
      OutputField(batch_output, high, first);
      OutputField(batch_output, low, first);
      OutputField(batch_output, close, first);
      OutputField(batch_output, volume, first);
      OutputField(batch_output, turnover, first);
    }

    batch_output << "\n";
  }

  file << batch_output.str();
  file.close();

  size_t dumped_count = records.size() - start_index;
  std::cout << "Dumped " << dumped_count << " " << suffix << " records to " << filename << "\n";
}

} // anonymous namespace

// Convenience wrappers for specific record types
void DumpSnapshotCSV(const std::vector<Table::Snapshot_3s_Record> &records,
                     const std::string &asset_code,
                     const std::string &output_dir,
                     size_t last_n) {
  DumpRecordsToCSV(records, asset_code, output_dir, "snapshot_3s", last_n);
}

void DumpBarCSV(const std::vector<Table::Bar_1m_Record> &records,
                const std::string &asset_code,
                const std::string &output_dir,
                size_t last_n) {
  DumpRecordsToCSV(records, asset_code, output_dir, "bar_1m", last_n);
}

} // namespace BinaryParser