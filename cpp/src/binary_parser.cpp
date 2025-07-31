#include <algorithm>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "binary_parser.hpp"
#include "nlohmann/json.hpp"

extern "C" {
#include "miniz.h"
}

namespace BinaryParser {

Config ReadConfigFromJson(const std::string &config_file) {
  std::ifstream file(config_file);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open config file: " + config_file);
  }

  nlohmann::json json_config;
  file >> json_config;

  Config config;
  config.input_root = json_config.at("input_root").get<std::string>();
  config.target_file = json_config.at("target_file").get<std::string>();
  config.output_file = json_config.at("output_file").get<std::string>();

  return config;
}

Parser::Parser(const std::string &input_root, const std::string &output_file)
    : input_root_(input_root), output_file_(output_file) {

  // Pre-allocate buffers for efficiency
  read_buffer_.reserve(BUFFER_SIZE);
  write_buffer_.resize(BUFFER_SIZE);

  // Open output file with optimized settings
  output_stream_.open(output_file_, std::ios::out | std::ios::trunc);
  if (!output_stream_) {
    throw std::runtime_error("Failed to open output file: " + output_file_);
  }

  // Set buffer for faster writing
  output_stream_.rdbuf()->pubsetbuf(write_buffer_.data(), write_buffer_.size());

  // Write CSV header
  output_stream_
      << "Symbol,Date,Time,LatestPrice,TradeCount,Turnover,Volume,Direction,";
  output_stream_ << "BidPrice1,BidPrice2,BidPrice3,BidPrice4,BidPrice5,";
  output_stream_ << "BidVol1,BidVol2,BidVol3,BidVol4,BidVol5,";
  output_stream_ << "AskPrice1,AskPrice2,AskPrice3,AskPrice4,AskPrice5,";
  output_stream_ << "AskVol1,AskVol2,AskVol3,AskVol4,AskVol5\n";
}

Parser::~Parser() {
  if (output_stream_.is_open()) {
    output_stream_.close();
  }

  std::cout << "Parsing completed!\n";
  std::cout << "Total files processed: " << total_files_processed_ << "\n";
  std::cout << "Total records processed: " << total_records_processed_ << "\n";
}

void Parser::ParseSingleFile(const std::string &target_file) {
  auto start_time = std::chrono::high_resolution_clock::now();

  // Construct full path
  std::filesystem::path full_path =
      std::filesystem::path(input_root_) / target_file;

  std::cout << "Processing single file: " << full_path.string() << "\n";

  try {
    // Check if file exists
    if (!std::filesystem::exists(full_path)) {
      throw std::runtime_error("File does not exist: " + full_path.string());
    }

    // Extract symbol from filename
    std::string symbol =
        ExtractSymbolFromFilename(full_path.filename().string());
    if (symbol.empty()) {
      throw std::runtime_error("Could not extract symbol from filename: " +
                               full_path.filename().string());
    }

    std::cout << "Symbol: " << symbol << "\n";

    // Decompress binary file
    auto decompressed_data = DecompressFile(full_path.string());
    if (decompressed_data.empty()) {
      throw std::runtime_error("Failed to decompress file or file is empty");
    }

    std::cout << "Decompressed " << decompressed_data.size() << " bytes\n";

    // Parse binary records
    auto records = ParseBinaryData(decompressed_data);
    if (records.empty()) {
      throw std::runtime_error("No records found in file");
    }

    std::cout << "Parsed " << records.size() << " records\n";

    // Reverse differential encoding
    ReverseDifferentialEncoding(records);

    // Write records to text
    WriteRecordsToText(records, symbol);

    total_files_processed_ = 1;
    total_records_processed_ = records.size();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
    std::cout << "Processing time: " << duration.count() << " ms\n";

  } catch (const std::exception &e) {
    std::cerr << "Error processing file " << full_path.string() << ": "
              << e.what() << "\n";
    throw;
  }
}

std::vector<uint8_t> Parser::DecompressFile(const std::string &filepath) {
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

  // Extract filename from path to get record count
  std::string filename = std::filesystem::path(filepath).filename().string();
  size_t record_count = ExtractRecordCountFromFilename(filename);

  std::vector<uint8_t> decompressed_data;
  int result;

  if (record_count > 0) {
    // Use exact buffer size from record count - extremely fast single
    // decompression
    mz_ulong exact_size =
        static_cast<mz_ulong>(record_count * sizeof(TickRecord));
    decompressed_data.resize(static_cast<size_t>(exact_size));

    result = mz_uncompress(decompressed_data.data(), &exact_size,
                           compressed_data.data(),
                           static_cast<mz_ulong>(compressed_size));

    if (result == MZ_OK) {
      decompressed_data.resize(exact_size);
      return decompressed_data;
    } else {
      std::cerr << "Fast decompression failed for: " << filepath
                << " (error: " << result
                << "), falling back to iterative method\n";
    }
  }

  // Fallback to iterative method for old format or if fast method fails
  mz_ulong decompressed_size = static_cast<mz_ulong>(compressed_size * 8);
  result = MZ_BUF_ERROR;

  while (result == MZ_BUF_ERROR &&
         decompressed_size < static_cast<mz_ulong>(compressed_size * 32)) {
    decompressed_data.resize(static_cast<size_t>(decompressed_size));
    result = mz_uncompress(decompressed_data.data(), &decompressed_size,
                           compressed_data.data(),
                           static_cast<mz_ulong>(compressed_size));
    if (result == MZ_BUF_ERROR) {
      decompressed_size *= 2;
    }
  }

  if (result != MZ_OK) {
    std::cerr << "Decompression failed for: " << filepath
              << " (error: " << result << ")\n";
    return {};
  }

  decompressed_data.resize(decompressed_size);
  return decompressed_data;
}

std::vector<TickRecord>
Parser::ParseBinaryData(const std::vector<uint8_t> &binary_data) {
  size_t record_count = binary_data.size() / sizeof(TickRecord);
  if (record_count == 0 || binary_data.size() % sizeof(TickRecord) != 0) {
    std::cerr << "Invalid binary data size: " << binary_data.size() << "\n";
    return {};
  }

  std::vector<TickRecord> records(record_count);
  std::memcpy(records.data(), binary_data.data(), binary_data.size());

  return records;
}

void Parser::ReverseDifferentialEncoding(std::vector<TickRecord> &records) {
  if (records.size() <= 1)
    return;

  // Process each record starting from index 1
  for (size_t i = 1; i < records.size(); ++i) {
    // Reverse differential encoding for date
    records[i].date += records[i - 1].date;

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

void Parser::WriteRecordsToText(const std::vector<TickRecord> &records,
                                const std::string &symbol) {
  // Use a stringstream for batch writing to improve performance
  std::ostringstream batch_output;
  batch_output.precision(2);
  batch_output << std::fixed;

  for (const auto &record : records) {
    // Symbol
    batch_output << symbol << ",";

    // Date (assuming it's day of month)
    batch_output << static_cast<int>(record.date) << ",";

    // Time
    batch_output << FormatTime(record.time_s) << ",";

    // Latest price
    batch_output << TickToPrice(record.latest_price_tick) << ",";

    // Trade count, turnover, volume
    batch_output << static_cast<int>(record.trade_count) << ","
                 << record.turnover << "," << record.volume << ",";

    // Direction
    batch_output << FormatDirection(record.direction) << ",";

    // Bid prices
    for (int i = 0; i < 5; ++i) {
      batch_output << TickToPrice(record.bid_price_ticks[i]);
      if (i < 4)
        batch_output << ",";
    }
    batch_output << ",";

    // Bid volumes
    for (int i = 0; i < 5; ++i) {
      batch_output << record.bid_volumes[i];
      if (i < 4)
        batch_output << ",";
    }
    batch_output << ",";

    // Ask prices
    for (int i = 0; i < 5; ++i) {
      batch_output << TickToPrice(record.ask_price_ticks[i]);
      if (i < 4)
        batch_output << ",";
    }
    batch_output << ",";

    // Ask volumes
    for (int i = 0; i < 5; ++i) {
      batch_output << record.ask_volumes[i];
      if (i < 4)
        batch_output << ",";
    }
    batch_output << "\n";
  }

  // Write entire batch to file
  output_stream_ << batch_output.str();
}

std::vector<std::filesystem::path> Parser::GetMonthFolders() {
  std::vector<std::filesystem::path> month_folders;

  try {
    for (const auto &entry : std::filesystem::directory_iterator(input_root_)) {
      if (entry.is_directory()) {
        month_folders.push_back(entry.path());
      }
    }

    // Sort folders for consistent processing order
    std::sort(month_folders.begin(), month_folders.end());

  } catch (const std::filesystem::filesystem_error &e) {
    std::cerr << "Error reading directory: " << e.what() << "\n";
  }

  return month_folders;
}

std::vector<std::filesystem::path>
Parser::GetBinaryFiles(const std::filesystem::path &month_folder) {
  std::vector<std::filesystem::path> binary_files;

  try {
    for (const auto &entry :
         std::filesystem::directory_iterator(month_folder)) {
      if (entry.is_regular_file() && entry.path().extension() == ".bin") {
        binary_files.push_back(entry.path());
      }
    }

    // Sort files for consistent processing order
    std::sort(binary_files.begin(), binary_files.end());

  } catch (const std::filesystem::filesystem_error &e) {
    std::cerr << "Error reading directory: " << e.what() << "\n";
  }

  return binary_files;
}

std::string Parser::ExtractSymbolFromFilename(const std::string &filename) {
  // Extract symbol from filename like "sh600000_12345.bin"
  if (filename.length() >= 10 &&
      filename.substr(filename.length() - 4) == ".bin") {
    std::string basename = filename.substr(0, filename.length() - 4);

    // Find last underscore to separate symbol from record count
    size_t last_underscore = basename.find_last_of('_');
    if (last_underscore != std::string::npos) {
      return basename.substr(0, last_underscore);
    }

    // Fallback for old format without record count
    return basename;
  }
  return "";
}

size_t Parser::ExtractRecordCountFromFilename(const std::string &filename) {
  // Extract record count from filename like "sh600000_12345.bin"
  if (filename.length() >= 10 &&
      filename.substr(filename.length() - 4) == ".bin") {
    std::string basename = filename.substr(0, filename.length() - 4);

    // Find last underscore to get record count
    size_t last_underscore = basename.find_last_of('_');
    if (last_underscore != std::string::npos &&
        last_underscore < basename.length() - 1) {
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

std::string Parser::FormatTime(uint16_t time_s) const {
  int hours = time_s / 3600;
  int minutes = (time_s % 3600) / 60;
  int seconds = time_s % 60;

  std::ostringstream oss;
  oss << std::setfill('0') << std::setw(2) << hours << ":" << std::setfill('0')
      << std::setw(2) << minutes << ":" << std::setfill('0') << std::setw(2)
      << seconds;
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

} // namespace BinaryParser