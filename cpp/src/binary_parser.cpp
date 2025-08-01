#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "binary_parser.hpp"
#include "misc/misc.hpp"

extern "C" {
#include "miniz.h"
}

namespace BinaryParser {

Parser::Parser() {
  // Pre-allocate buffers for efficiency
  read_buffer_.reserve(BUFFER_SIZE);
  write_buffer_.resize(BUFFER_SIZE);
}

Parser::~Parser() {
  std::cout << "Parsing completed!\n";
  std::cout << "Total files processed: " << total_files_processed_ << "\n";
  std::cout << "Total records processed: " << total_records_processed_ << "\n";
}

std::vector<uint8_t> Parser::DecompressFile(const std::string &filepath) {
  // misc::Timer timer("Decompression");

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
  // misc::Timer timer("Parse");

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
  // misc::Timer timer("Encoding reversal");

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

void Parser::ParseAssetLifespan(const std::string &asset_code,
                                const std::string &snapshot_dir,
                                const std::vector<std::string> &month_folders,
                                const std::string &output_dir) {

  std::string output_filename = output_dir + "/" + asset_code + "_lifespan.csv";
  std::ofstream csv_file(output_filename, std::ios::out | std::ios::trunc);

  if (!csv_file.is_open()) {
    throw std::runtime_error("Failed to create output file: " + output_filename);
  }

  // Pre-calculate total records for efficient allocation
  estimated_total_records_ = CalculateTotalRecordsForAsset(asset_code, snapshot_dir, month_folders);
  std::cout << "Processing asset " << asset_code << " across " << month_folders.size()
            << " months (estimated " << estimated_total_records_ << " total records)\n";

  // Pre-allocate buffer for entire asset lifespan - major optimization!
  asset_records_buffer_.clear();
  asset_records_buffer_.reserve(estimated_total_records_);

  size_t total_records = 0;

  for (const std::string &month_folder : month_folders) {
    std::string month_path = snapshot_dir + "/" + month_folder;
    std::string asset_file = FindAssetFile(month_path, asset_code);

    if (asset_file.empty()) {
      std::cout << "  No file found for " << asset_code << " in " << month_folder << "\n";
      continue;
    }

    try {
      // std::cout << "  Processing " << asset_file << "\n";

      // Decompress and parse binary data
      auto decompressed_data = DecompressFile(asset_file);
      if (decompressed_data.empty()) {
        std::cout << "  Warning: Empty file " << asset_file << "\n";
        continue;
      }

      auto records = ParseBinaryData(decompressed_data);
      ReverseDifferentialEncoding(records);

      // Append records to pre-allocated buffer for batch processing
      asset_records_buffer_.insert(asset_records_buffer_.end(), records.begin(), records.end());
      total_records += records.size();

    } catch (const std::exception &e) {
      std::cout << "  Warning: Error processing " << asset_file << ": " << e.what() << "\n";
    }
  }

  // Batch write all records to CSV - single operation for entire asset lifespan
  if (!asset_records_buffer_.empty()) {
    WriteRecordsToCSV(asset_records_buffer_, asset_code, csv_file, true);
    std::cout << "Batch wrote " << asset_records_buffer_.size() << " records to CSV\n";
  }

  csv_file.close();
  std::cout << "Completed " << asset_code << ": " << total_records << " records written to " << output_filename << "\n";

  // Clear buffer to free memory
  asset_records_buffer_.clear();
  asset_records_buffer_.shrink_to_fit();
}

void Parser::WriteRecordsToCSV(const std::vector<TickRecord> &records,
                               const std::string &symbol,
                               std::ofstream &csv_file,
                               bool write_header) {
  // misc::Timer timer("CSV write");

  if (write_header) {
    csv_file << "Symbol,Date,Time,LatestPrice,TradeCount,Turnover,Volume,Direction,";
    csv_file << "BidPrice1,BidPrice2,BidPrice3,BidPrice4,BidPrice5,";
    csv_file << "BidVol1,BidVol2,BidVol3,BidVol4,BidVol5,";
    csv_file << "AskPrice1,AskPrice2,AskPrice3,AskPrice4,AskPrice5,";
    csv_file << "AskVol1,AskVol2,AskVol3,AskVol4,AskVol5\n";
  }

  std::ostringstream batch_output;
  batch_output << std::fixed << std::setprecision(2);

  for (const auto &record : records) {
    batch_output << symbol << "," << static_cast<int>(record.date) << ","
                 << FormatTime(record.time_s) << ","
                 << TickToPrice(record.latest_price_tick) << ","
                 << static_cast<int>(record.trade_count) << ","
                 << record.turnover << "," << record.volume << ","
                 << FormatDirection(record.direction);

    // Bid prices and volumes
    for (int i = 0; i < 5; ++i) {
      batch_output << "," << TickToPrice(record.bid_price_ticks[i]);
    }
    for (int i = 0; i < 5; ++i) {
      batch_output << "," << record.bid_volumes[i];
    }

    // Ask prices and volumes
    for (int i = 0; i < 5; ++i) {
      batch_output << "," << TickToPrice(record.ask_price_ticks[i]);
    }
    for (int i = 0; i < 5; ++i) {
      batch_output << "," << record.ask_volumes[i];
    }

    batch_output << "\n";
  }

  csv_file << batch_output.str();
}

size_t Parser::CalculateTotalRecordsForAsset(const std::string &asset_code,
                                             const std::string &snapshot_dir,
                                             const std::vector<std::string> &month_folders) {
  // misc::Timer timer("Record count calculation");

  size_t total_records = 0;
  // std::cout << "Calculating total records for " << asset_code << ":\n";

  for (const std::string &month_folder : month_folders) {
    std::string month_path = snapshot_dir + "/" + month_folder;
    std::string asset_file = FindAssetFile(month_path, asset_code);

    if (!asset_file.empty()) {
      // Extract record count from filename (e.g., "sh600004_59482.bin" -> 59482)
      std::string filename = std::filesystem::path(asset_file).filename().string();
      size_t month_records = ExtractRecordCountFromFilename(filename);
      total_records += month_records;

      // std::cout << "  " << month_folder << ": " << month_records << " records\n";
    }
  }

  // std::cout << "Total estimated records: " << total_records << "\n";
  return total_records;
}

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

} // namespace BinaryParser