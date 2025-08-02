#pragma once

#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>

namespace BinaryParser {

// Binary record structure (54 bytes total)
#pragma pack(push, 1)
struct TickRecord {
  bool sync;                  // 1 byte
  uint8_t date;               // 1 byte
  uint16_t time_s;            // 2 bytes - seconds in day
  int16_t latest_price_tick;  // 2 bytes - price * 100
  uint8_t trade_count;        // 1 byte
  uint32_t turnover;          // 4 bytes - RMB
  uint16_t volume;            // 2 bytes - units of 100 shares
  int16_t bid_price_ticks[5]; // 10 bytes - prices * 100
  uint16_t bid_volumes[5];    // 10 bytes - units of 100 shares
  int16_t ask_price_ticks[5]; // 10 bytes - prices * 100
  uint16_t ask_volumes[5];    // 10 bytes - units of 100 shares
  uint8_t direction;          // 1 byte
                              // Total: 54 bytes
};
#pragma pack(pop)

// Differential encoding fields (from Python code)
constexpr bool DIFF_FIELDS[] = {
    false, // sync
    true,  // date
    true,  // time_s
    true,  // latest_price_tick
    false, // trade_count
    false, // turnover
    false, // volume
    true,  // bid_price_ticks (array)
    false, // bid_volumes
    true,  // ask_price_ticks (array)
    false, // ask_volumes
    false  // direction
};

class Parser {
private:
  // Efficient I/O buffers
  static constexpr size_t BUFFER_SIZE = 1024 * 1024; // 1MB buffer
  std::vector<uint8_t> read_buffer_;
  std::vector<char> write_buffer_;
  
  // Optimization: pre-calculated asset info
  size_t estimated_total_records_ = 0;
  std::vector<TickRecord> asset_records_buffer_;

public:
  Parser();
  ~Parser();

  // Main parsing function
  void ParseAssetLifespan(const std::string &asset_code, 
                         const std::string &snapshot_dir,
                         const std::vector<std::string> &month_folders,
                         const std::string &output_dir);

private:
  // Core parsing functions
  std::vector<uint8_t> DecompressFile(const std::string &filepath);
  std::vector<TickRecord>
  ParseBinaryData(const std::vector<uint8_t> &binary_data);
  void ReverseDifferentialEncoding(std::vector<TickRecord> &records);
  void WriteRecordsToCSV(const std::vector<TickRecord> &records,
                         const std::string &symbol,
                         std::ofstream &csv_file,
                         bool write_header = false);

  // File system utilities
  size_t ExtractRecordCountFromFilename(const std::string &filename);
  std::string FindAssetFile(const std::string &month_folder, 
                           const std::string &asset_code);
  
  // Optimization: pre-calculate total records for efficient allocation
  size_t CalculateTotalRecordsForAsset(const std::string &asset_code,
                                      const std::string &snapshot_dir,
                                      const std::vector<std::string> &month_folders);

  // Formatting utilities
  inline double TickToPrice(int16_t tick) const { return tick * 0.01; }
  inline std::string FormatTime(uint16_t time_s) const;
  inline const char *FormatDirection(uint8_t direction) const;
};

} // namespace BinaryParser