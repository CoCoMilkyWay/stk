#pragma once

#include "technical_analysis.hpp"
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace BinaryParser {

// ============================================================================
// DATA STRUCTURES AND CONSTANTS
// ============================================================================

// Binary record structure (54 bytes total)
#pragma pack(push, 1)
struct BinaryRecord {
  bool sync;                  // 1 byte
  uint8_t day;                // 1 byte
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

// Differential encoding configuration
constexpr bool DIFF_FIELDS[] = {
    false, // sync
    true,  // day
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

// ============================================================================
// MAIN PARSER CLASS
// ============================================================================

class Parser {
public:
  // Constructor and destructor
  Parser();
  ~Parser();

  // Main public interface
  void ParseAsset(const std::string &asset_code,
                  const std::string &snapshot_dir,
                  const std::vector<std::string> &month_folders,
                  const std::string &output_dir);

private:
  // ========================================================================
  // CORE PARSING FUNCTIONS
  // ========================================================================

  // File I/O and decompression
  std::vector<uint8_t> DecompressFile(const std::string &filepath, size_t record_count);
  std::vector<BinaryRecord> ParseBinaryData(const std::vector<uint8_t> &binary_data);
  void ReverseDifferentialEncoding(std::vector<BinaryRecord> &records);

  // ========================================================================
  // DATA CONVERSION FUNCTIONS
  // ========================================================================

  void ProcessBinaryRecords(const std::vector<BinaryRecord> &binary_records, uint16_t year, uint8_t month);

  // ========================================================================
  // FILE SYSTEM UTILITIES
  // ========================================================================

  std::string FindAssetFile(const std::string &month_folder,
                            const std::string &asset_code);
  size_t ExtractRecordCountFromFilename(const std::string &filename);
  size_t CalculateTotalRecordsForAsset(const std::string &asset_code,
                                       const std::string &snapshot_dir,
                                       const std::vector<std::string> &month_folders);

  // Helper function to extract record count and year/month from filename and folder
  std::tuple<size_t, uint16_t, uint8_t> ExtractRecordCountAndDateFromPath(const std::string &filepath);

  // ========================================================================
  // FORMATTING UTILITIES
  // ========================================================================

  inline float TickToPrice(int16_t tick) const { return static_cast<float>(tick * 0.01); }

  // ========================================================================
  // MEMBER VARIABLES
  // ========================================================================

  // Buffer configuration
  static constexpr size_t BUFFER_SIZE = 1024 * 1024; // 1MB buffer

  // I/O buffers
  std::vector<uint8_t> read_buffer_;
  std::vector<char> write_buffer_;

  // Progress tracking
  size_t total_records_ = 0;
  size_t records_count_ = 0;

  // Technical analysis engine
  std::unique_ptr<::TechnicalAnalysis> technical_analysis_;
};

} // namespace BinaryParser