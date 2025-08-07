#include "technical_analysis.hpp"
#include "define/CBuffer.hpp"
#include "define/Dtype.hpp"
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

// #define PRINT_SNAPSHOT
// #define PRINT_BAR

#if defined(PRINT_BAR) || defined(PRINT_SNAPSHOT)
#include "misc/print.hpp"
#endif

TechnicalAnalysis::TechnicalAnalysis() {
  // Reserve memory for efficient operation - reduce reallocations
  continuous_snapshots_.reserve(100000);                    // Increased for better performance
  minute_bars_.reserve(15 * 250 * trade_hrs_in_a_day * 60); // 15 years of 1m bars

  // Note: CBuffer objects are fixed-size arrays, no reservation needed
}

TechnicalAnalysis::~TechnicalAnalysis() {
  // Cleanup completed
}

void TechnicalAnalysis::AnalyzeSnapshot(const Table::Snapshot_Record &snapshot) {
  // Optimized version with reduced calculations and better memory access patterns
  const float bid_price = snapshot.bid_price_ticks[0];
  const float ask_price = snapshot.ask_price_ticks[0];

  // Calculate derived metrics with conditional moves instead of branches
  const float mid_price = bid_price + ask_price * 0.5f;
  const float spread = ask_price - bid_price;

  // Optimize volume calculations
  const float volume_f = static_cast<float>(snapshot.volume);
  const float volume_scaled = volume_f * 100.0f;
  const float turnover_f = static_cast<float>(snapshot.turnover);
  const float vwap = (volume_f > 0) ? (turnover_f / volume_scaled) : snapshot_vwaps_.back();

  // Batch update analysis buffers for better cache locality
  snapshot_timestamps_.push_back(snapshot.seconds_in_day);
  snapshot_prices_.push_back(snapshot.latest_price_tick);
  snapshot_volumes_.push_back(volume_scaled);
  snapshot_turnovers_.push_back(turnover_f);
  snapshot_vwaps_.push_back(vwap);
  snapshot_directions_.push_back(snapshot.direction);
  snapshot_spreads_.push_back(spread);
  snapshot_mid_prices_.push_back(mid_price);

  // Debug output
#ifdef PRINT_SNAPSHOT
  println(
      static_cast<int>(snapshot.year),
      static_cast<int>(snapshot.month),
      static_cast<int>(snapshot.day),
      static_cast<int>(snapshot.hour),
      static_cast<int>(snapshot.minute),
      static_cast<int>(snapshot.second),
      snapshot.seconds_in_day,
      snapshot.latest_price_tick,
      snapshot.volume,
      vwap,
      spread,
      snapshot.direction);
#endif
}

void TechnicalAnalysis::AnalyzeMinuteBar(const Table::Bar_1m_Record &bar) {
  // Optimized VWAP calculation with branchless operation
  const float vwap = (bar.volume > PRICE_EPSILON) ? (bar.turnover / bar.volume) : 0.0f;

  // Batch update analysis buffers for better cache locality
  const uint32_t timestamp = static_cast<uint32_t>(bar.hour) * 60 + bar.minute;
  bar_timestamps_.push_back(timestamp);
  bar_opens_.push_back(bar.open);
  bar_highs_.push_back(bar.high);
  bar_lows_.push_back(bar.low);
  bar_closes_.push_back(bar.close);
  bar_volumes_.push_back(bar.volume);
  bar_turnovers_.push_back(bar.turnover);
  bar_vwaps_.push_back(vwap);

#ifdef PRINT_BAR
  println(
      static_cast<int>(bar.year),
      static_cast<int>(bar.month),
      static_cast<int>(bar.day),
      static_cast<int>(bar.hour),
      static_cast<int>(bar.minute),
      bar.open,
      bar.high,
      bar.low,
      bar.close,
      bar.volume,
      bar.turnover,
      vwap);
#endif
}

void TechnicalAnalysis::ProcessSingleSnapshot(const Table::Snapshot_Record &snapshot) {

  // Fill gaps by creating intermediate snapshots and processing each one
  if (has_previous_snapshot_) [[likely]] {
    uint32_t gap_time = last_processed_time_ + snapshot_interval;
    // NOTE: this also check the causality of last and current snapshot
    // making sure time between 2 days are not filled (as last(yesterday close) > current(today open))
    while (gap_time < snapshot.seconds_in_day) {
      GetGapSnapshot(gap_time);
      ProcessSnapshotInternal(gap_snapshot_);
      gap_time += snapshot_interval;
    }
  }

  // Process the actual incoming snapshot
  ProcessSnapshotInternal(snapshot);

  // Update state
  last_snapshot_ = snapshot;
  has_previous_snapshot_ = true;
  last_processed_time_ = snapshot.seconds_in_day;
}

void TechnicalAnalysis::ProcessSnapshotInternal(const Table::Snapshot_Record &snapshot) {

  // 1. Store in continuous snapshots table - use emplace_back for efficiency
  continuous_snapshots_.emplace_back(snapshot);

  // 2. Immediate snapshot analysis and buffer updates - optimized inline
  AnalyzeSnapshot(snapshot);

  // 3. Handle minute bar logic
  if (IsNewMinuteBar(snapshot)) [[unlikely]] {
    // Finalize current bar if exists and analyze it
    if (has_current_bar_) [[likely]] {
      FinalizeCurrentBar();
    }
    // Start new bar
    StartNewBar(snapshot);
  } else {
    // Update current bar
    UpdateCurrentBar(snapshot);
  }
}

bool TechnicalAnalysis::IsNewMinuteBar(const Table::Snapshot_Record &snapshot) {
  if (!has_current_bar_) [[unlikely]] {
    return true; // First bar
  }

  // Check if minute changed
  return (snapshot.hour != current_bar_.hour || snapshot.minute != current_bar_.minute);
}

void TechnicalAnalysis::FinalizeCurrentBar() {
  // Add to minute bars table - use emplace_back for efficiency
  minute_bars_.emplace_back(current_bar_);

  // Immediate analysis
  AnalyzeMinuteBar(current_bar_);
}

void TechnicalAnalysis::StartNewBar(const Table::Snapshot_Record &snapshot) {
  current_bar_.year = snapshot.year;
  current_bar_.month = snapshot.month;
  current_bar_.day = snapshot.day;
  current_bar_.hour = snapshot.hour;
  current_bar_.minute = snapshot.minute;
  current_bar_.open = snapshot.latest_price_tick;
  current_bar_.high = snapshot.latest_price_tick;
  current_bar_.low = snapshot.latest_price_tick;
  current_bar_.close = snapshot.latest_price_tick;
  current_bar_.volume = static_cast<float>(snapshot.volume * 100);
  current_bar_.turnover = static_cast<float>(snapshot.turnover);

  has_current_bar_ = true;
}

void TechnicalAnalysis::UpdateCurrentBar(const Table::Snapshot_Record &snapshot) {
  if (!has_current_bar_) [[unlikely]] {
    StartNewBar(snapshot);
    return;
  }

  const float current_price = snapshot.latest_price_tick;

  // Update OHLC with branchless min/max
  current_bar_.high = (current_price > current_bar_.high) ? current_price : current_bar_.high;
  current_bar_.low = (current_price < current_bar_.low) ? current_price : current_bar_.low;
  current_bar_.close = current_price;

  // Accumulate volume and turnover - reduce type conversions
  const float volume_scaled = static_cast<float>(snapshot.volume * 100);
  const float turnover_f = static_cast<float>(snapshot.turnover);
  current_bar_.volume += volume_scaled;
  current_bar_.turnover += turnover_f;
}
void TechnicalAnalysis::GetGapSnapshot(uint32_t timestamp) {
  // TODO

  // Preserve date and static price information from the last valid snapshot
  gap_snapshot_ = last_snapshot_;

  // Update intraday time components derived from the gap timestamp
  gap_snapshot_.seconds_in_day = static_cast<uint32_t>(timestamp);
  gap_snapshot_.hour = static_cast<uint8_t>(timestamp / 3600);
  gap_snapshot_.minute = static_cast<uint8_t>((timestamp % 3600) / 60);
  gap_snapshot_.second = static_cast<uint8_t>(timestamp % 60);

  // Reset trading activity for gap periods
  gap_snapshot_.trade_count = 0;
  gap_snapshot_.volume = 0;
  gap_snapshot_.turnover = 0;
}
// ============================================================================
// CSV OUTPUT UTILITIES
// ============================================================================

// Helper to convert price ticks to actual prices
inline double TickToPrice(int16_t tick) {
  return tick * 0.01;
}

namespace {

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
  if constexpr (std::is_same_v<RecordType, Table::Snapshot_Record>) {
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

    if constexpr (std::is_same_v<RecordType, Table::Snapshot_Record>) {
      // Manual field access instead of structured binding due to field count mismatch
      const auto &r = record;

      OutputField(batch_output, r.seconds_in_day, first);
      OutputField(batch_output, r.second, first);
      OutputField(batch_output, r.latest_price_tick, first);
      OutputField(batch_output, r.trade_count, first);
      OutputField(batch_output, r.turnover, first);
      OutputField(batch_output, r.volume, first);
      OutputArray(batch_output, r.bid_price_ticks, first);
      OutputArray(batch_output, r.bid_volumes, first);
      OutputArray(batch_output, r.ask_price_ticks, first);
      OutputArray(batch_output, r.ask_volumes, first);
      OutputField(batch_output, r.direction, first);

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

// Public interface methods
void TechnicalAnalysis::DumpSnapshotCSV(const std::string &asset_code, const std::string &output_dir, size_t last_n) const {
  DumpRecordsToCSV(continuous_snapshots_, asset_code, output_dir, "snapshot_3s", last_n);
}

void TechnicalAnalysis::DumpBarCSV(const std::string &asset_code, const std::string &output_dir, size_t last_n) const {
  DumpRecordsToCSV(minute_bars_, asset_code, output_dir, "bar_1m", last_n);
}
