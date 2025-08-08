#pragma once

#include "define/CBuffer.hpp"
#include "define/Dtype.hpp"
#include <cstdint>
#include <string>
#include <vector>


class TechnicalAnalysis {
public:
  TechnicalAnalysis();
  ~TechnicalAnalysis();

  // Main interface - processes single snapshot with richer info
  void ProcessSingleSnapshot(const Table::Snapshot_Record &snapshot);

  // Export functionality
  void DumpSnapshotCSV(const std::string &asset_code, const std::string &output_dir, size_t last_n = 0) const;
  void DumpBarCSV(const std::string &asset_code, const std::string &output_dir, size_t last_n = 0) const;

  // Access to internal data for size reporting
  size_t GetSnapshotCount() const { return continuous_snapshots_.size(); }
  size_t GetBarCount() const { return minute_bars_.size(); }

private:
  // Core unified processing function
  inline void ProcessSnapshotInternal(const Table::Snapshot_Record &snapshot);

  // Analysis functions
  inline void AnalyzeSnapshot(const Table::Snapshot_Record &snapshot);
  inline void AnalyzeMinuteBar(const Table::Bar_1m_Record &bar);

  // Bar management
  inline bool IsNewMinuteBar(const Table::Snapshot_Record &snapshot);
  inline void FinalizeCurrentBar();
  inline void StartNewBar(const Table::Snapshot_Record &snapshot);
  inline void UpdateCurrentBar(const Table::Snapshot_Record &snapshot);

  // Helper functions
  inline void GetGapSnapshot(uint32_t timestamp);

  // State tracking - grouped for cache efficiency
  bool has_previous_snapshot_ = false;
  bool has_current_bar_ = false;
  uint32_t last_processed_time_ = 0;
  Table::Snapshot_Record last_snapshot_;
  Table::Snapshot_Record gap_snapshot_;
  Table::Bar_1m_Record current_bar_;

  // Data storage
  std::vector<Table::Snapshot_Record> continuous_snapshots_;
  std::vector<Table::Bar_1m_Record> minute_bars_;

  // Analysis buffers for snapshot data
  CBuffer<uint32_t, BLen> snapshot_timestamps_;
  CBuffer<float, BLen> snapshot_prices_;
  CBuffer<float, BLen> snapshot_volumes_;
  CBuffer<float, BLen> snapshot_turnovers_;
  CBuffer<float, BLen> snapshot_vwaps_;
  CBuffer<uint8_t, BLen> snapshot_directions_;
  CBuffer<float, BLen> snapshot_spreads_;
  CBuffer<float, BLen> snapshot_mid_prices_;

  // Analysis buffers for minute bar data
  CBuffer<uint32_t, BLen> bar_timestamps_;
  CBuffer<float, BLen> bar_opens_;
  CBuffer<float, BLen> bar_highs_;
  CBuffer<float, BLen> bar_lows_;
  CBuffer<float, BLen> bar_closes_;
  CBuffer<float, BLen> bar_volumes_;
  CBuffer<float, BLen> bar_turnovers_;
  CBuffer<float, BLen> bar_vwaps_;
};

// Configuration constants
inline constexpr float PRICE_EPSILON = 1e-6f;
