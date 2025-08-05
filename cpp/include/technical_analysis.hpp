#pragma once

#include "define/Dtype.hpp"
#include "define/CBuffer.hpp"
#include <vector>

class TechnicalAnalysis {
public:
  TechnicalAnalysis(std::vector<Table::Snapshot_3s_Record> &snapshots,
                    std::vector<Table::Bar_1m_Record> &bars);

  void update();

private:
  void fill_snapshot_gaps();
  void fill_bar_gaps();
  
  // Input data references
  std::vector<Table::Snapshot_3s_Record> *snapshots_;
  std::vector<Table::Bar_1m_Record> *bars_;
  
  // Time tracking
  int64_t last_snapshot_time_; // in seconds since market open
  int64_t last_bar_time_;      // in minutes since market open
  
  // 3s snapshot buffers
  CBuffer<int64_t, BLen> snapshot_time;  // absolute seconds
  CBuffer<float, BLen> snapshot_price;    // latest price
  CBuffer<float, BLen> snapshot_volume;   // volume in this 3s
  CBuffer<float, BLen> snapshot_turnover; // turnover in this 3s
  CBuffer<float, BLen> snapshot_vwap;     // volume weighted avg price
  
  // 1m bar buffers
  CBuffer<int64_t, BLen> bar_time;  // absolute minutes
  CBuffer<float, BLen> bar_open;
  CBuffer<float, BLen> bar_high;
  CBuffer<float, BLen> bar_low;
  CBuffer<float, BLen> bar_close;
  CBuffer<float, BLen> bar_volume;
  CBuffer<float, BLen> bar_turnover;
};
