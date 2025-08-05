#include "technical_analysis.hpp"
#include "define/CBuffer.hpp"
#include "define/Dtype.hpp"

TechnicalAnalysis::TechnicalAnalysis(std::vector<Table::Snapshot_3s_Record> &snapshots,
                                     std::vector<Table::Bar_1m_Record> &bars)
    : snapshots_(&snapshots), bars_(&bars), last_snapshot_time_(0), last_bar_time_(0) {
  // All CBuffers are already initialized as class members
}

void TechnicalAnalysis::fill_snapshot_gaps() {
  if (snapshots_->empty())
    return;

  auto &latest = snapshots_->back();
  int64_t current_time = latest.index_1m * 60 + latest.seconds;

  // If this is first record or time is valid (<=3s gap), update timer and return
  if (last_snapshot_time_ == 0 || current_time - last_snapshot_time_ <= snapshot_interval) {
    last_snapshot_time_ = current_time;
    snapshot_time.push_back(current_time);
    snapshot_price.push_back(latest.latest_price_tick / 100.0f);
    snapshot_volume.push_back(latest.volume * 100.0f);
    snapshot_turnover.push_back(latest.turnover / 100.0f);
    snapshot_vwap.push_back(latest.turnover / (latest.volume * 100.0f + 1e-6f));
    return;
  }

  // Fill gaps by duplicating last values
  while (last_snapshot_time_ + snapshot_interval < current_time) {
    last_snapshot_time_ += snapshot_interval;
    snapshot_time.push_back(last_snapshot_time_);
    snapshot_price.push_back(snapshot_price.back());
    snapshot_volume.push_back(0.0f);                // No volume in gap
    snapshot_turnover.push_back(0.0f);              // No turnover in gap
    snapshot_vwap.push_back(snapshot_price.back()); // Use last price as VWAP
  }

  last_snapshot_time_ = current_time;
}

void TechnicalAnalysis::fill_bar_gaps() {
  if (bars_->empty())
    return;

  auto &latest = bars_->back();
  int64_t current_time = (latest.hour * 60) + latest.minute;

  // If this is first record or time is valid (1m gap), update timer and return
  if (last_bar_time_ == 0 || current_time - last_bar_time_ <= 1) {
    last_bar_time_ = current_time;
    bar_time.push_back(current_time);
    bar_open.push_back(latest.open);
    bar_high.push_back(latest.high);
    bar_low.push_back(latest.low);
    bar_close.push_back(latest.close);
    bar_volume.push_back(latest.volume);
    bar_turnover.push_back(latest.turnover);
    return;
  }

  // Fill gaps by duplicating last values
  while (last_bar_time_ + 1 < current_time) {
    last_bar_time_ += 1;
    bar_time.push_back(last_bar_time_);
    float last_close = bar_close.back();
    bar_open.push_back(last_close);
    bar_high.push_back(last_close);
    bar_low.push_back(last_close);
    bar_close.push_back(last_close);
    bar_volume.push_back(0.0f);   // No volume in gap
    bar_turnover.push_back(0.0f); // No turnover in gap
  }

  last_bar_time_ = current_time;
}

void TechnicalAnalysis::update() {
  fill_snapshot_gaps();
  fill_bar_gaps();
}
