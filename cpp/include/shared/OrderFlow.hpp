// OrderFlow - Optimized data structure for OrderFlow visualization
// Design:
//   - Even time indexing: global_idx = day_n * CAPACITY + local_idx
//   - Sparse storage: only store valid data points
//   - Pre-reserved vectors based on known capacities
//   - X-axis: uniform time display (HH:MM for L1, HH:MM:SS for L0)
#pragma once

#include "features/FeaturesDefine.hpp"
#include "features/backend/FeatureStoreConfig.hpp"
#include "gui/coro/CoroManager.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace OrderFlowConst {
// Capacity per day (from FeatureStoreConfig)
constexpr size_t L0_CAPACITY = MAX_ROWS_PER_LEVEL[0]; // ~15300 ticks/day
constexpr size_t L1_CAPACITY = MAX_ROWS_PER_LEVEL[1]; // ~255 bars/day
constexpr size_t LOB_DEPTH = 30;
} // namespace OrderFlowConst

// ============================================================================
// Time Structure
// ============================================================================

struct TimeHMS {
  uint8_t hour = 0;
  uint8_t minute = 0;
  uint8_t second = 0;
};

// ============================================================================
// L1 Day - Sparse storage for one day's minute bars
// ============================================================================

struct L1Day {
  std::string date;
  size_t day_idx = 0; // Day number in dataset (0, 1, 2, ...)

  // Sparse storage: only valid bars
  std::vector<size_t> indices; // Original minute indices [0, L1_CAPACITY)
  std::vector<float> open;
  std::vector<float> high;
  std::vector<float> low;
  std::vector<float> close;
  std::vector<float> volume;

  size_t valid_count() const { return indices.size(); }

  // Reserve space for expected valid count
  void reserve(size_t expected) {
    indices.reserve(expected);
    open.reserve(expected);
    high.reserve(expected);
    low.reserve(expected);
    close.reserve(expected);
    volume.reserve(expected);
  }

  // Add valid bar
  void push(size_t idx, float o, float h, float l, float c, float v) {
    indices.push_back(idx);
    open.push_back(o);
    high.push_back(h);
    low.push_back(l);
    close.push_back(c);
    volume.push_back(v);
  }

  // Get global X for plotting: day_idx * L1_CAPACITY + local_idx
  double global_x(size_t i) const {
    return static_cast<double>(day_idx * OrderFlowConst::L1_CAPACITY + indices[i]);
  }

  void clear() {
    date.clear();
    day_idx = 0;
    indices.clear();
    open.clear();
    high.clear();
    low.clear();
    close.clear();
    volume.clear();
  }
};

// ============================================================================
// L0 Day - Sparse storage for one day's ticks
// ============================================================================

struct L0Day {
  std::string date;
  size_t day_idx = 0;

  // Sparse storage: only valid ticks
  std::vector<size_t> indices; // Original tick indices [0, L0_CAPACITY)
  std::vector<float> mid_price;
  std::vector<std::array<float, OrderFlowConst::LOB_DEPTH>> bid_price;
  std::vector<std::array<float, OrderFlowConst::LOB_DEPTH>> ask_price;
  std::vector<std::array<float, OrderFlowConst::LOB_DEPTH>> bid_volume;
  std::vector<std::array<float, OrderFlowConst::LOB_DEPTH>> ask_volume;

  size_t valid_count() const { return indices.size(); }

  void reserve(size_t expected) {
    indices.reserve(expected);
    mid_price.reserve(expected);
    bid_price.reserve(expected);
    ask_price.reserve(expected);
    bid_volume.reserve(expected);
    ask_volume.reserve(expected);
  }

  // Add valid tick
  void push(size_t idx, float mid,
            const std::array<float, OrderFlowConst::LOB_DEPTH> &bp,
            const std::array<float, OrderFlowConst::LOB_DEPTH> &ap,
            const std::array<float, OrderFlowConst::LOB_DEPTH> &bv,
            const std::array<float, OrderFlowConst::LOB_DEPTH> &av) {
    indices.push_back(idx);
    mid_price.push_back(mid);
    bid_price.push_back(bp);
    ask_price.push_back(ap);
    bid_volume.push_back(bv);
    ask_volume.push_back(av);
  }

  // Get global X for plotting: day_idx * L0_CAPACITY + local_idx
  double global_x(size_t i) const {
    return static_cast<double>(day_idx * OrderFlowConst::L0_CAPACITY + indices[i]);
  }

  void clear() {
    date.clear();
    day_idx = 0;
    indices.clear();
    mid_price.clear();
    bid_price.clear();
    ask_price.clear();
    bid_volume.clear();
    ask_volume.clear();
  }
};

// ============================================================================
// L1 Cache - All dates, all assets (sparse, pre-reserved)
// ============================================================================

struct L1Cache {
  std::vector<std::string> dates;
  std::vector<std::vector<L1Day>> days; // [date_idx][asset_idx]
  std::map<std::string, size_t> date_to_idx;

  bool loaded = false;
  size_t num_assets = 0;
  size_t num_days = 0;

  // Pre-computed plot data per asset (only valid points, global X)
  struct AssetPlotData {
    std::vector<double> x;                  // Global X (day_n * L1_CAPACITY + minute_idx)
    std::vector<double> y;                  // Close price (valid only)
    std::vector<size_t> day_start_plot_idx; // Index in x/y where each day starts
    std::vector<size_t> day_start_global_x; // Global X at day start (for boundary lines)
  };
  std::vector<AssetPlotData> plot_data; // [asset_idx]

  // Build plot data for an asset (call after load)
  void build_plot_data(size_t asset_idx) {
    if (asset_idx >= num_assets)
      return;
    if (plot_data.size() < num_assets)
      plot_data.resize(num_assets);

    auto &pd = plot_data[asset_idx];
    pd.x.clear();
    pd.y.clear();
    pd.day_start_plot_idx.clear();
    pd.day_start_global_x.clear();

    for (size_t d = 0; d < num_days; ++d) {
      const auto &day = days[d][asset_idx];

      // Record day boundary
      pd.day_start_plot_idx.push_back(pd.x.size());
      pd.day_start_global_x.push_back(d * OrderFlowConst::L1_CAPACITY);

      // Add valid bars
      for (size_t i = 0; i < day.valid_count(); ++i) {
        pd.x.push_back(day.global_x(i));
        pd.y.push_back(static_cast<double>(day.close[i]));
      }
    }
  }

  // Get day index from global X
  size_t get_day_idx(double global_x) const {
    return static_cast<size_t>(global_x) / OrderFlowConst::L1_CAPACITY;
  }

  // Get date string from global X
  const std::string &get_date(double global_x) const {
    static const std::string empty;
    size_t d = get_day_idx(global_x);
    return (d < dates.size()) ? dates[d] : empty;
  }

  // Snap to day start global X
  double snap_to_day_start(double global_x) const {
    size_t d = get_day_idx(global_x);
    return static_cast<double>(d * OrderFlowConst::L1_CAPACITY);
  }

  void clear() {
    dates.clear();
    days.clear();
    date_to_idx.clear();
    plot_data.clear();
    loaded = false;
    num_assets = 0;
    num_days = 0;
  }
};

// ============================================================================
// L0 Cache - Single day, single asset (or multi-day in future)
// ============================================================================

struct L0Cache {
  std::vector<L0Day> days; // Support multi-day in future
  size_t asset_idx = 0;
  bool loaded = false;

  // Pre-computed plot data
  std::vector<double> plot_x;             // Global X (day_n * L0_CAPACITY + tick_idx)
  std::vector<double> plot_y;             // Mid price (valid only)
  std::vector<size_t> day_start_plot_idx; // Index in plot_x/y where each day starts
  std::vector<size_t> day_start_global_x; // Global X at day start

  // Build plot data (call after load)
  void build_plot_data() {
    plot_x.clear();
    plot_y.clear();
    day_start_plot_idx.clear();
    day_start_global_x.clear();

    for (const auto &day : days) {
      day_start_plot_idx.push_back(plot_x.size());
      day_start_global_x.push_back(day.day_idx * OrderFlowConst::L0_CAPACITY);

      for (size_t i = 0; i < day.valid_count(); ++i) {
        plot_x.push_back(day.global_x(i));
        plot_y.push_back(static_cast<double>(day.mid_price[i]));
      }
    }
  }

  // Get day index from global X
  size_t get_day_idx(double global_x) const {
    return static_cast<size_t>(global_x) / OrderFlowConst::L0_CAPACITY;
  }

  // Get tick local index from global X
  size_t get_local_idx(double global_x) const {
    return static_cast<size_t>(global_x) % OrderFlowConst::L0_CAPACITY;
  }

  // Find sparse index for a global X (binary search in plot_x)
  // Returns the index in plot_x/plot_y, or SIZE_MAX if not found
  size_t find_plot_idx(double global_x) const {
    auto it = std::lower_bound(plot_x.begin(), plot_x.end(), global_x);
    if (it == plot_x.end())
      return plot_x.empty() ? SIZE_MAX : plot_x.size() - 1;
    return static_cast<size_t>(it - plot_x.begin());
  }

  // Snap to next valid tick (in plot_x space)
  size_t snap_to_valid_plot_idx(double global_x) const {
    return find_plot_idx(global_x);
  }

  // Get LOB depth data at plot index
  struct DepthData {
    float mid_price = 0;
    const std::array<float, OrderFlowConst::LOB_DEPTH> *bid_price = nullptr;
    const std::array<float, OrderFlowConst::LOB_DEPTH> *ask_price = nullptr;
    const std::array<float, OrderFlowConst::LOB_DEPTH> *bid_volume = nullptr;
    const std::array<float, OrderFlowConst::LOB_DEPTH> *ask_volume = nullptr;
    size_t tick_idx = 0;
    size_t day_idx = 0;
    TimeHMS time{};
    bool valid = false;
  };

  DepthData get_depth(size_t plot_idx) const {
    DepthData d;
    if (plot_idx >= plot_x.size())
      return d;

    // Find which day this plot_idx belongs to
    size_t day_i = 0;
    for (size_t i = 1; i < day_start_plot_idx.size(); ++i) {
      if (plot_idx < day_start_plot_idx[i])
        break;
      day_i = i;
    }

    const auto &day = days[day_i];
    size_t local_plot_idx = plot_idx - day_start_plot_idx[day_i];
    if (local_plot_idx >= day.valid_count())
      return d;

    d.mid_price = day.mid_price[local_plot_idx];
    d.bid_price = &day.bid_price[local_plot_idx];
    d.ask_price = &day.ask_price[local_plot_idx];
    d.bid_volume = &day.bid_volume[local_plot_idx];
    d.ask_volume = &day.ask_volume[local_plot_idx];
    d.tick_idx = day.indices[local_plot_idx];
    d.day_idx = day.day_idx;

    // Convert tick index to time
    ClockTime ct = trading_seconds_to_clock(d.tick_idx);
    d.time.hour = ct.hour;
    d.time.minute = ct.minute;
    d.time.second = ct.second;
    d.valid = true;

    return d;
  }

  // Get date for plot index
  const std::string &get_date(size_t plot_idx) const {
    static const std::string empty;
    if (plot_idx >= plot_x.size() || days.empty())
      return empty;

    // Find which day
    size_t day_i = 0;
    for (size_t i = 1; i < day_start_plot_idx.size(); ++i) {
      if (plot_idx < day_start_plot_idx[i])
        break;
      day_i = i;
    }
    return days[day_i].date;
  }

  // Check if cache matches request
  bool matches(const std::string &date, size_t asset) const {
    if (!loaded || days.empty())
      return false;
    // For single day load, check if first day matches
    return days[0].date == date && asset_idx == asset;
  }

  size_t total_valid() const { return plot_x.size(); }

  void clear() {
    days.clear();
    asset_idx = 0;
    plot_x.clear();
    plot_y.clear();
    day_start_plot_idx.clear();
    day_start_global_x.clear();
    loaded = false;
  }
};

// ============================================================================
// Loader State (for L0 async loading via coroutine)
// ============================================================================

struct OrderFlowLoaderState {
  // L0 request (set by UI, consumed by coroutine)
  std::atomic<bool> l0_load_requested{false};
  std::string l0_request_date;
  size_t l0_request_asset = 0;

  // L1 reload flag (set after compute completes)
  std::atomic<bool> l1_needs_reload{false};

  // Coroutine lifecycle
  std::unique_ptr<CoroutineHandle> handle;
  std::atomic<bool> coro_running{false};  // True while coroutine is alive
  std::atomic<bool> coro_should_exit{false};  // Signal to exit

  void clear() {
    l0_load_requested = false;
    l0_request_date.clear();
    l0_request_asset = 0;
    l1_needs_reload = false;
    handle.reset();
    coro_running = false;
    coro_should_exit = false;
  }
};

// ============================================================================
// OrderFlow UI State
// ============================================================================

struct OrderFlowUI {
  int selected_asset_idx = 0;

  // L1 anchor (global X, snaps to day start)
  double l1_anchor_x = 0;
  std::string l1_anchor_date;

  // L0 anchor (plot index, snaps to valid tick)
  size_t l0_anchor_plot_idx = 0;

  // Track changes
  int prev_asset_idx = -1;
  std::string prev_l1_anchor_date;

  bool check_and_update() {
    bool changed = (selected_asset_idx != prev_asset_idx ||
                    l1_anchor_date != prev_l1_anchor_date);
    prev_asset_idx = selected_asset_idx;
    prev_l1_anchor_date = l1_anchor_date;
    return changed;
  }
};

// ============================================================================
// Main OrderFlow Data
// ============================================================================

struct OrderFlow {
  L1Cache l1;
  L0Cache l0;
  OrderFlowUI ui;
  OrderFlowLoaderState loader;

  void clear() {
    l1.clear();
    l0.clear();
    ui = OrderFlowUI{};
    loader.clear();
  }
};
