// OrderFlow - Optimized data structure for OrderFlow visualization
// Design:
//   - Even time indexing: global_idx = day_n * CAPACITY + local_idx
//   - Sparse storage: only store valid data points
//   - Pre-reserved vectors based on known capacities
//   - X-axis: uniform time display (HH:MM for L1, HH:MM:SS for L0)
#pragma once

#include "features/backend/FeatureStoreConfig.hpp"
#include "gui/coro/CoroManager.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

// ============================================================================
// Constants
// ============================================================================

namespace OrderFlowConst {
// ============================================================================
// Data Capacity
// ============================================================================
constexpr size_t L0_CAPACITY = MAX_ROWS_PER_LEVEL[0]; // ~15300 ticks/day
constexpr size_t L1_CAPACITY = MAX_ROWS_PER_LEVEL[1]; // ~255 bars/day
constexpr size_t LOB_DEPTH = L2::LOB_DEPTH;           // 30 levels

// ============================================================================
// Price and Volume Conversion
// ============================================================================
constexpr float TICK_SIZE = 0.01f;           // Minimum price step (RMB)
constexpr float SHARES_PER_LOT = 100.0f;     // 1 lot = 100 shares
constexpr float PRICE_SCALE = 100.0f;        // Price stored as integer * 100
constexpr float ROUNDING_OFFSET = 0.5f;      // For float to int conversion
constexpr int32_t AMOUNT_ROUND_TO_RMB = 1000; // Round amount to nearest 100 RMB

// ============================================================================
// Cache Reserve Sizes (Aggressive Pre-allocation)
// ============================================================================
constexpr size_t ESTIMATED_PRICE_LEVELS = 100;      // Unique price levels per side
constexpr size_t ESTIMATED_RECTS_PER_LEVEL = 1000;  // Merged rects per price level
constexpr size_t MAX_KEYS_PER_TICK = LOB_DEPTH * 2; // bid30 + ask30 = 60

// ============================================================================
// Amount Thresholds (RMB)
// ============================================================================
constexpr float AMOUNT_MIN_VISIBLE = 1000.0f;     // 1K RMB (transparent in heatmap)
constexpr float AMOUNT_MAX_VISIBLE = 10000000.0f; // 10M RMB (solid in heatmap)
constexpr float AMOUNT_FILTER_MIN = 1000.0f;      // Filter sentinel data below this

// ============================================================================
// Price Validity Bounds (Sentinel Filtering)
// ============================================================================
constexpr float PRICE_MIN_VALID = 0.01f;  // Minimum valid price
constexpr float PRICE_MAX_VALID = 650.0f; // Maximum valid price (650 RMB)

// ============================================================================
// GUI Layout Parameters
// ============================================================================
constexpr float DEPTH_PANEL_WIDTH = 160.0f; // Width of depth panel (pixels)
constexpr float TOP_VIEW_RATIO = 0.55f;     // Top view height ratio (55%)
constexpr float Y_MARGIN_RATIO = 0.15f;     // Y-axis margin for plots (15%)

// ============================================================================
// GUI Rendering Parameters
// ============================================================================
constexpr float MIN_CANDLESTICK_BODY_HEIGHT = 1.0f; // Minimum visible body (pixels)
constexpr double CANDLESTICK_HALF_WIDTH = 0.5;      // Half width of candlestick bar

// ============================================================================
// Time Parameters
// ============================================================================
constexpr size_t L0_TICK_INTERVAL = 15 * 60; // 15 minutes in seconds (for tick labels)
} // namespace OrderFlowConst

// ============================================================================
// Basic Types
// ============================================================================

struct TimeHMS {
  uint8_t hour = 0;
  uint8_t minute = 0;
  uint8_t second = 0;
};

// ============================================================================
// L1 Data Structures (Minute-level, ~255 bars/day)
// ============================================================================

struct L1Day {
  std::string date;
  size_t day_idx = 0;

  // Sparse storage: only valid bars
  std::vector<size_t> indices;
  std::vector<float> open;
  std::vector<float> high;
  std::vector<float> low;
  std::vector<float> close;
  std::vector<float> volume;

  // Query
  size_t valid_count() const { return indices.size(); }
  double global_x(size_t i) const;

  // Modification
  void reserve(size_t expected);
  void push(size_t idx, float o, float h, float l, float c, float v);
  void clear();
};

struct L1Cache {
  std::vector<std::string> dates;
  std::vector<std::vector<L1Day>> days; // [date_idx][asset_idx]
  std::map<std::string, size_t> date_to_idx;

  bool loaded = false;
  size_t num_assets = 0;
  size_t num_days = 0;

  // Pre-computed plot data per asset
  struct AssetPlotData {
    std::vector<double> x;
    std::vector<double> open;
    std::vector<double> high;
    std::vector<double> low;
    std::vector<double> close;
    std::vector<size_t> day_start_plot_idx;
    std::vector<size_t> day_start_global_x;
    double y_min = 0.0;
    double y_max = 0.0;
    bool built = false;
  };
  std::vector<AssetPlotData> plot_data; // [asset_idx]

  // Query
  size_t get_day_idx(double global_x) const;
  const std::string &get_date(double global_x) const;
  double snap_to_day_start(double global_x) const;

  // Modification
  void build_plot_data(size_t asset_idx);
  void invalidate_plot_data();
  void clear();
};

// ============================================================================
// L0 Data Structures (Tick-level, ~15300 ticks/day)
// ============================================================================

// L0 Tick - Single tick with complete LOB snapshot
struct L0Tick {
  size_t tick_idx;

  // Validity flags
  bool depth_valid; // LOB depth buffer complete
  bool data_valid;  // Event-driven data present

  // LOB depth features (requires depth_valid=true)
  float mid_price;
  std::array<float, OrderFlowConst::LOB_DEPTH> bid_price;
  std::array<float, OrderFlowConst::LOB_DEPTH> ask_price;
  std::array<float, OrderFlowConst::LOB_DEPTH> bid_volume; // In lots (100 shares)
  std::array<float, OrderFlowConst::LOB_DEPTH> ask_volume; // In lots (100 shares)
};

struct L0Day {
  std::string date;
  size_t day_idx = 0;
  std::vector<L0Tick> ticks; // Sparse: only valid ticks

  // Query
  size_t valid_count() const { return ticks.size(); }
  double global_x(size_t i) const;

  // Modification
  void reserve(size_t expected);
  void push(size_t idx, bool depth_valid, bool data_valid, float mid,
            const std::array<float, OrderFlowConst::LOB_DEPTH> &bp,
            const std::array<float, OrderFlowConst::LOB_DEPTH> &ap,
            const std::array<float, OrderFlowConst::LOB_DEPTH> &bv,
            const std::array<float, OrderFlowConst::LOB_DEPTH> &av);
  void clear();
};

// ============================================================================
// L0 Heatmap Cache (Multi-level cache for efficient rendering)
// ============================================================================

// Level 1: Merged rectangles by price level
struct L0HeatmapMergedRect {
  size_t tick_start;
  size_t tick_end;
  float price_top;
  float price_bottom;
  int32_t amount_rmb; // Signed amount in RMB (rounded to integer, +bid/-ask)
};

struct L0HeatmapPriceLevel {
  float price;
  std::vector<L0HeatmapMergedRect> rects;

  void reserve(size_t expected);
  void clear();
};

struct L0HeatmapMergedCache {
  std::vector<L0HeatmapPriceLevel> levels; // Unified: all price levels (bid and ask)
  size_t data_version = 0;
  bool valid = false;

  void reserve_levels(size_t expected);
  void clear();
};

// Level 2: Colored rectangles ready for rendering
struct L0HeatmapColoredRect {
  double x1, y1, x2, y2;
  uint32_t color;
};

struct L0HeatmapColoredCache {
  std::vector<L0HeatmapColoredRect> rects;
  float cached_threshold = -1.0f;
  size_t cached_data_version = 0;
  bool valid = false;

  void reserve(size_t expected);
  void clear();
};

// ============================================================================
// L0 Cache (Main cache for L0 data and heatmap)
// ============================================================================

struct L0Cache {
  // Raw storage (sparse)
  std::vector<L0Day> days;
  size_t asset_idx = 0;
  bool loaded = false;
  size_t data_version = 0;

  // Plot data (for line plots: mid/bid/ask)
  std::vector<double> plot_t;
  std::vector<double> plot_mid_price;
  std::vector<double> plot_best_bid;
  std::vector<double> plot_best_ask;
  std::vector<size_t> day_start_plot_idx;
  std::vector<size_t> day_start_global_x;
  double y_min_with_margin = 0.0;
  double y_max_with_margin = 0.0;

  // Heatmap cache (multi-level)
  L0HeatmapMergedCache heatmap_merged_cache;
  L0HeatmapColoredCache heatmap_colored_cache;

  // Scratch buffers for heatmap build (reused across ticks, avoid reallocation)
  struct {
    std::vector<int> price_keys;     // Current tick's price keys (fixed capacity)
    std::vector<int32_t> amounts;    // Current tick's amounts (parallel to price_keys)
    std::vector<int> keys_to_update; // All keys needing update (fixed capacity)
  } heatmap_scratch_;

  // Query - Index conversion
  size_t get_day_idx(double global_x) const;
  size_t get_local_idx(double global_x) const;
  size_t find_plot_idx(double global_x) const;
  size_t snap_to_valid_plot_idx(double global_x) const;

  // Query - Data access
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
  DepthData get_depth(size_t plot_idx) const;
  const std::string &get_date(size_t plot_idx) const;

  // Query - Statistics
  struct ValidCounts {
    size_t rect_merged = 0; // Total merged rectangles in heatmap cache
    size_t depth_valid = 0; // Ticks with valid depth data
    size_t data_valid = 0;  // Ticks with valid event-driven data
  };
  ValidCounts count_valid() const;
  size_t total_valid() const { return plot_t.size(); }
  bool matches(const std::string &date, size_t asset) const;

  // Modification - Build caches
  void build_plot_data();
  void build_heatmap_merged_cache();
  void build_heatmap_colored_cache(float log_threshold);
  void clear();
};

// ============================================================================
// Auxiliary Structures
// ============================================================================

// Loader state for async L0 loading
struct OrderFlowLoaderState {
  std::atomic<bool> l0_load_requested{false};
  std::string l0_request_date;
  size_t l0_request_asset = 0;

  std::atomic<bool> l1_needs_reload{false};

  std::unique_ptr<CoroutineHandle> handle;
  std::atomic<bool> coro_running{false};
  std::atomic<bool> coro_should_exit{false};

  void clear();
};

// UI state tracking
struct OrderFlowUI {
  int selected_asset_idx = 0;

  double l1_anchor_x = 0;
  std::string l1_anchor_date;

  size_t l0_anchor_plot_idx = 0;

  bool show_heatmap = true;
  float log_amount_threshold = 3.0f; // log10(amount) [3.0, 7.0]

  int prev_asset_idx = -1;
  std::string prev_l1_anchor_date;

  bool check_and_update();
};

// ============================================================================
// Main OrderFlow Structure
// ============================================================================

struct OrderFlow {
  L1Cache l1;
  L0Cache l0;
  OrderFlowUI ui;
  OrderFlowLoaderState loader;

  void clear();
};
