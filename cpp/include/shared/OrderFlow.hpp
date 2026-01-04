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
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/*
OrderFlow
├── L1Cache (分钟数据)
│   ├── Day (单日数据)
│   └── PlotData (预计算渲染数据)
├── L0Cache (Tick数据)
│   ├── Day (单日数据)
│   │   └── Tick (单个tick快照)
│   ├── PlotData (线图缓存)
│   ├── HeatmapMerged (热力图Level 2)
│   │   ├── Level (价格档位)
│   │   │   └── Rect (合并矩形)
│   │   └── Rect (矩形定义)
│   ├── HeatmapColored (热力图Level 3)
│   │   └── Rect (着色矩形)
│   ├── DepthSnapshot (深度查询结果)
│   ├── Stats (统计结果)
│   └── Scratch (临时缓冲区)
├── UI (用户状态)
└── Loader (异步加载)
*/

// ============================================================================
// Global Static Lookup Tables (Computed once at program startup)
// ============================================================================

// Time axis labels for L0 plots (trading seconds → time strings)
struct TimeAxisLUT {
  std::vector<size_t> l0_tick_offsets;     // [0, 900, 1800, ...] every 15min
  std::vector<std::string> l0_tick_labels; // ["09:15", "09:30", ...]

  static const TimeAxisLUT &instance();

private:
  TimeAxisLUT(); // Construct once in cpp
};

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
constexpr float TICK_SIZE = 0.01f;            // Minimum price step (RMB)
constexpr float SHARES_PER_LOT = 100.0f;      // 1 lot = N shares
constexpr float PRICE_SCALE = 100.0f;         // Price stored as integer * N
constexpr float ROUNDING_OFFSET = 0.5f;       // For float to int conversion
constexpr int32_t AMOUNT_ROUND_TO_RMB = 1000; // Round amount to nearest N RMB

// ============================================================================
// Cache Reserve Sizes (Aggressive Pre-allocation)
// ============================================================================
constexpr size_t ESTIMATED_PRICE_LEVELS = 100;      // Unique price levels per side
constexpr size_t ESTIMATED_RECTS_PER_LEVEL = 1000;  // Merged rects per price level
constexpr size_t MAX_KEYS_PER_TICK = LOB_DEPTH * 2; // bid30 + ask30 = 60

// ============================================================================
// Amount Thresholds (RMB)
// ============================================================================
constexpr float AMOUNT_MIN_VISIBLE = 1000.0f;      // 1K RMB (transparent in heatmap)
constexpr float AMOUNT_MAX_VISIBLE = 10000000.0f;  // 10M RMB (solid in heatmap)
constexpr float AMOUNT_FILTER_MIN = 1000.0f;       // Filter sentinel data below this
constexpr float DEPTH_BAR_MAX_AMOUNT = 1000000.0f; // 100W RMB (full bar in depth panel)

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
constexpr float Y_MARGIN_RATIO = 0.20f;     // Y-axis margin for plots (20%)

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
// Main OrderFlow Structure - All nested data structures
// ============================================================================

struct OrderFlow {
  // ==========================================================================
  // L1 Cache - Minute-level OHLCV data (~255 bars/day)
  // ==========================================================================

  struct L1Cache {
    // Nested: Single day data
    struct Day {
      std::string date;
      size_t day_idx = 0;

      std::vector<size_t> indices;
      std::vector<float> open, high, low, close, volume;

      size_t count_valid() const { return indices.size(); }
      double to_global_x(size_t i) const;
      void reserve(size_t n);
      void push(size_t idx, float o, float h, float l, float c, float v);
      void clear();
    };

    // Nested: Pre-computed plot data per asset
    struct PlotData {
      std::vector<double> x, open, high, low, close;
      std::vector<size_t> day_boundaries; // plot_idx where each day starts
      double y_min = 0.0, y_max = 0.0;
      bool valid = false;

      void invalidate() { valid = false; }
      void clear();
    };

    // Data members
    std::vector<std::string> dates;
    std::vector<std::vector<Day>> days; // [date_idx][asset_idx]
    std::map<std::string, size_t> date_to_idx;
    std::vector<PlotData> plot_data; // [asset_idx]

    bool loaded = false;
    size_t num_assets = 0;
    size_t num_days = 0;

    // Query methods
    size_t day_idx_from_x(double global_x) const;
    const std::string &date_from_x(double global_x) const;
    double snap_to_day_start(double global_x) const;

    // Build methods
    void build_plot_data(size_t asset_idx);
    void invalidate_all_plots();
    void clear();
  };

  // ==========================================================================
  // L0 Cache - Tick-level LOB depth data (~15300 ticks/day)
  // ==========================================================================

  struct L0Cache {
    // Nested: Single day data
    struct Day {
      // Nested: Single tick snapshot
      struct Tick {
        size_t tick_idx;
        bool depth_valid, data_valid;

        float mid_price;
        std::array<float, OrderFlowConst::LOB_DEPTH> bid_price, ask_price;
        std::array<float, OrderFlowConst::LOB_DEPTH> bid_volume; // SIGNED: > 0
        std::array<float, OrderFlowConst::LOB_DEPTH> ask_volume; // SIGNED: < 0
      };

      std::string date;
      size_t day_idx = 0;
      std::vector<Tick> ticks;

      size_t count_valid() const { return ticks.size(); }
      double to_global_x(size_t i) const;
      void reserve(size_t n);
      void push(size_t idx, bool depth_valid, bool data_valid, float mid,
                const std::array<float, OrderFlowConst::LOB_DEPTH> &bp,
                const std::array<float, OrderFlowConst::LOB_DEPTH> &ap,
                const std::array<float, OrderFlowConst::LOB_DEPTH> &bv,
                const std::array<float, OrderFlowConst::LOB_DEPTH> &av);
      void clear();
    };

    // Nested: Plot data cache (Level 1: sparse → continuous)
    struct PlotData {
      std::vector<double> x, mid_price, best_bid, best_ask;
      std::vector<size_t> day_boundaries; // plot_idx where each day starts
      std::vector<size_t> tick_indices;   // plot_idx -> day.ticks index (depth_valid only)
      double y_min = 0.0, y_max = 0.0;
      double y_min_with_margin = 0.0, y_max_with_margin = 0.0; // Pre-computed with margin

      // Index mapping: O(1) tick_idx → plot_idx lookup
      // tick_idx_map[tick_idx] = plot_idx (SIZE_MAX if tick not in plot)
      std::vector<size_t> tick_idx_map; // Size: L0_CAPACITY per day

      size_t version = 0;
      bool valid = false;

      void invalidate() { valid = false; }
      void clear();
    };

    // Nested: Heatmap merged cache (Level 2: per-tick → merged rects)
    struct HeatmapMerged {
      // Nested: Single merged rectangle
      struct Rect {
        size_t tick_start, tick_end;
        float price_high, price_low; // high >= low always
        int32_t amount_rmb;          // SIGNED: +bid, -ask
      };

      // Nested: Price level with merged rects
      struct Level {
        float price;
        std::vector<Rect> rects;

        void reserve(size_t n);
        void clear();
      };

      std::vector<Level> levels;

      size_t version = 0;
      bool valid = false;

      void reserve_levels(size_t n);
      void clear();
    };

    // Nested: Heatmap colored cache (Level 3: merged rects → colored rects)
    struct HeatmapColored {
      // Nested: Final colored rectangle for rendering
      struct Rect {
        double x1, y1, x2, y2;
        uint32_t color;
      };

      // Nested: Metadata for tooltip (avoids re-searching merged cache)
      struct Metadata {
        int32_t amount_rmb;
        float price; // bid: price_high, ask: price_low
        size_t tick_start, tick_end;
      };

      std::vector<Rect> rects;
      std::vector<Metadata> metadata; // Same size as rects, 1:1 mapping

      float threshold = -1.0f; // Cached log_amount_threshold
      size_t version = 0;      // Bound to HeatmapMerged::version
      bool valid = false;

      void reserve(size_t n);
      void clear();
    };

    // Nested: Query result for depth panel
    struct DepthSnapshot {
      float mid_price = 0;
      const std::array<float, OrderFlowConst::LOB_DEPTH> *bid_price = nullptr;
      const std::array<float, OrderFlowConst::LOB_DEPTH> *ask_price = nullptr;
      const std::array<float, OrderFlowConst::LOB_DEPTH> *bid_volume = nullptr;
      const std::array<float, OrderFlowConst::LOB_DEPTH> *ask_volume = nullptr;
      size_t tick_idx = 0;
      size_t day_idx = 0;
      struct {
        uint8_t hour, minute, second;
      } time;
      bool valid = false;
    };

    // Nested: Statistics result
    struct Stats {
      size_t heatmap_rects = 0;
      size_t depth_valid = 0;
      size_t data_valid = 0;
    };

    // Data members
    std::vector<Day> days;
    size_t asset_idx = 0;
    bool loaded = false;

    // Version control (incremented on data change, invalidates all caches)
    size_t version = 0;

    // Cache Level 1: Plot data
    PlotData plot;

    // Cache Level 2 & 3: Heatmap
    HeatmapMerged heatmap_merged;
    HeatmapColored heatmap_colored;

    // Scratch buffers (reused per tick during heatmap build)
    struct Scratch {
      // Current tick's data: price_key → amount_rmb
      std::unordered_map<int, int32_t> current_tick;

      // Keys to update (automatic dedup with unordered_set)
      std::unordered_set<int> keys_to_update;

      // Price key range for efficient iteration
      int min_key = (std::numeric_limits<int>::max)();
      int max_key = (std::numeric_limits<int>::min)();

      void clear_per_tick() {
        current_tick.clear();
        keys_to_update.clear();
        min_key = (std::numeric_limits<int>::max)();
        max_key = (std::numeric_limits<int>::min)();
      }
    } scratch_;

    // Query methods (coordinate conversion)
    size_t day_idx_from_x(double global_x) const;
    size_t local_idx_from_x(double global_x) const;
    size_t plot_idx_from_x(double global_x) const;
    size_t snap_to_valid_plot_idx(double global_x) const;

    // Query methods (data access)
    DepthSnapshot query_depth(size_t plot_idx) const;
    const std::string &date_from_plot_idx(size_t plot_idx) const;
    Stats compute_stats() const;
    bool matches(const std::string &date, size_t asset) const;

    // Build methods
    void build_plot();
    void build_heatmap_merged();
    void build_heatmap_colored(float log_threshold);

    // Utility methods
    void invalidate_all_caches(); // Increment version, clear all caches
    bool check_plot_cache() const { return plot.valid && plot.version == version; }
    bool check_heatmap_merged_cache() const { return heatmap_merged.valid && heatmap_merged.version == version; }
    bool check_heatmap_colored_cache(float threshold) const {
      return heatmap_colored.valid &&
             heatmap_colored.version == heatmap_merged.version &&
             heatmap_colored.threshold == threshold;
    }

    void clear();
  };

  // ==========================================================================
  // UI State - User interaction and rendering parameters
  // ==========================================================================

  struct UI {
    // Selection state
    int selected_asset_idx = 0;
    double l1_anchor_x = 0;
    std::string l1_anchor_date;
    size_t l0_anchor_plot_idx = 0;

    // Rendering parameters
    bool show_heatmap = true;
    float log_amount_threshold = 5.0f; // log10(amount) threshold [3.0, 7.0]

    // Change detection (avoid redundant reloads)
    int cached_asset_idx = -1;
    std::string cached_anchor_date;

    // L0 axis cache (avoid redundant tick label generation)
    std::vector<double> l0_tick_positions;
    std::vector<const char *> l0_tick_labels;
    size_t l0_cached_day_idx = SIZE_MAX;

    // L1 axis cache (avoid redundant tick label generation)
    std::vector<double> l1_tick_positions;
    std::vector<const char *> l1_tick_labels;
    std::vector<std::string> l1_tick_label_storage;
    size_t l1_cached_num_days = 0;

    // L0 load detection (for auto-zoom)
    bool prev_l0_loading = false;

    // Feature overlay Y2 range cache (for next frame setup)
    double feat_y2_min = 0.0;
    double feat_y2_max = 1.0;
    bool feat_y2_valid = false;

    bool detect_and_update_changes();
    void clear();
  };

  // ==========================================================================
  // Loader State - Async data loading coordination
  // ==========================================================================

  struct Loader {
    // L0 load request
    std::atomic<bool> l0_requested{false};
    std::string l0_date;
    size_t l0_asset = 0;

    // L1 reload flag
    std::atomic<bool> l1_needs_reload{false};

    // Coroutine handle
    std::unique_ptr<CoroutineHandle> coro;
    std::atomic<bool> coro_running{false};
    std::atomic<bool> coro_should_stop{false};

    void clear();
  };

  // ==========================================================================
  // Main Data Members
  // ==========================================================================

  L1Cache l1;
  L0Cache l0;
  UI ui;
  Loader loader;

  void clear();
};

// ============================================================================
// Helper Functions
// ============================================================================

// Convert volume and price to amount (RMB)
// NOTE: volume is SIGNED (bid > 0, ask < 0), so amount preserves the sign
inline float volume_to_amount(float volume, float price) {
  return volume * price * OrderFlowConst::SHARES_PER_LOT;
}

// Convert price to integer key for heatmap
inline int price_to_key(float price) {
  return static_cast<int>(price * OrderFlowConst::PRICE_SCALE + OrderFlowConst::ROUNDING_OFFSET);
}

// Round amount to nearest AMOUNT_ROUND_TO_RMB
inline int32_t round_amount_to_rmb(float amount) {
  return static_cast<int32_t>(std::round(amount / static_cast<float>(OrderFlowConst::AMOUNT_ROUND_TO_RMB))) * OrderFlowConst::AMOUNT_ROUND_TO_RMB;
}

// Convert amount (RMB) to 万元 (10K RMB)
inline float amount_to_wan(float amount) {
  return amount / 10000.0f;
}