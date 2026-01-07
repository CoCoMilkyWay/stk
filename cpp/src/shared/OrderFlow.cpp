// OrderFlow Implementation
#include "shared/OrderFlow.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <cmath>
#include <cstdio>

// ============================================================================
// TimeAxisLUT Implementation (Global Singleton)
// ============================================================================

TimeAxisLUT::TimeAxisLUT() {
  // Pre-compute L0 time axis labels (every 15 minutes = 900 seconds)
  // offset is time index (0-15299), directly used for X-axis positioning
  // These offsets MUST match the tick_idx stored in tensor data
  for (size_t offset = 0; offset < OrderFlowConst::L0_CAPACITY; offset += OrderFlowConst::L0_TICK_INTERVAL) {
    l0_tick_offsets.push_back(offset);
    
    ClockTime ct = L0_to_Clock(offset);
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%02d:%02d", ct.hour, ct.minute);
    l0_tick_labels.push_back(buf);
  }
}

const TimeAxisLUT& TimeAxisLUT::instance() {
  static TimeAxisLUT lut;
  return lut;
}

// ============================================================================
// OrderFlow::L1Cache::Day Implementation
// ============================================================================

double OrderFlow::L1Cache::Day::to_global_x(size_t i) const {
  return static_cast<double>(day_idx * OrderFlowConst::L1_CAPACITY + indices[i]);
}

void OrderFlow::L1Cache::Day::reserve(size_t n) {
  indices.reserve(n);
  open.reserve(n);
  high.reserve(n);
  low.reserve(n);
  close.reserve(n);
  volume.reserve(n);
}

void OrderFlow::L1Cache::Day::push(size_t idx, float o, float h, float l, float c, float v) {
  indices.push_back(idx);
  open.push_back(o);
  high.push_back(h);
  low.push_back(l);
  close.push_back(c);
  volume.push_back(v);
}

void OrderFlow::L1Cache::Day::clear() {
  date.clear();
  day_idx = 0;
  indices.clear();
  open.clear();
  high.clear();
  low.clear();
  close.clear();
  volume.clear();
}

// ============================================================================
// OrderFlow::L1Cache::PlotData Implementation
// ============================================================================

void OrderFlow::L1Cache::PlotData::clear() {
  x.clear();
  open.clear();
  high.clear();
  low.clear();
  close.clear();
  day_boundaries.clear();
  y_min = y_max = 0.0;
  valid = false;
}

// ============================================================================
// OrderFlow::L1Cache Implementation
// ============================================================================

size_t OrderFlow::L1Cache::day_idx_from_x(double global_x) const {
  return static_cast<size_t>(global_x) / OrderFlowConst::L1_CAPACITY;
}

const std::string &OrderFlow::L1Cache::date_from_x(double global_x) const {
  static const std::string empty;
  size_t d = day_idx_from_x(global_x);
  return (d < dates.size()) ? dates[d] : empty;
}

double OrderFlow::L1Cache::snap_to_day_start(double global_x) const {
  size_t d = day_idx_from_x(global_x);
  return static_cast<double>(d * OrderFlowConst::L1_CAPACITY);
}

void OrderFlow::L1Cache::build_plot_data(size_t asset_idx) {
  Trace;
  
  if (asset_idx >= num_assets)
    return;
  if (plot_data.size() < num_assets)
    plot_data.resize(num_assets);

  auto &pd = plot_data[asset_idx];
  if (pd.valid)
    return;

  pd.clear();

  for (size_t d = 0; d < num_days; ++d) {
    const auto &day = days[d][asset_idx];

    pd.day_boundaries.push_back(pd.x.size());

    for (size_t i = 0; i < day.count_valid(); ++i) {
      pd.x.push_back(day.to_global_x(i));
      pd.open.push_back(static_cast<double>(day.open[i]));
      pd.high.push_back(static_cast<double>(day.high[i]));
      pd.low.push_back(static_cast<double>(day.low[i]));
      pd.close.push_back(static_cast<double>(day.close[i]));
    }
  }

  if (!pd.low.empty()) {
    pd.y_min = *std::min_element(pd.low.begin(), pd.low.end());
    pd.y_max = *std::max_element(pd.high.begin(), pd.high.end());
    pd.valid = true;
  }
}

void OrderFlow::L1Cache::invalidate_all_plots() {
  for (auto &pd : plot_data) {
    pd.invalidate();
  }
}

void OrderFlow::L1Cache::clear() {
  dates.clear();
  days.clear();
  date_to_idx.clear();
  plot_data.clear();
  loaded = false;
  num_assets = 0;
  num_days = 0;
}

// ============================================================================
// OrderFlow::L0Cache::Day::Tick Implementation (inline, no cpp needed)
// ============================================================================

// ============================================================================
// OrderFlow::L0Cache::Day Implementation
// ============================================================================

double OrderFlow::L0Cache::Day::to_global_x(size_t i) const {
  return static_cast<double>(day_idx * OrderFlowConst::L0_CAPACITY + ticks[i].tick_idx);
}

void OrderFlow::L0Cache::Day::reserve(size_t n) {
  ticks.reserve(n);
}

void OrderFlow::L0Cache::Day::push(size_t idx, bool depth_valid, bool data_valid, float mid,
                 const std::array<float, OrderFlowConst::LOB_DEPTH> &bp,
                 const std::array<float, OrderFlowConst::LOB_DEPTH> &ap,
                 const std::array<float, OrderFlowConst::LOB_DEPTH> &bv,
                 const std::array<float, OrderFlowConst::LOB_DEPTH> &av) {
  ticks.push_back({idx, depth_valid, data_valid, mid, bp, ap, bv, av});
}

void OrderFlow::L0Cache::Day::clear() {
  date.clear();
  day_idx = 0;
  ticks.clear();
}

// ============================================================================
// OrderFlow::L0Cache::PlotData Implementation
// ============================================================================

void OrderFlow::L0Cache::PlotData::clear() {
  x.clear();
  mid_price.clear();
  best_bid.clear();
  best_ask.clear();
  day_boundaries.clear();
  tick_indices.clear();
  tick_idx_map.clear();
  y_min = y_max = 0.0;
  version = 0;
  valid = false;
}

// ============================================================================
// OrderFlow::L0Cache::HeatmapMerged::Level Implementation
// ============================================================================

void OrderFlow::L0Cache::HeatmapMerged::Level::reserve(size_t n) {
  rects.reserve(n);
}

void OrderFlow::L0Cache::HeatmapMerged::Level::clear() {
  rects.clear();
}

// ============================================================================
// OrderFlow::L0Cache::HeatmapMerged Implementation
// ============================================================================

void OrderFlow::L0Cache::HeatmapMerged::reserve_levels(size_t n) {
  levels.reserve(n);
}

void OrderFlow::L0Cache::HeatmapMerged::clear() {
  levels.clear();
  valid = false;
}

// ============================================================================
// OrderFlow::L0Cache::HeatmapColored Implementation
// ============================================================================

void OrderFlow::L0Cache::HeatmapColored::reserve(size_t n) {
  rects.reserve(n);
  metadata.reserve(n);
}

void OrderFlow::L0Cache::HeatmapColored::clear() {
  rects.clear();
  metadata.clear();
  valid = false;
}

// ============================================================================
// OrderFlow::L0Cache Implementation
// ============================================================================

size_t OrderFlow::L0Cache::day_idx_from_x(double global_x) const {
  return static_cast<size_t>(global_x) / OrderFlowConst::L0_CAPACITY;
}

size_t OrderFlow::L0Cache::local_idx_from_x(double global_x) const {
  return static_cast<size_t>(global_x) % OrderFlowConst::L0_CAPACITY;
}

size_t OrderFlow::L0Cache::plot_idx_from_x(double global_x) const {
  auto it = std::lower_bound(plot.x.begin(), plot.x.end(), global_x);
  if (it == plot.x.end())
    return plot.x.empty() ? SIZE_MAX : plot.x.size() - 1;
  return static_cast<size_t>(it - plot.x.begin());
}

size_t OrderFlow::L0Cache::snap_to_valid_plot_idx(double global_x) const {
  // Fast path: O(1) lookup if tick_idx_map is built
  size_t global_tick_idx = static_cast<size_t>(global_x);
  if (global_tick_idx < plot.tick_idx_map.size()) {
    size_t mapped = plot.tick_idx_map[global_tick_idx];
    if (mapped != SIZE_MAX) {
      return mapped;
    }
  }
  
  // Fallback: O(log n) binary search
  return plot_idx_from_x(global_x);
}

OrderFlow::L0Cache::DepthSnapshot OrderFlow::L0Cache::query_depth(size_t plot_idx) const {
  DepthSnapshot result;
  if (plot_idx >= plot.x.size())
    return result;

  // Find which day this plot_idx belongs to
  size_t day_i = 0;
  for (size_t i = 1; i < plot.day_boundaries.size(); ++i) {
    if (plot_idx < plot.day_boundaries[i])
      break;
    day_i = i;
  }

  const auto &day = days[day_i];
  if (plot_idx >= plot.tick_indices.size())
    return result;

  size_t tick_i = plot.tick_indices[plot_idx];
  if (tick_i >= day.count_valid())
    return result;

  const auto &tick = day.ticks[tick_i];
  result.mid_price = tick.mid_price;
  result.bid_price = &tick.bid_price;
  result.ask_price = &tick.ask_price;
  result.bid_volume = &tick.bid_volume;
  result.ask_volume = &tick.ask_volume;
  result.tick_idx = tick.tick_idx;
  result.day_idx = day.day_idx;

  // Convert tick index to time
  ClockTime ct = L0_to_Clock(result.tick_idx);
  result.time.hour = ct.hour;
  result.time.minute = ct.minute;
  result.time.second = ct.second;
  result.valid = true;

  return result;
}

const std::string &OrderFlow::L0Cache::date_from_plot_idx(size_t plot_idx) const {
  static const std::string empty;
  if (plot_idx >= plot.x.size() || days.empty())
    return empty;

  size_t day_i = 0;
  for (size_t i = 1; i < plot.day_boundaries.size(); ++i) {
    if (plot_idx < plot.day_boundaries[i])
      break;
    day_i = i;
  }
  return days[day_i].date;
}

OrderFlow::L0Cache::Stats OrderFlow::L0Cache::compute_stats() const {
  Stats stats;

  // Count merged rectangles in heatmap cache
  if (heatmap_merged.valid) {
    for (const auto &level : heatmap_merged.levels) {
      stats.heatmap_rects += level.rects.size();
    }
  }

  // Count valid ticks
  for (const auto &day : days) {
    for (const auto &tick : day.ticks) {
      if (tick.depth_valid)
        ++stats.depth_valid;
      if (tick.data_valid)
        ++stats.data_valid;
    }
  }

  return stats;
}

bool OrderFlow::L0Cache::matches(const std::string &date, size_t asset) const {
  if (!loaded || days.empty())
    return false;
  return days[0].date == date && asset_idx == asset;
}

void OrderFlow::L0Cache::build_plot() {
  plot.clear();

  // Reserve space for plot data
  size_t total_estimated = days.size() * OrderFlowConst::L0_CAPACITY;
  plot.x.reserve(total_estimated);
  plot.mid_price.reserve(total_estimated);
  plot.best_bid.reserve(total_estimated);
  plot.best_ask.reserve(total_estimated);
  plot.tick_indices.reserve(total_estimated);
  
  // Pre-allocate tick_idx_map for all days (O(1) lookup)
  plot.tick_idx_map.resize(days.size() * OrderFlowConst::L0_CAPACITY, SIZE_MAX);

  for (const auto &day : days) {
    plot.day_boundaries.push_back(plot.x.size());
    size_t day_base = day.day_idx * OrderFlowConst::L0_CAPACITY;

    // Only add depth_valid ticks (sparse)
    for (size_t i = 0; i < day.count_valid(); ++i) {
      const auto &tick = day.ticks[i];
      if (!tick.depth_valid)
        continue;

      assert(tick.tick_idx < OrderFlowConst::L0_CAPACITY && "tick_idx out of intra-day range");

      size_t plot_idx = plot.x.size();
      double global_x = day.to_global_x(i);
      plot.x.push_back(global_x);
      plot.mid_price.push_back(static_cast<double>(tick.mid_price));
      plot.best_bid.push_back(static_cast<double>(tick.bid_price[0]));
      plot.best_ask.push_back(static_cast<double>(tick.ask_price[0]));
      plot.tick_indices.push_back(i);
      
      // Build reverse mapping: tick_idx → plot_idx
      size_t global_tick_idx = day_base + tick.tick_idx;
      if (global_tick_idx < plot.tick_idx_map.size()) {
        plot.tick_idx_map[global_tick_idx] = plot_idx;
      }
    }
  }

  // Cache Y range with margin (pre-compute for rendering)
  if (!plot.mid_price.empty()) {
    plot.y_min = *std::min_element(plot.mid_price.begin(), plot.mid_price.end());
    plot.y_max = *std::max_element(plot.mid_price.begin(), plot.mid_price.end());
    
    double y_range = plot.y_max - plot.y_min;
    double margin = std::max(y_range * OrderFlowConst::Y_MARGIN_RATIO, 0.1);
    plot.y_min_with_margin = plot.y_min - margin;
    plot.y_max_with_margin = plot.y_max + margin;
  }

  plot.version = version;
  plot.valid = true;
}

void OrderFlow::L0Cache::build_heatmap_merged() {
  heatmap_merged.clear();

  if (days.empty())
    return;

  constexpr size_t DEPTH = OrderFlowConst::LOB_DEPTH;

  heatmap_merged.reserve_levels(OrderFlowConst::ESTIMATED_PRICE_LEVELS * 2);

  // Unified map: price_key -> level index (handles both bid and ask)
  std::map<int, size_t> price_to_level;

  // Helper lambda to update a single price level with amount
  auto update_price_level = [&](int price_key, float price, int32_t amount_rmb, size_t global_tick_idx) {
    auto [it, inserted] = price_to_level.try_emplace(price_key, 0);
    if (inserted) {
      // New price level: create and reserve
      size_t level_idx = heatmap_merged.levels.size();
      it->second = level_idx;
      heatmap_merged.levels.push_back({price, {}});
      heatmap_merged.levels[level_idx].reserve(OrderFlowConst::ESTIMATED_RECTS_PER_LEVEL);
    }

    size_t level_idx = it->second;
    auto &level = heatmap_merged.levels[level_idx];

    if (!level.rects.empty()) {
      auto &last_rect = level.rects.back();
      if (last_rect.amount_rmb == amount_rmb) {
        // Same signed volume: extend (挂单未动)
        last_rect.tick_end = global_tick_idx + 1;
        return;
      } else {
        // Volume changed: close last rect (保证首尾相连)
        last_rect.tick_end = global_tick_idx;
      }
    }

    // Skip creating rect for amount=0 if no previous rect exists
    if (amount_rmb == 0 && level.rects.empty())
      return;

    // Create new rectangle
    // NOTE: Use price_high/price_low naming (always high >= low)
    // - Bid (amount > 0): high=price, low=price-tick → rect extends downward
    // - Ask (amount < 0): high=price+tick, low=price → rect extends upward
    bool is_bid = (amount_rmb > 0);
    float price_high, price_low;
    
    if (is_bid) {
      price_high = price;
      price_low = price - OrderFlowConst::TICK_SIZE;
    } else {
      price_high = price + OrderFlowConst::TICK_SIZE;
      price_low = price;
    }

    level.rects.push_back({global_tick_idx,
                           global_tick_idx + 1,
                           price_high,
                           price_low,
                           amount_rmb});
  };

  for (const auto &day : days) {
    size_t day_base = day.day_idx * OrderFlowConst::L0_CAPACITY;

    for (size_t tick_i = 0; tick_i < day.count_valid(); ++tick_i) {
      const auto &tick = day.ticks[tick_i];

      if (!tick.depth_valid)
        continue;

      size_t global_tick_idx = day_base + tick.tick_idx;

      // Step 1: Collect current tick's bid/ask data (use new scratch structure)
      scratch_.clear_per_tick();

      // Helper lambda to collect a single price level
      // NOTE: volume is SIGNED (bid_volume > 0, ask_volume < 0), so amount_rmb preserves sign
      auto collect_level = [&](float price, float volume) {
        if (price <= 0)
          return;
        
        float amount_float = volume_to_amount(volume, price);  // Preserves sign: bid+, ask-
        int32_t amount_rmb = round_amount_to_rmb(amount_float);

        if (amount_rmb != 0) {
          int price_key = price_to_key(price);
          scratch_.current_tick[price_key] = amount_rmb;
          scratch_.min_key = std::min(scratch_.min_key, price_key);
          scratch_.max_key = std::max(scratch_.max_key, price_key);
        }
      };

      // Collect bid and ask levels
      for (size_t level = 0; level < DEPTH; ++level) {
        collect_level(tick.bid_price[level], tick.bid_volume[level]);
        collect_level(tick.ask_price[level], tick.ask_volume[level]);
      }

      // Step 2: Collect all price keys that need updating
      // Add current tick's keys
      for (const auto &[key, amount] : scratch_.current_tick) {
        scratch_.keys_to_update.insert(key);
      }

      // Add existing keys within current min/max range (may need to close as amount=0)
      if (scratch_.min_key <= scratch_.max_key) {
        for (const auto &[key, level_idx] : price_to_level) {
          if (key >= scratch_.min_key && key <= scratch_.max_key) {
            // Check if this level has active rects that need closing
            const auto &level = heatmap_merged.levels[level_idx];
            if (!level.rects.empty()) {
              scratch_.keys_to_update.insert(key);
            }
          }
        }
      }

      // Step 3: Update only the collected keys
      for (int price_key : scratch_.keys_to_update) {
        float price = static_cast<float>(price_key) / OrderFlowConst::PRICE_SCALE;

        // O(1) lookup in unordered_map
        int32_t amount_rmb = 0;
        auto it = scratch_.current_tick.find(price_key);
        if (it != scratch_.current_tick.end()) {
          amount_rmb = it->second;
        }

        update_price_level(price_key, price, amount_rmb, global_tick_idx);
      }
    }
  }

  heatmap_merged.version = version;
  heatmap_merged.valid = true;
}

// Helper: Map log10(amount) to color intensity [0, 1]
static float map_amount_to_intensity(float amount, float log_threshold) {
  float abs_amount = std::abs(amount);
  if (abs_amount < std::pow(10.0f, log_threshold))
    return 0.0f; // Below threshold

  float log_amount = std::log10(abs_amount);
  float log_max = std::log10(OrderFlowConst::AMOUNT_MAX_VISIBLE);

  // Map [threshold, log_max] to [0, 1]
  float normalized = (log_amount - log_threshold) / (log_max - log_threshold);
  return std::min(1.0f, std::max(0.0f, normalized));
}

// Helper: Convert signed amount (int32_t) to color based on sign and intensity
// Positive amount (bid) = green, Negative amount (ask) = red
// Returns RGBA color packed as uint32_t (compatible with ImU32)
static uint32_t amount_to_color(int32_t amount_rmb, float log_threshold) {
  float amount = static_cast<float>(amount_rmb);
  float intensity = map_amount_to_intensity(amount, log_threshold);
  if (intensity <= 0.0f)
    return 0; // Transparent (0x00000000)

  uint8_t alpha = static_cast<uint8_t>(intensity * 200 + 55); // [55, 255]

  if (amount_rmb > 0) {
    // Positive amount (bid side): green spectrum
    uint8_t g = static_cast<uint8_t>(100 + intensity * 155);
    uint8_t b = static_cast<uint8_t>(intensity * 100);
    // Pack as ABGR (ImGui format)
    return static_cast<uint32_t>(alpha) << 24 | static_cast<uint32_t>(b) << 16 | 
           static_cast<uint32_t>(g) << 8 | 0;
  } else {
    // Negative amount (ask side): red spectrum
    uint8_t r = static_cast<uint8_t>(150 + intensity * 105);
    uint8_t g = static_cast<uint8_t>(intensity * 50);
    // Pack as ABGR (ImGui format)
    return static_cast<uint32_t>(alpha) << 24 | 0 << 16 | 
           static_cast<uint32_t>(g) << 8 | static_cast<uint32_t>(r);
  }
}

void OrderFlow::L0Cache::build_heatmap_colored(float log_threshold) {
  heatmap_colored.clear();

  if (!heatmap_merged.valid)
    return;

  // Reserve space for colored rects and metadata (aggressive allocation)
  constexpr size_t ESTIMATED_COLORED_RECTS =
      OrderFlowConst::ESTIMATED_PRICE_LEVELS * OrderFlowConst::ESTIMATED_RECTS_PER_LEVEL;

  heatmap_colored.reserve(ESTIMATED_COLORED_RECTS);

  // Convert merged rects to colored rects with metadata
  for (const auto &level : heatmap_merged.levels) {
    for (const auto &merged_rect : level.rects) {
      uint32_t color = amount_to_color(merged_rect.amount_rmb, log_threshold);
      if (color == 0) // Skip transparent
        continue;

      // Convert tick indices to global X coordinates
      double x1 = static_cast<double>(merged_rect.tick_start);
      double x2 = static_cast<double>(merged_rect.tick_end);
      double y1 = static_cast<double>(merged_rect.price_high);
      double y2 = static_cast<double>(merged_rect.price_low);

      // Pick price based on side: bid (positive) uses high, ask (negative) uses low
      float display_price = merged_rect.amount_rmb > 0 ? merged_rect.price_high : merged_rect.price_low;

      heatmap_colored.rects.push_back({x1, y1, x2, y2, color});
      
      HeatmapColored::Metadata meta;
      meta.amount_rmb = merged_rect.amount_rmb;
      meta.price = display_price;
      meta.tick_start = merged_rect.tick_start;
      meta.tick_end = merged_rect.tick_end;
      heatmap_colored.metadata.push_back(meta);
    }
  }

  heatmap_colored.threshold = log_threshold;
  heatmap_colored.version = heatmap_merged.version;
  heatmap_colored.valid = true;
}

void OrderFlow::L0Cache::invalidate_all_caches() {
  ++version;
  plot.invalidate();
  heatmap_merged.clear();
  heatmap_colored.clear();
}

void OrderFlow::L0Cache::clear() {
  days.clear();
  asset_idx = 0;
  plot.clear();
  heatmap_merged.clear();
  heatmap_colored.clear();
  loaded = false;
  version = 0;
}

// ============================================================================
// OrderFlow::UI Implementation
// ============================================================================

bool OrderFlow::UI::detect_and_update_changes() {
  bool changed = (cached_asset_idx != selected_asset_idx ||
                  cached_anchor_date != l1_anchor_date);
  if (changed) {
    cached_asset_idx = selected_asset_idx;
    cached_anchor_date = l1_anchor_date;
  }
  return changed;
}

void OrderFlow::UI::clear() {
  *this = UI{};
}

// ============================================================================
// OrderFlow::Loader Implementation
// ============================================================================

void OrderFlow::Loader::clear() {
  l0_requested = false;
  l0_date.clear();
  l0_asset = 0;
  l1_needs_reload = false;
  coro.reset();
  coro_running = false;
  coro_should_stop = false;
}

// ============================================================================
// OrderFlow Implementation
// ============================================================================

void OrderFlow::clear() {
  l1.clear();
  l0.clear();
  ui.clear();
  loader.clear();
}

