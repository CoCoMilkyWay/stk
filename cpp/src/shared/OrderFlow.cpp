// OrderFlow Implementation
#include "shared/OrderFlow.hpp"

#include <algorithm>
#include <cmath>

// ============================================================================
// L1Day Implementation
// ============================================================================

double L1Day::global_x(size_t i) const {
  return static_cast<double>(day_idx * OrderFlowConst::L1_CAPACITY + indices[i]);
}

void L1Day::reserve(size_t expected) {
  indices.reserve(expected);
  open.reserve(expected);
  high.reserve(expected);
  low.reserve(expected);
  close.reserve(expected);
  volume.reserve(expected);
}

void L1Day::push(size_t idx, float o, float h, float l, float c, float v) {
  indices.push_back(idx);
  open.push_back(o);
  high.push_back(h);
  low.push_back(l);
  close.push_back(c);
  volume.push_back(v);
}

void L1Day::clear() {
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
// L1Cache Implementation
// ============================================================================

size_t L1Cache::get_day_idx(double global_x) const {
  return static_cast<size_t>(global_x) / OrderFlowConst::L1_CAPACITY;
}

const std::string &L1Cache::get_date(double global_x) const {
  static const std::string empty;
  size_t d = get_day_idx(global_x);
  return (d < dates.size()) ? dates[d] : empty;
}

double L1Cache::snap_to_day_start(double global_x) const {
  size_t d = get_day_idx(global_x);
  return static_cast<double>(d * OrderFlowConst::L1_CAPACITY);
}

void L1Cache::build_plot_data(size_t asset_idx) {
  if (asset_idx >= num_assets)
    return;
  if (plot_data.size() < num_assets)
    plot_data.resize(num_assets);

  auto &pd = plot_data[asset_idx];

  if (pd.built)
    return;

  pd.x.clear();
  pd.open.clear();
  pd.high.clear();
  pd.low.clear();
  pd.close.clear();
  pd.day_start_plot_idx.clear();
  pd.day_start_global_x.clear();

  for (size_t d = 0; d < num_days; ++d) {
    const auto &day = days[d][asset_idx];

    pd.day_start_plot_idx.push_back(pd.x.size());
    pd.day_start_global_x.push_back(d * OrderFlowConst::L1_CAPACITY);

    for (size_t i = 0; i < day.valid_count(); ++i) {
      pd.x.push_back(day.global_x(i));
      pd.open.push_back(static_cast<double>(day.open[i]));
      pd.high.push_back(static_cast<double>(day.high[i]));
      pd.low.push_back(static_cast<double>(day.low[i]));
      pd.close.push_back(static_cast<double>(day.close[i]));
    }
  }

  if (!pd.low.empty()) {
    pd.y_min = *std::min_element(pd.low.begin(), pd.low.end());
    pd.y_max = *std::max_element(pd.high.begin(), pd.high.end());
    pd.built = true;
  }
}

void L1Cache::invalidate_plot_data() {
  for (auto &pd : plot_data) {
    pd.built = false;
  }
}

void L1Cache::clear() {
  dates.clear();
  days.clear();
  date_to_idx.clear();
  plot_data.clear();
  loaded = false;
  num_assets = 0;
  num_days = 0;
}

// ============================================================================
// L0Day Implementation
// ============================================================================

double L0Day::global_x(size_t i) const {
  return static_cast<double>(day_idx * OrderFlowConst::L0_CAPACITY + ticks[i].tick_idx);
}

void L0Day::reserve(size_t expected) {
  ticks.reserve(expected);
}

void L0Day::push(size_t idx, bool depth_valid, bool data_valid, float mid,
                 const std::array<float, OrderFlowConst::LOB_DEPTH> &bp,
                 const std::array<float, OrderFlowConst::LOB_DEPTH> &ap,
                 const std::array<float, OrderFlowConst::LOB_DEPTH> &bv,
                 const std::array<float, OrderFlowConst::LOB_DEPTH> &av) {
  ticks.push_back({idx, depth_valid, data_valid, mid, bp, ap, bv, av});
}

void L0Day::clear() {
  date.clear();
  day_idx = 0;
  ticks.clear();
}

// ============================================================================
// L0 Heatmap Cache Implementation
// ============================================================================

void L0HeatmapPriceLevel::reserve(size_t expected) {
  rects.reserve(expected);
}

void L0HeatmapPriceLevel::clear() {
  rects.clear();
}

void L0HeatmapMergedCache::reserve_levels(size_t expected) {
  levels.reserve(expected);
}

void L0HeatmapMergedCache::clear() {
  levels.clear();
  valid = false;
}

void L0HeatmapColoredCache::reserve(size_t expected) {
  rects.reserve(expected);
}

void L0HeatmapColoredCache::clear() {
  rects.clear();
  valid = false;
}

// ============================================================================
// L0Cache Implementation
// ============================================================================

// Query - Index conversion

size_t L0Cache::get_day_idx(double global_x) const {
  return static_cast<size_t>(global_x) / OrderFlowConst::L0_CAPACITY;
}

size_t L0Cache::get_local_idx(double global_x) const {
  return static_cast<size_t>(global_x) % OrderFlowConst::L0_CAPACITY;
}

size_t L0Cache::find_plot_idx(double global_x) const {
  auto it = std::lower_bound(plot_t.begin(), plot_t.end(), global_x);
  if (it == plot_t.end())
    return plot_t.empty() ? SIZE_MAX : plot_t.size() - 1;
  return static_cast<size_t>(it - plot_t.begin());
}

size_t L0Cache::snap_to_valid_plot_idx(double global_x) const {
  return find_plot_idx(global_x);
}

// Query - Data access

L0Cache::DepthData L0Cache::get_depth(size_t plot_idx) const {
  DepthData d;
  if (plot_idx >= plot_t.size())
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

  const auto &tick = day.ticks[local_plot_idx];
  d.mid_price = tick.mid_price;
  d.bid_price = &tick.bid_price;
  d.ask_price = &tick.ask_price;
  d.bid_volume = &tick.bid_volume;
  d.ask_volume = &tick.ask_volume;
  d.tick_idx = tick.tick_idx;
  d.day_idx = day.day_idx;

  // Convert tick index to time
  ClockTime ct = trading_seconds_to_clock(d.tick_idx);
  d.time.hour = ct.hour;
  d.time.minute = ct.minute;
  d.time.second = ct.second;
  d.valid = true;

  return d;
}

const std::string &L0Cache::get_date(size_t plot_idx) const {
  static const std::string empty;
  if (plot_idx >= plot_t.size() || days.empty())
    return empty;

  size_t day_i = 0;
  for (size_t i = 1; i < day_start_plot_idx.size(); ++i) {
    if (plot_idx < day_start_plot_idx[i])
      break;
    day_i = i;
  }
  return days[day_i].date;
}

// Query - Statistics

L0Cache::ValidCounts L0Cache::count_valid() const {
  ValidCounts counts;
  
  // Count merged rectangles in heatmap cache
  if (heatmap_merged_cache.valid) {
    for (const auto &level : heatmap_merged_cache.levels) {
      counts.rect_merged += level.rects.size();
    }
  }
  
  // Count valid ticks
  for (const auto &day : days) {
    for (const auto &tick : day.ticks) {
      if (tick.depth_valid)
        ++counts.depth_valid;
      if (tick.data_valid)
        ++counts.data_valid;
    }
  }
  
  return counts;
}

bool L0Cache::matches(const std::string &date, size_t asset) const {
  if (!loaded || days.empty())
    return false;
  return days[0].date == date && asset_idx == asset;
}

// Modification - Build caches

void L0Cache::build_plot_data() {
  plot_t.clear();
  plot_mid_price.clear();
  plot_best_bid.clear();
  plot_best_ask.clear();
  day_start_plot_idx.clear();
  day_start_global_x.clear();

  // Reserve space for plot data (full capacity for aggressive allocation)
  size_t total_estimated = days.size() * OrderFlowConst::L0_CAPACITY;
  plot_t.reserve(total_estimated);
  plot_mid_price.reserve(total_estimated);
  plot_best_bid.reserve(total_estimated);
  plot_best_ask.reserve(total_estimated);

  for (const auto &day : days) {
    day_start_plot_idx.push_back(plot_t.size());
    day_start_global_x.push_back(day.day_idx * OrderFlowConst::L0_CAPACITY);

    // Only add depth_valid ticks (sparse)
    for (size_t i = 0; i < day.valid_count(); ++i) {
      const auto &tick = day.ticks[i];
      if (!tick.depth_valid)
        continue;
      
      plot_t.push_back(day.global_x(i));
      plot_mid_price.push_back(static_cast<double>(tick.mid_price));
      plot_best_bid.push_back(static_cast<double>(tick.bid_price[0]));
      plot_best_ask.push_back(static_cast<double>(tick.ask_price[0]));
    }
  }

  // Cache Y range with margin
  if (!plot_mid_price.empty()) {
    double y_min = *std::min_element(plot_mid_price.begin(), plot_mid_price.end());
    double y_max = *std::max_element(plot_mid_price.begin(), plot_mid_price.end());
    double y_range = y_max - y_min;
    double margin = y_range * OrderFlowConst::Y_MARGIN_RATIO;
    y_min_with_margin = y_min - margin;
    y_max_with_margin = y_max + margin;
  }

  // Increment data version and invalidate heatmap caches
  ++data_version;
  heatmap_merged_cache.clear();
  heatmap_colored_cache.clear();
}

void L0Cache::build_heatmap_merged_cache() {
  heatmap_merged_cache.clear();
  
  if (days.empty())
    return;

  constexpr size_t DEPTH = OrderFlowConst::LOB_DEPTH;
  constexpr size_t MAX_KEYS_PER_TICK = DEPTH * 2;  // bid30 + ask30

  heatmap_merged_cache.reserve_levels(OrderFlowConst::ESTIMATED_PRICE_LEVELS * 2);

  // Unified map: price_key -> level index (handles both bid and ask)
  std::map<int, size_t> price_to_level;
  
  // Reserve scratch buffers (reused across all ticks)
  heatmap_scratch_.price_keys.reserve(MAX_KEYS_PER_TICK);
  heatmap_scratch_.amounts.reserve(MAX_KEYS_PER_TICK);
  heatmap_scratch_.keys_to_update.reserve(MAX_KEYS_PER_TICK * 2);

  // Helper lambda to update a single price level with amount
  auto update_price_level = [&](int price_key, float price, int32_t amount_rmb, size_t global_tick_idx) {
    auto it = price_to_level.find(price_key);
    if (it == price_to_level.end()) {
      size_t level_idx = heatmap_merged_cache.levels.size();
      heatmap_merged_cache.levels.push_back({price, {}});
      heatmap_merged_cache.levels[level_idx].reserve(OrderFlowConst::ESTIMATED_RECTS_PER_LEVEL);
      price_to_level[price_key] = level_idx;
      it = price_to_level.find(price_key);
    }

    size_t level_idx = it->second;
    auto &level_cache = heatmap_merged_cache.levels[level_idx];

    if (!level_cache.rects.empty()) {
      auto &last_rect = level_cache.rects.back();
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
    if (amount_rmb == 0 && level_cache.rects.empty())
      return;

    // Create new rectangle (bid: bottom, ask: top determines direction)
    bool is_bid = (amount_rmb > 0);
    float price_bottom = is_bid ? (price - OrderFlowConst::TICK_SIZE) 
                                : (price + OrderFlowConst::TICK_SIZE);
    
    level_cache.rects.push_back({
        global_tick_idx,
        global_tick_idx + 1,
        price,
        price_bottom,
        amount_rmb
    });
  };

  for (const auto &day : days) {
    size_t day_base = day.day_idx * OrderFlowConst::L0_CAPACITY;

    for (size_t tick_i = 0; tick_i < day.valid_count(); ++tick_i) {
      const auto &tick = day.ticks[tick_i];
      
      if (!tick.depth_valid)
        continue;
      
      size_t global_tick_idx = day_base + tick.tick_idx;

      // Step 1: Collect current tick's bid/ask data (reuse scratch buffers)
      auto &price_keys = heatmap_scratch_.price_keys;
      auto &amounts = heatmap_scratch_.amounts;
      auto &keys_to_update = heatmap_scratch_.keys_to_update;
      
      price_keys.clear();
      amounts.clear();
      keys_to_update.clear();
      
      int min_key = std::numeric_limits<int>::max();
      int max_key = std::numeric_limits<int>::min();
      
      for (size_t level = 0; level < DEPTH; ++level) {
        // Collect bid
        {
          float price = tick.bid_price[level];
          if (price > 0) {
            float volume = tick.bid_volume[level];
            float amount_float = volume * price * OrderFlowConst::SHARES_PER_LOT;
            int32_t amount_rmb = static_cast<int32_t>(std::round(amount_float / 100.0f)) * 100;
            
            if (amount_rmb != 0) {
              int price_key = static_cast<int>(price * OrderFlowConst::PRICE_SCALE + 
                                               OrderFlowConst::ROUNDING_OFFSET);
              price_keys.push_back(price_key);
              amounts.push_back(amount_rmb);
              min_key = std::min(min_key, price_key);
              max_key = std::max(max_key, price_key);
            }
          }
        }
        
        // Collect ask
        {
          float price = tick.ask_price[level];
          if (price > 0) {
            float volume = tick.ask_volume[level];
            float amount_float = volume * price * OrderFlowConst::SHARES_PER_LOT;
            int32_t amount_rmb = static_cast<int32_t>(std::round(amount_float / 100.0f)) * 100;
            
            if (amount_rmb != 0) {
              int price_key = static_cast<int>(price * OrderFlowConst::PRICE_SCALE + 
                                               OrderFlowConst::ROUNDING_OFFSET);
              price_keys.push_back(price_key);
              amounts.push_back(amount_rmb);
              min_key = std::min(min_key, price_key);
              max_key = std::max(max_key, price_key);
            }
          }
        }
      }
      
      // Step 2: Collect all price keys that need updating
      // Add current tick's keys
      for (int key : price_keys) {
        keys_to_update.push_back(key);
      }
      
      // Add existing keys within current min/max range (may need to close as amount=0)
      if (min_key <= max_key) {
        for (const auto &[key, level_idx] : price_to_level) {
          if (key >= min_key && key <= max_key) {
            // Check if this level has active rects that need closing
            const auto &level_cache = heatmap_merged_cache.levels[level_idx];
            if (!level_cache.rects.empty()) {
              // Check if already in keys_to_update
              bool found = false;
              for (int k : keys_to_update) {
                if (k == key) {
                  found = true;
                  break;
                }
              }
              if (!found) {
                keys_to_update.push_back(key);
              }
            }
          }
        }
      }
      
      // Step 3: Update only the collected keys
      for (int price_key : keys_to_update) {
        float price = static_cast<float>(price_key) / OrderFlowConst::PRICE_SCALE;
        
        // Linear search in price_keys (small size ~60)
        int32_t amount_rmb = 0;
        for (size_t i = 0; i < price_keys.size(); ++i) {
          if (price_keys[i] == price_key) {
            amount_rmb = amounts[i];
            break;
          }
        }
        
        update_price_level(price_key, price, amount_rmb, global_tick_idx);
      }
    }
  }

  heatmap_merged_cache.data_version = data_version;
  heatmap_merged_cache.valid = true;
}

void L0Cache::build_heatmap_colored_cache(float log_threshold) {
  heatmap_colored_cache.clear();
  heatmap_colored_cache.cached_threshold = log_threshold;
  heatmap_colored_cache.cached_data_version = data_version;
  heatmap_colored_cache.valid = true;
}

void L0Cache::clear() {
  days.clear();
  asset_idx = 0;
  plot_t.clear();
  plot_mid_price.clear();
  plot_best_bid.clear();
  plot_best_ask.clear();
  day_start_plot_idx.clear();
  day_start_global_x.clear();
  heatmap_merged_cache.clear();
  heatmap_colored_cache.clear();
  loaded = false;
  data_version = 0;
  asset_idx = 0;
}

// ============================================================================
// Auxiliary Structures Implementation
// ============================================================================

void OrderFlowLoaderState::clear() {
  l0_load_requested = false;
  l0_request_date.clear();
  l0_request_asset = 0;
  l1_needs_reload = false;
  handle.reset();
  coro_running = false;
  coro_should_exit = false;
}

bool OrderFlowUI::check_and_update() {
  bool changed = (selected_asset_idx != prev_asset_idx ||
                  l1_anchor_date != prev_l1_anchor_date);
  prev_asset_idx = selected_asset_idx;
  prev_l1_anchor_date = l1_anchor_date;
  return changed;
}

// ============================================================================
// Main OrderFlow Implementation
// ============================================================================

void OrderFlow::clear() {
  l1.clear();
  l0.clear();
  ui = OrderFlowUI{};
  loader.clear();
}
