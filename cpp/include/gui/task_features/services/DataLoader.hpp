// DataLoader - Load tensor data into OrderFlow data structure
// Design:
//   - L1: Synchronous loading (blocking, first tab open)
//   - L0: Coroutine for async loading (triggered by K-line anchor)
//   - Tab switch: Blocking start/stop of coroutine
#pragma once

#include "features/backend/FeatureReader.hpp"
#include "gui/coro/CoroManager.hpp"
#include "misc/profiler.hpp"
#include "shared/OrderFlow.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace asio = boost::asio;

namespace GUI::Features {

class DataLoader {
public:
  explicit DataLoader(const std::string &features_dir)
      : reader_(features_dir), features_dir_(features_dir) {}

  // ========================================================================
  // L1: Synchronous Loading (blocking)
  // ========================================================================

  void EnsureL1Loaded(OrderFlow &of, size_t num_assets) {
    // Check if reload is needed (e.g., after compute)
    bool force_reload = of.loader.l1_needs_reload.exchange(false);

    if (of.l1.loaded && !force_reload)
      return;

    TraceN("L1_Load");

    // Clear existing data before reload
    if (force_reload) {
      TraceN("L1_Clear");
      of.l1.clear();
    }

    load_all_l1(of.l1, num_assets);

    // Set initial anchor
    if (of.l1.loaded && !of.l1.dates.empty()) {
      of.ui.l1_anchor_x = 0;
      of.ui.l1_anchor_date = of.l1.dates[0];
      of.ui.cached_anchor_date = of.ui.l1_anchor_date;
    }
  }

  // ========================================================================
  // L0: Coroutine for async loading
  // ========================================================================

  asio::awaitable<void> L0LoaderLoop(OrderFlow &of, int &feature_idx_ref) {
    of.loader.coro_running = true;

    // Create depth buffer once for entire coroutine lifetime (~500MB)
    // Reused across all L0 loads within this tab session
    FeatureReader::DepthTensor depth_buffer;
    depth_buffer.preallocate(of.l1.num_assets);

    // Create L0 feature buffer (preallocated for L0 level)
    FeatureReader::DayTensor l0_tensor;
    l0_tensor.preallocate_level(of.l1.num_assets, 0);

    while (!of.loader.coro_should_stop) {
      // Check for L0 load request
      if (of.loader.l0_requested.exchange(false)) {
        load_l0(of.l0, of.loader.l0_date, of.loader.l0_asset, of.l1, depth_buffer);
        of.ui.l0_anchor_plot_idx = 0;
        // Clear L0 feature cache when L0 data changes (force reload)
        of.l0_feature.clear();
      }

      // Load L0 feature if needed (lazy, only when selection exists)
      if (feature_idx_ref >= 0 && of.l0.loaded &&
          !of.l0_feature.matches(of.loader.l0_date, of.loader.l0_asset, feature_idx_ref)) {
        load_l0_feature(of.l0_feature, of.loader.l0_date, of.loader.l0_asset,
                        feature_idx_ref, of.l0, l0_tensor);
      }

      // Yield to allow other tasks
      co_await asio::steady_timer(
          co_await asio::this_coro::executor,
          std::chrono::milliseconds(16))
          .async_wait(asio::use_awaitable);
    }

    of.loader.coro_running = false;
    // buffers automatically destroyed when coroutine exits
  }

  // Start L0 loader coroutine (blocking until started)
  void StartL0Loader(CoroManager &coromgr, OrderFlow &of, int &feature_idx_ref) {
    if (of.loader.coro_running)
      return;

    TraceN("L0_StartCoroutine");

    of.loader.coro_should_stop = false;
    of.loader.coro = coromgr.Spawn(L0LoaderLoop(of, feature_idx_ref));

    // Blocking wait until coroutine starts
    {
      TraceN("L0_WaitStart");
      while (!of.loader.coro_running) {
        coromgr.Poll();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    }
  }

  // Stop L0 loader coroutine (blocking until stopped)
  void StopL0Loader(CoroManager &coromgr, OrderFlow &of) {
    if (!of.loader.coro_running)
      return;

    TraceN("L0_StopCoroutine");

    of.loader.coro_should_stop = true;

    // Blocking wait until coroutine exits
    {
      TraceN("L0_WaitStop");
      while (of.loader.coro_running) {
        coromgr.Poll();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    }

    of.loader.coro.reset();
  }

  // Request L0 load (non-blocking, coroutine will handle)
  void RequestL0Load(OrderFlow &of, const std::string &date, size_t asset_idx) {
    if (of.l0.matches(date, asset_idx))
      return;

    of.loader.l0_date = date;
    of.loader.l0_asset = asset_idx;
    of.loader.l0_requested = true;
  }

  // ========================================================================
  // Load all L1 data (sparse, pre-reserved)
  // ========================================================================
  bool load_all_l1(OrderFlow::L1Cache &cache, size_t num_assets) {
    Trace;

    if (cache.loaded)
      return true;

    cache.clear();
    cache.invalidate_all_plots(); // Force rebuild on reload
    cache.num_assets = num_assets;

    std::vector<std::string> dates = scan_available_dates();
    if (dates.empty())
      return false;

    cache.dates = dates;
    cache.num_days = dates.size();
    cache.days.resize(dates.size());

    // Create L1 buffer once, reuse across all days
    // Only allocate L1 level memory (T=255, F=~20, A=num_assets)
    // Avoids repeated allocation/deallocation of ~50MB buffer per day
    FeatureReader::DayTensor tensor;
    tensor.preallocate_level(num_assets, 1);

    {
      TraceN("LoadAllDays");
      for (size_t d = 0; d < dates.size(); ++d) {
        const std::string &date = dates[d];
        cache.date_to_idx[date] = d;
        cache.days[d].resize(num_assets);

        // Initialize all days with correct day_idx (even if load fails)
        for (size_t a = 0; a < num_assets; ++a) {
          cache.days[d][a].date = date;
          cache.days[d][a].day_idx = d;
        }

        reader_.load_day_level(date, 1, tensor); // Reuse same buffer, reads actual T/F from header

        for (size_t a = 0; a < num_assets && a < tensor.A; ++a) {
          auto &day = cache.days[d][a];
          day.reserve(OrderFlowConst::L1_CAPACITY);

          // Use actual T from file header (not constant)
          for (size_t t = 0; t < tensor.T[1]; ++t) {
            float valid_flag = static_cast<float>(tensor.get<1>(t, L1_FieldOffset::_data_valid, a));
            if (valid_flag <= 0.5f)
              continue;

            // Read prices as integer cents and convert to yuan
            float o_cents = static_cast<float>(tensor.get<1>(t, L1_FieldOffset::_ohlc_open, a));
            float h_cents = static_cast<float>(tensor.get<1>(t, L1_FieldOffset::_ohlc_high, a));
            float l_cents = static_cast<float>(tensor.get<1>(t, L1_FieldOffset::_ohlc_low, a));
            float c_cents = static_cast<float>(tensor.get<1>(t, L1_FieldOffset::_ohlc_close, a));
            float v = static_cast<float>(tensor.get<1>(t, L1_FieldOffset::_ohlc_volume, a));

            float o = o_cents * 0.01f;
            float h = h_cents * 0.01f;
            float l = l_cents * 0.01f;
            float c = c_cents * 0.01f;

            day.push(t, o, h, l, c, v);
          }
        }
      }
    }

    cache.plot_data.resize(num_assets);

    {
      TraceN("BuildAllPlotData");
      for (size_t a = 0; a < num_assets; ++a) {
        cache.build_plot_data(a);
      }
    }

    // Debug: print load stats
    std::cerr << "[DataLoader] L1 loaded: " << dates.size() << " days, " << num_assets << " assets" << std::endl;
    for (size_t d = 0; d < std::min(dates.size(), size_t(5)); ++d) {
      size_t total_valid = 0;
      for (size_t a = 0; a < num_assets; ++a) {
        total_valid += cache.days[d][a].count_valid();
      }
      std::cerr << "  Day " << d << " (" << dates[d] << "): " << total_valid << " valid bars across all assets" << std::endl;
    }
    // if (num_assets > 0) {
    //   std::cerr << "  Asset 0 plot_data: " << cache.plot_data[0].x.size() << " points" << std::endl;
    // }

    cache.loaded = true;
    return true;
  }

  // ========================================================================
  // Load L0 data for single day (sparse, pre-reserved)
  // ========================================================================
  bool load_l0(OrderFlow::L0Cache &cache, const std::string &date, size_t asset_idx, const OrderFlow::L1Cache &l1_cache, FeatureReader::DepthTensor &depth_tensor) {
    if (cache.matches(date, asset_idx))
      return true;

    TraceN("L0_Load");

    cache.clear();
    cache.asset_idx = asset_idx;

    auto it = l1_cache.date_to_idx.find(date);
    size_t day_idx = (it != l1_cache.date_to_idx.end()) ? it->second : 0;

    // Load depth data into reusable buffer (for orderflow visualization and validity flags)
    // Buffer is managed by coroutine lifetime, not reallocated per load
    // Actual T/F dimensions read from file header
    {
      TraceN("L0_LoadDepth");
      reader_.load_depth(date, depth_tensor);
    }

    assert(MAX_ROWS_PER_LEVEL[0] == OrderFlowConst::L0_CAPACITY && "Depth tensor must be dense with time index semantics");

    if (asset_idx >= depth_tensor.A) {
      cache.loaded = true;
      return false;
    }

    OrderFlow::L0Cache::Day day;
    day.date = date;
    day.day_idx = day_idx;

    constexpr size_t N = OrderFlowConst::LOB_DEPTH;

    // Reserve full capacity for aggressive allocation
    day.reserve(OrderFlowConst::L0_CAPACITY);

    // Opening price captured from first valid tick (reset per day)
    float opening_price = 0.0f;
    float price_min = 0.0f;
    float price_max = 0.0f;

    // Sparse loading: only store valid ticks (depth_valid=true or data_valid=true)
    // Line plots and heatmap will use ImPlot step mode to handle sparsity
    // Use actual T from file header (not constant)
    for (size_t t = 0; t < depth_tensor.T; ++t) {
      assert(t < OrderFlowConst::L0_CAPACITY && "depth tensor row exceeds L0_CAPACITY");

      // Read validity flags (from depth tensor)
      float depth_valid_val = static_cast<float>(depth_tensor.get(t, DepthFieldOffset::_depth_valid, asset_idx));
      float data_valid_val = static_cast<float>(depth_tensor.get(t, DepthFieldOffset::_data_valid, asset_idx));

      bool depth_valid = (depth_valid_val > 0.5f);
      bool data_valid = (data_valid_val > 0.5f);

      // Skip if neither valid
      if (!depth_valid && !data_valid)
        continue;

      // Load depth features (only if depth_valid)
      float mid = 0.0f;
      std::array<float, N> bp{}, ap{}, bv{}, av{};

      if (depth_valid) {
        // Read prices (already in yuan, no conversion needed)
        mid = static_cast<float>(depth_tensor.get(t, DepthFieldOffset::_mid_price, asset_idx));

        // Capture opening price from first valid tick
        if (opening_price == 0.0f && mid > 0) [[unlikely]] {
          opening_price = mid;
          price_min = opening_price * 0.75f;
          price_max = opening_price * 1.25f;
        }

        // Multi-width fields: manual offset calculation
        // Filter sentinel values at each depth level (zero out if outside ±25% cage)
        // If no opening price yet, mark entire tick invalid
        if (opening_price == 0.0f) [[unlikely]] {
          depth_valid = false;
        } else [[likely]] {
          mid = (mid < price_min || mid > price_max) ? std::numeric_limits<float>::quiet_NaN() : mid;

          for (size_t i = 0; i < N; ++i) {
            size_t bp_offset = DEPTH_FIELD_OFFSETS[DepthFieldOffset::_bid_price] + i;
            size_t ap_offset = DEPTH_FIELD_OFFSETS[DepthFieldOffset::_ask_price] + i;
            size_t bv_offset = DEPTH_FIELD_OFFSETS[DepthFieldOffset::_bid_volume] + i;
            size_t av_offset = DEPTH_FIELD_OFFSETS[DepthFieldOffset::_ask_volume] + i;

            // Prices are already in yuan (no conversion needed)
            float bp_yuan = static_cast<float>(depth_tensor.data[(t * DEPTH_TOTAL_WIDTH + bp_offset) * depth_tensor.A + asset_idx]);
            float ap_yuan = static_cast<float>(depth_tensor.data[(t * DEPTH_TOTAL_WIDTH + ap_offset) * depth_tensor.A + asset_idx]);

            // Check if prices are outside cage (sentinel detection)
            bool bp_outside = (bp_yuan < price_min || bp_yuan > price_max);
            bool ap_outside = (ap_yuan < price_min || ap_yuan > price_max);

            // Use NaN for sentinel values (filtered at source)
            bp[i] = bp_outside ? std::numeric_limits<float>::quiet_NaN() : bp_yuan;
            bv[i] = bp_outside ? 0.0f : static_cast<float>(depth_tensor.data[(t * DEPTH_TOTAL_WIDTH + bv_offset) * depth_tensor.A + asset_idx]);

            ap[i] = ap_outside ? std::numeric_limits<float>::quiet_NaN() : ap_yuan;
            av[i] = ap_outside ? 0.0f : static_cast<float>(depth_tensor.data[(t * DEPTH_TOTAL_WIDTH + av_offset) * depth_tensor.A + asset_idx]);
          }
        }
      }

      // Push sparse tick with validity flags
      // CRITICAL: t is time index (0-15299), directly used as tick_idx
      day.push(t, depth_valid, data_valid, mid, bp, ap, bv, av);
    }

    cache.days.push_back(std::move(day));
    cache.build_plot();
    cache.build_heatmap_merged(); // Build Level 2 heatmap cache
    cache.loaded = true;
    return true;
  }

  // ========================================================================
  // Load L0 feature data for single day/asset (for L0 plot overlay)
  // ========================================================================
  bool load_l0_feature(OrderFlow::L0FeatureCache &cache, const std::string &date, size_t asset_idx,
                       int feature_idx, const OrderFlow::L0Cache &l0_cache,
                       FeatureReader::DayTensor &day_tensor) {
    if (cache.matches(date, asset_idx, feature_idx))
      return true;

    TraceN("L0_Feature_Load");

    cache.clear();
    cache.date = date;
    cache.asset_idx = asset_idx;
    cache.feature_idx = feature_idx;

    // Load L0 features (uses preallocated buffer)
    reader_.load_day_level(date, 0, day_tensor);

    if (asset_idx >= day_tensor.A || day_tensor.T[0] == 0)
      return false;

    // Build plot data from L0 cache (use same X coordinates as depth data)
    cache.plot.x.reserve(l0_cache.plot.x.size());
    cache.plot.values.reserve(l0_cache.plot.x.size());

    float y_min = std::numeric_limits<float>::max();
    float y_max = std::numeric_limits<float>::lowest();

    // Iterate through valid ticks in L0 cache
    for (const auto &day : l0_cache.days) {
      for (size_t i = 0; i < day.count_valid(); ++i) {
        const auto &tick = day.ticks[i];
        if (!tick.depth_valid)
          continue;

        size_t t = tick.tick_idx;
        if (t >= day_tensor.T[0])
          continue;

        float val = static_cast<float>(day_tensor.get<0>(t, feature_idx, asset_idx));

        double global_x = day.to_global_x(i);
        cache.plot.x.push_back(global_x);
        cache.plot.values.push_back(val);

        if (val < y_min) y_min = val;
        if (val > y_max) y_max = val;
      }
    }

    if (!cache.plot.x.empty() && y_max > y_min) {
      cache.plot.y_min = y_min;
      cache.plot.y_max = y_max;
      cache.plot.valid = true;
    }

    return cache.plot.valid;
  }

  // ========================================================================
  // Utility
  // ========================================================================

  std::vector<std::string> scan_available_dates() const {
    std::vector<std::string> dates;

    if (!std::filesystem::exists(features_dir_))
      return dates;

    for (const auto &year_entry : std::filesystem::directory_iterator(features_dir_)) {
      if (!year_entry.is_directory())
        continue;
      std::string year = year_entry.path().filename().string();
      if (year.size() != 4)
        continue;

      for (const auto &month_entry : std::filesystem::directory_iterator(year_entry.path())) {
        if (!month_entry.is_directory())
          continue;
        std::string month = month_entry.path().filename().string();
        if (month.size() != 2)
          continue;

        for (const auto &day_entry : std::filesystem::directory_iterator(month_entry.path())) {
          if (!day_entry.is_directory())
            continue;
          std::string day = day_entry.path().filename().string();
          if (day.size() != 2)
            continue;

          std::string l1_path = day_entry.path().string() + "/features_L1.zst";
          if (std::filesystem::exists(l1_path)) {
            dates.push_back(year + month + day);
          }
        }
      }
    }

    std::sort(dates.begin(), dates.end());
    return dates;
  }

private:
  FeatureReader reader_;
  std::string features_dir_;
};

} // namespace GUI::Features
