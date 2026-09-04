// DataLoader - Load tensor data into OrderFlow data structure
// Design:
//   - L1: Synchronous loading (blocking, first tab open)
//   - L0: Coroutine for async loading (triggered by K-line anchor)
//   - Tab switch: Blocking start/stop of coroutine
#pragma once

#include "features/Backend/FeatureRead.hpp"
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
#include <limits>
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
      of.l1_feature.clear(); // 与 dates 对齐, 一起失效
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

  asio::awaitable<void> L0LoaderLoop(OrderFlow &of, int &selected_level_ref, int &feature_idx_ref) {
    of.loader.coro_running = true;

    // Create depth buffer once for entire coroutine lifetime (~500MB)
    // Reused across all L0 loads within this tab session
    FeatureRead::DayTensor depth_buffer;
    depth_buffer.preallocate(of.l1.num_assets, 2);

    // L0 特征选列缓冲 [T][2][A]: 特征列 + _data_valid (整层 DayTensor 会把全部 L0 列文件读一遍)
    FeatureRead::DayColumns l0_cols;
    l0_cols.preallocate(of.l1.num_assets, 0, 2);

    // L1 特征选列缓冲 (overlay 逐日流式加载, 每日 2 个列文件)
    FeatureRead::DayColumns l1_cols;
    l1_cols.preallocate(of.l1.num_assets, 1, 2);

    while (!of.loader.coro_should_stop) {
      // Check for L0 load request
      if (of.loader.l0_requested.exchange(false)) {
        load_l0(of.l0, of.loader.l0_date, of.loader.l0_asset, of.l1, depth_buffer);
        of.ui.l0_anchor_plot_idx = 0;
        // Clear L0 feature cache when L0 data changes (force reload)
        of.l0_feature.clear();
      }

      // Load L0 feature if needed (lazy, only when L0 level selected)
      if (selected_level_ref == 0 && feature_idx_ref >= 0 && of.l0.loaded &&
          !of.l0_feature.matches(of.loader.l0_date, of.loader.l0_asset, feature_idx_ref)) {
        load_l0_feature(of.l0_feature, of.loader.l0_date, of.loader.l0_asset,
                        feature_idx_ref, of.l0, l0_cols);
      }

      // L1 特征 overlay: 逐日流式填充 (选中 asset/feature 变了就重来; 每 tick 4 天, 控帧)
      if (selected_level_ref == 1 && feature_idx_ref >= 0 && of.l1.loaded) {
        auto &fc = of.l1_feature;
        const size_t asset_idx = static_cast<size_t>(of.ui.selected_asset_idx);
        if (!fc.matches(asset_idx, feature_idx_ref)) {
          fc.reset(asset_idx, feature_idx_ref, of.l1.num_days);
        }
        const size_t end = std::min(fc.days_loaded + 4, of.l1.num_days);
        for (; fc.days_loaded < end;) {
          load_l1_feature_day(fc, of.l1.dates[fc.days_loaded], l1_cols);
          ++fc.days_loaded; // 先填后发布 (单调)
        }
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
  void StartL0Loader(CoroManager &coromgr, OrderFlow &of, int &selected_level_ref, int &feature_idx_ref) {
    if (of.loader.coro_running)
      return;

    TraceN("L0_StartCoroutine");

    of.loader.coro_should_stop = false;
    of.loader.coro = coromgr.Spawn(L0LoaderLoop(of, selected_level_ref, feature_idx_ref));

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
    FeatureRead::DayTensor tensor;
    tensor.preallocate(num_assets, 1);

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

        reader_.load_day(date, tensor); // Reuse same buffer

        for (size_t a = 0; a < num_assets && a < tensor.A; ++a) {
          auto &day = cache.days[d][a];
          day.reserve(OrderFlowConst::L1_CAPACITY);

          // 末行是哨兵, 不消费
          for (size_t t = 0; t < level_valid_rows(1); ++t) {
            float valid_flag = static_cast<float>(tensor.get<1>(t, L1_Field::_data_valid, a));
            if (valid_flag <= 0.5f)
              continue;

            // OHLC 已是元 (Ohlc 节点直写 MinuteData)
            float o = static_cast<float>(tensor.get<1>(t, L1_Field::_ohlc_open, a));
            float h = static_cast<float>(tensor.get<1>(t, L1_Field::_ohlc_high, a));
            float l = static_cast<float>(tensor.get<1>(t, L1_Field::_ohlc_low, a));
            float c = static_cast<float>(tensor.get<1>(t, L1_Field::_ohlc_close, a));
            float v = static_cast<float>(tensor.get<1>(t, L1_Field::_ohlc_volume, a));

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
  bool load_l0(OrderFlow::L0Cache &cache, const std::string &date, size_t asset_idx, const OrderFlow::L1Cache &l1_cache, FeatureRead::DayTensor &depth_tensor) {
    if (cache.matches(date, asset_idx))
      return true;

    TraceN("L0_Load");

    cache.clear();
    cache.asset_idx = asset_idx;

    auto it = l1_cache.date_to_idx.find(date);
    size_t day_idx = (it != l1_cache.date_to_idx.end()) ? it->second : 0;

    // Load depth data into reusable buffer (for orderflow visualization and validity flags)
    // Buffer is managed by coroutine lifetime, not reallocated per load
    {
      TraceN("L0_LoadDepth");
      reader_.load_day(date, depth_tensor);
    }

    if (asset_idx >= depth_tensor.A) {
      cache.loaded = true;
      return false;
    }

    OrderFlow::L0Cache::Day day;
    day.date = date;
    day.day_idx = day_idx;

    constexpr size_t N = OrderFlowConst::LOB_DEPTH;

    // Reserve full capacity for aggressive allocation (分钟频)
    day.reserve(LEVELS[2].rows);

    // Opening price captured from first valid tick (reset per day)
    float opening_price = 0.0f;
    float price_min = 0.0f;
    float price_max = 0.0f;

    // Sparse loading: only store valid rows (depth_valid=true or data_valid=true)
    // Depth 张量为分钟频 (行 m = 分钟末盘口快照); GUI 保持秒级 X 轴:
    // 映射到该分钟最后一秒, step 渲染自然铺满整分钟
    // 末行是哨兵, 不是时间: 必须在坐标映射前排除 (L1_to_L0(255)+59 会冲出 L0_CAPACITY)
    for (size_t m = 0; m < level_valid_rows(2); ++m) {
      const size_t t = L1_to_L0(m) + 59; // 分钟末秒
      assert(t < OrderFlowConst::L0_CAPACITY && "depth minute row exceeds L0_CAPACITY");

      // Read validity flags (from depth tensor)
      float depth_valid_val = static_cast<float>(depth_tensor.get<2>(m, DEPTH_Field::_depth_valid, asset_idx));
      float data_valid_val = static_cast<float>(depth_tensor.get<2>(m, DEPTH_Field::_data_valid, asset_idx));

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
        mid = static_cast<float>(depth_tensor.get<2>(m, DEPTH_Field::_mid_price, asset_idx));

        // Capture opening price from first valid tick
        if (opening_price == 0.0f && mid > 0) [[unlikely]] {
          opening_price = mid;
          price_min = opening_price * 0.75f;
          price_max = opening_price * 1.25f;
        }

        // 宽字段按档 (sub) 取; 各档价格出 ±25% 笼子的视为哨兵值
        // If no opening price yet, mark entire tick invalid
        if (opening_price == 0.0f) [[unlikely]] {
          depth_valid = false;
        } else [[likely]] {
          mid = (mid < price_min || mid > price_max) ? std::numeric_limits<float>::quiet_NaN() : mid;

          for (size_t i = 0; i < N; ++i) {
            // Prices are already in yuan (no conversion needed)
            float bp_yuan = static_cast<float>(depth_tensor.get<2>(m, DEPTH_Field::_bid_price, asset_idx, i));
            float ap_yuan = static_cast<float>(depth_tensor.get<2>(m, DEPTH_Field::_ask_price, asset_idx, i));

            // Check if prices are outside cage (sentinel detection)
            bool bp_outside = (bp_yuan < price_min || bp_yuan > price_max);
            bool ap_outside = (ap_yuan < price_min || ap_yuan > price_max);

            // Use NaN for sentinel values (filtered at source)
            bp[i] = bp_outside ? std::numeric_limits<float>::quiet_NaN() : bp_yuan;
            bv[i] = bp_outside ? 0.0f : static_cast<float>(depth_tensor.get<2>(m, DEPTH_Field::_bid_volume, asset_idx, i));

            ap[i] = ap_outside ? std::numeric_limits<float>::quiet_NaN() : ap_yuan;
            av[i] = ap_outside ? 0.0f : static_cast<float>(depth_tensor.get<2>(m, DEPTH_Field::_ask_volume, asset_idx, i));
          }
        }
      }

      // Push sparse tick with validity flags
      // CRITICAL: t 是秒级时间索引 (分钟末秒), 直接用作 tick_idx
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
                       FeatureRead::DayColumns &l0_cols) {
    if (cache.matches(date, asset_idx, feature_idx))
      return true;

    TraceN("L0_Feature_Load");

    cache.clear();
    cache.date = date;
    cache.asset_idx = asset_idx;
    cache.feature_idx = feature_idx;

    // 选列加载: 特征列 + _data_valid (L0 逐列文件, 只读 2 个列文件, 免整层)
    const std::vector<size_t> columns = {static_cast<size_t>(feature_idx),
                                         static_cast<size_t>(L0_Field::_data_valid)};
    reader_.load_day_columns(date, columns, l0_cols);

    if (asset_idx >= l0_cols.A)
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

        size_t t = tick.tick_idx; // 分钟末秒, 恒 < 有效行数 (哨兵行在 load_l0 已排除)
        assert(t < level_valid_rows(0));

        // tick_idx 是分钟末秒 (depth 分钟频); L0 特征逐笔稀疏, 该秒不一定有写入
        // → 在本分钟内向前回溯到最近一个 _data_valid 秒
        {
          size_t lo = (t >= 59) ? t - 59 : 0;
          while (t > lo && static_cast<float>(l0_cols.get(t, 1, asset_idx)) <= 0.5f)
            --t;
        }

        float val = static_cast<float>(l0_cols.get(t, 0, asset_idx));

        double global_x = day.to_global_x(i);
        cache.plot.x.push_back(global_x);
        cache.plot.values.push_back(val);

        if (val < y_min)
          y_min = val;
        if (val > y_max)
          y_max = val;
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
  // Load L1 feature data for single day (for L1 plot overlay, 流式逐日)
  // ========================================================================
  void load_l1_feature_day(OrderFlow::L1FeatureCache &fc, const std::string &date,
                           FeatureRead::DayColumns &l1_cols) {
    TraceN("L1_Feature_Day");

    // 选列加载: 特征列 + _data_valid (L1 逐列文件, 只读 2 个列文件)
    const std::vector<size_t> columns = {static_cast<size_t>(fc.feature_idx),
                                         static_cast<size_t>(L1_Field::_data_valid)};
    reader_.load_day_columns(date, columns, l1_cols);
    assert(fc.asset_idx < l1_cols.A);

    auto &dst = fc.days[fc.days_loaded];
    for (size_t m = 0; m < level_valid_rows(1); ++m) {
      const bool valid = static_cast<float>(l1_cols.get(m, 1, fc.asset_idx)) > 0.5f;
      dst[m] = valid ? static_cast<float>(l1_cols.get(m, 0, fc.asset_idx))
                     : std::numeric_limits<float>::quiet_NaN();
    }
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

          if (FeatureRead::has_date(features_dir_, year + month + day))
            dates.push_back(year + month + day);
        }
      }
    }

    std::sort(dates.begin(), dates.end());
    return dates;
  }

private:
  FeatureRead reader_;
  std::string features_dir_;
};

} // namespace GUI::Features
