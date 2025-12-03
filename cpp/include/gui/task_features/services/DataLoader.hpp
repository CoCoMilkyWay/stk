// DataLoader - Load tensor data into OrderFlow data structure
// Design:
//   - L1: Synchronous loading (blocking, first tab open)
//   - L0: Coroutine for async loading (triggered by K-line anchor)
//   - Tab switch: Blocking start/stop of coroutine
#pragma once

#include "features/backend/FeatureReader.hpp"
#include "features/backend/FeatureStoreConfig.hpp"
#include "gui/coro/CoroManager.hpp"
#include "shared/OrderFlow.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
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

    // Clear existing data before reload
    if (force_reload) {
      of.l1.clear();
    }

    load_all_l1(of.l1, num_assets);

    // Set initial anchor
    if (of.l1.loaded && !of.l1.dates.empty()) {
      of.ui.l1_anchor_x = 0;
      of.ui.l1_anchor_date = of.l1.dates[0];
      of.ui.prev_l1_anchor_date = of.ui.l1_anchor_date;
    }
  }

  // ========================================================================
  // L0: Coroutine for async loading
  // ========================================================================

  asio::awaitable<void> L0LoaderLoop(OrderFlow &of) {
    of.loader.coro_running = true;

    while (!of.loader.coro_should_exit) {
      // Check for L0 load request
      if (of.loader.l0_load_requested.exchange(false)) {
        load_l0(of.l0, of.loader.l0_request_date, of.loader.l0_request_asset, of.l1);
        of.ui.l0_anchor_plot_idx = 0;
      }

      // Yield to allow other tasks
      co_await asio::steady_timer(
          co_await asio::this_coro::executor,
          std::chrono::milliseconds(16))
          .async_wait(asio::use_awaitable);
    }

    of.loader.coro_running = false;
  }

  // Start L0 loader coroutine (blocking until started)
  void StartL0Loader(CoroManager &coro_mgr, OrderFlow &of) {
    if (of.loader.coro_running)
      return;

    of.loader.coro_should_exit = false;
    of.loader.handle = coro_mgr.Spawn(L0LoaderLoop(of));

    // Blocking wait until coroutine starts
    while (!of.loader.coro_running) {
      coro_mgr.Poll();
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }

  // Stop L0 loader coroutine (blocking until stopped)
  void StopL0Loader(CoroManager &coro_mgr, OrderFlow &of) {
    if (!of.loader.coro_running)
      return;

    of.loader.coro_should_exit = true;

    // Blocking wait until coroutine exits
    while (of.loader.coro_running) {
      coro_mgr.Poll();
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    of.loader.handle.reset();
  }

  // Request L0 load (non-blocking, coroutine will handle)
  void RequestL0Load(OrderFlow &of, const std::string &date, size_t asset_idx) {
    if (of.l0.matches(date, asset_idx))
      return;

    of.loader.l0_request_date = date;
    of.loader.l0_request_asset = asset_idx;
    of.loader.l0_load_requested = true;
  }

  // ========================================================================
  // Load all L1 data (sparse, pre-reserved)
  // ========================================================================
  bool load_all_l1(L1Cache &cache, size_t num_assets) {
    if (cache.loaded)
      return true;

    cache.clear();
    cache.num_assets = num_assets;

    std::vector<std::string> dates = scan_available_dates();
    if (dates.empty())
      return false;

    cache.dates = dates;
    cache.num_days = dates.size();
    cache.days.resize(dates.size());

    for (size_t d = 0; d < dates.size(); ++d) {
      const std::string &date = dates[d];
      cache.date_to_idx[date] = d;
      cache.days[d].resize(num_assets);

      // Initialize all days with correct day_idx (even if load fails)
      for (size_t a = 0; a < num_assets; ++a) {
        cache.days[d][a].date = date;
        cache.days[d][a].day_idx = d;
      }

      FeatureReader::DayTensor tensor;
      if (!reader_.load_day_level(date, 1, tensor)) {
        std::cerr << "[DataLoader] Failed to load L1 for date: " << date << std::endl;
        continue;
      }

      for (size_t a = 0; a < num_assets && a < tensor.A; ++a) {
        auto &day = cache.days[d][a];
        day.reserve(OrderFlowConst::L1_CAPACITY);

        for (size_t t = 0; t < tensor.T[1]; ++t) {
          float valid_flag = static_cast<float>(tensor.get(1, t, L1_FieldOffset::data_valid, a));
          if (valid_flag <= 0.5f)
            continue;

          float o = static_cast<float>(tensor.get(1, t, L1_FieldOffset::ohlc_open, a));
          float h = static_cast<float>(tensor.get(1, t, L1_FieldOffset::ohlc_high, a));
          float l = static_cast<float>(tensor.get(1, t, L1_FieldOffset::ohlc_low, a));
          float c = static_cast<float>(tensor.get(1, t, L1_FieldOffset::ohlc_close, a));
          float v = static_cast<float>(tensor.get(1, t, L1_FieldOffset::ohlc_volume, a));

          day.push(t, o, h, l, c, v);
        }
      }
    }

    cache.plot_data.resize(num_assets);
    for (size_t a = 0; a < num_assets; ++a) {
      cache.build_plot_data(a);
    }

    // Debug: print load stats
    std::cerr << "[DataLoader] L1 loaded: " << dates.size() << " days, " << num_assets << " assets" << std::endl;
    for (size_t d = 0; d < std::min(dates.size(), size_t(5)); ++d) {
      size_t total_valid = 0;
      for (size_t a = 0; a < num_assets; ++a) {
        total_valid += cache.days[d][a].valid_count();
      }
      std::cerr << "  Day " << d << " (" << dates[d] << "): " << total_valid << " valid bars across all assets" << std::endl;
    }
    if (num_assets > 0) {
      std::cerr << "  Asset 0 plot_data: " << cache.plot_data[0].x.size() << " points" << std::endl;
    }

    cache.loaded = true;
    return true;
  }

  // ========================================================================
  // Load L0 data for single day (sparse, pre-reserved)
  // ========================================================================
  bool load_l0(L0Cache &cache, const std::string &date, size_t asset_idx, const L1Cache &l1_cache) {
    if (cache.matches(date, asset_idx))
      return true;

    cache.clear();
    cache.asset_idx = asset_idx;

    auto it = l1_cache.date_to_idx.find(date);
    size_t day_idx = (it != l1_cache.date_to_idx.end()) ? it->second : 0;

    FeatureReader::DayTensor tensor;
    if (!reader_.load_day_level(date, 0, tensor)) {
      cache.loaded = true;
      return false;
    }

    if (asset_idx >= tensor.A) {
      cache.loaded = true;
      return false;
    }

    L0Day day;
    day.date = date;
    day.day_idx = day_idx;
    day.reserve(OrderFlowConst::L0_CAPACITY);

    constexpr size_t N = OrderFlowConst::LOB_DEPTH;

    for (size_t t = 0; t < tensor.T[0]; ++t) {
      float valid_flag = static_cast<float>(tensor.get(0, t, L0_FieldOffset::data_valid, asset_idx));
      if (valid_flag <= 0.5f)
        continue;

      float mid = static_cast<float>(tensor.get(0, t, L0_FieldOffset::mid_price, asset_idx));

      std::array<float, N> bp{}, ap{}, bv{}, av{};
      for (size_t i = 0; i < N; ++i) {
        bp[i] = static_cast<float>(tensor.get(0, t, L0_FIELD_OFFSETS[L0_FieldOffset::bid_price] + i, asset_idx));
        ap[i] = static_cast<float>(tensor.get(0, t, L0_FIELD_OFFSETS[L0_FieldOffset::ask_price] + i, asset_idx));
        bv[i] = static_cast<float>(tensor.get(0, t, L0_FIELD_OFFSETS[L0_FieldOffset::bid_volume] + i, asset_idx));
        av[i] = static_cast<float>(tensor.get(0, t, L0_FIELD_OFFSETS[L0_FieldOffset::ask_volume] + i, asset_idx));
      }

      day.push(t, mid, bp, ap, bv, av);
    }

    cache.days.push_back(std::move(day));
    cache.build_plot_data();
    cache.loaded = true;
    return true;
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

          std::string l1_path = day_entry.path().string() + "/features_L1.bin";
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
