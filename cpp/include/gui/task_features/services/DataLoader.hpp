// DataLoader - Load tensor data into OrderFlow data structure
// Design:
//   - Sparse storage: only load valid data points
//   - Pre-reserved vectors for performance
//   - Even time indexing: global_idx = day_n * CAPACITY + local_idx
#pragma once

#include "features/backend/FeatureReader.hpp"
#include "features/backend/FeatureStoreConfig.hpp"
#include "gui/coro/CoroManager.hpp"
#include "shared/OrderFlow.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <filesystem>
#include <string>
#include <vector>

namespace asio = boost::asio;

namespace GUI::Features {

class DataLoader {
public:
  explicit DataLoader(const std::string &features_dir)
      : reader_(features_dir), features_dir_(features_dir) {}

  // ========================================================================
  // Loader Coroutine
  // ========================================================================

  asio::awaitable<void> LoaderLoop(OrderFlow &of, size_t num_assets) {
    // Load L1 once at startup
    of.loader.l1_loading = true;
    load_all_l1(of.l1, num_assets);
    of.loader.l1_loading = false;

    // Set initial anchor
    if (of.l1.loaded && !of.l1.dates.empty()) {
      of.ui.l1_anchor_x = 0; // Day 0, minute 0
      of.ui.l1_anchor_date = of.l1.dates[0];
      of.ui.prev_l1_anchor_date = of.ui.l1_anchor_date;

      of.loader.l0_request_date = of.ui.l1_anchor_date;
      of.loader.l0_request_asset = static_cast<size_t>(of.ui.selected_asset_idx);
      of.loader.l0_load_requested = true;
    }

    // Monitor loop
    while (true) {
      if (of.loader.l0_load_requested.exchange(false)) {
        of.loader.l0_loading = true;
        load_l0(of.l0, of.loader.l0_request_date, of.loader.l0_request_asset, of.l1);
        of.loader.l0_loading = false;

        // Snap L0 anchor to first valid tick
        of.ui.l0_anchor_plot_idx = 0;
      }

      co_await asio::steady_timer(
          co_await asio::this_coro::executor,
          std::chrono::milliseconds(16))
          .async_wait(asio::use_awaitable);
    }
  }

  void StartLoader(CoroManager &coro_mgr, OrderFlow &of, size_t num_assets) {
    if (of.loader.tab_active)
      return;
    of.loader.tab_active = true;
    of.loader.handle = coro_mgr.Spawn(LoaderLoop(of, num_assets));
  }

  void StopLoader(OrderFlow &of) {
    of.loader.handle.reset();
    of.loader.tab_active = false;
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

      // Load only L1 data (skip L0/L2 for speed)
      FeatureReader::DayTensor tensor;
      if (!reader_.load_day_level(date, 1, tensor))
        continue;

      for (size_t a = 0; a < num_assets && a < tensor.A; ++a) {
        auto &day = cache.days[d][a];
        day.date = date;
        day.day_idx = d;
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

    // Build plot data for all assets
    cache.plot_data.resize(num_assets);
    for (size_t a = 0; a < num_assets; ++a) {
      cache.build_plot_data(a);
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

    // Find day index from L1 cache
    auto it = l1_cache.date_to_idx.find(date);
    size_t day_idx = (it != l1_cache.date_to_idx.end()) ? it->second : 0;

    // Load only L0 data (skip L1/L2 for speed)
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
