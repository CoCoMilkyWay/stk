// Bar 档管线 (bar1m + 基本面, 全轴): 档的源 TierFeed<bar::Tier> + pipeline<bar::Tier> 实例化.
// 本 TU precise-math (CMakeLists PRECISE 表): Fund 缺失值 / PoolKey 母集判定 / cs_valid 依赖 NaN 比较语义.
#include "worker/feature_pipeline.hpp"

#include "features/Bar1mDay.hpp"
#include "features/CoreBar.hpp"
#include "fundamental/Scan.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <memory>
#include <span>
#include <vector>

// 硬源: 预取线程把日 d 的 bar1m 分片解码进源环格 ring[d % R], TS 等 pf_ready(d) 后读.
//   R = StoreRing::feed_ring_size(): 预取窗口跨度全部在格, 日 d 的格被 d + R 复用时 d 已全员计满 (再无读者).
//   日期轴止于末个 bar1m 日 (ComputeService), 轴内每日都须有分片, 构造时断言.
template <>
struct TierFeed<bar::Tier> {
  using Core = CoreBar;
  static constexpr bool kHardFeed = true;
  static constexpr const char *kUnit = "bar";

  const fund::Panel &fund_panel;
  const PoolAxis &axis;
  std::span<const misc::Ymd> dates;
  const fundamental::Catalog catalog;
  fundamental::Ids ids;                      // A 轴码表 (id = sub); 构造后只读, 多预取核并发 find
  std::vector<Bar1mDay> ring;                // 源环 (预取线程写, TS 经 pf_close / pf_ready 的 release/acquire 读)
  std::vector<fundamental::Scratch> scratch; // 与 ring 逐格对应 (一格同一时刻只有一个预取核在写)

  TierFeed(const PipelineArgs &args, const StoreRing &store)
      : fund_panel(args.data.fund_panel), axis(args.libs[TierId::Bar].axis), dates(args.dates),
        ring(store.feed_ring_size()), scratch(ring.size()) {
    assert(axis.size() == asset_axis().size() && "Bar 档 A 轴 = 全轴");
    for (size_t sub = 0; sub < axis.size(); ++sub)
      ids.add(asset_axis().code(axis.global(sub)));
    assert(std::ranges::all_of(dates, [this](misc::Ymd d) { return catalog.find_day("bar1m", d) != nullptr; }) &&
           "bar1m: 日期轴内缺日 (同步漏日)");
  }

  size_t weight(size_t) const { return 1; } // 每资产每日恰 255 根, 负载均匀

  std::unique_ptr<CoreBar> make_core(size_t sub, int) const {
    return std::make_unique<CoreBar>(fund_panel, asset_axis().code(axis.global(sub)), sub);
  }

  size_t prefetch(size_t d) {
    Bar1mDay &b = ring[d % ring.size()];
    b.load(catalog, ids, scratch[d % ring.size()], dates[d]);
    return b.rows.size() * sizeof(Bar1m);
  }

  const Bar1mDay &bars(size_t d) const {
    const Bar1mDay &b = ring[d % ring.size()];
    assert(b.date == dates[d] && "源环格不是日 d (pf_ready 流控 / 环长不符)");
    return b;
  }

  // 无工作区: 一个 asset-day = 本资产当日 255 行 (无 bar → 零事件日), 返回 bar 数
  struct Runner {
    const TierFeed &feed;
    Runner(TierFeed &f, int) : feed(f) {}
    size_t run(CoreBar &core, size_t sub, size_t d, const BarFeatureStore::Day &day, BarFeatureStore &store) {
      const misc::Ymd date = feed.dates[d];
      const Bar1m *bars = feed.bars(d).asset(sub);
      if (!bars) {
        core.empty_day(date, day, store); // 无 bar1m = 零事件日: 与正常日同路径
        return 0;
      }

      core.begin_day(date, day);
      for (size_t m = 0; m < TRADE_MINUTES_PER_DAY; ++m)
        core.compute_and_store(m, bars[m]);
      core.end_day(store);
      return TRADE_MINUTES_PER_DAY;
    }
  };
};

template bool pipeline<bar::Tier>(const PipelineArgs &);
