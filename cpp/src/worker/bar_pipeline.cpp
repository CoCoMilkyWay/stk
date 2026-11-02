// Bar 档管线 (bar1m + 基本面): 档的源 TierFeed<bar::Tier> + 档入口表 bar_ops.
// 本 TU precise-math (CMakeLists PRECISE 表): Fund 缺失值 / PoolKey 母集判定 / cs_valid 依赖 NaN 比较语义.
#include "worker/feature_pipeline.hpp"

#include "features/Bar1mDay.hpp"
#include "features/CoreBar.hpp"
#include "fundamental/Scan.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <span>
#include <string>
#include <vector>

// 硬源: 预取线程把日 d 的 bar1m 分片解码进源环格 ring[d % R], TS 等 pf_ready(d) 后读.
//   R = StoreRing::feed_ring_size(): 预取窗口跨度全部在格, 日 d 的格被 d + R 复用时 d 已全员计满 (再无读者).
//   日期轴止于末个 bar1m 日 (source_dates), 轴内每日都须有分片, 构造时断言.
template <>
struct TierFeed<bar::Tier> {
  using Core = CoreBar;
  static constexpr bool kHardFeed = true;
  static constexpr const char *kUnit = "bar";

  // 源日历: 交易日 [PIPELINE_START_DATE, assetinfo.bar1m_end]; Fund 收盘推进到次日盘前 ⇒ 还须早于日历末个交易日 (日历截到 today)
  static std::vector<misc::Ymd> source_dates(const SharedData &data) {
    const StockDaysVec &days = data.assetinfo.get_stock_days();
    assert(!days.empty() && days.back().trading && "交易日历未就绪 / 末行不是交易日");
    std::vector<misc::Ymd> dates;
    for (const CalendarDay &day : days)
      if (day.trading && day.date >= ::config::PIPELINE_START_DATE && day.date <= data.assetinfo.bar1m_end && day.date < days.back().date)
        dates.push_back(day.date);
    return dates;
  }

  const PoolAxis &axis;
  std::span<const misc::Ymd> dates;
  const fundamental::Catalog catalog;
  fundamental::Ids ids;                      // A 轴码表 (id = sub; 子池 = 子集, 不查漏收); 构造后只读, 多预取核并发 find
  std::vector<Bar1mDay> ring;                // 源环 (预取线程写, TS 经 pf_close / pf_ready 的 release/acquire 读)
  std::vector<fundamental::Scratch> scratch; // 与 ring 逐格对应 (一格同一时刻只有一个预取核在写)

  TierFeed(const PipelineArgs &args, const StoreRing &store)
      : axis(args.libs[TierId::Bar].axis), dates(args.dates),
        ids(args.libs[TierId::Bar].pool() == kAllPool ? fundamental::Ids::Cover::Full : fundamental::Ids::Cover::Part),
        ring(store.feed_ring_size()), scratch(ring.size()) {
    for (size_t sub = 0; sub < axis.size(); ++sub)
      ids.add(asset_axis().code(axis.global(sub)));
    assert(std::ranges::all_of(dates, [this](misc::Ymd d) { return catalog.find_day("bar1m", d) != nullptr; }) &&
           "bar1m: 日期轴内缺日 (同步漏日)");
  }

  size_t weight(size_t) const { return 1; } // 每资产每日恰 255 根, 负载均匀

  size_t prefetch(size_t d) {
    Bar1mDay &b = ring[d % ring.size()];
    b.load(catalog, ids, scratch[d % ring.size()], dates[d]);
    return b.rows.size() * sizeof(Bar1m);
  }

  // 源格 = 源环里本资产当日 255 根 bar1m 的首根 (nullptr = 无 bar)
  const Bar1m *cell(size_t sub, size_t d) const {
    const Bar1mDay &b = ring[d % ring.size()];
    assert(b.date == dates[d] && "源环格不是日 d (pf_ready 流控 / 环长不符)");
    return b.asset(sub);
  }

  // 无工作区
  class Runner {
  public:
    Runner(TierFeed &f, int) : feed_(f) {}

    // 源格的 255 根 bar1m 逐分钟进 core, 返回 bar 数; 缺源 = 不喂 (零事件日)
    size_t feed(CoreBar &core, size_t sub, size_t d) {
      const Bar1m *bars = feed_.cell(sub, d);
      if (!bars)
        return 0;
      for (size_t m = 0; m < TRADE_MINUTES_PER_DAY; ++m)
        core.compute_and_store(m, bars[m]);
      return TRADE_MINUTES_PER_DAY;
    }

    void release() {}

    std::string day_log() { return {}; }

  private:
    const TierFeed &feed_;
  };
};

const TierOps bar_ops = tier_ops_of<bar::Tier>();
