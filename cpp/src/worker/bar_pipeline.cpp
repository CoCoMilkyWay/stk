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
#include <memory>
#include <span>
#include <string>
#include <vector>

// 硬源: 预取线程把日 d 的 bar1m 分片解码进源格 cells[store.feed_cell(d)], TS 等 pf_ready(d) 后读.
//   源格随源窗定址 (见 StoreRing): 日 d 的格交给下一个进窗日时 d 已全员计满 (再无读者); 源窗扩格的新格首次备源时才建.
//   日期轴不越过末个 bar1m 日 (source_dates), 轴内每日都须有分片, 构造时断言.
template <>
struct TierFeed<bar::Tier> {
  using Core = CoreBar;
  static constexpr bool kHardFeed = true;
  static constexpr const char *kUnit = "bar";

  // 源日历: 交易日 [PIPELINE_START_DATE, min(assetinfo.bar1m_end, assetinfo.pit_end)] —— 分钟线与基本面 (行 D 所需的
  //   D 盘后 / D+1 盘前表, 见 fund::last_day) 都齐的日才进库
  static std::vector<misc::Ymd> source_dates(const SharedData &data) {
    const std::vector<CalendarDay> &days = data.assetinfo.days;
    assert(!days.empty() && "交易日历未就绪");
    const misc::Ymd end = std::min(data.assetinfo.bar1m_end, data.assetinfo.pit_end);
    std::vector<misc::Ymd> dates;
    for (const CalendarDay &day : days)
      if (day.trading && day.date >= ::config::PIPELINE_START_DATE && day.date <= end)
        dates.push_back(day.date);
    return dates;
  }

  const PoolAxis &axis;
  std::span<const misc::Ymd> dates;
  const fundamental::Catalog catalog;
  fundamental::Ids ids; // A 轴码表 (id = sub; 子池 = 子集, 不查漏收); 构造后只读, 多预取核并发 find
  // 一个源格: 当日分片 + 解码暂存 (一格同一时刻只有一个预取核在写)
  struct Cell {
    Bar1mDay day;
    fundamental::Scratch scratch;
  };
  const StoreRing *store = nullptr;         // bind 后: 日 → 源格号
  std::vector<std::unique_ptr<Cell>> cells; // [源窗格数上限] (预取线程建 / 写, TS 经 pf_close / pf_ready 的 release/acquire 读)

  static size_t cell_bytes(size_t num_assets) { return num_assets * (TRADE_MINUTES_PER_DAY * sizeof(Bar1m) + 1); }

  explicit TierFeed(const PipelineArgs &args)
      : axis(args.libs[TierId::Bar].axis), dates(args.dates),
        ids(args.libs[TierId::Bar].pool() == kAllPool ? fundamental::Ids::Cover::Full : fundamental::Ids::Cover::Part) {
    for (size_t sub = 0; sub < axis.size(); ++sub)
      ids.add(asset_axis().code(axis.global(sub)));
    assert(std::ranges::all_of(dates, [this](misc::Ymd d) { return catalog.find_day("bar1m", d) != nullptr; }) &&
           "bar1m: 日期轴内缺日 (同步漏日)");
  }

  void bind(const StoreRing &s) {
    store = &s;
    cells.resize(s.feed_cap());
  }

  size_t weight(size_t, size_t) const { return 1; } // 每资产每日恰 255 根, 负载均匀

  size_t prefetch(size_t d) {
    std::unique_ptr<Cell> &c = cells[store->feed_cell(d)];
    if (!c)
      c = std::make_unique<Cell>();
    c->day.load(catalog, ids, c->scratch, dates[d]);
    return c->day.rows.size() * sizeof(Bar1m);
  }

  // 源格 = 本资产当日 255 根 bar1m 的首根 (nullptr = 无 bar)
  const Bar1m *cell(size_t sub, size_t d) const {
    const Bar1mDay &b = cells[store->feed_cell(d)]->day;
    assert(b.date == dates[d] && "源格不是日 d (pf_ready 流控 / 源窗定址不符)");
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
