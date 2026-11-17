// Bar 档管线 (bar1m + 基本面): 档的源 TierFeed<bar::Tier> + 档入口表 bar_ops.
// 本 TU precise-math (CMakeLists PRECISE 表): Fund 缺失值 / cs_valid 依赖 NaN 比较语义.
#include "worker/compute/Pipeline.hpp"

#include "features/Bar1mDay.hpp"
#include "features/CoreBar.hpp"
#include "fundamental/AssetAxis.hpp"
#include "fundamental/Scan.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <memory>
#include <new>
#include <span>
#include <string>
#include <vector>

// 硬源: 预取线程把日 d 的 bar1m 分片解码进源格 cells[store.feed_cell(d)], TS 等 pf_ready(d) 后读.
//   源格随源格环定址 (见 StoreRing): 日 d 恒占格 d % F, 交给日 d + F 时 d 已全员计满 (再无读者); 各格首次备源时才建.
//   日期轴不越过末个 bar1m 日 (source_dates), 轴内每日都须有分片, 构造时断言.
template <>
struct TierFeed<bar::Tier> {
  using Core = CoreBar;
  static constexpr bool kHardFeed = true;
  static constexpr const char *kUnit = "bar";
  // 组宽上限 = 张量一条缓存行上的资产数 ([T][F][A] 的一个 (t, f) 里相邻 kGroup 个 a): 组内逐分钟轮流推进, 每条缓存行在 L1 里被组内连续写满.
  //   一个 asset-day 写上万条互不相邻的缓存行 (每条 2 字节), 逐资产推进则每条行要被组内各资产各从 L3 / 内存搬一次, TS 卡在存储缓冲区满
  static constexpr size_t kGroup = std::hardware_constructive_interference_size / sizeof(feature_storage_t);

  // 源日历: 交易日 [fundamental::kSyncStart, min(asset_info.bar1m_end, asset_info.pit_end)] —— 分钟线与基本面 (行 D 所需的
  //   D 盘后 / D+1 盘前表, 见 fund::last_day) 都齐的日才进库
  static std::vector<misc::Ymd> source_dates(const SharedData &data) {
    const std::vector<CalendarDay> &days = data.asset_info.days;
    assert(!days.empty() && "交易日历未就绪");
    const misc::Ymd end = std::min(data.asset_info.bar1m_end, data.asset_info.pit_end);
    std::vector<misc::Ymd> dates;
    for (const CalendarDay &day : days)
      if (day.trading && day.date >= fundamental::kSyncStart && day.date <= end)
        dates.push_back(day.date);
    return dates;
  }

  const PoolAxis &axis;
  const pool::Panel &members;
  std::span<const misc::Ymd> dates;
  const fundamental::Catalog catalog;
  fundamental::Ids ids;                         // A 轴码表 (id = sub; 子池 = 子集, 不查漏收); 构造后只读, 多预取核并发 find
  const StoreRing *store = nullptr;             // bind 后: 日 → 源格号
  std::vector<std::unique_ptr<Bar1mDay>> cells; // [源格数 F] 当日分片 (预取线程建 / 写, TS 经 pf_close / pf_ready 的 release/acquire 读)

  static size_t cell_bytes(size_t num_assets) { return num_assets * (TRADE_MINUTES_PER_DAY * sizeof(Bar1m) + 1); }

  explicit TierFeed(const PipelineArgs &args)
      : axis(args.libs[TierId::Bar].axis), members(args.members), dates(args.dates),
        ids(args.libs[TierId::Bar].pool() == kAllPool ? fundamental::Ids::Cover::Full : fundamental::Ids::Cover::Part) {
    for (size_t sub = 0; sub < axis.size(); ++sub)
      ids.add(asset_axis().code(axis.global(sub)));
    assert(std::ranges::all_of(dates, [this](misc::Ymd d) { return catalog.find("bar1m", d) != nullptr; }) &&
           "bar1m: 日期轴内缺日 (同步漏日)");
  }

  void bind(const StoreRing &s) {
    store = &s;
    cells.resize(s.feed_size());
  }

  size_t weight(size_t sub, size_t d) const { return members.in_pool_at(sub, d) ? 3 : 1; }

  // 解码暂存 (整张分片表) 每预取线程一份, 不随源格: 源格只留稠密分钟 K
  size_t prefetch(size_t d) {
    thread_local fundamental::Scratch scratch;
    std::unique_ptr<Bar1mDay> &c = cells[store->feed_cell(d)];
    if (!c)
      c = std::make_unique<Bar1mDay>();
    c->load(catalog, ids, scratch, dates[d]);
    return c->rows.size() * sizeof(Bar1m);
  }

  // 源格 = 本资产当日 255 根 bar1m 的首根 (nullptr = 无 bar)
  const Bar1m *cell(size_t sub, size_t d) const {
    const Bar1mDay &b = *cells[store->feed_cell(d)];
    assert(b.date == dates[d] && "源格不是日 d (pf_ready 流控 / 源窗定址不符)");
    return b.asset(sub);
  }

  // 无工作区
  class Runner {
  public:
    Runner(TierFeed &f, int) : feed_(f) {}

    // 组内各资产源格的 255 根 bar1m 逐分钟轮流进各自 core, 返回 bar 数; 缺源 = 不喂, 只把时钟推进到同一分钟 (零事件日;
    //   与收盘 end_day 一次补齐逐分钟结算同序同值)
    size_t feed(std::span<CoreBar *const> cores, std::span<const size_t> subs, size_t d) {
      assert(cores.size() == subs.size() && cores.size() <= kGroup);
      std::array<const Bar1m *, kGroup> bars;
      size_t fed = 0;
      for (size_t i = 0; i < subs.size(); ++i) {
        bars[i] = feed_.cell(subs[i], d);
        fed += bars[i] != nullptr;
      }
      for (size_t m = 0; m < TRADE_MINUTES_PER_DAY; ++m)
        for (size_t i = 0; i < cores.size(); ++i) {
          if (bars[i])
            cores[i]->compute_and_store(m, bars[i][m]);
          else
            cores[i]->advance(m);
        }
      return fed * TRADE_MINUTES_PER_DAY;
    }

    void release() {}

    std::string day_log() { return {}; }

  private:
    const TierFeed &feed_;
  };
};

const TierOps bar_ops = tier_ops_of<bar::Tier>();
