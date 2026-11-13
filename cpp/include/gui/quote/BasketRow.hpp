// 篮子合成件 (PriceLib 的库段 / 补段同一份): 成员 × 源 → Lane (Fold 一日一份) → 各篮子一日; 普查 census 出发布的成员统计.
//   成员 (谁 × 权) 两步对仗: members = 选股表 → 交易日 D 的成员表 (判据 = shared/Basket.hpp BasketTable::each, 权 = Tape::omega)
//                             drift   = 成员冻结, 过一个交易日 (库末日之后的补段: 随价漂移, 逢换源回目标权)
//   源 (每只一日的 Leg) 三种对仗: LibDay  = all.bar L_1day 日行 (基准 = D−1 行 pre_close) + L_1min 分钟根
//                                 FundDay = 基本面 bar1d 行 (基准 = 当行 pre_close) + bar1m 分钟根
//                                 SinaDay = 新浪归档的末快照 (基准 = 昨收) + 分钟根
//   各源同一个形 (date / size / id / minutes / leg), Lane::run 不分源; 当日 (新浪快照) 的增量合成在 QuoteService (Fold 增量).
//   资产一律全局 id (成员表 / 源 / 漂移比同一下标): 库源逐库轴行给其全局 id, 补段源稠密在资产轴上.
//   库段一趟 lib_pass: 各日的合成互不相干 (前日 = 1) ⇒ 按线程一日一件并行, 日间接续 (点位 / 因子链) 归调用方按日序串行;
//     续算件的盘上头 + 体同一读写 (snap_read / snap_write).
#pragma once

#include "disk/Disk.hpp"
#include "features/Panel/PoolSelect.hpp" // pool::Picks / day_cols
#include "features/Storage/FeatureRead.hpp"
#include "fundamental/AssetAxis.hpp"
#include "gui/quote/Quote.hpp"
#include "misc/exec.hpp"
#include "misc/snap.hpp"
#include "shared/Basket.hpp" // basket_table
#include "shared/Progress.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

struct Bar1m;
struct Bar1mDay;
namespace fundamental {
class Ids;
class Scratch;
struct Shard;
} // namespace fundamental

namespace GUI::Quote {

// 一日的 L_1day 日根行 (A 轴, 日行前 kQuoteCols 列); 缺 / 非有限 → 0
struct Row {
  std::vector<float> px[4], amount, pre; // 开 高 低 收 / 成交额 [元] / 前收 (该行 = 次日除权后前收)
  std::vector<uint8_t> trade;            // ts_valid
};

// 日根行 / 分钟的读: 预分配 (A 轴) / 读交易日 d
void row_alloc(FeatureRead::DayColumns &dc, size_t A);
void read_row(const FeatureRead &rd, misc::Ymd d, FeatureRead::DayColumns &dc, Row &r);
void minute_alloc(FeatureRead::DayColumns &mc, size_t A);
void read_minutes(const FeatureRead &rd, misc::Ymd d, FeatureRead::DayColumns &mc);

// 交易日 d 的成员表 (资产 = 全局 id): 事实 = 当日所在的池 (子池 = 选股表 d 的成员) + 成员源行 (D−1) 的行业, 过 BasketTable::each,
//   权 = Tape::omega 取该篮的权重 (源 = 该篮所依的池 BasketTable::base 的源行; ≤ 0 不进); pk 与 axis 同为 all.bar 轴; Members::cap = D−1 收盘总市值
void members(const pool::Picks &pk, const PoolAxis &axis, misc::Ymd d, Members &m);
// 成员表 m 过交易日 prev → d (成员冻结): 权与 cap 随价 × rel [全局 id] (收 / 前收); 篮子逢其周期换源 (pool::rebal_at) 回目标权 pool::omega (加权 = 漂移后的 cap)
void drift(Members &m, misc::Ymd prev, misc::Ymd d, std::span<const float> rel);
// 一只一日的 L_1min (mc = read_minutes 的读; a = 库轴行) → 分钟根: 无成交的行不收 (与 bar1m 的 bars_of 同一并根)
void bars_of(const FeatureRead::DayColumns &mc, size_t a, Bars &b);

// 基本面源 (补段两侧同一读法): 日根 = bar1d 行 (ref = pre_close), 分钟根 = bar1m 行按 row_slot 并根; 基本面段末日 = AssetInfo::quote_end
inline constexpr const char *kBar1d = "cn_stock_real_bar1d";
// bar1d 一张月分片逐行 f(id, 日根): ref = pre_close; 无成交的行照给 (traded 判)
void each_daily(fundamental::Scratch &xs, const fundamental::Shard &sh, const fundamental::Ids &ids,
                const std::function<void(uint32_t, const Candle &)> &f);
// bar1m 一只一日 (Bar1mDay::asset; nullptr = 无 bar) → 分钟根: 无成交的行不收
void bars_of(const Bar1m *m, Bars &b);

// 源 (三种同形): date = 交易日; size() 行, 行 j 的全局 id = id(j) (升序); minutes() = 合成分钟;
//   leg(j, c, b) = 一行的 Leg (c / b = 调用方给的格, Leg 可指向它们)
// all.bar 库源 (行 = 库轴): 基准 = 前一行 prev 的 pre_close, 日根 = 当日行 cur, 分钟根 = mc 的 L_1min (nullptr = 只合日根)
struct LibDay {
  misc::Ymd date;
  const PoolAxis &axis;
  const Row &prev, &cur;
  const FeatureRead::DayColumns *mc;
  size_t size() const { return cur.trade.size(); }
  uint32_t id(size_t j) const { return axis.global(j); }
  bool minutes() const { return mc != nullptr; }
  Leg leg(size_t j, Cell &c, Bars &b) const;
};
// 基本面源 (行 = 全局 id): 基准 / 日根 = bar1d 行 px (ref = pre_close; 无行 → 收 0), 分钟根 = bar1m (bd 已载 date, 资产同序)
struct FundDay {
  misc::Ymd date;
  std::span<const Candle> px;
  const Bar1mDay &bd;
  size_t size() const { return px.size(); }
  uint32_t id(size_t j) const { return static_cast<uint32_t>(j); }
  bool minutes() const { return true; }
  Leg leg(size_t j, Cell &c, Bars &b) const;
};
// 新浪归档源 (archived_day 的读; 行 = 全局 id): 基准 / 日根 = 末快照 px (ref = 昨收; 归档无该只 → 收 0), 分钟根 = bars
struct SinaDay {
  misc::Ymd date;
  std::span<const Candle> px;
  std::span<const Bars> bars;
  size_t size() const { return px.size(); }
  uint32_t id(size_t j) const { return static_cast<uint32_t>(j); }
  bool minutes() const { return true; }
  Leg leg(size_t j, Cell &c, Bars &b) const;
};

// 一日合成件 (逐日复用): 成员表 mem × 源 → Fold → day [篮子]
struct Lane {
  explicit Lane(size_t baskets) : fold(baskets), day(baskets) {}
  Members mem; // 交易日 src.date 的成员表
  Fold fold;
  std::vector<Day> day; // [篮子] 前日 = 1 (rebase 接到点位上)
  // 合成源 src 的一日; each(id, Leg) 逐行 (含非成员)
  template <class Src, class Each>
  void run(const Src &src, Each &&each);
  template <class Src>
  void run(const Src &src) {
    run(src, [](uint32_t, const Leg &) {});
  }

private:
  Cell cell_;
  Bars bars_;
};

template <class Src, class Each>
void Lane::run(const Src &src, Each &&each) {
  fold.open(src.minutes() ? kSlots : 0);
  size_t k = 0;
  for (size_t j = 0; j < src.size(); ++j) {
    const uint32_t id = src.id(j);
    const Leg g = src.leg(j, cell_, bars_);
    each(id, g);
    if (k < mem.size() && mem.id[k] == id)
      fold.add(g, mem.of(k++));
  }
  assert(k == mem.size() && "成员都在源的行里");
  fold.finish(src.date, day);
}

// 库段一趟的一件 (逐日复用): D−1 / D 日根行 + 读件 + 一日合成件; minutes = 本日合了分钟
struct LibWork {
  LibWork(size_t A, size_t B) : lane(B) {
    row_alloc(dc, A);
    minute_alloc(mc, A);
  }
  Lane lane;
  Row prev, cur;
  FeatureRead::DayColumns dc, mc; // 日根行 / 分钟的读
  bool minutes = false;
};

// 库段一趟: 选股表 pk 的日轴 dates [from, N) (from ≥ 1; pk 与 axis 同为 all.bar 轴), 每批 T = 线程数日:
//   并行  一日一件 (批内第 k 件): 读 D−1 / D 日根行 (+ D 的 L_1min, i ≥ minutes_from) → members → Lane::run; each(i, id, Leg) 逐只 (各日各只互不相干)
//   串行  按日序 chain(i, k, LibWork &) (日间接续: 点位 / 因子链), 批末 batch()
//   x 记 Loading 天; false = 取消 / 判废 (rd.stale)
template <class Each, class Chain, class Batch>
bool lib_pass(const PoolAxis &axis, const FeatureRead &rd, const pool::Picks &pk, size_t from, size_t minutes_from, misc::exec::ForkJoin &pool,
              const std::atomic<bool> &cancel, Progress &x, Each &&each, Chain &&chain, Batch &&batch) {
  const std::vector<misc::Ymd> &dates = pk.tape->dates;
  const size_t N = dates.size(), T = static_cast<size_t>(pool.threads());
  assert(from >= 1 && from <= N);
  std::vector<std::unique_ptr<LibWork>> works;
  for (size_t k = 0; k < std::min(T, N - from); ++k)
    works.push_back(std::make_unique<LibWork>(axis.size(), basket_table().size()));
  x.begin(Phase::Loading, N - from);
  for (size_t i0 = from; i0 < N; i0 += T) {
    if (cancel.load(std::memory_order_relaxed) || rd.stale())
      return false;
    const size_t n = std::min(T, N - i0);
    pool.tasks(static_cast<int>(n), [&](int kk, int) {
      const size_t k = static_cast<size_t>(kk), i = i0 + k;
      LibWork &w = *works[k];
      const misc::Ymd d = dates[i];
      w.minutes = i >= minutes_from;
      read_row(rd, dates[i - 1], w.dc, w.prev);
      read_row(rd, d, w.dc, w.cur);
      if (w.minutes)
        read_minutes(rd, d, w.mc);
      members(pk, axis, d, w.lane.mem);
      w.lane.run(LibDay{d, axis, w.prev, w.cur, w.minutes ? &w.mc : nullptr}, [&](uint32_t id, const Leg &g) { each(i, id, g); });
    });
    if (rd.stale())
      return false;
    for (size_t k = 0; k < n; ++k) {
      chain(i0 + k, k, *works[k]);
      x.publish(1);
    }
    batch();
  }
  return true;
}

// 续算件的盘上一份 = 头 + 体 (体 = body(ar)): 读 = 有文件且头 == want 才读体, 返回 = 读了体
template <class Head, class Body>
bool snap_read(disk::Node n, const disk::Key &k, const Head &want, Body &&body) {
  if (!disk::exists(n, k))
    return false;
  const std::string blob = disk::read(n, k);
  misc::snap::Load ar(std::as_bytes(std::span(blob)));
  Head h;
  ar(h);
  if (!(h == want))
    return false;
  body(ar);
  assert(ar.done() && "续算件: 文件尾有多余字节");
  return true;
}
template <class Head, class Body>
void snap_write(disk::Node n, const disk::Key &k, const Head &head, Body &&body) {
  std::vector<std::byte> blob;
  misc::snap::Save ar(blob);
  ar(head);
  body(ar);
  disk::write(n, k, {reinterpret_cast<const char *>(blob.data()), blob.size()});
}

// 普查: 成员表 m 在键集 keys (篮子) 上 → t 的 members / weight / cap / cons / overlap (见 Tape)
void census(const Members &m, std::span<const uint32_t> keys, Tape &t);

} // namespace GUI::Quote
