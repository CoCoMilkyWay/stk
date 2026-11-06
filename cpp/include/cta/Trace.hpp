#pragma once

// 单资产的滚动调参全期轨迹 (只用本资产, 不用未来信息)
//   取列: 网格各配置的中线 / 半宽从 Tape 取齐 (串行), 之后 Tape 只读
//   评分遍 (并行): 网格逐配置 带 → 仓位 → 仓位层 → 目标项日前缀
//   选参: 每日以前 W 日各配置目标值拟岭回归曲面 (Tune.hpp), 最高点配置用到当日 (前 W 日 / 窗内根数不足满窗一半 = 空仓)
//   拼接遍 (并行): 选中过的配置各算全期 (状态常热), 逐日取当日选中者的带与目标仓位拼成一条; 再回测 (切换计成本)
#include "cta/Metrics.hpp"
#include "cta/Tune.hpp"

#include <algorithm>
#include <cassert>
#include <utility>
#include <vector>

namespace cta {

struct Track {
  Grid grid;                               // 所选模型的参数网格 + 曲面
  std::vector<std::pair<size_t, int>> seg; // 选参段: (起根, grid.specs 下标; −1 = 空仓), 起根升序
  Sig sig;                                 // [根] 当日选中配置的带 + 目标仓位 (空仓日: 带 NaN / 仓 0)
  Book strat, hold;                        // 策略 / 买入持有
};

// 根段 [b, e) 的带与目标仓位 src → dst
inline void splice(Sig &dst, const Sig &src, size_t b, size_t e) {
  const auto cut = [&](Col &y, const Col &x) { std::copy(x.begin() + b, x.begin() + e, y.begin() + b); };
  cut(dst.band.up, src.band.up), cut(dst.band.dn, src.band.dn), cut(dst.pos.w, src.pos.w), cut(dst.pos.stop, src.pos.stop);
}

// se = 有效根 (t = 日下标 · S + 槽), 须跨过 W 日以上; grid = 所选模型的网格; W = 滚动窗 (日, ≥ kMinRoll); ex = 执行器 (tasks)
// stop() → true = 放弃 (返回 false); tick() 评分遍每算完一个网格配置 (共 grid.specs.size() 个); 两者由并行任务调用
template <class Exec, class Stop, class Tick>
bool trace(const Series &se, size_t S, Grid grid, size_t W, const Setup &u, Exec &ex, Track &out, Stop stop, Tick tick) {
  const Days days = days_of(se, S);
  const size_t n = se.c.size(), nd = days.size(), ns = grid.specs.size();
  assert(n > 1 && W >= kMinRoll && nd > W && ns > 0 && "样本须长于滚动窗");
  Tape t(se);
  std::vector<Src> src(ns);
  for (size_t x = 0; x < ns; ++x)
    src[x] = src_of(t, grid.specs[x].g);

  std::vector<Terms> acc(ns); // [spec] 目标项日前缀
  ex.tasks(static_cast<int>(ns), [&](int k, int) {
    if (stop())
      return;
    const size_t x = static_cast<size_t>(k);
    const Sig g = sig_of(t, grid.specs[x].g, src[x]);
    acc[x] = terms(t, g.band, position(t, g.pos.w, u), u.cost, days);
    tick();
  });
  if (stop())
    return false;

  std::vector<int> pick(nd, -1); // [日] → specs 下标
  std::vector<double> y(ns);
  for (size_t d = W; d < nd; ++d) {
    if (2 * (days.off[d] - days.off[d - W]) < W * S)
      continue;
    for (size_t x = 0; x < ns; ++x)
      y[x] = acc[x].over(d - W, d);
    pick[d] = static_cast<int>(grid.ridge.best(y));
  }

  std::vector<std::vector<size_t>> on(ns); // [spec] 选中它的日
  for (size_t d = 0; d < nd; ++d)
    if (pick[d] >= 0)
      on[static_cast<size_t>(pick[d])].push_back(d);
  std::vector<size_t> used;
  for (size_t x = 0; x < ns; ++x)
    if (!on[x].empty())
      used.push_back(x);
  Sig sig{{Col(n, kNaN), Col(n, kNaN)}, {Col(n, 0), Col(n, kNaN)}};
  ex.tasks(static_cast<int>(used.size()), [&](int k, int) {
    if (stop())
      return;
    const size_t x = used[static_cast<size_t>(k)];
    const Sig g = sig_of(t, grid.specs[x].g, src[x]);
    for (const size_t d : on[x])
      splice(sig, g, days.off[d], days.off[d + 1]);
  });
  if (stop())
    return false;

  out.seg.clear();
  for (size_t d = 0; d < nd; ++d)
    if (days.off[d] < days.off[d + 1] && (out.seg.empty() || out.seg.back().second != pick[d]))
      out.seg.emplace_back(days.off[d], pick[d]);
  out.strat = book_of(t, simulate(t, sig.pos.w, u), u);
  out.hold = book_of(t, simulate(t, hold(t), u), u);
  out.sig = std::move(sig);
  out.grid = std::move(grid);
  return true;
}

} // namespace cta
