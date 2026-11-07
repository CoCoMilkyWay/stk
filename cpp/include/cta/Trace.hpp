#pragma once

// 单资产的滚动调参全期轨迹 (只用本资产, 不用未来信息); 各遍同一骨架: 逐根核串起来跑, 不落中间列
//   取列 (串行): 网格各配置的中线 / 半宽从 Tape 取齐, 之后 Tape 只读
//   评分遍 (并行, 网格逐配置): Signal → Size → Split, 出目标项日前缀
//   选参: 每日以此前至多 W 日 (不足 W 日 = 自首日起全部, expanding) 各配置目标值拟岭回归曲面 (Tune.hpp), 最高点配置用到当日
//         (首日 / 窗内根数不足窗长一半 / 窗内各配置的带都未就绪 = 未选参, 买入持有, 与基准同口径)
//   拼接遍 (串行, 逐日): 信号核无状态, 只跑当日选中配置, 刀写进轨迹列 Cuts
//   回测: 轨迹的刀 → Size → Pnl (切换计成本, 成本分项见 Engine.hpp); 买入持有 = 满仓不动 (不定仓, 无不交易带, 无层线 / 越线差)
#include "cta/Metrics.hpp"
#include "cta/Tune.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <utility>
#include <vector>

namespace cta {

struct Track {
  Grid grid;                               // 所选模型的参数网格 + 曲面
  std::vector<std::pair<size_t, int>> seg; // 选参段: (起根, grid.at 下标; −1 = 未选参), 起根升序
  Cuts cut;                                // [根] 当日选中配置的刀 (未选参日: 带 NaN / 仓 1 = 买入持有)
  Book strat, hold;                        // 策略 / 买入持有
};

// se = 有效根 (t = 日下标 · grid.S + 槽), 须跨 2 日以上; grid = 所选模型的网格; goal = 优化目标 (kGoals 下标); W = 滚动窗上限 (日, ≥ kMinRoll); ex = 执行器 (tasks)
// stop() → true = 放弃 (返回 false); tick() 评分遍每算完一个网格配置 (共 grid.at.size() 个); 两者由并行任务调用
template <class Exec, class Stop, class Tick>
bool trace(const Series &se, Grid grid, size_t goal, size_t W, const Setup &u, Exec &ex, Track &out, Stop stop, Tick tick) {
  const size_t S = grid.S;
  const Days days = days_of(se, S);
  const size_t n = se.c.size(), nd = days.size(), ns = grid.at.size();
  assert(n > 1 && W >= kMinRoll && nd > 1 && ns > 0 && "样本须跨 2 日以上");
  const Ladder m = grid.ladder(0); // 拼成的刀列的分层 (越线差按其层线计)
  for (size_t x = 1; x < ns; ++x)
    assert(grid.p(x, kLayer) == grid.p(0, kLayer) && "各配置同一分层");
  Tape t(se, S);
  std::vector<Src> src(ns);
  for (size_t x = 0; x < ns; ++x)
    src[x] = grid.src(t, x);

  std::vector<Terms> acc(ns); // [配置] 目标项日前缀
  ex.tasks(static_cast<int>(ns), [&](int k, int) {
    if (stop())
      return;
    const size_t x = static_cast<size_t>(k);
    const Signal sig = grid.signal(t, x, src[x]);
    Size size(t, u);
    Split split = goal_of(goal, t, u, sig.layer, nd);
    for (size_t d = 0; d < nd; ++d) {
      for (size_t i = days.off[d]; i < days.off[d + 1]; ++i) {
        const Cut c = sig(i);
        split(i, c, size(i, c.w));
      }
      split.seal();
    }
    acc[x] = std::move(split.acc);
    tick();
  });
  if (stop())
    return false;

  std::vector<int> pick(nd, -1); // [日] → 配置
  std::vector<double> y(ns);
  for (size_t d = 1; d < nd; ++d) {
    const size_t d0 = d > W ? d - W : 0;
    if (2 * (days.off[d] - days.off[d0]) < (d - d0) * S)
      continue;
    bool live = false;
    for (size_t x = 0; x < ns; ++x)
      y[x] = acc[x].over(d0, d), live |= acc[x].live(d0, d);
    if (live)
      pick[d] = static_cast<int>(grid.ridge.best(y));
  }

  Cuts cut(n);
  for (size_t d = 0; d < nd; ++d) {
    if (pick[d] < 0) {
      std::fill(cut.w.begin() + static_cast<std::ptrdiff_t>(days.off[d]), cut.w.begin() + static_cast<std::ptrdiff_t>(days.off[d + 1]), 1.0f);
      continue;
    }
    const size_t x = static_cast<size_t>(pick[d]);
    const Signal sig = grid.signal(t, x, src[x]);
    for (size_t i = days.off[d]; i < days.off[d + 1]; ++i)
      cut.put(i, sig(i));
  }

  out.seg.clear();
  for (size_t d = 0; d < nd; ++d)
    if (days.off[d] < days.off[d + 1] && (out.seg.empty() || out.seg.back().second != pick[d]))
      out.seg.emplace_back(days.off[d], pick[d]);
  Cuts hold(n);
  std::fill(hold.w.begin(), hold.w.end(), 1.0f);
  out.strat = book_of(t, cut, m, u);
  Setup still = u;
  still.vol = still.buffer = 0;
  out.hold = book_of(t, hold, m, still);
  out.cut = std::move(cut);
  out.grid = std::move(grid);
  return true;
}

} // namespace cta
