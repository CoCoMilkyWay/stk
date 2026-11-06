#pragma once

// 单资产的滚动调参全期轨迹 (只用本资产, 不用未来信息); 各遍同一骨架: 逐根核串起来跑, 不落中间列
//   取列 (串行): 网格各配置的中线 / 半宽从 Tape 取齐, 之后 Tape 只读
//   评分遍 (并行, 网格逐配置): Signal → Size → Split, 出目标项日前缀
//   选参: 每日以前 W 日各配置目标值拟岭回归曲面 (Tune.hpp), 最高点配置用到当日 (前 W 日 / 窗内根数不足满窗一半 = 空仓)
//   拼接遍 (并行, 选中过的配置): Signal 自首根跑到末个选中日 (状态常热), 选中日的刀写进轨迹列 Cuts
//   回测: 轨迹的目标仓位 → Size → Pnl (切换计成本); 买入持有同一份
#include "cta/Metrics.hpp"
#include "cta/Tune.hpp"

#include <cassert>
#include <utility>
#include <vector>

namespace cta {

struct Track {
  Grid grid;                               // 所选模型的参数网格 + 曲面
  std::vector<std::pair<size_t, int>> seg; // 选参段: (起根, grid.at 下标; −1 = 空仓), 起根升序
  Cuts cut;                                // [根] 当日选中配置的刀 (空仓日: 带 NaN / 仓 0)
  Book strat, hold;                        // 策略 / 买入持有
};

// se = 有效根 (t = 日下标 · grid.S + 槽), 须跨过 W 日以上; grid = 所选模型的网格; W = 滚动窗 (日, ≥ kMinRoll); ex = 执行器 (tasks)
// stop() → true = 放弃 (返回 false); tick() 评分遍每算完一个网格配置 (共 grid.at.size() 个); 两者由并行任务调用
template <class Exec, class Stop, class Tick>
bool trace(const Series &se, Grid grid, size_t W, const Setup &u, Exec &ex, Track &out, Stop stop, Tick tick) {
  const size_t S = grid.S;
  const Days days = days_of(se, S);
  const size_t n = se.c.size(), nd = days.size(), ns = grid.at.size();
  assert(n > 1 && W >= kMinRoll && nd > W && ns > 0 && "样本须长于滚动窗");
  Tape t(se);
  std::vector<Src> src(ns);
  for (size_t x = 0; x < ns; ++x)
    src[x] = grid.src(t, x);

  std::vector<Terms> acc(ns); // [配置] 目标项日前缀
  ex.tasks(static_cast<int>(ns), [&](int k, int) {
    if (stop())
      return;
    const size_t x = static_cast<size_t>(k);
    grid.signal(t, x, src[x], [&](auto sig) {
      Size size(t, u);
      Split split(t, u.cost, nd);
      for (size_t d = 0; d < nd; ++d) {
        for (size_t i = days.off[d]; i < days.off[d + 1]; ++i) {
          const Cut c = sig(i);
          split(i, c, size(i, c.w));
        }
        split.seal();
      }
      acc[x] = std::move(split.acc);
    });
    tick();
  });
  if (stop())
    return false;

  std::vector<int> pick(nd, -1); // [日] → 配置
  std::vector<double> y(ns);
  for (size_t d = W; d < nd; ++d) {
    if (2 * (days.off[d] - days.off[d - W]) < W * S)
      continue;
    for (size_t x = 0; x < ns; ++x)
      y[x] = acc[x].over(d - W, d);
    pick[d] = static_cast<int>(grid.ridge.best(y));
  }

  std::vector<size_t> end(ns, 0); // [配置] 末个选中它的日 + 1 (0 = 未选中)
  for (size_t d = 0; d < nd; ++d)
    if (pick[d] >= 0)
      end[static_cast<size_t>(pick[d])] = d + 1;
  std::vector<size_t> used;
  for (size_t x = 0; x < ns; ++x)
    if (end[x])
      used.push_back(x);
  Cuts cut(n);
  ex.tasks(static_cast<int>(used.size()), [&](int k, int) {
    if (stop())
      return;
    const size_t x = used[static_cast<size_t>(k)];
    grid.signal(t, x, src[x], [&](auto sig) {
      for (size_t d = 0; d < end[x]; ++d) {
        const bool mine = pick[d] == static_cast<int>(x);
        for (size_t i = days.off[d]; i < days.off[d + 1]; ++i) {
          const Cut c = sig(i);
          if (mine)
            cut.put(i, c);
        }
      }
    });
  });
  if (stop())
    return false;

  out.seg.clear();
  for (size_t d = 0; d < nd; ++d)
    if (days.off[d] < days.off[d + 1] && (out.seg.empty() || out.seg.back().second != pick[d]))
      out.seg.emplace_back(days.off[d], pick[d]);
  out.strat = book_of(t, cut.w, u);
  out.hold = book_of(t, Col(n, 1), u);
  out.cut = std::move(cut);
  out.grid = std::move(grid);
  return true;
}

} // namespace cta
