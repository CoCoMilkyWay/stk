#pragma once

// 单资产的滚动调参全期轨迹 (只用本资产, 不用未来信息): 中线逐日选参, 分层 (固定) 沿拼好的中线跑一遍
//   打分 (格点首次进网格时, 一批): 取中线列 (串行, 写 Tape) → 目标项日前缀 goal_of 全期一遍 (并行, 只读 Tape)
//   选参 (逐日): 以此前至多 W 日 (不足 W 日 = 自首日起全部, expanding) 网格各配置的目标值拟岭回归曲面 (Tune.hpp), 取连续最高点的参数 (小数);
//         最近的格点落在网格边上 (未到硬边界) → 以它为中心移网格, 补打新格点再拟, 当日至多 kHops 次; 中心留到次日 (各日只读当日之前的窗)
//         (首日 / 窗内根数不足窗长一半 / 窗内各配置的中线都未就绪 = 未选参, 买入持有, 与基准同口径)
//         阻尼: 所用参数 (对数) 每日朝最高点走 1 / kDamp, 用到当日
//   拼接遍 (串行, 逐根): 变参中线 center_path (逐日所用参数, 状态接续) + 分层波动 (Band 所选度量 / 窗) → 分层 Ladder (状态跨日接续; 未选参日 = Hold, 分层复位; 中线 / 波动预热 = Hold),
//         刀写进轨迹列 Cuts, 档线快照随之交给 Cuts
//   两本账: drive (Cuts → Pnl) = 策略 (调仓计成本, 成本分项见 Engine.hpp) ↔ drive (Hold → Pnl, 仓位层不调) = 买入持有 (满仓不动, 无越线差)
#include "cta/Metrics.hpp"
#include "cta/Tune.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <map>
#include <utility>
#include <vector>

namespace cta {

inline constexpr int kHops = 8;    // 一日内最多移网格几次
inline constexpr double kDamp = 5; // 参数阻尼 (日): 所用参数 (对数) 每日朝曲面最高点走 1 / kDamp

struct Track {
  Grid grid;                                 // 所选中线模型的格点 + 网格 + 曲面
  Days days;                                 // 日轴 (days_of)
  std::vector<uint8_t> sel;                  // [日] 已选参
  std::vector<std::vector<double>> raw, use; // [日][轴] 曲面最高点的参数 / 阻尼后所用的参数 (轴的单位; 未选参 = 空)
  Cuts cut;                                  // [根] 分层的刀 + 档线快照 (未选参日 / 预热 = Hold: 无档线 / 仓 1)
  Book strat, hold;                          // 策略 / 买入持有
};

// se = 有效根 (t = 日下标 · grid.S + 槽), 须跨 2 日以上; grid = 所选中线模型的网格; goal = 优化目标 (kGoals 下标); dir = 方向 (kDirs 下标)
// W = 滚动窗上限 (日, kMinRoll ~ kMaxRoll); ex = 执行器 (tasks); stop() → true = 放弃 (返回 false), 由并行任务调用; tick() 选参每过一日 (共 days_of(se, grid.S).size() − 1 日)
template <class Exec, class Stop, class Tick>
bool trace(const Series &se, Grid grid, size_t goal, size_t dir, size_t W, const Setup &u, Exec &ex, Track &out, Stop stop, Tick tick) {
  const size_t S = grid.S;
  Days days = days_of(se, S);
  const size_t n = se.c.size(), nd = days.size();
  assert(n > 1 && W >= kMinRoll && W <= kMaxRoll && nd > 1 && "样本须跨 2 日以上");
  assert(dir < std::size(kDirs) && "方向");
  const float s = kDirs[dir].s;
  Tape t(se, S);

  std::vector<Q> pts;
  std::map<Q, uint32_t> index;
  std::vector<const Col *> mid; // [配置] 中线列 (Tape 缓存里)
  std::vector<Terms> acc;
  // 网格 g 里尚未打分的格点进来: 取列 (串行) → 全期打分 (并行)
  const auto enroll = [&](const std::vector<Q> &g) {
    const size_t b = pts.size();
    for (const Q &q : g)
      if (index.emplace(q, static_cast<uint32_t>(pts.size())).second)
        pts.push_back(q), mid.push_back(&grid.mid(t, q));
    acc.resize(pts.size());
    if (pts.size() == b)
      return;
    ex.tasks(static_cast<int>(pts.size() - b), [&](int k, int) {
      if (stop())
        return;
      const size_t x = b + static_cast<size_t>(k);
      acc[x] = goal_of(goal, t, days, *mid[x], s, u.rates);
    });
  };

  std::vector<uint8_t> sel(nd, 0);
  std::vector<std::vector<double>> raw(nd), use(nd);
  std::vector<double> th; // 阻尼状态: 所用参数的对数 (空 = 未起)
  Q c = grid.home(), q;
  std::vector<double> y(grid.at.size());
  for (size_t d = 1; d < nd; ++d) {
    tick();
    const size_t d0 = d > W ? d - W : 0;
    if (2 * (days.off[d] - days.off[d0]) >= (d - d0) * S)
      for (int hop = 0;; ++hop) {
        const std::vector<Q> g = grid.around(c);
        enroll(g);
        if (stop())
          return false;
        bool live = false;
        for (size_t x = 0; x < g.size(); ++x) {
          const Terms &e = acc[index.at(g[x])];
          y[x] = e.over(d0, d), live |= e.live(d0, d);
        }
        if (!live)
          break;
        std::vector<double> v = grid.values(c, grid.ridge.peak(y), q);
        if (hop < kHops && grid.edge(c, q)) {
          c = q;
          continue;
        }
        raw[d] = std::move(v), sel[d] = 1;
        break;
      }
    if (!sel[d])
      continue;
    if (th.empty())
      for (const double v : raw[d])
        th.push_back(std::log(v));
    else
      for (size_t a = 0; a < th.size(); ++a)
        th[a] += (std::log(raw[d][a]) - th[a]) / kDamp;
    for (const double v : th)
      use[d].push_back(std::exp(v));
  }

  // 变参中线: 首个选参日之前沿用其参数, 未选参日沿用上一日的
  Col path;
  if (const auto first = std::ranges::find(sel, 1); first != sel.end()) {
    std::vector<P> pd(nd);
    const std::vector<double> *v = &use[static_cast<size_t>(first - sel.begin())];
    for (size_t d = 0; d < nd; ++d) {
      if (sel[d])
        v = &use[d];
      pd[d] = grid.bars(*v);
    }
    path = center_path(t, days, grid.kind, pd);
  }
  Cuts cut(n);
  const Col &N = t.band(u.band.swing, u.band.days);
  Ladder lad{s, u.band};
  for (size_t d = 0; d < nd; ++d) {
    const bool on = sel[d];
    if (!on)
      lad.reset();
    for (size_t i = days.off[d]; i < days.off[d + 1]; ++i) {
      const Cut x = on ? lad(t.c[i], path[i], N[i], i + 1 == days.off[d + 1]) : Cut{};
      cut.put(i, x.h > 0 ? x : Hold{}(i));
    }
  }
  cut.bands = std::move(lad.bands), cut.s = s, cut.units = u.band.units;

  Pnl strat(t), hold(t);
  drive(t, u.rates, Size(t, u.sizing, u.bpy), cut, strat);
  drive(t, u.rates, Size(t, {}, u.bpy), Hold{}, hold);
  out.strat = book_of(t, std::move(strat.run), u.bpy);
  out.hold = book_of(t, std::move(hold.run), u.bpy);
  out.cut = std::move(cut);
  out.grid = std::move(grid);
  out.days = std::move(days);
  out.sel = std::move(sel), out.raw = std::move(raw), out.use = std::move(use);
  return true;
}

} // namespace cta
