#pragma once

// 单资产的滚动调参全期轨迹 (只用本资产, 不用未来信息): 中线逐日选参, 分层 (固定) 沿拼好的中线跑一遍
//   打分 (格点首次进网格时, 一批): 取中线列 (串行, 写 Tape) → 目标项日前缀 goal_of 全期一遍 (并行, 只读 Tape)
//   选参 (逐日): 以此前至多 W 日 (不足 W 日 = 自首日起全部, expanding) 网格各配置的目标值拟岭回归曲面 (Tune.hpp), 最高点配置用到当日;
//         最高点落在网格边上 (未到硬边界) → 以它为中心移网格, 补打新格点再拟, 当日至多 kHops 次; 中心留到次日 (各日只读当日之前的窗)
//         (首日 / 窗内根数不足窗长一半 / 窗内各配置的中线都未就绪 = 未选参, 买入持有, 与基准同口径)
//         各配置的目标项日前缀留作 acc (参数点云: 至某日打过分的全部配置在其选参窗内的逐日目标值 → 行为坐标 cloud_of; 当日网格着色 = 窗内目标值, 其余 = 历史点)
//   拼接遍 (串行, 逐根): 当日选中配置的中线 + 层距 kSpace · ATR → 分层 Ladder (状态跨日接续; 未选参日 = Hold, 分层复位), 刀写进轨迹列 Cuts
//   两本账: drive (Cuts → Pnl) = 策略 (调仓计成本, 成本分项见 Engine.hpp) ↔ drive (Hold → Pnl, 仓位层不调) = 买入持有 (满仓不动, 无越线差)
#include "cta/Metrics.hpp"
#include "cta/Tune.hpp"

#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <map>
#include <utility>
#include <vector>

namespace cta {

using P3 = std::array<float, 3>;

// 参数点云: 配置的行为坐标 (cloud_of)
struct Cloud {
  std::vector<P3> at; // [配置] 坐标
  P3 share{};         // 前 3 主成分各占总方差
};

inline constexpr int kHops = 8; // 一日内最多移网格几次

// 一日的选参
struct Pick {
  Q center;        // 网格中心 (未选参 = 空)
  size_t from = 0; // 选参窗首日
  int pt = -1;     // 所用配置 (pts 下标; −1 = 未选参)
  size_t seen = 0; // 至当日打过分的配置数 (pts 前缀)
};

struct Track {
  Grid grid;                               // 所选中线模型的格点 + 网格 + 曲面
  std::vector<Q> pts;                      // [配置] 打过分的格点, 按首次进网格的先后
  std::map<Q, uint32_t> index;             // 格点 → pts 下标
  std::vector<Terms> acc;                  // [配置] 目标项日前缀
  std::vector<Pick> pick;                  // [日] 日下标同 days_of
  std::vector<std::pair<size_t, int>> seg; // 选参段: (起根, pts 下标; −1 = 未选参), 起根升序
  Cuts cut;                                // [根] 分层的刀 (未选参日 = Hold: 线 NaN / 仓 1)
  Book strat, hold;                        // 策略 / 买入持有
};

// 行为坐标: score [日][配置] 里各配置的逐日目标值序列作向量, 每日配置间去均值 → R [nd][ns]; 前 3 主成分坐标 = √λ · (RᵀR 的特征向量) = Rᵀ · (RRᵀ 的单位特征向量)
//   幂迭代在两个 Gram 里小的那个上做 (配置多于日 = RRᵀ [nd][nd]); 表现相近的配置挨在一起; 秩不足 3 的维 = 0
inline Cloud cloud_of(const std::vector<float> &score, size_t ns) {
  const size_t nd = score.size() / ns;
  std::vector<double> mu(nd, 0); // [日] 配置间均值
  for (size_t d = 0; d < nd; ++d) {
    for (size_t x = 0; x < ns; ++x)
      mu[d] += score[d * ns + x];
    mu[d] /= static_cast<double>(ns);
  }
  const auto R = [&](size_t d, size_t x) { return score[d * ns + x] - mu[d]; };
  const bool dual = nd < ns;
  const size_t p = dual ? nd : ns;
  std::vector<double> G(p * p, 0);
  if (dual)
    for (size_t i = 0; i < nd; ++i)
      for (size_t j = 0; j <= i; ++j) {
        double s = 0;
        for (size_t x = 0; x < ns; ++x)
          s += R(i, x) * R(j, x);
        G[i * p + j] = G[j * p + i] = s;
      }
  else
    for (size_t d = 0; d < nd; ++d)
      for (size_t i = 0; i < ns; ++i) {
        const double r = R(d, i);
        for (size_t j = 0; j < ns; ++j)
          G[i * p + j] += r * R(d, j);
      }
  const auto dot = [](const std::vector<double> &a, const std::vector<double> &b) {
    double s = 0;
    for (size_t i = 0; i < a.size(); ++i)
      s += a[i] * b[i];
    return s;
  };
  double tr = 0;
  for (size_t i = 0; i < p; ++i)
    tr += G[i * p + i];
  Cloud o{std::vector<P3>(ns, P3{0, 0, 0}), {}};
  std::vector<std::vector<double>> vs; // 已取的特征向量
  for (size_t k = 0; k < 3 && tr > 0; ++k) {
    std::vector<double> v(p), u(p);
    for (size_t i = 0; i < p; ++i)
      v[i] = std::cos(1.0 + static_cast<double>(i) * (static_cast<double>(k) + 1.7)); // 确定性起点
    double lam = 0;
    for (int it = 0; it < 200; ++it) {
      if (it)
        v = u;
      for (const std::vector<double> &e : vs) {
        const double c = dot(v, e);
        for (size_t i = 0; i < p; ++i)
          v[i] -= c * e[i];
      }
      const double nv = std::sqrt(dot(v, v));
      if (!(nv > 0)) {
        lam = 0;
        break;
      }
      for (double &x : v)
        x /= nv;
      for (size_t i = 0; i < p; ++i) {
        u[i] = 0;
        for (size_t j = 0; j < p; ++j)
          u[i] += G[i * p + j] * v[j];
      }
      lam = dot(v, u);
    }
    if (!(lam > 1e-12 * tr))
      break;
    for (size_t x = 0; x < ns; ++x) {
      double s = 0;
      if (dual)
        for (size_t d = 0; d < nd; ++d)
          s += R(d, x) * v[d];
      else
        s = std::sqrt(lam) * v[x];
      o.at[x][k] = static_cast<float>(s);
    }
    o.share[k] = static_cast<float>(lam / tr);
    vs.push_back(std::move(v));
  }
  return o;
}

// se = 有效根 (t = 日下标 · grid.S + 槽), 须跨 2 日以上; grid = 所选中线模型的网格; goal = 优化目标 (kGoals 下标); dir = 方向 (kDirs 下标)
// W = 滚动窗上限 (日, ≥ kMinRoll); ex = 执行器 (tasks); stop() → true = 放弃 (返回 false), 由并行任务调用; tick() 选参每过一日 (共 days_of(se, grid.S).size() − 1 日)
template <class Exec, class Stop, class Tick>
bool trace(const Series &se, Grid grid, size_t goal, size_t dir, size_t W, const Setup &u, Exec &ex, Track &out, Stop stop, Tick tick) {
  const size_t S = grid.S;
  const Days days = days_of(se, S);
  const size_t n = se.c.size(), nd = days.size();
  assert(n > 1 && W >= kMinRoll && nd > 1 && "样本须跨 2 日以上");
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

  std::vector<Pick> pick(nd);
  Q c = grid.home();
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
        const Q &q = g[grid.ridge.best(y)];
        if (hop < kHops && grid.edge(c, q)) {
          c = q;
          continue;
        }
        pick[d] = {c, d0, static_cast<int>(index.at(q))};
        break;
      }
    pick[d].seen = pts.size();
  }

  Cuts cut(n);
  Ladder lad{s};
  for (size_t d = 0; d < nd; ++d) {
    const int x = pick[d].pt;
    if (x < 0)
      lad = Ladder{s};
    for (size_t i = days.off[d]; i < days.off[d + 1]; ++i)
      cut.put(i, x < 0 ? Hold{}(i) : lad(t.c[i], (*mid[static_cast<size_t>(x)])[i], kSpace * t.atr[i]));
  }

  out.seg.clear();
  for (size_t d = 0; d < nd; ++d)
    if (days.off[d] < days.off[d + 1] && (out.seg.empty() || out.seg.back().second != pick[d].pt))
      out.seg.emplace_back(days.off[d], pick[d].pt);
  Pnl strat(t), hold(t);
  drive(t, u.rates, Size(t, u.sizing, u.bpy), cut, strat);
  drive(t, u.rates, Size(t, {}, u.bpy), Hold{}, hold);
  out.strat = book_of(t, std::move(strat.run), u.bpy);
  out.hold = book_of(t, std::move(hold.run), u.bpy);
  out.cut = std::move(cut);
  out.grid = std::move(grid);
  out.pts = std::move(pts), out.index = std::move(index), out.acc = std::move(acc);
  out.pick = std::move(pick);
  return true;
}

} // namespace cta
