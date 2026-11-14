#pragma once

// 单资产的滚动调参全期轨迹 (只用本资产, 不用未来信息): 中线逐日选参, 分层 (固定) 沿变参中线一遍跑完
//   打分 (格点首次进网格时, 一批并行; 一个配置一个任务, 只读 Tape): 定参中线核当开关的逐根链 goal_of, 全期一遍出日前缀 (不留中线列)
//   选参 (逐日): 以此前至多 W 日 (不足 W 日 = 自首日起全部, expanding) 网格各配置的目标值拟岭回归曲面 (Tune.hpp), 取连续最高点的参数 (小数);
//         最近的格点落在网格边上 (未到硬边界) → 以它为中心移网格, 补打新格点再拟, 当日至多 kHops 次; 中心留到次日 (各日只读当日之前的窗)
//         (首日 / 窗内根数不足窗长一半 / 窗内各配置的中线都未就绪 = 未选参, 买入持有, 与基准同口径)
//         阻尼: 所用参数 (对数) 每日朝最高点走 1 / kDamp, 用到当日
//   策略 (一遍, 逐根): 源 Laddered = 变参中线核 (逐日所用参数, 状态接续) + 分层波动 (Spec 所选度量 / 窗) → 分层 Ladder
//         (状态跨日接续; 未选参日 = Hold, 分层复位; 中线 / 波动预热 = Hold) → 仓位层 → 汇 = 损益 Pnl + 录刀 Record (档线快照随之交给 Cuts)
//   两本账: 策略 (调仓计成本, 成本分项见 Engine.hpp) ↔ drive (Hold → Pnl, 仓位层不调) = 买入持有 (满仓不动, 无越线差)
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

// 模型与回测设定 (费率另给: 系统配置)
struct Spec {
  size_t goal = kCapture;                  // 优化目标 (kGoals 下标)
  size_t kind = kKama;                     // 中线模型 (kCenters 下标)
  int dir = kLong;                         // 方向 (kDirs 下标)
  int roll = static_cast<int>(kRoll);      // 滚动窗 (日, kMinRoll ~ kMaxRoll)
  int grid = 9;                            // 网格配置数上限 (各多值轴轮流分格点)
  Sizing sizing;                           // 仓位层
  size_t swing = kDev;                     // 分层波动度量 (kSwings 下标)
  int days = static_cast<int>(kSwingDays); // 分层波动窗 (日; dev ≥ 2)
  Rule rule;                               // 分层规则
  bool operator==(const Spec &) const = default;
};

struct Track {
  Grid grid;                // 所选中线模型的格点 + 网格 + 曲面
  Days days;                // 日轴 (days_of)
  std::vector<uint8_t> sel; // [日] 已选参
  std::vector<V> raw, use;  // [日] 曲面最高点的参数 / 阻尼后所用的参数 (轴的单位; 未选参 = 0)
  Cuts cut;                 // [根] 分层的刀 + 档线快照 (未选参日 / 预热 = Hold: 无档线 / 仓 1)
  Book strat, hold;         // 策略 / 买入持有
};

// 策略源: 变参中线核 c (日 d 用 pd[d]) + 分层波动 N [日] → 分层 lad; 未选参日 = Hold (分层复位), 刀未就绪 = Hold; 核逐根都走 (状态接续)
template <class K>
struct Laddered {
  K &c;
  const Tape &t;
  const std::vector<P> &pd;
  const std::vector<uint8_t> &sel;
  const Col &N;
  Ladder &lad;
  size_t d = 0;
  void day(size_t x) {
    c.day(d = x, pd[x]);
    if (!sel[x])
      lad.reset();
  }
  Cut operator()(size_t i) {
    const float m = c.bar(i);
    const Cut x = sel[d] ? lad(t.c[i], m, N[d], i + 1 == t.days.off[d + 1]) : Cut{};
    return x.h > 0 ? x : Hold{}(i);
  }
};

// se = 有效根 (t = 日下标 · S + 槽), 须跨 2 日以上; S = 每日根数; sp = 设定; u = 费率; bpy = 每年根数
// ex = 执行器 (tasks); stop() → true = 放弃 (返回 false), 由并行任务调用; tick() 选参每过一日 (共 days_of(se, S).size() − 1 日)
template <class Exec, class Stop, class Tick>
bool trace(const Series &se, size_t S, const Spec &sp, const Rates &u, double bpy, Exec &ex, Track &out, Stop stop, Tick tick) {
  const Tape t(se, S);
  const Days &days = t.days;
  const size_t n = t.n, nd = days.size(), W = static_cast<size_t>(sp.roll);
  assert(n > 1 && W >= kMinRoll && W <= kMaxRoll && nd > 1 && "样本须跨 2 日以上");
  assert(static_cast<size_t>(sp.dir) < std::size(kDirs) && "方向");
  const float s = kDirs[sp.dir].s;
  Grid grid = grid_of(S, sp.kind, static_cast<size_t>(sp.grid));

  std::vector<Q> pts;
  std::map<Q, uint32_t> index;
  std::vector<Terms> acc;
  // 网格 g 里尚未打分的格点进来: 一个配置一个任务 (并行)
  const auto enroll = [&](const std::vector<Q> &g) {
    const size_t b = pts.size();
    for (const Q &q : g)
      if (index.emplace(q, static_cast<uint32_t>(pts.size())).second)
        pts.push_back(q);
    acc.resize(pts.size());
    ex.tasks(static_cast<int>(pts.size() - b), [&](int k, int) {
      if (stop())
        return;
      const size_t x = b + static_cast<size_t>(k);
      acc[x] = goal_of(sp.goal, t, grid.kind, grid.p(pts[x]), s, u);
    });
  };

  std::vector<uint8_t> sel(nd, 0);
  std::vector<V> raw(nd), use(nd);
  V th{};            // 阻尼状态: 所用参数的对数
  bool warm = false; // 阻尼已起
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
        const V v = grid.values(c, grid.ridge.peak(y), q);
        if (hop < kHops && grid.edge(c, q)) {
          c = q;
          continue;
        }
        raw[d] = v, sel[d] = 1;
        break;
      }
    if (!sel[d])
      continue;
    for (size_t a = 0; a < grid.na; ++a) {
      const double x = std::log(raw[d][a]);
      th[a] = warm ? th[a] + (x - th[a]) / kDamp : x;
      use[d][a] = std::exp(th[a]);
    }
    warm = true;
  }

  // 逐日所用参数: 首个选参日之前沿用其参数, 未选参日沿用上一日的; 全期未选参 = 初始中心 (各日都按 Hold)
  std::vector<P> pd(nd, grid.p(grid.home()));
  if (const auto first = std::ranges::find(sel, 1); first != sel.end())
    for (size_t d = 0, f = static_cast<size_t>(first - sel.begin()); d < nd; ++d) {
      if (sel[d])
        f = d;
      pd[d] = grid.bars(use[f]);
    }

  Cuts cut(n);
  const Col N = swing_of(t, sp.swing, static_cast<size_t>(sp.days));
  Ladder lad{s, sp.rule};
  Pnl strat(t), hold(t);
  Record rec{cut};
  with_center(grid.kind, t, [&]<class K>(K &k) {
    drive(t, u, Size::of(t, sp.sizing, bpy), Laddered<K>{k, t, pd, sel, N, lad}, Tee<Pnl, Record>{strat, rec});
  });
  drive(t, u, Size{t}, Hold{}, hold);
  cut.bands = std::move(lad.bands), cut.s = s, cut.units = sp.rule.units;

  out.strat = book_of(t, std::move(strat.run), bpy);
  out.hold = book_of(t, std::move(hold.run), bpy);
  out.cut = std::move(cut);
  out.grid = std::move(grid);
  out.days = days;
  out.sel = std::move(sel), out.raw = std::move(raw), out.use = std::move(use);
  return true;
}

} // namespace cta
