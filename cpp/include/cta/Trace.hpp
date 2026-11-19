#pragma once

// 单资产的滚动调参全期轨迹 (只用本资产, 不用未来信息): band 人选固定, 减仓线 / 加仓线逐日依次选参, 分层沿两条变参线一遍跑完
//   打分 (格点首次进网格时, 一批并行; 一个配置一个任务, 只读 Tape; Tune.hpp):
//     减仓线 = 保敞口目标 keep_of, 全期一遍出日前缀; 加仓线 = (减仓格点, 加仓格点) 对的定参完整分层 trips_of, 全期一遍出逐笔
//   选参 (逐日, 先减仓线后加仓线): 各以此前至多自己的窗 W 日 (不足 W 日 = 自首日起全部, expanding) 网格各配置的分拟岭回归曲面 (Tune.hpp), 取连续最高点的参数 (小数);
//         最近的格点落在网格边上 (未到硬边界) → 以它为中心移网格, 补打新格点再拟, 当日至多 kHops 次; 中心留到次日 (各日只读当日之前的窗)
//         加仓线在减仓线最高点的最近格点下打分 (窗内卖出的各笔的中位夏普)
//         (首日 / 减仓线窗内根数不足窗长一半 / 窗内各配置的减仓线都未就绪 = 未选参, 买入持有, 与基准同口径; 加仓线窗内无笔 = 沿用上一日的)
//         阻尼: 所用参数 (对数) 每日朝最高点走 1 / kDamp, 用到当日
//   策略 (一遍, 逐根): 源 Laddered = 变参减仓线核 (逐日所用参数, 状态接续) + 加仓线核 (各份用它卖出当日所用的参数, 之后不变) + band → 分层 Ladder
//         (状态跨日接续; 未选参日 = Hold, 分层复位; 减仓线 / band 预热 = Hold) → 汇 = 损益 Pnl + 录刀 Record (档线快照随之交给 Cuts)
//   两本账: 策略 (调仓计成本, 成本分项见 Engine.hpp) ↔ drive (Hold → Pnl) = 买入持有 (满仓不动, 无越线差)
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

// 模型与回测设定 (费率另给: 系统配置); 改了即全部重算
struct Spec {
  size_t band = kDev;    // band 模型 (kBandKinds 下标)
  float band_win = 1;    // band 窗 (年, kMinYears ~ kMaxYears)
  size_t reduce = kKama; // 减仓线模型 (kReduceKinds 下标)
  float reduce_win = 1;  // 减仓线滚动调参窗 (年)
  size_t add = kDecay;   // 加仓线模型 (kAddKinds 下标)
  float add_win = 1;     // 加仓线滚动调参窗 (年)
  int grid = kGrid;      // 网格配置数上限 (减仓线 / 加仓线各自按它分到自己的参数轴; split)
  Rule rule;             // 分层规则
  int dir = kLong;       // 方向 (kDirs 下标)
  bool operator==(const Spec &) const = default;
};

// 一个模型的逐日选参
struct Path {
  Grid grid;                // 格点 + 网格 + 曲面
  std::vector<uint8_t> sel; // [日] 已选参
  std::vector<V> raw, use;  // [日] 曲面最高点的参数 / 阻尼后所用的参数 (轴的单位; 未选参 = 0)
};

struct Track {
  Path reduce, add; // 减仓线 / 加仓线
  Days days;        // 日轴 (days_of)
  Cuts cut;         // [根] 分层的刀 + 档线快照 (未选参日 / 预热 = Hold: 无档线 / 仓 1)
  Book strat, hold; // 策略 / 买入持有
};

// 打分表: 键 → 分; 键首次进来时一批并行打分 (一个键一个任务)
template <class Key, class R>
struct Scores {
  std::map<Key, uint32_t> index;
  std::vector<Key> keys;
  std::vector<R> val;
  const R &at(const Key &k) const { return val[index.at(k)]; }
  template <class Exec, class Stop, class F>
  void enroll(Exec &ex, const std::vector<Key> &g, Stop &stop, F f) {
    const size_t b = keys.size();
    for (const Key &k : g)
      if (index.emplace(k, static_cast<uint32_t>(keys.size())).second)
        keys.push_back(k);
    val.resize(keys.size());
    ex.tasks(static_cast<int>(keys.size() - b), [&](int k, int) {
      if (stop())
        return;
      const size_t x = b + static_cast<size_t>(k);
      val[x] = f(keys[x]);
    });
  }
};

// 一个模型当日选参: 中心 c 的网格先 enroll (补打新格点; false = 放弃), 各配置的分 y[x] = score(格点, live) 拟曲面取连续最高点 v (q = 最近的格点);
//   q 落在网格边上 → 以它为中心移网格再拟 (至多 kHops 次); 各配置都不 live = 不选 (false)
template <class Enroll, class Score>
bool climb(const Grid &g, Q &c, Q &q, V &v, std::vector<double> &y, Enroll enroll, Score score) {
  for (int hop = 0;; ++hop) {
    const std::vector<Q> net = g.around(c);
    if (!enroll(net))
      return false;
    bool live = false;
    for (size_t x = 0; x < net.size(); ++x)
      y[x] = score(net[x], live);
    if (!live)
      return false;
    v = g.values(c, g.ridge.peak(y), q);
    if (hop < kHops && g.edge(c, q)) {
      c = q;
      continue;
    }
    return true;
  }
}

// 阻尼: 所用参数的对数每日朝最高点走 1 / kDamp (首个选参日 = 最高点)
struct Damp {
  V th{};
  bool warm = false;
  V operator()(const V &raw, size_t na) {
    V o{};
    for (size_t a = 0; a < na; ++a) {
      const double x = std::log(raw[a]);
      th[a] = warm ? th[a] + (x - th[a]) / kDamp : x;
      o[a] = std::exp(th[a]);
    }
    warm = true;
    return o;
  }
};

// 逐日所用参数 (以根计): 未选参日沿用上一日的, 首个选参日之前 = 初始中心
inline std::vector<P> daily_of(const Path &p) {
  std::vector<P> o(p.sel.size(), p.grid.p(p.grid.home()));
  for (size_t d = 0; d < o.size(); ++d)
    if (p.sel[d])
      o[d] = p.grid.bars(p.use[d]);
    else if (d)
      o[d] = o[d - 1];
  return o;
}

// se = 有效根 (t = 日下标 · S + 槽), 须跨 2 日以上; S = 每日根数; sp = 设定; u = 费率; bpy = 每年根数
// ex = 执行器 (tasks); stop() → true = 放弃 (返回 false), 由并行任务调用; tick() 选参每过一日 (共 days_of(se, S).size() − 1 日)
template <class Exec, class Stop, class Tick>
bool trace(const Series &se, size_t S, const Spec &sp, const Rates &u, double bpy, Exec &ex, Track &out, Stop stop, Tick tick) {
  const Tape t(se, S);
  const Days &days = t.days;
  const size_t n = t.n, nd = days.size(), Wr = win_of(sp.reduce_win), Wa = win_of(sp.add_win);
  assert(n > 1 && nd > 1 && "样本须跨 2 日以上");
  assert(static_cast<size_t>(sp.dir) < std::size(kDirs) && "方向");
  assert(sp.reduce < std::size(kReduceKinds) && sp.add < std::size(kAddKinds) && "减仓线 / 加仓线模型");
  const float s = kDirs[sp.dir].s;
  const Col N = band_of(t, sp.band, win_of(sp.band_win));
  Path pr{grid_of(S, kReduceKinds[sp.reduce], static_cast<size_t>(sp.grid))}, pa{grid_of(S, kAddKinds[sp.add], static_cast<size_t>(sp.grid))};
  for (Path *p : {&pr, &pa})
    p->sel.assign(nd, 0), p->raw.resize(nd), p->use.resize(nd);
  const Grid &gr = pr.grid, &ga = pa.grid;

  Scores<Q, Terms> rs;                           // 减仓格点 → 保敞口日前缀
  Scores<std::pair<Q, Q>, std::vector<Trip>> as; // (减仓格点, 加仓格点) → 逐笔
  std::vector<double> yr(gr.at.size()), ya(ga.at.size()), buf;
  Q cr = gr.home(), ca = ga.home(), qr, qa; // 网格中心 / 最高点的最近格点
  Damp dr, da;
  const auto enroll_r = [&](const std::vector<Q> &g) {
    rs.enroll(ex, g, stop, [&](const Q &q) { return keep_of(t, sp.reduce, gr.p(q), N, s, sp.rule.units, u); });
    return !stop();
  };
  const auto enroll_a = [&](const std::vector<Q> &g) {
    std::vector<std::pair<Q, Q>> ks;
    for (const Q &q : g)
      ks.push_back({qr, q});
    as.enroll(ex, ks, stop, [&](const std::pair<Q, Q> &k) { return trips_of(t, sp.reduce, sp.add, gr.p(k.first), ga.p(k.second), N, s, sp.rule); });
    return !stop();
  };
  for (size_t d = 1; d < nd; ++d) {
    tick();
    const size_t d0 = d > Wr ? d - Wr : 0, da0 = d > Wa ? d - Wa : 0;
    if (2 * (days.off[d] - days.off[d0]) < (d - d0) * S)
      continue;
    V v;
    const bool ok = climb(gr, cr, qr, v, yr, enroll_r, [&](const Q &q, bool &live) {
      const Terms &e = rs.at(q);
      live |= e.live(d0, d);
      return e.over(d0, d);
    });
    if (stop())
      return false;
    if (!ok)
      continue;
    pr.raw[d] = v, pr.sel[d] = 1, pr.use[d] = dr(v, gr.na);
    if (climb(ga, ca, qa, v, ya, enroll_a, [&](const Q &q, bool &live) { return median_of(as.at({qr, q}), t, da0, d, s, u, live, buf); }))
      pa.raw[d] = v, pa.sel[d] = 1, pa.use[d] = da(v, ga.na);
    if (stop())
      return false;
  }

  const std::vector<P> pdr = daily_of(pr), pda = daily_of(pa);
  Cuts cut(n);
  Pnl strat(t), hold(t);
  Record rec{cut};
  with_reduce(sp.reduce, t, [&]<class K>(K &k) {
    with_add(sp.add, t, s, [&]<class A>(A &a) {
      Ladder<A> lad{t, s, sp.rule, a};
      drive(t, u, Laddered<K, A>{k, lad, N, pdr, pda, &pr.sel}, Tee<Pnl, Record>{strat, rec});
      cut.bands = std::move(lad.bands);
    });
  });
  drive(t, u, Hold{}, hold);
  cut.s = s, cut.units = sp.rule.units;

  out.strat = book_of(t, std::move(strat.run), bpy);
  out.hold = book_of(t, std::move(hold.run), bpy);
  out.cut = std::move(cut);
  out.reduce = std::move(pr), out.add = std::move(pa);
  out.days = days;
  return true;
}

} // namespace cta
