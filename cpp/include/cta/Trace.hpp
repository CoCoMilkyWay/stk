#pragma once

// 单资产的滚动调参全期轨迹 (只用本资产, 不用未来信息): band 人选固定, 减仓 / 跟随各一个 Tuner 逐日依次选参, 分层沿两条变参线一遍跑完
//   打分 (格点首次进网格时, 一批并行; 一个配置一个任务, 只读日线原料带 (日 K, 每日 1 根); Tune.hpp):
//     减仓 = 保敞口目标 keep_of, 全期一遍出逐根前缀; 跟随 = (减仓格点, 跟随格点) 对的定参完整分层 trips_of, 全期一遍出逐笔
//   选参 (逐日, 先减仓后跟随, Tuner::step): 各以此前至多自己的窗 W 日 (不足 W 日 = 自首日起全部, expanding) 网格各配置的分拟岭回归曲面 (Tune.hpp), 取连续最高点的参数 (小数);
//         最近的格点落在网格边上 (未到硬边界) → 以它为中心移网格, 补打新格点再拟, 当日至多 kHops 次; 中心留到次日 (各日只读当日之前的窗)
//         跟随在减仓最高点的最近格点下打分 (窗内卖出的各笔的中位夏普)
//         (首日 / 窗内根数不足窗长一半 / 窗内各配置的中线都未就绪 = 未选参, 买入持有, 与基准同口径; 跟随窗内无笔 = 沿用上一日的)
//         阻尼: 所用参数 (对数) 每日朝最高点走 1 / kDamp, 用到当日
//   策略 (一遍, 逐根): 源 Laddered = 变参减仓核 (逐日所用参数, 状态接续) + 分层 Ladder (跟随线各份用它成交当日所用的参数, 之后不变)
//         (状态跨日接续; 未选参日 = Hold, 分层复位; 中线 / band 预热 = Hold) → 汇 = 损益 Run + 档线 Cuts (档线快照随之交给 Cuts)
//   两本账: 策略 (调仓计成本, 成本分项见 Engine.hpp) ↔ Hold → Run = 买入持有 (满仓不动, 无越线差)
#include "cta/Metrics.hpp"
#include "cta/Tune.hpp"

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <map>
#include <optional>
#include <utility>
#include <vector>

namespace cta {

inline constexpr int kHops = 8;    // 一日内最多移网格几次
inline constexpr double kDamp = 5; // 参数阻尼 (日): 所用参数 (对数) 每日朝曲面最高点走 1 / kDamp

// 一个模型的选择: 族表下标 + 窗 (整年, kMinYears ~ kMaxYears; band = 自身的窗, 减仓 / 跟随 = 滚动调参窗)
struct Pick {
  size_t kind;
  int win = 1;
  bool operator==(const Pick &) const = default;
};

// 模型与回测设定 (费率另给: 系统配置); 改了即全部重算
struct Spec {
  Pick band{Band::of<Dev>}, reduce{Reduce::of<Kama>}, follow{Follow::of<Decay>};
  int grid = kGrid; // 网格配置数上限 (减仓 / 跟随各自按它分到自己的参数轴; split)
  Rule rule;        // 分层规则
  int dir = kLong;  // 方向 (kDirs 下标)
  bool operator==(const Spec &) const = default;
};

// 一个模型的逐日选参
struct Path {
  Grid grid;                // 格点 + 网格 + 曲面
  std::vector<uint8_t> sel; // [日] 已选参
  std::vector<P> raw, use;  // [日] 曲面最高点的参数 / 阻尼后所用的参数 (轴的单位; 未选参 = 0)
};

struct Track {
  Path reduce, follow; // 减仓 / 跟随
  Days days;           // 日轴 (days_of)
  Cuts cut;            // [根] 分层的档线 + 快照 (未选参日 / 预热 = Hold: 无档线)
  Book strat, hold;    // 策略 / 买入持有
};

// 打分表: 键 → 分; 键首次进来时一批并行打分 (一个键一个任务); false = 放弃
template <class Key, class R>
struct Scores {
  std::map<Key, R> val;
  const R &at(const Key &k) const { return val.at(k); }
  template <class Exec, class Stop, class F>
  bool enroll(Exec &ex, const std::vector<Key> &g, Stop &stop, F f) {
    std::vector<std::pair<const Key, R> *> fresh;
    for (const Key &k : g)
      if (const auto [it, ok] = val.try_emplace(k); ok)
        fresh.push_back(&*it);
    ex.tasks(static_cast<int>(fresh.size()), [&](int k, int) {
      if (!stop())
        fresh[static_cast<size_t>(k)]->second = f(fresh[static_cast<size_t>(k)]->first);
    });
    return !stop();
  }
};

// 一个模型的逐日选参: 输出 Path + 网格中心 / 最高点的最近格点 + 阻尼
struct Tuner {
  Path p;
  Q c{}, q{};            // 网格中心 / 最高点的最近格点
  P th{};                // 阻尼后参数的对数
  bool warm = false;     // 已有选参日
  std::vector<double> y; // [网格配置] 分

  Tuner(const Kind &k, size_t grid, size_t nd) : p{grid_of(k, grid), std::vector<uint8_t>(nd, 0), std::vector<P>(nd), std::vector<P>(nd)}, y(p.grid.at.size()) {}

  // 日 d 选参: 中心 c 的网格先 enroll (补打新格点; false = 放弃), 各配置的分 score(格点) (NaN = 不可打分, 记 0) 拟曲面取连续最高点 (q = 最近的格点);
  //   q 落在网格边上 → 以它为中心移网格再拟 (至多 kHops 次); 各配置都不可打分 = 不选 (false)
  //   选了 → 记最高点, 所用参数 (对数) 朝它走 1 / kDamp (首个选参日 = 最高点)
  template <class Enroll, class Score>
  bool step(size_t d, Enroll enroll, Score score) {
    const Grid &g = p.grid;
    for (int hop = 0;; ++hop) {
      const std::vector<Q> net = g.around(c);
      if (!enroll(net))
        return false;
      bool live = false;
      for (size_t x = 0; x < net.size(); ++x) {
        const double v = score(net[x]);
        live |= !std::isnan(v), y[x] = std::isnan(v) ? 0 : v;
      }
      if (!live)
        return false;
      const P v = g.values(c, g.ridge.peak(y), q);
      if (hop < kHops && g.edge(c, q)) {
        c = q;
        continue;
      }
      p.sel[d] = 1, p.raw[d] = v;
      for (size_t a = 0; a < g.kind->na; ++a) {
        const double l = std::log(v[a]);
        th[a] = warm ? th[a] + (l - th[a]) / kDamp : l;
        p.use[d][a] = std::exp(th[a]);
      }
      warm = true;
      return true;
    }
  }
  // 逐日所用参数: 未选参日沿用上一日的, 首个选参日之前 = 初始中心
  std::vector<P> used() const {
    std::vector<P> o(p.sel.size(), p.grid.p({}));
    for (size_t d = 0; d < o.size(); ++d)
      if (p.sel[d])
        o[d] = p.use[d];
      else if (d)
        o[d] = o[d - 1];
    return o;
  }
};

// se = 有效根 (t = 日下标 · S + 槽), 须跨 2 日以上; S = 每日根数; sp = 设定; u = 费率; bpy = 每年根数
// ex = 执行器 (tasks); stop() → true = 放弃 (返回空), 由并行任务调用; tick() 选参每过一日 (共 days_of(se, S).size() − 1 日)
template <class Exec, class Stop, class Tick>
std::optional<Track> trace(const Series &se, size_t S, const Spec &sp, const Rates &u, double bpy, Exec &ex, Stop stop, Tick tick) {
  const Tape t(se, S);
  const Days &days = t.days;
  const size_t n = t.n, nd = days.size(), Wr = win_of(sp.reduce.win), Wf = win_of(sp.follow.win);
  assert(n > 1 && nd > 1 && "样本须跨 2 日以上");
  assert(static_cast<size_t>(sp.dir) < std::size(kDirs) && "方向");
  const float s = kDirs[sp.dir].s;
  Col N;
  Band::with(sp.band.kind, [&]<class B>() { N = B::of(t, win_of(sp.band.win)); });
  const Series ds = day_series(t);
  const Tape td(ds, 1); // 打分用的日线原料带 (日下标同 t)
  const Ctx x{t, N, s}, dx{td, N, s};
  Tuner tr(Reduce::kinds[sp.reduce.kind], static_cast<size_t>(sp.grid), nd), tf(Follow::kinds[sp.follow.kind], static_cast<size_t>(sp.grid), nd);
  const Grid &gr = tr.p.grid, &gf = tf.p.grid;

  Scores<Q, Terms> rs;                           // 减仓格点 → 保敞口前缀
  Scores<std::pair<Q, Q>, std::vector<Trip>> fs; // (减仓格点, 跟随格点) → 逐笔
  std::vector<double> buf;
  const auto enroll_r = [&](const std::vector<Q> &g) {
    return rs.enroll(ex, g, stop, [&](const Q &q) { return keep_of(dx, sp.reduce.kind, gr.p(q), sp.rule.units, u); });
  };
  const auto enroll_f = [&](const std::vector<Q> &g) {
    std::vector<std::pair<Q, Q>> ks;
    for (const Q &q : g)
      ks.push_back({tr.q, q});
    return fs.enroll(ex, ks, stop, [&](const std::pair<Q, Q> &k) { return trips_of(dx, sp.reduce.kind, sp.follow.kind, gr.p(k.first), gf.p(k.second), sp.rule); });
  };
  const std::vector<size_t> &o = td.days.off; // 日 → 日线原料带的根
  for (size_t d = 1; d < nd; ++d) {
    tick();
    const size_t r0 = d > Wr ? d - Wr : 0, f0 = d > Wf ? d - Wf : 0;
    if (2 * (days.off[d] - days.off[r0]) < (d - r0) * S)
      continue;
    const bool ok = tr.step(d, enroll_r, [&](const Q &q) { return rs.at(q).over(o[r0], o[d]); });
    if (stop())
      return std::nullopt;
    if (!ok)
      continue;
    tf.step(d, enroll_f, [&](const Q &q) { return median_of(fs.at({tr.q, q}), td, o[f0], o[d], s, u, buf); });
    if (stop())
      return std::nullopt;
  }

  const std::vector<P> ur = tr.used(), uf = tf.used();
  Track out{std::move(tr.p), std::move(tf.p), days, Cuts(n), {}, {}};
  Run strat(n), hold(n);
  Reduce::with(sp.reduce.kind, [&]<class K>() {
    Follow::with(sp.follow.kind, [&]<class F>() {
      Laddered<K, F> src{K{x}, Ladder<F>{x, sp.rule}, ur, uf, out.reduce.sel};
      drive(t, u, src, [&](const Fill &f) { strat.put(t, f), out.cut.put(f.i, f.x); });
      out.cut.rungs = std::move(src.lad.rungs);
    });
  });
  drive(t, u, Hold{}, [&](const Fill &f) { hold.put(t, f); });
  out.cut.s = s, out.cut.units = sp.rule.units;
  out.strat = book_of(t, std::move(strat), bpy), out.hold = book_of(t, std::move(hold), bpy);
  return out;
}

} // namespace cta
