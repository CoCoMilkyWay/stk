#pragma once

// 单资产的滚动调参全期轨迹 (只用本资产, 不用未来信息)
//   所选模型的参数网格逐配置回测 → 每日以前 W 日各配置目标值拟岭回归曲面 (Tune.hpp), 最高点配置用到当日
//   (前 W 日 / 窗内根数不足满窗一半 = 空仓); 选中过的配置各算全期 (状态常热), 逐根取当日选中者的仓位与线, 切换计成本
#include "cta/Metrics.hpp"
#include "cta/Tune.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <utility>
#include <vector>

namespace cta {

struct Track {
  std::vector<Spec> specs;                 // 所选模型的参数网格
  std::vector<std::pair<size_t, int>> seg; // 选参段: (起根, specs 下标; −1 = 空仓), 起根升序
  Run rs;                                  // 拼成的一条: 仓位 / 毛 / 净
  Stat st, sh;                             // 全期指标: 策略 / 买入持有
  Col up, dn, stop;                        // [根] 当日选中配置的上线 / 下线 / 吊灯线
  std::vector<double> nav;                 // [根] 策略全期净值
};

// 有效根跨的日数 (首根所在日起; S = 每日根数)
inline size_t days_of(const Series &se, size_t S) { return se.t.empty() ? 0 : se.t.back() / S - se.t.front() / S + 1; }

// se = 有效根 (t = 日下标 · S + 槽), 须跨过 W 日以上; kind = 各组模型; W = 滚动窗 (日, ≥ kMinRoll)
// stop() → true = 放弃 (返回 false); tick() 每算完一个网格配置 (共 specs_of(S, kind).size() 个)
template <class Stop, class Tick>
bool trace(const Series &se, size_t S, const std::array<size_t, kNP> &kind, size_t W, const Setup &u, Track &out, Stop stop, Tick tick) {
  const size_t n = se.c.size(), d0 = se.t[0] / S, nd = days_of(se, S);
  assert(n > 1 && W >= kMinRoll && nd > W && "样本须长于滚动窗");
  const auto day = [&](size_t i) { return se.t[i] / S - d0; }; // 自首根所在日起
  std::vector<double> cnt(nd, 0);                              // 根数前缀和
  for (size_t i = 0; i < n; ++i)
    cnt[day(i)] += 1;
  for (size_t d = 1; d < nd; ++d)
    cnt[d] += cnt[d - 1];

  out.specs = specs_of(static_cast<double>(S), kind);
  assert(!out.specs.empty() && "所选模型无可用网格");
  const Ridge rg = ridge_of(out.specs);
  Tape t(se);
  std::vector<std::vector<double>> acc; // [spec] 日前缀和
  for (const Spec &x : out.specs) {
    if (stop())
      return false;
    const Sig g = band(t, x.g);
    acc.push_back(day_sums(terms(t, g, simulate(t, g.w, u), u.cost), nd, day));
    tick();
  }
  std::vector<int> pick(nd, -1); // [日] → specs 下标
  const double min_n = static_cast<double>(W * S) / 2;
  std::vector<double> y(out.specs.size());
  for (size_t d = W; d < nd; ++d) {
    const auto win = [&](const std::vector<double> &p, size_t k) { return p[k + 2 * (d - 1)] - (d > W ? p[k + 2 * (d - W - 1)] : 0); };
    const double m = cnt[d - 1] - (d > W ? cnt[d - W - 1] : 0);
    if (m < min_n)
      continue;
    for (size_t x = 0; x < y.size(); ++x)
      y[x] = score(win(acc[x], 0), win(acc[x], 1));
    const std::vector<double> f = rg.fit(y);
    pick[d] = static_cast<int>(std::max_element(f.begin(), f.end()) - f.begin());
  }

  Col w(n, 0);
  out.up.assign(n, kNaN), out.dn.assign(n, kNaN), out.stop.assign(n, kNaN);
  std::vector<char> used(out.specs.size(), 0);
  for (const int p : pick)
    if (p >= 0)
      used[static_cast<size_t>(p)] = 1;
  for (size_t x = 0; x < out.specs.size(); ++x) {
    if (!used[x])
      continue;
    if (stop())
      return false;
    const Sig g = band(t, out.specs[x].g);
    for (size_t i = 0; i < n; ++i)
      if (pick[day(i)] == static_cast<int>(x))
        w[i] = g.w[i], out.up[i] = g.up[i], out.dn[i] = g.dn[i], out.stop[i] = g.stop[i];
  }
  out.seg.clear();
  for (size_t i = 0; i < n; ++i)
    if (const int p = pick[day(i)]; out.seg.empty() || out.seg.back().second != p)
      out.seg.emplace_back(i, p);
  out.rs = simulate(t, std::move(w), u);
  out.st = stat_of(t, out.rs, u), out.sh = stat_of(t, simulate(t, hold(t), u), u);
  out.nav.resize(out.rs.r.size());
  double nav = 1;
  for (size_t i = 0; i < out.nav.size(); ++i)
    out.nav[i] = nav *= 1 + out.rs.r[i];
  return true;
}

} // namespace cta
