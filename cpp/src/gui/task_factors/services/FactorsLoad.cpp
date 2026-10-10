// FactorsLoad — 见头文件. include 因子 CPU 后端头 (covers), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsLoad.hpp"

#include "features/Backend/FeatureRead.hpp"
#include "features/MetaFlag.hpp" // fmeta::valid
#include "features/TimeIndex.hpp"
#include "misc/profiler.hpp"
#include "shared/Analysis.hpp" // analysis::kLevel

#include <algorithm>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstdio>

// 因子契约的段 = 一个交易日的 L1 行数 (含 09:15 起的集合竞价 15 分钟); 特征库 L1 有效行数与之对账
static_assert(factor::kSegLen == static_cast<int>(TRADE_MINUTES_PER_DAY), "factor::kSegLen 必须等于 L1 每日分钟数");

namespace GUI::Factors {

namespace {
constexpr size_t kLevel = analysis::kLevel; // 因子只在 L1 算
} // namespace

void init_loaded(const FeatureTable &ft, int days, int A, Loaded &L) {
  L.days = days;
  L.A = A;
  L.T = days * factor::kSegLen;
  assert(L.A >= 2 && L.A <= factor::stat::kMaxA && "资产轴超 Stat 容量 (kMaxA)");
  assert(static_cast<long long>(L.T) * L.A < (1LL << 31));
  L.n_hold = static_cast<int>(ft.labels.size());
  assert(L.n_hold >= 1 && L.n_hold <= factor::stat::kMaxHold && "持有期数超 Stat 标签组容量 (kMaxHold)");
  L.hd.n = L.n_hold;
  for (int hi = 0; hi < L.n_hold; ++hi)
    L.hd.h[hi] = ft.labels[static_cast<size_t>(hi)].hold;
  factor::stat::assert_holds(L.hd);
}

bool load_planes(const FeatureTable &ft, FeatureRead &reader, const std::vector<std::string> &dates, Loaded &L, factor::exec::ForkJoin &ex,
                 std::atomic<bool> &cancel, std::atomic<int> &done, bool with_labels, bool with_costs) {
  TraceN("FactorsLoad");
  assert(static_cast<int>(dates.size()) == L.days);
  assert(!with_costs || with_labels);
  const size_t A = static_cast<size_t>(L.A), n = L.n();
  const size_t VR = level_valid_rows(kLevel);
  assert(VR == static_cast<size_t>(factor::kSegLen));
  const size_t H = with_labels ? static_cast<size_t>(L.n_hold) : 0;
  const size_t C = with_costs ? ft.costs.size() : 0;
  const size_t nf = L.feat_codes.size();
  // 列表: [特征 nf][标签 long/short × hold][成本 buy/sell × amt][ts_valid][cs_valid]
  std::vector<size_t> cols;
  std::vector<L2::ValidType> vts;
  for (const std::string &c : L.feat_codes) {
    const FeatCol *fc = ft.find(c);
    assert(fc && fc->allowed);
    cols.push_back(fc->col), vts.push_back(fc->vt);
  }
  if (with_labels)
    for (const LabelCol &lc : ft.labels) {
      cols.push_back(lc.long_col), vts.push_back(ft.cols[lc.long_col].vt);
      cols.push_back(lc.short_col), vts.push_back(ft.cols[lc.short_col].vt);
    }
  if (with_costs)
    for (const CostCol &cc : ft.costs) {
      cols.push_back(cc.buy_col), vts.push_back(ft.cols[cc.buy_col].vt);
      cols.push_back(cc.sell_col), vts.push_back(ft.cols[cc.sell_col].vt);
    }
  cols.push_back(ft.ts_col);
  cols.push_back(ft.cs_col);
  const size_t ts_i = cols.size() - 2, cs_i = cols.size() - 1, cost_i = nf + 2 * H;

  L.planes.resize(nf);
  for (factor::check::Plane &p : L.planes)
    p.resize(n);
  L.cs.resize(n);
  L.labels.resize(H);
  for (LabelPlane &lb : L.labels)
    lb.lv.assign(n, 0), lb.sv.assign(n, 0), lb.m.assign(n, 0);
  L.costs.resize(C);
  for (CostPlane &cp : L.costs)
    cp.buy.assign(n, 0), cp.sell.assign(n, 0), cp.m.assign(n, 0);

  const size_t n_threads = static_cast<size_t>(ex.threads());
  std::vector<FeatureRead::DayColumns> staging(n_threads);
  for (FeatureRead::DayColumns &dc : staging)
    dc.preallocate(A, kLevel, cols.size());
  std::vector<std::vector<uint8_t>> has_data(n_threads, std::vector<uint8_t>(A)); // 每线程: 当日各资产是否有成交
  const size_t nc = cols.size();

  ex.tasks(static_cast<int>(dates.size()), [&](int di, int ti) {
    if (cancel.load(std::memory_order_relaxed))
      return;
    const size_t d = static_cast<size_t>(di), tid = static_cast<size_t>(ti);
    FeatureRead::DayColumns &dc = staging[tid];
    reader.load_day_columns(dates[d], cols, dc);
    if (reader.stale())
      return;
    // 当日有数据 = 该资产当日至少一分钟有成交 (ts_valid ≠ 0). 缺 binary / 整日停牌 → 全日 0: 这种 asset-day 不进截面池门控
    // (值是 carry / 0, 标签 0), valid% / Stat / CS 算子的 universe 因此只看"真有行情"的格, 不与数据完整性耦合
    std::vector<uint8_t> &has = has_data[tid];
    std::fill(has.begin(), has.end(), uint8_t{0});
    for (size_t t = 0; t < VR; ++t) {
      const feature_storage_t *ts_gate = dc.data.data() + (t * nc + ts_i) * A;
      for (size_t a = 0; a < A; ++a)
        has[a] |= fmeta::data_valid(static_cast<float>(ts_gate[a]));
    }
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      const feature_storage_t *ts_gate = dc.data.data() + (t * nc + ts_i) * A;
      const feature_storage_t *cs_gate = dc.data.data() + (t * nc + cs_i) * A;
      for (size_t i = 0; i < nf; ++i) {
        const feature_storage_t *src = dc.data.data() + (t * nc + i) * A;
        float *v = L.planes[i].v.data() + row;
        uint8_t *m = L.planes[i].m.data() + row;
        for (size_t a = 0; a < A; ++a) {
          const float x = static_cast<float>(src[a]);
          const bool ok = fmeta::valid(static_cast<float>(ts_gate[a]), vts[i]) && std::isfinite(x);
          v[a] = ok ? x : 0.f;
          m[a] = ok;
        }
      }
      for (size_t a = 0; a < A; ++a) {
        const float g = static_cast<float>(cs_gate[a]);
        L.cs.v[row + a] = g;
        L.cs.m[row + a] = fmeta::data_valid(g) && has[a];
      }
      for (size_t h = 0; h < H; ++h) {
        const size_t il = nf + 2 * h, is = il + 1;
        const feature_storage_t *sl = dc.data.data() + (t * nc + il) * A;
        const feature_storage_t *ss = dc.data.data() + (t * nc + is) * A;
        LabelPlane &lb = L.labels[h];
        for (size_t a = 0; a < A; ++a) {
          const float lv = static_cast<float>(sl[a]), sv = static_cast<float>(ss[a]);
          const bool ok = L.cs.m[row + a] && std::isfinite(lv) && std::isfinite(sv); // 池内且当日有数据
          lb.lv[row + a] = ok ? std::bit_cast<uint16_t>(sl[a]) : 0;
          lb.sv[row + a] = ok ? std::bit_cast<uint16_t>(ss[a]) : 0;
          lb.m[row + a] = ok;
        }
      }
      for (size_t c = 0; c < C; ++c) {
        const size_t ib = cost_i + 2 * c, is = ib + 1;
        const feature_storage_t *sb = dc.data.data() + (t * nc + ib) * A;
        const feature_storage_t *ss = dc.data.data() + (t * nc + is) * A;
        CostPlane &cp = L.costs[c];
        for (size_t a = 0; a < A; ++a) {
          const float b = static_cast<float>(sb[a]), s = static_cast<float>(ss[a]);
          const bool ok = L.cs.m[row + a] && std::isfinite(b) && std::isfinite(s); // 池内且当日有数据; 吃不到 = NaN → 无效
          cp.buy[row + a] = ok ? std::bit_cast<uint16_t>(sb[a]) : 0;
          cp.sell[row + a] = ok ? std::bit_cast<uint16_t>(ss[a]) : 0;
          cp.m[row + a] = ok;
        }
      }
    }
    done.fetch_add(1, std::memory_order_relaxed);
  });
  return !cancel.load(std::memory_order_relaxed) && !reader.stale();
}

void net_labels(factor::exec::ForkJoin &ex, const Loaded &L, const CostPlane &cost, float sell_impact, std::vector<LabelPlane> &out) {
  TraceN("FactorsNetLabels");
  const size_t n = L.n(), H = L.labels.size();
  assert(cost.m.size() == n && sell_impact >= 0.f);
  out.resize(H);
  for (LabelPlane &o : out)
    if (o.m.size() != n)
      o.lv.resize(n), o.sv.resize(n), o.m.resize(n);
  ex.rows(L.T, [&](int t0, int t1, int) {
    const size_t i0 = static_cast<size_t>(t0) * L.A, i1 = static_cast<size_t>(t1) * L.A;
    for (size_t h = 0; h < H; ++h) {
      const LabelPlane &g = L.labels[h];
      LabelPlane &o = out[h];
      for (size_t i = i0; i < i1; ++i) {
        const bool ok = g.m[i] && cost.m[i];
        const float lv = static_cast<float>(std::bit_cast<_Float16>(g.lv[i])) - static_cast<float>(std::bit_cast<_Float16>(cost.buy[i])) - sell_impact;
        const float sv = static_cast<float>(std::bit_cast<_Float16>(g.sv[i])) - static_cast<float>(std::bit_cast<_Float16>(cost.sell[i])) - sell_impact;
        o.lv[i] = ok ? std::bit_cast<uint16_t>(static_cast<_Float16>(lv)) : 0;
        o.sv[i] = ok ? std::bit_cast<uint16_t>(static_cast<_Float16>(sv)) : 0;
        o.m[i] = ok;
      }
    }
  });
}

// 算子节点的特征叶元: parse 时放行 (值域只能查数据), 这里按 OpTable in 列的严格域 (dom_strict, 即组 id 的 INT) 逐元查数据
// (有效格全部落在值域内): CS 算子内部 max_gid 断言炸不得由数据触发. 非严格域越界格由算子自身置无效, 不在此判
void check_leaf_domains(const factor::Dag &d, int node, const Loaded &L, std::string &err) {
  const factor::DagNode &nd = d.nodes[static_cast<size_t>(node)];
  if (nd.op < 0)
    return;
  const factor::expr::OpInfo &o = factor::expr::kOps[nd.op];
  for (int s = 0; s < o.arity; ++s) {
    const factor::DagNode &g = d.nodes[static_cast<size_t>(nd.in[s])];
    if (g.op >= 0 || !factor::dom_strict(o.in[s]))
      continue;
    const factor::check::Plane &p = L.planes[static_cast<size_t>(g.feat)];
    for (size_t i = 0; i < p.v.size(); ++i) {
      if (!p.m[i] || factor::expr::dom_holds(o.in[s], p.v[i]))
        continue;
      char buf[64];
      std::snprintf(buf, sizeof(buf), "%g", static_cast<double>(p.v[i]));
      err = std::string(o.name) + " 第 " + std::to_string(s + 1) + " 元要求 " + factor::dom_name(o.in[s]) + ", 特征 " +
            L.feat_codes[static_cast<size_t>(g.feat)] + " 含越界值 " + buf;
      return;
    }
  }
}

float valid_pct_of(const uint8_t *m, const uint8_t *gate, size_t n) {
  size_t c = 0, g = 0;
  for (size_t i = 0; i < n; ++i) {
    c += m[i] & gate[i];
    g += gate[i];
  }
  assert(g > 0 && "截面池全空: 区间内无在池格");
  return 100.f * static_cast<float>(c) / static_cast<float>(g);
}

} // namespace GUI::Factors
