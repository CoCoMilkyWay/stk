// FactorsLoad — 见头文件. include 因子契约 / Stat 行核头 (cost_row), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsLoad.hpp"

#include "factor/Stat/Host.hpp" // cost_row
#include "features/Backend/FeatureRead.hpp"
#include "features/MetaFlag.hpp"                      // fmeta::valid
#include "features/Operator/TS/Label/LabelReturn.hpp" // 只取 kContCloseL1 对账尾部界 (不含字段表)
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
// 尾部界对账: 标签层最后一个可成交 entry 锚点 = 连续竞价末分钟 (kContCloseL1 − 1) 的起始 = 行 kContCloseL1 − 2; Stat tail_masked 用同一条界
static_assert(static_cast<size_t>(factor::stat::kLastAnchorRow) + 2 == LabelReturn::kContCloseL1, "Stat 尾部界与 LabelReturn 的收盘竞价起点不一致");

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
                 std::atomic<bool> &cancel, std::atomic<int> &done, bool with_labels, bool with_costs, bool with_index) {
  TraceN("FactorsLoad");
  assert(static_cast<int>(dates.size()) == L.days);
  assert(!with_costs || with_labels);
  assert(!with_costs || L.max_impact > 0.f && "装成本前先设 Loaded::max_impact (Config::max_impact)");
  const feature_storage_t cap = static_cast<feature_storage_t>(L.max_impact); // 买入冲击封顶 (fp16 位直接替换, 与原值同格式)
  const size_t A = static_cast<size_t>(L.A), n = L.n();
  const size_t VR = level_valid_rows(kLevel);
  assert(VR == static_cast<size_t>(factor::kSegLen));
  const size_t H = with_labels ? static_cast<size_t>(L.n_hold) : 0;
  const size_t C = with_costs ? ft.costs.size() : 0;
  const size_t nf = L.feat_codes.size();
  // 列表: [特征 nf][标签 long/short × hold][成本 buy/sell × amt][close (with_index)][ts_valid][cs_valid]
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
  if (with_index) {
    const FeatCol *fc = ft.find("close");
    assert(fc && "字段表缺 close 列 (universe 指数)");
    cols.push_back(fc->col), vts.push_back(fc->vt);
  }
  cols.push_back(ft.ts_col);
  cols.push_back(ft.cs_col);
  const size_t ts_i = cols.size() - 2, cs_i = cols.size() - 1, cost_i = nf + 2 * H, close_i = ts_i - 1;

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
  // 指数: 逐天并行攒日内项 (isum / icnt [T]); 当日首笔的前价在前一天 (跨日), 记下首笔 / 末笔 [days][A] (0 = 无) 收尾串行补
  std::vector<double> isum;
  std::vector<int> icnt;
  std::vector<float> first_c, last_c;
  std::vector<uint16_t> first_m;
  std::vector<std::vector<float>> px; // 每线程: 当日各资产上一有效 close (0 = 当日尚无成交)
  if (with_index) {
    isum.assign(static_cast<size_t>(L.T), 0.0);
    icnt.assign(static_cast<size_t>(L.T), 0);
    first_c.assign(dates.size() * A, 0.f);
    last_c.assign(dates.size() * A, 0.f);
    first_m.assign(dates.size() * A, 0);
    px.assign(n_threads, std::vector<float>(A));
  }

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
      if (with_index) {
        if (t == 0)
          std::fill(px[tid].begin(), px[tid].end(), 0.f);
        const feature_storage_t *cl = dc.data.data() + (t * nc + close_i) * A;
        double s = 0.0;
        int k = 0;
        for (size_t a = 0; a < A; ++a) {
          const float c = static_cast<float>(cl[a]);
          if (!fmeta::data_valid(static_cast<float>(ts_gate[a])) || !(c > 0.f))
            continue;
          const bool in = fmeta::data_valid(static_cast<float>(cs_gate[a]));
          float &p = px[tid][a];
          if (p > 0.f) {
            s += in ? c / p - 1.0 : 0.0;
            k += in;
          } else { // 当日首笔: 前价在前一天, 收尾补 (首笔时不在池 → 不补)
            first_c[d * A + a] = in ? c : 0.f;
            first_m[d * A + a] = static_cast<uint16_t>(t);
          }
          p = c;
        }
        isum[d * VR + t] = s;
        icnt[d * VR + t] = k;
        if (t + 1 == VR)
          std::copy(px[tid].begin(), px[tid].end(), last_c.begin() + static_cast<std::ptrdiff_t>(d * A));
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
          cp.buy[row + a] = ok ? std::bit_cast<uint16_t>(b > L.max_impact ? cap : sb[a]) : 0;
          cp.sell[row + a] = ok ? std::bit_cast<uint16_t>(ss[a]) : 0;
          cp.m[row + a] = ok;
        }
      }
    }
    done.fetch_add(1, std::memory_order_relaxed);
  });
  if (cancel.load(std::memory_order_relaxed) || reader.stale())
    return false;
  if (with_index) {
    std::vector<float> prev(A, 0.f); // 各资产最近一个有成交日的末笔 close
    for (size_t d = 0; d < dates.size(); ++d)
      for (size_t a = 0; a < A; ++a) {
        const size_t i = d * A + a;
        if (first_c[i] > 0.f && prev[a] > 0.f) {
          isum[d * VR + first_m[i]] += first_c[i] / prev[a] - 1.0;
          ++icnt[d * VR + first_m[i]];
        }
        if (last_c[i] > 0.f)
          prev[a] = last_c[i];
      }
    L.index.resize(static_cast<size_t>(L.T));
    double lv = 1.0;
    for (size_t t = 0; t < L.index.size(); ++t) {
      if (icnt[t])
        lv *= 1.0 + isum[t] / icnt[t];
      L.index[t] = static_cast<float>(lv);
    }
  }
  return true;
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

double n_full_of(const Loaded &L) {
  const uint8_t *g = L.cs.m.data();
  long long on = 0;
  for (size_t k = 0, n = L.n(); k < n; ++k)
    on += g[k] != 0;
  const double nf = static_cast<double>(on) / static_cast<double>(L.T) / factor::stat::kGroups;
  assert(nf > 0.0 && "池内无有效票");
  return nf;
}

void cost_tiers(factor::exec::ForkJoin &ex, const uint16_t *rx, const Loaded &L, const FeatureTable &ft, std::vector<CostTier> &out) {
  const int C = static_cast<int>(ft.costs.size()), H = L.hd.n, A = L.A, T = L.T, nt = ex.threads();
  assert(rx && L.costs.size() == static_cast<size_t>(C) && "成本平面未装 (with_costs)");
  std::vector<factor::stat::CostAcc> cacc(static_cast<size_t>(nt) * C * H); // [tid][c][hi]
  std::vector<long long> cnx(static_cast<size_t>(nt) * (1 + C), 0);         // [tid][0 = rx 有效格数, 1 + c = rx ∧ 成本有效]
  ex.rows(T, [&](int t0, int t1, int tid) {
    factor::stat::CostAcc *acc = cacc.data() + static_cast<size_t>(tid) * C * H;
    long long *nx = cnx.data() + static_cast<size_t>(tid) * (1 + C);
    for (int t = t0; t < t1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      for (int c = 0; c < C; ++c) {
        const CostPlane &cp = L.costs[static_cast<size_t>(c)];
        const factor::stat::Cost cost{cp.buy.data() + base, cp.sell.data() + base, cp.m.data() + base};
        for (int i = 0; i < H; ++i) {
          const int h = factor::stat::hold_minutes(L.hd.h[i]);
          if (t >= h)
            factor::cpu::stat::cost_row(rx + base, rx + base - static_cast<size_t>(h) * A, cost, A, acc[c * H + i]);
        }
        for (int a = 0; a < A; ++a)
          nx[1 + c] += rx[base + a] != factor::stat::kRankNone && cp.m[base + a] != 0;
      }
      for (int a = 0; a < A; ++a)
        nx[0] += rx[base + a] != factor::stat::kRankNone;
    }
  });
  std::vector<long long> nx(static_cast<size_t>(1 + C), 0);
  for (int tid = 0; tid < nt; ++tid)
    for (int c = 0; c <= C; ++c)
      nx[static_cast<size_t>(c)] += cnx[static_cast<size_t>(tid) * (1 + C) + c];
  out.assign(static_cast<size_t>(C), CostTier{});
  for (int c = 0; c < C; ++c) {
    CostTier &tier = out[static_cast<size_t>(c)];
    tier.amt = ft.costs[static_cast<size_t>(c)].amt;
    for (int i = 0; i < H; ++i)
      for (int tid = 0; tid < nt; ++tid)
        tier.acc[i].merge(cacc[(static_cast<size_t>(tid) * C + c) * H + i]);
    tier.cover = nx[0] ? static_cast<float>(static_cast<double>(nx[static_cast<size_t>(1 + c)]) / static_cast<double>(nx[0])) : 0.f;
  }
}

} // namespace GUI::Factors
