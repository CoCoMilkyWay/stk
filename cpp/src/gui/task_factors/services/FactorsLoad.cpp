// FactorsLoad — 见头文件. include 因子契约 / Stat CPU 流程头 (cost_rows), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsLoad.hpp"

#include "factor/Stat/Cpu.hpp"                     // cost_rows
#include "features/MetaFlag.hpp"                   // fmeta::valid / keep / finite
#include "features/Node/Lob/TS/Label/LabelLob.hpp" // 只取 kContCloseMin 对账尾部界 (不含字段表)
#include "features/Storage/FeatureRead.hpp"
#include "features/TimeIndex.hpp"
#include "misc/cross_platform.hpp" // memory_usage
#include "misc/date.hpp"           // months_between
#include "misc/profiler.hpp"
#include "shared/Analysis.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <limits>

// 因子层几何 (factor::Geo) 与特征库层对账: 每日行数 = 层有效行数; 行 t 的末分钟 = L_1min 分钟号 (箱末 bin_last_min / 日层 = 收盘分钟)
static_assert(factor::kSegMax == static_cast<int>(TRADE_MINUTES_PER_DAY), "factor::kSegMax 必须等于 L_1min 每日分钟数");
static_assert([] {
  for (const GUI::Factors::FactorLevel &l : GUI::Factors::kFactorLevels) {
    const factor::Geo g = factor::geo_of(l.seg);
    if (level_valid_rows(l.lvl) != static_cast<size_t>(l.seg))
      return false;
    for (int t = 0; t < g.S; ++t) {
      const size_t m = l.lvl == L_1min ? static_cast<size_t>(t) : l.lvl == L_1day ? TRADE_MINUTES_PER_DAY - 1
                                                                                  : bin_last_min(level_bin_minutes(l.lvl), static_cast<size_t>(t));
      if (static_cast<size_t>(g.minute(t)) != m)
        return false;
    }
  }
  return true;
}(),
              "kFactorLevels 与 factor::kGeos / 特征库层 (有效行数 / 箱末分钟) 不符");
// 尾部界对账: 标签层最后一个可成交 entry 锚点 = 连续竞价末分钟 (kContCloseMin − 1) 的起始 = 行 kContCloseMin − 2; Stat tail_masked 用同一条界
static_assert(static_cast<size_t>(factor::stat::kLastAnchorRow) + 2 == LabelLob::kContCloseMin, "Stat 尾部界与 LabelLob 的收盘竞价起点不一致");

namespace GUI::Factors {

void open_libs(const FactorScope &sc, LibReaders &lr, std::atomic<bool> *abort) {
  PerPool<FeatureLib> ready;
  for (const PoolId l : pool_ids())
    if (sc.libs[l].ready())
      ready[l] = sc.libs[l];
  lr.open(ready, abort);
}

std::string EvalScope::key() const {
  std::string k = std::to_string(pool);
  for (const PoolId l : pool_ids())
    if (used & pool_bit(l))
      k += "|" + rs.libs[l].dir + "#" + std::to_string(rs.libs[l].axis.hash);
  return k + "|" + std::to_string(dates.front()) + "-" + std::to_string(dates.back()) + "#" + std::to_string(dates.size());
}

bool eval_scope(const FeatureTable &ft, const FactorScope &sc, const std::vector<std::string> &feats, EvalScope &es, std::string &why) {
  TraceN("EvalScope");
  assert(sc.seg == ft.seg && "作用域层与字段表层不符");
  const FactorLevel &lv = factor_level(ft.seg);
  const PoolId pool = pool_id(sc.pool);
  assert(level_for_pool(lv, pool_tier(pool)) && "该池不能用这个因子层");
  const PoolId lab = lv.lab(pool);
  assert(ft.ts.ref(pool).pool == lab && ft.cs.ref(pool).pool == lab && "门控 / 标签 / 成本 / OHLC 在标签库");
  const bool sub = pool != lab; // Lob 池 × Bar 标签: 截面再与上池成员
  const PerPool<FeatureLib> &libs = sc.libs;
  es = EvalScope{};
  es.pool = pool;
  es.used = pool_bit(lab);
  for (const std::string &f : feats) {
    const FeatCol *c = ft.find(f);
    assert(c && c->allowed);
    es.used |= pool_bit(c->ref(pool).pool);
  }
  for (const PoolId l : pool_ids())
    if (es.used & pool_bit(l)) {
      assert(!libs[l].name.empty() && "请求没带该库清单 (factor_libs)");
      if (!libs[l].ready()) {
        why = std::string(pool_def(l).name) + " 池特征库未建 (先算特征)";
        return false;
      }
      es.rs.libs[l] = libs[l];
    }
  // 评估期: 标签库日期轴上 used 各库都落了盘的日; 子池 × 日内网格的成员读 D−1 收盘行, 还要 all 池前一日落盘
  const FeatureLib &L0 = libs[lab];
  const auto landed = [&](PoolId l, misc::Ymd d) {
    const FeatureLib &b = libs[l];
    return std::binary_search(b.dates.begin(), b.dates.end(), d) && FeatureRead::has_date(b.tier, b.dir, d);
  };
  const bool need_prev = sub && lv.lvl != L_1day;
  PoolMask m_prev = 0; // 标签库日期轴前一日的落盘位
  for (size_t i = 0; i < L0.dates.size(); ++i) {
    const misc::Ymd d = L0.dates[i];
    PoolMask m = 0;
    for (const PoolId l : pool_ids())
      if ((es.used & pool_bit(l)) && landed(l, d))
        m |= pool_bit(l);
    if (m == es.used && (!need_prev || (i > 0 && (m_prev & pool_bit(kAllPool))))) {
      es.dates.push_back(d);
      es.days.push_back(Day{d, i > 0 ? L0.dates[i - 1] : 0, m, i > 0 ? m_prev : 0});
    }
    m_prev = m;
  }
  if (es.dates.empty()) {
    why = "所用特征库无共同落盘日";
    for (const PoolId l : pool_ids())
      if (es.used & pool_bit(l))
        why += std::string(" ") + pool_def(l).name;
    return false;
  }
  if (sub)
    es.member = pool::Panel::build(libs[kAllPool], pool_def(pool).pool_col, es.dates, lv.lvl);
  es.rs.main = lab;
  es.rs.days = es.used;
  es.rs.months = misc::months_between(es.dates.front(), es.dates.back());
  analysis::map_axis(es.rs, sub ? es.member->axis().ids : std::vector<uint32_t>{});
  return true;
}

double host_budget() {
  const MemoryUsage mu = memory_usage();
  return kHostFrac * static_cast<double>(mu.total_bytes - mu.used_bytes);
}

void assert_fits(const char *who, double bytes, int days, int A, int seg) {
  const double budget = host_budget();
  if (bytes > budget)
    std::fprintf(stderr, "[factors] %s: 装载 %.1f GB > 内存预算 %.1f GB (%d 天 × %d 行 × %d 只); 换更粗的因子层\n", who, bytes / 1e9, budget / 1e9, days,
                 seg, A);
  assert(bytes <= budget && "因子整张量装载放不下内存 (因子层太细 / 评估期太长), 拒绝评估");
}

double cell_bytes(const LoadSpec &s) {
  return 5.0 * static_cast<double>(s.feats.size() + 1 + s.holds.size() + s.amts.size() + (s.index ? 1 : 0));
}

namespace {

// 池等权指数 (Loaded::index): 资产条带并行, 各条带沿 t 推本条带资产的上一有效 close (跨日, 不看在池), 成分格 (在池 ∧ 前价有) 攒进本线程
// 逐行 Σ / 数; 线程间按 tid 序归并后串行连乘
void pool_index(misc::exec::ForkJoin &ex, const factor::check::Plane &close, const uint8_t *in, int T, int A, std::vector<float> &out) {
  const size_t nT = static_cast<size_t>(T), nt = static_cast<size_t>(ex.threads());
  std::vector<double> sum(nt * nT, 0.0);
  std::vector<int> cnt(nt * nT, 0);
  ex.rows(A, [&](int a0, int a1, int tid) {
    std::vector<float> prev(static_cast<size_t>(a1 - a0), 0.f);
    double *s = sum.data() + static_cast<size_t>(tid) * nT;
    int *k = cnt.data() + static_cast<size_t>(tid) * nT;
    for (size_t t = 0; t < nT; ++t)
      for (int a = a0; a < a1; ++a) {
        const size_t i = t * static_cast<size_t>(A) + static_cast<size_t>(a);
        const float c = close.v[i];
        if (!close.m[i] || !(c > 0.f))
          continue;
        float &p = prev[static_cast<size_t>(a - a0)];
        if (p > 0.f && in[i]) {
          s[t] += c / p - 1.0;
          ++k[t];
        }
        p = c;
      }
  });
  out.resize(nT);
  double lv = 1.0;
  for (size_t t = 0; t < nT; ++t) {
    double s = 0.0;
    int k = 0;
    for (size_t j = 0; j < nt; ++j)
      s += sum[j * nT + t], k += cnt[j * nT + t];
    if (k)
      lv *= 1.0 + s / k;
    out[t] = static_cast<float>(lv);
  }
}

} // namespace

bool load_planes(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, const LoadSpec &spec, Loaded &L,
                 misc::exec::ForkJoin &ex, std::atomic<bool> &cancel, Progress &prog) {
  TraceN("FactorsLoad");
  constexpr size_t K = factor::stat::kLegs;
  assert(d0 < d1 && d1 <= es.days.size());
  assert((spec.amts.empty() || spec.max_impact > 0.f) && "装成本要封顶 (Config::max_impact)");
  const size_t lvl = factor_level(ft.seg).lvl;
  const size_t VR = level_valid_rows(lvl);
  assert(VR == static_cast<size_t>(ft.seg));
  // ---- 维度 / 标签组 / 金额段 (容量断言) ----
  L.days = static_cast<int>(d1 - d0);
  L.A = static_cast<int>(es.rs.axis.size());
  L.seg = ft.seg;
  L.T = L.days * L.seg;
  assert(L.A >= 2 && L.A <= factor::stat::kMaxA && "资产轴超 Stat 容量 (kMaxA)");
  assert(static_cast<long long>(L.T) * L.A < (1LL << 31));
  assert(spec.holds.size() <= static_cast<size_t>(factor::stat::kMaxHold) && "持有期数超 Stat 标签组容量 (kMaxHold)");
  assert(spec.amts.size() <= static_cast<size_t>(factor::stat::kMaxCost) && "金额段数超 Stat 成本容量 (kMaxCost)");
  L.feat_codes = spec.feats;
  L.hd = factor::stat::Holds{};
  L.hd.n = static_cast<int>(spec.holds.size());
  std::copy(spec.holds.begin(), spec.holds.end(), L.hd.h);
  if (L.hd.n > 0)
    factor::stat::assert_holds(L.hd, L.seg);
  L.amts = spec.amts;
  L.index.clear();
  const size_t A = static_cast<size_t>(L.A), n = L.n(), days = d1 - d0;
  const size_t nf = spec.feats.size(), H = spec.holds.size(), C = spec.amts.size();
  // ---- 读计划: 列段 叶 [nf] | ts_valid | cs_valid | close (index) | 标签 [H][K] | 成本 [C][K], 全部 Col::ref(es.pool);
  //      叶按所在 (池, 层) 投影到网格行, 其余是标签库网格层本层列 (同行) ----
  std::vector<ColRef> cols;
  const auto add = [&](const Col &c) {
    cols.push_back(c.ref(es.pool));
    return cols.size() - 1;
  };
  for (const std::string &f : spec.feats) {
    const FeatCol *c = ft.find(f);
    assert(c && c->allowed);
    add(*c);
  }
  const size_t ts_i = add(ft.ts), cs_i = add(ft.cs);
  const size_t close_i = spec.index ? add(ft.ohlc[3]) : SIZE_MAX;
  assert(cols[ts_i].pool == es.rs.main && "门控 / 标签 / 成本 / OHLC 在标签库");
  const size_t lab_i = cols.size();
  for (const int h : spec.holds)
    for (const Col &c : leg_of(ft.labels, h).leg)
      add(c);
  const size_t cost_i = cols.size();
  for (const int a : spec.amts)
    for (const Col &c : leg_of(ft.costs, a).leg)
      add(c);
  const size_t n_cols = cols.size();
  const ReadPlan plan = analysis::read_plan(*ft.feature, lvl, analysis::Rows::AsOf, /*carry=*/true, std::move(cols));
  std::vector<std::pair<uint32_t, uint32_t>> at(n_cols); // 平面列 → (源, 源内值列)
  for (size_t si = 0; si < plan.srcs.size(); ++si)
    for (size_t k = 0; k < plan.srcs[si].slot.size(); ++k)
      at[plan.srcs[si].slot[k]] = {static_cast<uint32_t>(si), static_cast<uint32_t>(k)};
  const Readers rd = es.rs.readers(lr.ptr);
  // 平面列 c 在网格行 t 的源行视图 (v nullptr = 无源); 格 a ← ids[a] (nullptr = 同轴; kAbsent = 资产不在该库)
  struct View {
    const feature_storage_t *v = nullptr, *g = nullptr;
    const uint32_t *ids = nullptr;
    L2::ValidType vt = L2::ValidType::ALL;
  };
  const auto view = [&](const IoSlot &io, size_t c, size_t t) {
    const auto [si, k] = at[c];
    const Source &s = plan.srcs[si];
    View w;
    w.ids = rd.ids[s.pool];
    w.vt = s.cs.vts[k];
    size_t r = 0;
    if (const FeatureRead::DayColumns *dc = source_row(s, io.src[si], t, r)) {
      w.v = col_row(dc, k, r);
      w.g = s.cs.gated() ? col_row(dc, s.cs.n(), r) : nullptr;
    }
    return w;
  };
  // 格 a: 有源 → x = 值, g = 同行门控 (不门控 = 1)
  const auto cell = [](const View &w, size_t a, feature_storage_t &x, float &g) {
    const uint32_t i = w.ids ? w.ids[a] : static_cast<uint32_t>(a);
    if (!w.v || i == kAbsent)
      return false;
    x = w.v[i];
    g = w.g ? static_cast<float>(w.g[i]) : 1.f;
    return true;
  };

  L.planes.resize(nf);
  for (factor::check::Plane &p : L.planes)
    p.resize(n);
  L.cs.resize(n);
  const auto alloc = [&](std::vector<LegPlane> &ps, size_t k) {
    ps.resize(k);
    for (LegPlane &p : ps) {
      for (std::vector<uint16_t> &v : p.v)
        v.assign(n, 0);
      p.m.assign(n, 0);
    }
  };
  alloc(L.labels, H);
  alloc(L.costs, C);
  factor::check::Plane close; // 本行有成交的 close (index), 装完算指数即弃
  if (spec.index)
    close.resize(n);
  // 两腿封顶 (fp16 位直接替换, 与原值同格式): 标签不截; 成本买入按 max_impact 截, 卖出原值
  constexpr float kNoCap = std::numeric_limits<float>::infinity();
  const float lab_cap[K] = {kNoCap, kNoCap}, cost_cap[K] = {spec.max_impact, kNoCap};
  // 两腿列组 (标签 / 成本同一份折叠): 列段起点 base, 组 k 的腿 l = 列 base + K·k + l; 池内 (cs.m) ∧ 两腿皆有限 → fp16 位, 其余 0 + 无效
  // (成本吃不到 = NaN → 无效)
  const auto fold_legs = [&](const IoSlot &io, size_t base, const float (&cap)[K], std::vector<LegPlane> &out, size_t t, size_t row) {
    for (size_t k = 0; k < out.size(); ++k) {
      View w[K];
      feature_storage_t top[K];
      for (size_t l = 0; l < K; ++l) {
        w[l] = view(io, base + K * k + l, t);
        top[l] = static_cast<feature_storage_t>(cap[l]);
      }
      LegPlane &p = out[k];
      for (size_t a = 0; a < A; ++a) {
        feature_storage_t x[K] = {};
        float g;
        bool ok = L.cs.m[row + a];
        for (size_t l = 0; l < K; ++l)
          ok = ok && cell(w[l], a, x[l], g) && fmeta::finite(x[l]);
        for (size_t l = 0; l < K; ++l)
          p.v[l][row + a] = ok ? std::bit_cast<uint16_t>(static_cast<float>(x[l]) > cap[l] ? top[l] : x[l]) : 0;
        p.m[row + a] = ok;
      }
    }
  };

  const size_t n_threads = static_cast<size_t>(ex.threads());
  std::vector<IoSlot> slots(n_threads);
  for (IoSlot &s : slots)
    s.prepare(plan, es.rs.sizes(), n_cols);
  std::vector<std::vector<uint8_t>> has_data(n_threads, std::vector<uint8_t>(A)); // 每线程: 当日各资产是否有成交 (∧ 在子池)

  ex.tasks(static_cast<int>(days), [&](int di, int ti) {
    if (cancel.load(std::memory_order_relaxed))
      return;
    const size_t d = static_cast<size_t>(di), tid = static_cast<size_t>(ti);
    const Day &day = es.days[d0 + d];
    IoSlot &io = slots[tid];
    for (size_t si = 0; si < plan.srcs.size(); ++si)
      load_source(rd, plan.srcs[si], day, plan.srcs[si].cs.io, io.src[si]);
    if (lr.stale())
      return;
    // 当日有数据 = 该资产当日至少一行有成交 (ts_valid ≠ 0). 缺 binary / 整日停牌 → 全日 0: 这种 asset-day 不进截面池门控
    // (值是 carry / 0, 标签 0), valid% / Stat / CS 算子的截面域因此只看"真有行情"的格, 不与数据完整性耦合.
    // 子池 (Bar 标签层): 再与当日成员相与, 池外格同样不进截面
    feature_storage_t x;
    float g;
    std::vector<uint8_t> &has = has_data[tid];
    std::fill(has.begin(), has.end(), static_cast<uint8_t>(0));
    for (size_t t = 0; t < VR; ++t) {
      const View wt = view(io, ts_i, t);
      for (size_t a = 0; a < A; ++a)
        has[a] |= cell(wt, a, x, g) && fmeta::data_valid(static_cast<float>(x));
    }
    if (es.member) {
      const uint8_t *mem = es.member->row(day.date);
      for (size_t a = 0; a < A; ++a)
        has[a] &= mem[a];
    }
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      const View wt = view(io, ts_i, t), wc = view(io, cs_i, t);
      for (size_t i = 0; i < nf; ++i) {
        const View w = view(io, i, t);
        float *v = L.planes[i].v.data() + row;
        uint8_t *m = L.planes[i].m.data() + row;
        for (size_t a = 0; a < A; ++a) {
          const bool ok = cell(w, a, x, g) && fmeta::keep(g, w.vt, x);
          v[a] = ok ? static_cast<float>(x) : 0.f;
          m[a] = ok;
        }
      }
      for (size_t a = 0; a < A; ++a) {
        const float c = cell(wc, a, x, g) ? static_cast<float>(x) : 0.f;
        L.cs.v[row + a] = c;
        L.cs.m[row + a] = fmeta::data_valid(c) && has[a];
      }
      if (spec.index) { // close: 本行有成交 (ts_valid 按 DATA 口径) ∧ 有限
        const View wl = view(io, close_i, t);
        for (size_t a = 0; a < A; ++a) {
          feature_storage_t c, s;
          const bool ok = cell(wl, a, c, g) && cell(wt, a, s, g) && fmeta::keep(static_cast<float>(s), L2::ValidType::DATA, c);
          close.v[row + a] = ok ? static_cast<float>(c) : 0.f;
          close.m[row + a] = ok;
        }
      }
      fold_legs(io, lab_i, lab_cap, L.labels, t, row);
      fold_legs(io, cost_i, cost_cap, L.costs, t, row);
    }
    prog.publish(1);
  });
  if (cancel.load(std::memory_order_relaxed) || lr.stale())
    return false;
  if (spec.index)
    pool_index(ex, close, L.cs.m.data(), L.T, L.A, L.index);
  return true;
}

std::vector<const factor::check::Plane *> leaf_planes(const Loaded &L, size_t nf) {
  assert(nf <= L.planes.size());
  std::vector<const factor::check::Plane *> in(nf);
  for (size_t i = 0; i < nf; ++i)
    in[i] = &L.planes[i];
  return in;
}

// 算子节点的特征叶元: parse 时放行 (值域只能查数据), 这里按 OpTable in 列的严格域 (dom_strict, 即组 id 的 INT) 逐元查数据
// (有效格全部落在值域内): CS 算子内部 max_gid 断言炸不得由数据触发. 非严格域越界格由算子自身置无效, 不在此判
void check_leaf_domains(const factor::Dag &d, int node, const Loaded &L, std::string &err) {
  const factor::DagNode &nd = d.nodes[static_cast<size_t>(node)];
  if (nd.op < 0 || nd.fu.n > 0) // 融合节点只装逐点 TS 算子, 无严格域元
    return;
  const factor::expr::OpInfo &o = factor::expr::kOps[nd.op];
  assert(nd.n_in == o.arity);
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

float rank_pct_of(misc::exec::ForkJoin &ex, const uint16_t *rx, const uint8_t *m, const uint8_t *gate, int T, int A) {
  const int nt = ex.threads();
  std::vector<double> sum(static_cast<size_t>(nt), 0.0);
  std::vector<long long> rows(static_cast<size_t>(nt), 0);
  ex.rows(T, [&](int t0, int t1, int tid) {
    std::vector<uint16_t> buf(static_cast<size_t>(A));
    double s = 0.0;
    long long nr = 0;
    for (int t = t0; t < t1; ++t) {
      const size_t base = static_cast<size_t>(t) * A;
      int nv = 0, nb = 0;
      for (int a = 0; a < A; ++a) {
        nv += m[base + a] & gate[base + a];
        if (rx[base + a] != factor::stat::kRankNone)
          buf[static_cast<size_t>(nb++)] = rx[base + a];
      }
      if (nv == 0)
        continue;
      assert(nb <= nv && "有秩格须在池内有效格里");
      ++nr;
      std::sort(buf.begin(), buf.begin() + nb);
      const int wmax = (nv + factor::stat::kGroups - 1) / factor::stat::kGroups;
      int good = 0;
      for (int i = 0; i < nb;) {
        int j = i + 1;
        while (j < nb && buf[static_cast<size_t>(j)] == buf[static_cast<size_t>(i)])
          ++j;
        good += j - i <= wmax ? j - i : 0;
        i = j;
      }
      s += static_cast<double>(good) / nv;
    }
    sum[static_cast<size_t>(tid)] += s;
    rows[static_cast<size_t>(tid)] += nr;
  });
  double s = 0.0;
  long long nr = 0;
  for (int k = 0; k < nt; ++k)
    s += sum[static_cast<size_t>(k)], nr += rows[static_cast<size_t>(k)];
  assert(nr > 0 && "截面池全空: 区间内无在池格");
  return static_cast<float>(100.0 * s / static_cast<double>(nr));
}

double n_full_of(const Loaded &L) {
  const uint8_t *g = L.cs.m.data();
  long long on = 0;
  for (size_t k = 0, n = L.n(); k < n; ++k)
    on += g[k] != 0;
  const double nf = static_cast<double>(on) / static_cast<double>(L.T) / factor::stat::kGroups;
  assert(nf > 0.0 && "池内无有效资产");
  return nf;
}

void prep_labels(misc::exec::ForkJoin &ex, const Loaded &L, factor::cpu::stat::Workspace &w, LabelsHost &out) {
  const size_t H = static_cast<size_t>(L.hd.n);
  assert(H > 0 && L.labels.size() == H && "标签未装 (LoadSpec::holds 空)");
  out.ry.assign(H, std::vector<uint16_t>(L.n()));
  out.L = labels_raw(L);
  for (size_t h = 0; h < H; ++h) {
    factor::cpu::stat::prep_label(ex, out.L.l[h], L.span(), out.ry[h].data(), w);
    out.L.l[h].ry = out.ry[h].data();
  }
}

factor::stat::Labels labels_raw(const Loaded &L) {
  assert(L.hd.n > 0 && L.labels.size() == static_cast<size_t>(L.hd.n) && "标签未装 (LoadSpec::holds 空)");
  factor::stat::Labels out;
  out.hd = L.hd;
  out.cap = n_full_of(L);
  for (int h = 0; h < L.hd.n; ++h)
    out.l[h] = label_of(L.labels[static_cast<size_t>(h)], nullptr);
  return out;
}

void cost_tiers(misc::exec::ForkJoin &ex, const uint8_t *lc, const Loaded &L, factor::cpu::stat::Workspace &w, std::vector<CostTier> &out) {
  const int C = static_cast<int>(L.amts.size()), H = L.hd.n;
  assert(lc && L.costs.size() == static_cast<size_t>(C));
  out.assign(static_cast<size_t>(C), CostTier{});
  if (C == 0)
    return;
  std::vector<factor::stat::Cost> cost(static_cast<size_t>(C));
  for (int c = 0; c < C; ++c)
    cost[static_cast<size_t>(c)] = cost_of(L.costs[static_cast<size_t>(c)]);
  std::vector<factor::stat::CostAcc> acc(static_cast<size_t>(C) * H); // [c][hi]
  std::vector<long long> nx(static_cast<size_t>(1 + C));              // [0 = 有秩格数, 1 + c = 有秩 ∧ 成本 c 有效]
  factor::cpu::stat::cost_rows(ex, lc, L.span(), L.hd, cost.data(), C, n_full_of(L), w, acc.data(), nx.data());
  for (int c = 0; c < C; ++c) {
    CostTier &tier = out[static_cast<size_t>(c)];
    tier.amt = L.amts[static_cast<size_t>(c)];
    for (int i = 0; i < H; ++i)
      tier.acc[i] = acc[static_cast<size_t>(c) * H + i];
    tier.cover = nx[0] ? static_cast<float>(static_cast<double>(nx[static_cast<size_t>(1 + c)]) / static_cast<double>(nx[0])) : 0.f;
  }
}

void finish_root(misc::exec::ForkJoin &ex, const Loaded &L, const RootRaw &r, factor::cpu::stat::Workspace &w, RootOut &o) {
  assert(r.m && r.rx && r.lc && r.acc && r.x.v && r.x.m);
  const uint8_t *gate = L.cs.m.data();
  o.valid_pct = valid_pct_of(r.m, gate, L.n());
  o.rank_pct = rank_pct_of(ex, r.rx, r.m, gate, L.T, L.A);
  factor::stat::finish_all(r.acc, L.hd, L.seg, o.hold);
  x_dist(ex, r.x.v, r.x.m, r.rx, L.T, L.A, o.xd);
  cost_tiers(ex, r.lc, L, w, o.costs);
}

void x_dist(misc::exec::ForkJoin &ex, const float *v, const uint8_t *m, const uint16_t *rx, int T, int A, XDist &o) {
  constexpr int K = factor::stat::kGroups;
  constexpr size_t kBuf = 4096;
  const int nt = ex.threads();
  const auto ok = [&](size_t i) { return rx[i] != factor::stat::kRankNone && m[i] != 0; };
  std::vector<KLLcache> kll;
  const auto sketch_begin = [&] {
    kll.clear();
    for (int i = 0; i < nt; ++i)
      kll.emplace_back(analysis::kAggKllCapacity, analysis::kAggKllResolution);
  };
  const auto sketch_end = [&]() -> KLLcache & {
    for (int i = 1; i < nt; ++i)
      kll[0].mergeWith(kll[static_cast<size_t>(i)]);
    return kll[0];
  };
  struct Acc {
    double bsum[K - 1] = {}, gsum[K] = {};
    uint64_t bcnt[K - 1] = {}, gn[K] = {};
  };
  std::vector<Acc> acc(static_cast<size_t>(nt));
  std::vector<float> row_mean(static_cast<size_t>(T));
  std::vector<uint8_t> row_has(static_cast<size_t>(T));

  sketch_begin();
  ex.rows(T, [&](int t0, int t1, int tid) {
    std::vector<float> buf;
    buf.reserve(kBuf);
    Acc &c = acc[static_cast<size_t>(tid)];
    for (int t = t0; t < t1; ++t) {
      float mn[K] = {}, mx[K] = {};
      int cnt[K] = {};
      double s = 0.0;
      for (int a = 0; a < A; ++a) {
        const size_t i = static_cast<size_t>(t) * A + a;
        if (!ok(i))
          continue;
        const float x = v[i];
        const int g = factor::stat::grp_of(rx[i]);
        mn[g] = cnt[g] ? std::min(mn[g], x) : x;
        mx[g] = cnt[g] ? std::max(mx[g], x) : x;
        ++cnt[g];
        c.gsum[g] += x;
        s += x;
        buf.push_back(x);
        if (buf.size() == kBuf) {
          kll[static_cast<size_t>(tid)].addBatch(buf);
          buf.clear();
        }
      }
      int n = 0;
      for (int k = 0; k < K; ++k) {
        n += cnt[k];
        c.gn[k] += static_cast<uint64_t>(cnt[k]);
      }
      for (int k = 0; k + 1 < K; ++k)
        if (cnt[k] && cnt[k + 1]) {
          c.bsum[k] += 0.5 * (static_cast<double>(mx[k]) + mn[k + 1]);
          ++c.bcnt[k];
        }
      row_has[static_cast<size_t>(t)] = n > 0;
      row_mean[static_cast<size_t>(t)] = n > 0 ? static_cast<float>(s / n) : 0.f;
    }
    if (!buf.empty())
      kll[static_cast<size_t>(tid)].addBatch(buf);
  });
  const KLLcache &all = sketch_end();
  o = XDist{};
  o.n = all.totalCount();
  if (o.n < 3)
    return;
  o.mean = static_cast<float>(all.mean());
  o.sd = static_cast<float>(std::sqrt(std::max(0.0, static_cast<double>(all.var()))));
  o.skew = static_cast<float>(all.skew());
  o.kurt = static_cast<float>(all.kurt());
  {
    const auto q = all.exportICDF();
    assert(q.n >= 4);
    o.lo = q.y[1], o.hi = q.y[q.n - 2];
  }
  Acc tot;
  for (const Acc &c : acc)
    for (int k = 0; k < K; ++k) {
      tot.gsum[k] += c.gsum[k], tot.gn[k] += c.gn[k];
      if (k + 1 < K)
        tot.bsum[k] += c.bsum[k], tot.bcnt[k] += c.bcnt[k];
    }
  for (int k = 0; k < K; ++k) {
    o.share[k] = static_cast<float>(static_cast<double>(tot.gn[k]) / static_cast<double>(o.n));
    o.gmean[k] = tot.gn[k] ? static_cast<float>(tot.gsum[k] / static_cast<double>(tot.gn[k])) : 0.f;
    if (k + 1 < K) // 无一行两组同时有人 (稀疏 TS) → 沿用前一条边界, 该组段宽 0
      o.bound[k] = tot.bcnt[k] ? static_cast<float>(tot.bsum[k] / static_cast<double>(tot.bcnt[k])) : (k ? o.bound[k - 1] : o.lo);
  }
  for (int t = 0; t < T; ++t)
    if (row_has[static_cast<size_t>(t)])
      o.series.push_back(row_mean[static_cast<size_t>(t)]);

  sketch_begin();
  const float lo = o.lo, hi = o.hi;
  ex.rows(T, [&](int t0, int t1, int tid) {
    std::vector<float> buf;
    buf.reserve(kBuf);
    for (size_t i = static_cast<size_t>(t0) * A; i < static_cast<size_t>(t1) * A; ++i) {
      if (!ok(i) || v[i] < lo || v[i] > hi)
        continue;
      buf.push_back(v[i]);
      if (buf.size() == kBuf) {
        kll[static_cast<size_t>(tid)].addBatch(buf);
        buf.clear();
      }
    }
    if (!buf.empty())
      kll[static_cast<size_t>(tid)].addBatch(buf);
  });
  o.pdf.fill(sketch_end(), 3);
}

MiniPdf mini_pdf_of(const analysis::AggPdf &p) {
  MiniPdf o;
  const int n = static_cast<int>(p.n_pts);
  if (n < 2 || !(p.x[static_cast<size_t>(n - 1)] > p.x[0]))
    return o;
  o.lo = p.x[0], o.hi = p.x[static_cast<size_t>(n - 1)];
  float peak = 0.f;
  int j = 0;
  for (int i = 0; i < kMiniPdfPts; ++i) {
    const float x = o.lo + (o.hi - o.lo) * static_cast<float>(i) / (kMiniPdfPts - 1);
    while (j + 2 < n && p.x[static_cast<size_t>(j + 1)] < x)
      ++j;
    const float x0 = p.x[static_cast<size_t>(j)], x1 = p.x[static_cast<size_t>(j + 1)];
    assert(x1 >= x0 && "PDF x 须升序");
    const float u = x1 > x0 ? std::clamp((x - x0) / (x1 - x0), 0.f, 1.f) : 0.f;
    o.y[i] = std::max(0.f, p.y[static_cast<size_t>(j)] + u * (p.y[static_cast<size_t>(j + 1)] - p.y[static_cast<size_t>(j)]));
    peak = std::max(peak, o.y[i]);
  }
  if (peak > 0.f)
    for (float &y : o.y)
      y /= peak;
  return o;
}

void ic_mini_pdfs(const factor::stat::Row *rows, int H, int T, MiniPdf *out) {
  std::vector<float> ics;
  ics.reserve(static_cast<size_t>(T));
  for (int i = 0; i < H; ++i) {
    ics.clear();
    const factor::stat::Row *r = rows + static_cast<size_t>(i) * T;
    for (int t = 0; t < T; ++t)
      if (r[t].ok)
        ics.push_back(r[t].ric);
    KLLcache kll(analysis::kAggKllCapacity, analysis::kAggKllResolution);
    kll.addBatch(ics);
    analysis::AggPdf p;
    p.fill(kll, 3);
    out[i] = mini_pdf_of(p);
  }
}

} // namespace GUI::Factors
