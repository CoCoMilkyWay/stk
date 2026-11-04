// FactorsLoad — 见头文件. include 因子 Stat CPU 头 (标签秩 prep_label), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsLoad.hpp"

#include "factor/Stat/Cpu.hpp"                      // prep_label / Workspace
#include "features/MetaFlag.hpp"                    // fmeta::valid / keep / finite
#include "features/Method/Fundamental.hpp"          // fund::SW2021_L1_COUNT (行业 ID 上界)
#include "features/Node/Common/Label/LabelHold.hpp" // 只取 kContCloseMin 对账尾部界 (不含字段表)
#include "features/Storage/FeatureRead.hpp"
#include "features/TimeIndex.hpp"
#include "misc/profiler.hpp"
#include "shared/Analysis.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <chrono>
#include <functional>
#include <limits>

// 因子层几何 (factor::Geo) 与特征库层对账: 下标 = id; 行 t 的末分钟 = 该行末秒所在的 L_1min 分钟 (每日行数已由 FactorLevel::seg 取自特征库层)
static_assert(factor::kSegMax == static_cast<int>(TRADE_MINUTES_PER_DAY), "factor::kSegMax 必须等于 L_1min 每日分钟数");
static_assert([] {
  for (size_t i = 0; i < GUI::Factors::kLevelCount; ++i) {
    const GUI::Factors::FactorLevel &l = GUI::Factors::kFactorLevels[i];
    const factor::Geo g = l.geo();
    if (l.id != i)
      return false;
    for (int t = 0; t < g.S; ++t)
      if (static_cast<size_t>(g.minute(t)) != L_sec_to_L_1min(level_end_sec(l.lvl, static_cast<size_t>(t)) - 1))
        return false;
  }
  return true;
}(),
              "kFactorLevels 与 factor::kGeos / 特征库层 (行末分钟) 不符");
// 尾部界对账: 标签层最后一个可成交 entry 锚点 = 连续竞价末分钟 (kContCloseMin − 1) 的起始 = 行 kContCloseMin − 2; Stat tail_masked 用同一条界
static_assert(static_cast<size_t>(factor::stat::kLastAnchorRow) + 2 == LabelHold::kContCloseMin, "Stat 尾部界与 LabelHold 的收盘竞价起点不一致");

namespace GUI::Factors {

YSpec y_spec(const FeatureTable &ft, const Domain &dm, const Holds &hd, Segs sg, double max_impact) {
  YSpec s{hd, {}, 0.f};
  if (sg == Segs::None)
    return s;
  assert(ft.y.costs.front().flat() && "字段表成本段首个是固定段");
  std::vector<int> amts;
  for (const LegCol &c : ft.y.costs)
    if (c.flat() || dm.uses(c.leg[factor::stat::kLong]))
      amts.push_back(c.key);
  if (sg == Segs::First)
    amts = {amts.back() == kAmtFixed ? kAmtFixed : amts[1]};
  s.ad = keys_of<Amts>(amts);
  s.max_impact = static_cast<float>(max_impact);
  return s;
}

std::string YSpec::key() const {
  std::string k = "h";
  for (int i = 0; i < hd.n; ++i)
    k += "," + std::to_string(hd.k[i]);
  k += "|a";
  for (int i = 0; i < ad.n; ++i)
    k += "," + std::to_string(ad.k[i]);
  return k + "|cap" + std::to_string(max_impact);
}

double cell_bytes(const FeatureTable &ft, const BlockSpec &s, size_t nx) {
  int books = 0;
  for (int c = 0; c < s.y.ad.n; ++c)
    books += !leg_of(ft.y.costs, s.y.ad.k[c]).flat();
  return kPlaneBytes * static_cast<double>(nx) + 2.0 + kPlaneBytes * s.y.hd.n + 2.0 * books;
}

namespace {

// 一天门控 g[n] 的在池格数 (load_y / n_full_of 同一份: 逐天逐线程攒进 on[tid])
long long gate_on(const uint8_t *g, size_t n) {
  long long k = 0;
  for (size_t i = 0; i < n; ++i)
    k += g[i] != 0;
  return k;
}
// 满仓一组只数 (Stat 契约【持仓口径】的 cap) = 截面门控在池格数 on / 行数 T / kGroups (整期 YCore::on / n_full_of 同一份)
double n_full_from(long long on, int T) {
  const double n = static_cast<double>(on) / static_cast<double>(T) / factor::stat::kGroups;
  assert(n > 0.0 && "池内无有效资产");
  return n;
}

// 平面列 c 在网格行 t 的源行视图 (v nullptr = 无源); 格 a ← ids[a] (nullptr = 同轴; kAbsent = 资产不在该库)
struct View {
  const feature_storage_t *v = nullptr, *g = nullptr;
  const uint32_t *ids = nullptr;
  L2::ValidType vt = L2::ValidType::ALL;
};
// 格 a 的源下标 (kAbsent = 无源 / 资产不在该库)
uint32_t src_of(const View &w, size_t a) { return !w.v ? kAbsent : w.ids ? w.ids[a]
                                                                         : static_cast<uint32_t>(a); }
// 格 a: 有源 → x = 值 (+ g = 同行门控, 不门控 = 1)
bool cell(const View &w, size_t a, feature_storage_t &x) {
  const uint32_t i = src_of(w, a);
  if (i == kAbsent)
    return false;
  x = w.v[i];
  return true;
}
bool cell(const View &w, size_t a, feature_storage_t &x, float &g) {
  const uint32_t i = src_of(w, a);
  if (i == kAbsent)
    return false;
  x = w.v[i];
  g = w.g ? static_cast<float>(w.g[i]) : 1.f;
  return true;
}

// 一张读计划的列段 (load_x / load_y / load_b / n_full_of 同一份): 各列按因子池投影, add → 平面列号
struct ColList {
  PoolId pool;
  std::vector<ColRef> refs;
  size_t add(const Col &c) {
    refs.push_back(c.ref(pool));
    return refs.size() - 1;
  }
};

// 张量形 (容量断言在此): days × A → T = days · seg
Shape shape_of(const Io &io, const Chunk &k) {
  assert(k.d0 < k.d1 && k.d1 <= io.dm.days.size());
  Shape s;
  s.days = k.days();
  s.A = static_cast<int>(io.dm.rs.axis.size());
  s.seg = io.ft.seg;
  s.T = s.days * s.seg;
  assert(s.A >= 2 && s.A <= factor::stat::kMaxA && "资产轴超 Stat 容量 (kMaxA)");
  assert(static_cast<long long>(s.T) * s.A < (1LL << 31));
  return s;
}

// 逐天并行读骨架 (load_x / load_y / load_b / n_full_of 同一份): cols → 一张读计划 (同 (库, 层, 盘后) 一源, 网格行 level_asof 投影); 每天各源读进本线程暂存后
// on_day(view, day, d, tid) 散进平面 (view(c, t) = 平面列 c 在网格行 t 的源行视图). prog.begin Loading (日数), 每天 publish(1). false = 取消 / 库判废
template <class OnDay>
bool read_days(const Io &io, const Chunk &k, std::vector<ColRef> cols, OnDay &&on_day) {
  const size_t n_cols = cols.size();
  assert(n_cols > 0);
  io.prog.begin(Phase::Loading, k.d1 - k.d0);
  const ReadPlan plan = analysis::read_plan(*io.ft.feature, factor_level(io.ft.lv).lvl, analysis::Rows::AsOf, /*carry=*/true, std::move(cols));
  std::vector<std::pair<uint32_t, uint32_t>> at(n_cols); // 平面列 → (源, 源内值列)
  for (size_t si = 0; si < plan.srcs.size(); ++si)
    for (size_t j = 0; j < plan.srcs[si].slot.size(); ++j)
      at[plan.srcs[si].slot[j]] = {static_cast<uint32_t>(si), static_cast<uint32_t>(j)};
  const Readers rd = io.dm.rs.readers(io.lr.ptr);
  std::vector<IoSlot> slots(static_cast<size_t>(io.ex.threads()));
  for (IoSlot &s : slots)
    s.prepare(plan, io.dm.rs.sizes(), n_cols);

  io.ex.tasks(static_cast<int>(k.d1 - k.d0), [&](int di, int ti) {
    if (io.cancel.load(std::memory_order_relaxed))
      return;
    const size_t d = static_cast<size_t>(di), tid = static_cast<size_t>(ti);
    const Day &day = io.dm.days[k.d0 + d];
    IoSlot &slot = slots[tid];
    for (size_t si = 0; si < plan.srcs.size(); ++si)
      load_source(rd, plan.srcs[si], day, plan.srcs[si].cs.io, slot.src[si]);
    if (io.lr.stale())
      return;
    const auto view = [&](size_t c, size_t t) {
      const auto [si, j] = at[c];
      const Source &s = plan.srcs[si];
      View w;
      w.ids = rd.ids[s.lib];
      w.vt = s.cs.vts[j];
      size_t r = 0;
      if (const FeatureRead::DayColumns *dc = source_row(s, slot.src[si], t, r)) {
        w.v = col_row(dc, j, r);
        w.g = s.cs.gated() ? col_row(dc, s.cs.n(), r) : nullptr;
      }
      return w;
    };
    on_day(view, day, d, tid);
    io.prog.publish(1);
  });
  return !io.cancel.load(std::memory_order_relaxed) && !io.lr.stale();
}

// 一天的截面门控 (load_y / load_b / n_full_of 同一份): gate[t·A + a], t < VR = 截面池 ∧ 当日有数据; has = 本线程 [A] 暂存, 出 = 当日有数据.
// 当日有数据 = 该资产当日至少一行有成交 (ts_valid ≠ 0). 缺 binary / 整日停牌 → 全日 0: 这种 asset-day 不进截面池门控
// (值是 carry / 0, 标签 0), valid% / Stat / CS 算子的截面域因此只看"真有行情"的格, 不与数据完整性耦合.
// 截面池 = 标签库 cs_valid (all: 当日活跃; 子池: 当日成员)
template <class ViewFn>
void gate_day(const ViewFn &view, size_t ts_i, size_t cs_i, size_t VR, size_t A, std::vector<uint8_t> &has, uint8_t *gate) {
  feature_storage_t x;
  std::fill(has.begin(), has.end(), static_cast<uint8_t>(0));
  for (size_t t = 0; t < VR; ++t) {
    const View wt = view(ts_i, t);
    for (size_t a = 0; a < A; ++a)
      has[a] |= cell(wt, a, x) && fmeta::data_valid(static_cast<float>(x));
  }
  for (size_t t = 0; t < VR; ++t) {
    const View wc = view(cs_i, t);
    for (size_t a = 0; a < A; ++a)
      gate[t * A + a] = cell(wc, a, x) && fmeta::data_valid(static_cast<float>(x)) && has[a];
  }
}

} // namespace

bool load_x(const Io &io, const Chunk &k, const std::vector<LeafId> &ids, std::vector<Leaf> &out) {
  TraceN("FactorsLoadX");
  const FeatureTable &ft = io.ft;
  const Shape sh = shape_of(io, k);
  const size_t A = static_cast<size_t>(sh.A), n = sh.n(), VR = static_cast<size_t>(ft.seg), nf = ids.size();
  // ---- 列段 叶 [nf], 各按所在 (库, 层) 投影到网格行 ----
  ColList cols{io.dm.pool, {}};
  out.assign(nf, Leaf{});
  for (size_t i = 0; i < nf; ++i) {
    const FeatCol &c = ft.x.leaves[ids[i]];
    assert(c.allowed || c.target);
    cols.add(c);
    out[i].resize(n);
  }
  return read_days(io, k, std::move(cols.refs), [&](const auto &view, const Day &, size_t d, size_t) {
    feature_storage_t x;
    float g;
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      for (size_t i = 0; i < nf; ++i) {
        const View w = view(i, t);
        float *v = out[i].v.data() + row;
        uint8_t *m = out[i].m.data() + row;
        for (size_t a = 0; a < A; ++a) {
          const bool ok = cell(w, a, x, g) && fmeta::keep(g, w.vt, x);
          v[a] = ok ? static_cast<float>(x) : 0.f;
          m[a] = ok;
        }
      }
    }
  });
}

bool load_y(const Io &io, const Chunk &k, const YCore *have, YCore &core, const Holds &hd, const Amts &ad, std::vector<LegPlane> &labels,
            std::vector<std::vector<uint16_t>> &costs) {
  TraceN("FactorsLoadY");
  constexpr size_t K = factor::stat::kLegs;
  constexpr size_t kNone = SIZE_MAX;
  const FeatureTable &ft = io.ft;
  const Shape sh = shape_of(io, k);
  const size_t A = static_cast<size_t>(sh.A), n = sh.n(), VR = static_cast<size_t>(ft.seg);
  const size_t H = static_cast<size_t>(hd.n), C = static_cast<size_t>(ad.n);
  assert((!have || H + C > 0) && "无可装");
  // ---- 列段 [ts_valid | cs_valid | lb_entry_ok (门控未缓存才读)] | 标签 [H][K] (标签库网格层本层列) | 盘口段买入冲击 [C] (Lob 库网格层本层列), 同行.
  //      顺延已在标签里 (features LabelHold) ----
  ColList cols{io.dm.pool, {}};
  size_t ts_i = kNone, cs_i = kNone, eo_i = kNone;
  if (!have) {
    ts_i = cols.add(ft.y.ts), cs_i = cols.add(ft.y.cs), eo_i = cols.add(ft.y.entry);
    assert(cols.refs[ts_i].lib == io.dm.rs.main && "门控 / 标签在标签库");
  }
  std::vector<size_t> lab_at(H), cost_at(C);
  for (size_t i = 0; i < H; ++i) {
    lab_at[i] = cols.refs.size();
    for (const Col &c : leg_of(ft.y.labels, hd.k[i]).leg)
      cols.add(c);
  }
  for (size_t c = 0; c < C; ++c) {
    const LegCol &lc = leg_of(ft.y.costs, ad.k[c]);
    assert(!lc.flat() && io.dm.uses(lc.leg[factor::stat::kLong]) && "只装盘口段, 要评估域含 Lob 库 (y_spec)");
    cost_at[c] = cols.add(lc.leg[factor::stat::kLong]);
  }

  if (!have) {
    core = YCore{};
    core.gate.assign(n, 0);
    core.eo.assign(n, 0);
  }
  const YCore &g = have ? *have : core;
  assert(g.gate.size() == n && "门控与块不同形");
  labels.assign(H, LegPlane{});
  for (LegPlane &p : labels) {
    for (std::vector<uint16_t> &v : p.v)
      v.assign(n, 0);
    p.m.assign(n, 0);
  }
  costs.assign(C, std::vector<uint16_t>(n, 0));
  constexpr uint16_t kInf = 0x7C00;                                                                          // fp16 +inf
  std::vector<std::vector<uint8_t>> has_data(static_cast<size_t>(io.ex.threads()), std::vector<uint8_t>(A)); // 每线程: 当日各资产是否有成交
  std::vector<long long> on(static_cast<size_t>(io.ex.threads()), 0);                                        // 每线程: 门控在池格数

  const bool read = read_days(io, k, std::move(cols.refs), [&](const auto &view, const Day &, size_t d, size_t tid) {
    // 建仓可成交码: 池内 (gate) ∧ 有限 → 码 (1..3), 其余 0
    const auto fold_eo = [&](size_t t, size_t row) {
      const View w = view(eo_i, t);
      for (size_t a = 0; a < A; ++a) {
        feature_storage_t x = {};
        const bool ok = core.gate[row + a] && cell(w, a, x) && fmeta::finite(x);
        const float c = ok ? static_cast<float>(x) : 0.f;
        assert((c == 0.f || c == 1.f || c == 2.f || c == 3.f) && "lb_entry_ok 须是 kEoBuy | kEoSell");
        core.eo[row + a] = static_cast<uint8_t>(c);
      }
    };
    // 标签组 i 的侧 l = 列 lab_at[i] + l: 池内 (gate) ∧ 两侧皆有限 → fp16 位 + 有效位 (须有可成交码), 其余 0
    const auto fold_labels = [&](size_t t, size_t row) {
      for (size_t i = 0; i < H; ++i) {
        View w[K];
        for (size_t l = 0; l < K; ++l)
          w[l] = view(lab_at[i] + l, t);
        LegPlane &p = labels[i];
        for (size_t a = 0; a < A; ++a) {
          feature_storage_t x[K] = {};
          bool ok = g.gate[row + a];
          for (size_t l = 0; l < K; ++l)
            ok = ok && cell(w[l], a, x[l]) && fmeta::finite(x[l]);
          for (size_t l = 0; l < K; ++l)
            p.v[l][row + a] = ok ? std::bit_cast<uint16_t>(x[l]) : 0;
          assert((!ok || g.eo[row + a]) && "标签有效格须有建仓可成交码 (旧标签库: 重算)");
          p.m[row + a] = ok;
        }
      }
    };
    // 金额段 c 的买入冲击 = 列 cost_at[c]: 池内 ∧ 有限 → fp16 位 (原值); 池内而盘口缺 (建仓盘口封涨停 / 该资产当日无盘口,
    // 而标签的 VWAP 价点可买) → +inf (取用时封顶); 池外 0 (cost_rows 只在可买时取), 无有效位
    const auto fold_costs = [&](size_t t, size_t row) {
      for (size_t c = 0; c < C; ++c) {
        const View w = view(cost_at[c], t);
        std::vector<uint16_t> &p = costs[c];
        for (size_t a = 0; a < A; ++a) {
          feature_storage_t x = {};
          const bool fin = cell(w, a, x) && fmeta::finite(x);
          p[row + a] = !g.gate[row + a] ? 0 : fin ? std::bit_cast<uint16_t>(x)
                                                  : kInf;
        }
      }
    };
    if (!have) {
      uint8_t *gate = core.gate.data() + d * VR * A;
      gate_day(view, ts_i, cs_i, VR, A, has_data[tid], gate);
      on[tid] += gate_on(gate, VR * A);
    }
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      if (!have)
        fold_eo(t, row);
      fold_labels(t, row);
      fold_costs(t, row);
    }
  });
  if (!read)
    return false;
  if (!have)
    for (const long long x : on)
      core.on += x;
  return true;
}

bool load_b(const Io &io, const Chunk &k, BPlanes &B) {
  TraceN("FactorsLoadB");
  constexpr size_t K = factor::stat::kLegs;
  const FeatureTable &ft = io.ft;
  assert(io.dm.pool == kAllPool && "池篮子在 all 池评估域 (全轴; 各池成员读池列)");
  B = BPlanes{};
  B.sh = shape_of(io, k);
  const size_t A = static_cast<size_t>(B.sh.A), n = B.sh.n(), VR = static_cast<size_t>(ft.seg), NP = ft.b.pools.size();
  assert(NP == pool_count() && NP <= static_cast<size_t>(kPoolBits));
  // ---- 列段 ts_valid cs_valid lb_entry_ok T+1C 标签 [K] close (标签库网格层本层列) | adj_factor total_shares val[kPoolVals] ind_l1 池列 [1, NP)
  //      (日频原料 bar.1d, as-of 到网格行) ----
  ColList cols{io.dm.pool, {}};
  const size_t ts_i = cols.add(ft.y.ts), cs_i = cols.add(ft.y.cs), eo_i = cols.add(ft.y.entry);
  const size_t lab_i = cols.refs.size();
  for (const Col &c : leg_of(ft.y.labels, factor::stat::hold_close(1)).leg)
    cols.add(c);
  const size_t px_i = cols.add(ft.b.ohlc[3]), adj_i = cols.add(ft.b.adj_factor), sh_i = cols.add(ft.b.total_shares);
  const size_t val_i = cols.refs.size();
  for (const Col &v : ft.b.val)
    cols.add(v);
  const size_t ind_i = cols.add(ft.b.ind_l1);
  const size_t pool_i = cols.refs.size(); // 池 p 的列 = pool_i + p − 1
  for (size_t p = 1; p < NP; ++p)
    cols.add(ft.b.pools[p]);
  assert(cols.refs[ts_i].lib == io.dm.rs.main && "门控 / 标签 / K 线在标签库");
  std::vector<float> vadj(n, 0.f); // 本行 adj_factor (盘后列, as-of = 当日因子; 缺 / 非正 → 0): 装完把 close 乘成后复权, 即弃
  B.in.assign(n, 0);
  for (std::vector<uint16_t> &v : B.lab.v)
    v.assign(n, 0);
  B.lab.m.assign(n, 0);
  B.eo.assign(n, 0);
  B.close.resize(n);
  B.mcap.assign(n, 0.f);
  B.px.assign(n, 0.f);
  B.shares.resize(n);
  for (std::vector<float> &v : B.val)
    v.resize(n);
  B.ind.assign(n, 0);
  const size_t nt = static_cast<size_t>(io.ex.threads());
  std::vector<std::vector<uint8_t>> has(nt, std::vector<uint8_t>(A)), gate(nt, std::vector<uint8_t>(VR * A)); // 每线程: 当日有成交 / all 截面门控
  const bool read = read_days(io, k, std::move(cols.refs), [&](const auto &view, const Day &, size_t d, size_t tid) {
    gate_day(view, ts_i, cs_i, VR, A, has[tid], gate[tid].data());
    const std::vector<uint8_t> &on = has[tid];
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      // 池成员位: 位 kAllPool = all 截面门控; 位 p = 池列 > 0.5 (NaN = 键外 → 不在池) ∧ 当日有成交
      PoolMask *in = B.in.data() + row;
      for (size_t a = 0; a < A; ++a)
        in[a] = gate[tid][t * A + a] ? pool_bit(kAllPool) : PoolMask{0};
      for (size_t p = 1; p < NP; ++p) {
        const View w = view(pool_i + p - 1, t);
        const PoolMask bit = pool_bit(static_cast<PoolId>(p));
        for (size_t a = 0; a < A; ++a) {
          feature_storage_t x;
          if (on[a] && cell(w, a, x) && static_cast<float>(x) > 0.5f)
            in[a] |= bit;
        }
      }
      // 建仓可成交码 / T+1C 两侧 (同 load_y 的折叠, 池内 = 在任一池)
      const View we = view(eo_i, t), wl[K] = {view(lab_i, t), view(lab_i + 1, t)};
      for (size_t a = 0; a < A; ++a) {
        feature_storage_t e = {}, x[K] = {};
        const bool ok_e = in[a] && cell(we, a, e) && fmeta::finite(e);
        const float c = ok_e ? static_cast<float>(e) : 0.f;
        assert((c == 0.f || c == 1.f || c == 2.f || c == 3.f) && "lb_entry_ok 须是 kEoBuy | kEoSell");
        B.eo[row + a] = static_cast<uint8_t>(c);
        bool ok = in[a] != 0;
        for (size_t l = 0; l < K; ++l)
          ok = ok && cell(wl[l], a, x[l]) && fmeta::finite(x[l]);
        for (size_t l = 0; l < K; ++l)
          B.lab.v[l][row + a] = ok ? std::bit_cast<uint16_t>(x[l]) : 0;
        B.lab.m[row + a] = ok;
        assert((!ok || B.eo[row + a]) && "标签有效格须有建仓可成交码 (旧标签库: 重算)");
      }
      // close: 本行有成交 (ts_valid 按 DATA 口径) ∧ 有限 ∧ > 0 (此处不复权, 装完乘复权因子)
      const View wt = view(ts_i, t), wp = view(px_i, t), wa = view(adj_i, t);
      for (size_t a = 0; a < A; ++a) {
        feature_storage_t s, px, f;
        const bool ok = cell(wp, a, px) && cell(wt, a, s) && fmeta::keep(static_cast<float>(s), L2::ValidType::DATA, px) && static_cast<float>(px) > 0.f;
        B.close.v[row + a] = ok ? static_cast<float>(px) : 0.f, B.close.m[row + a] = ok;
        B.px[row + a] = B.close.v[row + a];
        vadj[row + a] = cell(wa, a, f) && fmeta::finite(f) && static_cast<float>(f) > 0.f ? static_cast<float>(f) : 0.f;
      }
      const View ws = view(sh_i, t), wi = view(ind_i, t);
      View wv[kPoolVals];
      for (int m = 0; m < kPoolVals; ++m)
        wv[m] = view(val_i + static_cast<size_t>(m), t);
      constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
      for (size_t a = 0; a < A; ++a) {
        feature_storage_t x;
        const float px = B.px[row + a];
        B.shares[row + a] = cell(ws, a, x) ? static_cast<float>(x) : kNaN;
        if (px > 0.f && B.shares[row + a] > 0.f)
          B.mcap[row + a] = px * B.shares[row + a];
        for (int m = 0; m < kPoolVals; ++m)
          B.val[m][row + a] = cell(wv[m], a, x) ? static_cast<float>(x) : kNaN;
        if (cell(wi, a, x) && static_cast<float>(x) >= 1.f && static_cast<float>(x) < static_cast<float>(fund::SW2021_L1_COUNT))
          B.ind[row + a] = static_cast<uint8_t>(static_cast<float>(x));
      }
    }
  });
  if (!read)
    return false;
  // 后复权 close = 本行 close × 当日复权因子 (本行 as-of). 只比值进收益, 首日归一无关
  io.ex.rows(B.sh.T, [&](int t0, int t1, int) {
    for (size_t i = static_cast<size_t>(t0) * A; i < static_cast<size_t>(t1) * A; ++i) {
      const float f = vadj[i];
      B.close.m[i] = B.close.m[i] && f > 0.f;
      B.close.v[i] = B.close.m[i] ? B.close.v[i] * f : 0.f;
    }
  });
  return true;
}

StatScope stat_scope(const FactorScope &sc, const Domain &dm, int seg, double n_full, const char *backend) {
  StatScope o;
  o.pool = sc.pool;
  o.start_date = dm.dates.front();
  o.end_date = dm.dates.back();
  o.days = static_cast<int>(dm.days.size()), o.T = o.days * seg, o.A = static_cast<int>(dm.rs.axis.size()); // 评估域全期
  o.max_impact = sc.max_impact;
  o.n_full = n_full;
  o.backend = backend;
  o.time = now_string();
  return o;
}

bool n_full_of(const Io &io, const Chunk &k, double &out) {
  TraceN("FactorsNFull");
  const FeatureTable &ft = io.ft;
  const Shape sh = shape_of(io, k);
  const size_t A = static_cast<size_t>(sh.A), VR = static_cast<size_t>(ft.seg), nt = static_cast<size_t>(io.ex.threads());
  ColList cols{io.dm.pool, {}};
  const size_t ts_i = cols.add(ft.y.ts), cs_i = cols.add(ft.y.cs);
  assert(cols.refs[ts_i].lib == io.dm.rs.main && "门控在标签库");
  std::vector<std::vector<uint8_t>> has(nt, std::vector<uint8_t>(A)), gate(nt, std::vector<uint8_t>(VR * A));
  std::vector<long long> on(nt, 0);
  if (!read_days(io, k, std::move(cols.refs), [&](const auto &view, const Day &, size_t, size_t tid) {
        gate_day(view, ts_i, cs_i, VR, A, has[tid], gate[tid].data());
        on[tid] += gate_on(gate[tid].data(), VR * A);
      }))
    return false;
  long long sum = 0;
  for (const long long x : on)
    sum += x;
  out = n_full_from(sum, sh.T);
  return true;
}

factor::stat::Labels labels_of(const YPlanes &y) {
  assert(y.hd.n > 0 && y.eo && "标签未钉");
  factor::stat::Labels out;
  out.hd = y.hd;
  out.cap = y.n_full;
  for (int h = 0; h < y.hd.n; ++h) {
    assert(y.labels[h]);
    out.l[h] = label_of(*y.labels[h], y.eo, y.ry[h]);
  }
  return out;
}

Pick pick_of(const Holds &all, const Holds &want) {
  Pick p;
  p.n = want.n;
  for (int i = 0; i < want.n; ++i) {
    p.at[i] = factor::stat::hold_index(all, want.k[i]);
    assert(p.at[i] >= 0 && "所评持有期不在 Y 的标签组里");
  }
  return p;
}

factor::stat::Labels labels_pick(const factor::stat::Labels &L, const Pick &p) {
  factor::stat::Labels out;
  out.hd.n = p.n;
  out.cap = L.cap;
  for (int i = 0; i < p.n; ++i) {
    out.hd.k[i] = L.hd.k[p.at[i]];
    out.l[i] = L.l[p.at[i]];
  }
  return out;
}

std::vector<const factor::check::Plane *> leaves(const Block &c, const std::vector<std::string> &feats) {
  assert(c.ft && "先 Store::pin");
  std::vector<const factor::check::Plane *> in;
  for (const LeafId id : c.ft->x.ids(feats)) {
    in.push_back(c.x.find(id));
    assert(in.back() && "叶不在块上 (BlockSpec::x)");
  }
  return in;
}

// ---- Store ----

Store::Store() : ex(misc::exec::hw()) {}

Store &Store::get() {
  static Store s;
  return s;
}

bool Store::acquire(Prio p, const std::atomic<bool> &cancel) {
  const size_t i = static_cast<size_t>(p), hi = static_cast<size_t>(Prio::Interactive);
  std::unique_lock<std::mutex> lock(mu_);
  ++waiting_[i];
  while (busy_ || (p == Prio::Batch && waiting_[hi] > 0)) {
    if (cancel.load(std::memory_order_relaxed)) {
      --waiting_[i];
      cv_.notify_all();
      return false;
    }
    cv_.wait_for(lock, std::chrono::milliseconds(20));
  }
  --waiting_[i];
  busy_ = true;
  owner_ = std::this_thread::get_id();
  return true;
}

void Store::release() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    own();
    busy_ = false;
    owner_ = {};
  }
  cv_.notify_all();
}

template <class T>
std::shared_ptr<const T> Store::find(const std::string &key) {
  const auto it = cache_.find(key);
  if (it == cache_.end())
    return nullptr;
  it->second.tick = ++tick_;
  return std::static_pointer_cast<const T>(it->second.p);
}

template <class T>
std::shared_ptr<const T> Store::put(const std::string &key, T &&v, double bytes) {
  std::shared_ptr<const T> p = std::make_shared<const T>(std::move(v));
  [[maybe_unused]] const bool fresh = cache_.emplace(key, Entry{p, bytes, ++tick_}).second;
  assert(fresh && "平面已在缓存");
  bytes_ += bytes;
  return p;
}

double Store::budget() const {
  own();
  return host_budget() + bytes_;
}

void Store::room(double bytes) {
  own();
  const double cap = host_budget() + bytes_;
  while (bytes_ + bytes > cap) {
    auto lru = cache_.end();
    for (auto it = cache_.begin(); it != cache_.end(); ++it)
      if (it->second.p.use_count() == 1 && (lru == cache_.end() || it->second.tick < lru->second.tick))
        lru = it;
    if (lru == cache_.end())
      return;
    bytes_ -= lru->second.bytes;
    cache_.erase(lru);
  }
}

void Store::clear() {
  own();
  cache_.clear();
  n_full_.clear();
  bytes_ = 0.0;
  ++epoch_;
}

Rc Store::pin(Block &c, const Io &io, const Chunk &k, const BlockSpec &s, double reserve) {
  TraceN("FactorsPin");
  own();
  assert(&io.lr == &lr && &io.ex == &ex && "装载环境取自 Store");
  assert(k.d0 <= k.s0 && k.s0 < k.s1 && k.s1 <= k.d1 && k.d1 <= io.dm.days.size());
  const FeatureTable &ft = io.ft;
  const std::string dom = std::string(factor_level(ft.lv).name) + "|" + io.dm.key();
  const std::string base = dom + "|" + std::to_string(k.d0) + "-" + std::to_string(k.d1) + "|";
  const Shape sh = shape_of(io, k);
  const double n = static_cast<double>(sh.n());
  c = Block{};
  c.d0 = k.d0, c.d1 = k.d1, c.nd = io.dm.days.size();
  c.t_lo = static_cast<int>(k.s0 - k.d0) * ft.seg, c.t_back = static_cast<int>(k.d1 - k.s1) * ft.seg;
  c.gen = std::hash<std::string>{}(base + s.y.key() + "#" + std::to_string(epoch_));
  c.ft = &ft;
  c.x.sh = sh;
  c.x.v.assign(ft.x.leaves.size(), nullptr);
  YPlanes &y = c.y;
  y.sh = sh;
  y.hd = s.y.hd, y.ad = s.y.ad, y.max_impact = s.y.max_impact;
  factor::stat::assert_holds(y.hd, sh.seg);

  // ---- 查缓存: 命中即钉 (随后的腾地不淘汰它们), 缺的记下 ----
  const auto hold_key = [&](int i) { return base + "h" + std::to_string(y.hd.k[i]); };
  const auto amt_key = [&](int i) { return base + "a" + std::to_string(y.ad.k[i]); };
  const auto rank_key = [&](int i) { return base + "r" + std::to_string(y.hd.k[i]); };
  const auto leaf_key = [&](LeafId id) { return base + "x" + std::to_string(id); };
  std::shared_ptr<const YCore> core = find<YCore>(base + "g");
  if (core)
    c.pins.push_back(core);
  Holds mh;
  Amts ma;
  std::vector<int> mh_at, ma_at, mr;
  std::vector<LeafId> mx;
  for (int i = 0; i < y.hd.n; ++i) {
    if (const std::shared_ptr<const LegPlane> p = find<LegPlane>(hold_key(i)))
      y.labels[i] = p.get(), c.pins.push_back(p);
    else
      mh.k[mh.n++] = y.hd.k[i], mh_at.push_back(i);
    if (!s.ry)
      continue;
    if (const std::shared_ptr<const std::vector<uint16_t>> p = find<std::vector<uint16_t>>(rank_key(i)))
      y.ry[i] = p->data(), c.pins.push_back(p);
    else
      mr.push_back(i);
  }
  for (int i = 0; i < y.ad.n; ++i) {
    if (leg_of(ft.y.costs, y.ad.k[i]).flat())
      continue;
    if (const std::shared_ptr<const std::vector<uint16_t>> p = find<std::vector<uint16_t>>(amt_key(i)))
      y.costs[i] = p->data(), c.pins.push_back(p);
    else
      ma.k[ma.n++] = y.ad.k[i], ma_at.push_back(i);
  }
  for (const LeafId id : s.x) {
    if (const std::shared_ptr<const Leaf> p = find<Leaf>(leaf_key(id)))
      c.x.v[id] = p.get(), c.pins.push_back(p);
    else
      mx.push_back(id);
  }

  // ---- 腾地 → 一趟补读 Y (门控 / 标签组 / 盘口段) + 一趟补读 X ----
  const double need = n * ((core ? 0.0 : 2.0) + kPlaneBytes * static_cast<double>(mh.n + static_cast<int>(mx.size())) + 2.0 * static_cast<double>(ma.n + static_cast<int>(mr.size())));
  room(need + reserve);
  c.fresh = need > 0.0;
  if (!core || mh.n > 0 || ma.n > 0) {
    YCore got;
    std::vector<LegPlane> labs;
    std::vector<std::vector<uint16_t>> cs;
    if (!load_y(io, k, core.get(), got, mh, ma, labs, cs))
      return rc_of(false, lr);
    if (!core) {
      core = put(base + "g", std::move(got), 2.0 * n);
      c.pins.push_back(core);
    }
    for (int j = 0; j < mh.n; ++j) {
      const std::shared_ptr<const LegPlane> p = put(hold_key(mh_at[static_cast<size_t>(j)]), std::move(labs[static_cast<size_t>(j)]), kPlaneBytes * n);
      y.labels[mh_at[static_cast<size_t>(j)]] = p.get(), c.pins.push_back(p);
    }
    for (int j = 0; j < ma.n; ++j) {
      const std::shared_ptr<const std::vector<uint16_t>> p = put(amt_key(ma_at[static_cast<size_t>(j)]), std::move(cs[static_cast<size_t>(j)]), 2.0 * n);
      y.costs[ma_at[static_cast<size_t>(j)]] = p->data(), c.pins.push_back(p);
    }
  }
  y.gate = core->gate.data(), y.eo = core->eo.data();
  if (!mx.empty()) {
    std::vector<Leaf> xs;
    if (!load_x(io, k, mx, xs))
      return rc_of(false, lr);
    for (size_t j = 0; j < mx.size(); ++j) {
      const std::shared_ptr<const Leaf> p = put(leaf_key(mx[j]), std::move(xs[j]), kPlaneBytes * n);
      c.x.v[mx[j]] = p.get(), c.pins.push_back(p);
    }
  }
  // ---- 标签秩 (CPU 会话的 ry): 做多标签逐行 r16 ----
  if (!mr.empty()) {
    const factor::stat::Span sp{sh.T, sh.A, factor::geo_of(sh.seg)};
    factor::stat::assert_span(sp);
    factor::cpu::stat::Workspace w;
    w.ensure(sh.A, 1, 0, ex.threads());
    for (const int i : mr) {
      std::vector<uint16_t> r(sh.n());
      factor::cpu::stat::prep_label(ex, label_of(*y.labels[i], y.eo, nullptr), sp, r.data(), w);
      const std::shared_ptr<const std::vector<uint16_t>> p = put(rank_key(i), std::move(r), 2.0 * n);
      y.ry[i] = p->data(), c.pins.push_back(p);
    }
  }
  // ---- 满仓只数: 按评估域 (整期一块 = 本块门控; 多块 = 全期预扫) ----
  if (const auto it = n_full_.find(dom); it != n_full_.end()) {
    y.n_full = it->second;
  } else {
    const size_t nd = io.dm.days.size();
    if (k.d0 == 0 && k.d1 == nd)
      y.n_full = n_full_from(core->on, sh.T);
    else if (!n_full_of(io, Chunk{0, 0, nd, nd}, y.n_full))
      return rc_of(false, lr);
    n_full_.emplace(dom, y.n_full);
  }
  return Rc::Ok;
}

} // namespace GUI::Factors
