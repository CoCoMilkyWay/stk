// FactorsLoad — 见头文件. include 因子契约头 (Stat/Contract.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsLoad.hpp"

#include "features/MetaFlag.hpp"                   // fmeta::valid / keep / finite
#include "features/Method/Fundamental.hpp"         // fund::SW2021_L1_COUNT (行业 ID 上界)
#include "features/Node/Lob/TS/Label/LabelLob.hpp" // 只取 kContCloseMin 对账尾部界 (不含字段表)
#include "features/Storage/FeatureRead.hpp"
#include "features/TimeIndex.hpp"
#include "misc/profiler.hpp"
#include "shared/Analysis.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
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
static_assert(static_cast<size_t>(factor::stat::kLastAnchorRow) + 2 == LabelLob::kContCloseMin, "Stat 尾部界与 LabelLob 的收盘竞价起点不一致");

namespace GUI::Factors {

YSpec y_spec(const FeatureTable &ft, const Holds &hd, Segs sg, double max_impact) {
  YSpec s{hd, {}, 0.f};
  if (sg == Segs::None)
    return s;
  s.ad = keys_of<Amts>(ft.y.costs);
  if (sg == Segs::First)
    s.ad.n = 1;
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
  return k + "|cap" + std::to_string(max_impact) + "|n" + std::to_string(cap);
}

double cell_bytes(const FeatureTable &ft, const BlockSpec &s, size_t nx) {
  int books = 0;
  for (int c = 0; c < s.y.ad.n; ++c)
    books += !leg_of(ft.y.costs, s.y.ad.k[c]).flat();
  return kPlaneBytes * static_cast<double>(nx) + 2.0 + kPlaneBytes * s.y.hd.n + 2.0 * books;
}

size_t XPlanes::count() const {
  size_t n = 0;
  for (const factor::check::Plane &p : v)
    n += !p.v.empty();
  return n;
}

double Block::bytes() const {
  const auto vb = [](const auto &v) { return static_cast<double>(v.size() * sizeof(v[0])); };
  const auto pb = [&](const factor::check::Plane &p) { return vb(p.v) + vb(p.m); };
  const auto lb = [&](const LegPlane &p) {
    double s = vb(p.m);
    for (const std::vector<uint16_t> &v : p.v)
      s += vb(v);
    return s;
  };
  double s = vb(y.gate) + vb(y.eo);
  for (const factor::check::Plane &p : x.v)
    s += pb(p);
  for (const LegPlane &p : y.labels)
    s += lb(p);
  for (const std::vector<uint16_t> &p : y.costs)
    s += vb(p);
  for (const std::vector<uint16_t> &p : ry)
    s += vb(p);
  return s;
}

namespace {

// 一天门控 g[n] 的在池格数 (load_y / n_full_of 同一份: 逐天逐线程攒进 on[tid])
long long gate_on(const uint8_t *g, size_t n) {
  long long k = 0;
  for (size_t i = 0; i < n; ++i)
    k += g[i] != 0;
  return k;
}
// 满仓一组只数 (Stat 契约【持仓口径】的 cap) = 截面门控在池格数 Σon / 行数 T / kGroups (load_y / n_full_of 同一份)
double n_full_from(const std::vector<long long> &on, int T) {
  long long k = 0;
  for (const long long x : on)
    k += x;
  const double n = static_cast<double>(k) / static_cast<double>(T) / factor::stat::kGroups;
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

// 逐天并行读骨架 (load_x / load_y / load_b / n_full_of 同一份): cols → 一张读计划 (同 (池, 层, 盘后) 一源, 网格行 level_asof 投影); 每天各源读进本线程暂存后
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
      w.ids = rd.ids[s.pool];
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

// 一天的截面门控 (load_y / load_b / n_full_of 同一份): gate[t·A + a], t < VR = 截面池 ∧ 当日有数据; has = 本线程 [A] 暂存, 出 = 当日有数据 (∧ 子池成员).
// 当日有数据 = 该资产当日至少一行有成交 (ts_valid ≠ 0). 缺 binary / 整日停牌 → 全日 0: 这种 asset-day 不进截面池门控
// (值是 carry / 0, 标签 0), valid% / Stat / CS 算子的截面域因此只看"真有行情"的格, 不与数据完整性耦合.
// 截面池 = 标签库 cs_valid (all = 母集; Lob 池 = 当日在池); 子池 (Bar 标签层) = 当日成员, 不看 cs_valid
// (ST 池在母集外; 其余子池 ⊂ 母集, 同一份盘前态)
template <class ViewFn>
void gate_day(const ViewFn &view, size_t ts_i, size_t cs_i, const Domain &dm, const Day &day, size_t VR, size_t A, std::vector<uint8_t> &has,
              uint8_t *gate) {
  feature_storage_t x;
  std::fill(has.begin(), has.end(), static_cast<uint8_t>(0));
  for (size_t t = 0; t < VR; ++t) {
    const View wt = view(ts_i, t);
    for (size_t a = 0; a < A; ++a)
      has[a] |= cell(wt, a, x) && fmeta::data_valid(static_cast<float>(x));
  }
  if (dm.member) {
    const uint8_t *mem = dm.member->row(day.date);
    for (size_t a = 0; a < A; ++a)
      has[a] &= mem[a];
    for (size_t t = 0; t < VR; ++t)
      std::copy(has.begin(), has.end(), gate + t * A);
    return;
  }
  for (size_t t = 0; t < VR; ++t) {
    const View wc = view(cs_i, t);
    for (size_t a = 0; a < A; ++a)
      gate[t * A + a] = cell(wc, a, x) && fmeta::data_valid(static_cast<float>(x)) && has[a];
  }
}

} // namespace

bool load_x(const Io &io, const Chunk &k, const std::vector<LeafId> &ids, XPlanes &X) {
  TraceN("FactorsLoadX");
  const FeatureTable &ft = io.ft;
  const Shape sh = shape_of(io, k);
  assert((X.v.empty() || X.sh == sh) && "X 与块不同形");
  X.sh = sh;
  X.v.resize(ft.x.leaves.size());
  const size_t A = static_cast<size_t>(sh.A), n = sh.n(), VR = static_cast<size_t>(ft.seg), nf = ids.size();
  // ---- 列段 叶 [nf], 各按所在 (池, 层) 投影到网格行 ----
  ColList cols{io.dm.pool, {}};
  std::vector<factor::check::Plane *> out(nf);
  for (size_t i = 0; i < nf; ++i) {
    const FeatCol &c = ft.x.leaves[ids[i]];
    assert(c.allowed && !X.find(ids[i]));
    cols.add(c);
    out[i] = &X.v[ids[i]];
    out[i]->resize(n);
  }
  const bool read = read_days(io, k, std::move(cols.refs), [&](const auto &view, const Day &, size_t d, size_t) {
    feature_storage_t x;
    float g;
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      for (size_t i = 0; i < nf; ++i) {
        const View w = view(i, t);
        float *v = out[i]->v.data() + row;
        uint8_t *m = out[i]->m.data() + row;
        for (size_t a = 0; a < A; ++a) {
          const bool ok = cell(w, a, x, g) && fmeta::keep(g, w.vt, x);
          v[a] = ok ? static_cast<float>(x) : 0.f;
          m[a] = ok;
        }
      }
    }
  });
  if (!read)
    for (factor::check::Plane *p : out)
      *p = {};
  return read;
}

bool load_y(const Io &io, const Chunk &k, const YSpec &s, YPlanes &Y) {
  TraceN("FactorsLoadY");
  constexpr size_t K = factor::stat::kLegs;
  constexpr size_t kFlat = SIZE_MAX; // 固定段: 无列无平面
  const FeatureTable &ft = io.ft;
  Y = YPlanes{};
  Y.sh = shape_of(io, k);
  Y.hd = s.hd;
  Y.ad = s.ad;
  factor::stat::assert_holds(Y.hd, Y.sh.seg);
  const size_t A = static_cast<size_t>(Y.sh.A), n = Y.sh.n(), VR = static_cast<size_t>(ft.seg);
  const size_t H = static_cast<size_t>(Y.hd.n), C = static_cast<size_t>(Y.ad.n);
  // ---- 列段 ts_valid | cs_valid | lb_entry_ok | 标签 [H][K] | 盘口段买入冲击 [·], 全是标签库网格层本层列 (同行). 顺延已在标签里 (features LabelHold) ----
  // 标签组 i / 金额段 c 的列段起点 (固定段 = kFlat)
  ColList cols{io.dm.pool, {}};
  const size_t ts_i = cols.add(ft.y.ts), cs_i = cols.add(ft.y.cs), eo_i = cols.add(ft.y.entry);
  assert(cols.refs[ts_i].pool == io.dm.rs.main && "门控 / 标签 / 成本在标签库");
  std::vector<size_t> lab_at(H), cost_at(C, kFlat);
  for (size_t i = 0; i < H; ++i) {
    lab_at[i] = cols.refs.size();
    for (const Col &c : leg_of(ft.y.labels, Y.hd.k[i]).leg)
      cols.add(c);
  }
  for (size_t c = 0; c < C; ++c)
    if (const LegCol &lc = leg_of(ft.y.costs, Y.ad.k[c]); !lc.flat()) {
      assert(s.max_impact > 0.f && "装盘口成本要封顶 (Config::max_impact)");
      cost_at[c] = cols.refs.size();
      cols.add(lc.leg[factor::stat::kLong]);
    }

  Y.gate.assign(n, 0);
  Y.eo.assign(n, 0);
  Y.labels.resize(H);
  for (LegPlane &p : Y.labels) {
    for (std::vector<uint16_t> &v : p.v)
      v.assign(n, 0);
    p.m.assign(n, 0);
  }
  Y.costs.resize(C);
  for (size_t c = 0; c < C; ++c)
    if (cost_at[c] != kFlat)
      Y.costs[c].assign(n, 0);
  // 买入冲击按 max_impact 封顶 (fp16 位直接替换, 与原值同格式); 标签不截
  const feature_storage_t imp_top = static_cast<feature_storage_t>(s.max_impact);
  std::vector<std::vector<uint8_t>> has_data(static_cast<size_t>(io.ex.threads()), std::vector<uint8_t>(A)); // 每线程: 当日各资产是否有成交 (∧ 在子池)
  std::vector<long long> on(static_cast<size_t>(io.ex.threads()), 0);                                        // 每线程: 门控在池格数 (n_full)

  const bool read = read_days(io, k, std::move(cols.refs), [&](const auto &view, const Day &day, size_t d, size_t tid) {
    // 建仓可成交码: 池内 (gate) ∧ 有限 → 码 (1..3), 其余 0
    const auto fold_eo = [&](size_t t, size_t row) {
      const View w = view(eo_i, t);
      for (size_t a = 0; a < A; ++a) {
        feature_storage_t x = {};
        const bool ok = Y.gate[row + a] && cell(w, a, x) && fmeta::finite(x);
        const float c = ok ? static_cast<float>(x) : 0.f;
        assert((c == 0.f || c == 1.f || c == 2.f || c == 3.f) && "lb_entry_ok 须是 kEoBuy | kEoSell");
        Y.eo[row + a] = static_cast<uint8_t>(c);
      }
    };
    // 标签组 i 的侧 l = 列 lab_at[i] + l: 池内 (gate) ∧ 两侧皆有限 → fp16 位 + 有效位 (须有可成交码), 其余 0
    const auto fold_labels = [&](size_t t, size_t row) {
      for (size_t i = 0; i < H; ++i) {
        View w[K];
        for (size_t l = 0; l < K; ++l)
          w[l] = view(lab_at[i] + l, t);
        LegPlane &p = Y.labels[i];
        for (size_t a = 0; a < A; ++a) {
          feature_storage_t x[K] = {};
          bool ok = Y.gate[row + a];
          for (size_t l = 0; l < K; ++l)
            ok = ok && cell(w[l], a, x[l]) && fmeta::finite(x[l]);
          for (size_t l = 0; l < K; ++l)
            p.v[l][row + a] = ok ? std::bit_cast<uint16_t>(x[l]) : 0;
          assert((!ok || Y.eo[row + a]) && "标签有效格须有建仓可成交码 (旧标签库: 重算)");
          p.m[row + a] = ok;
        }
      }
    };
    // 金额段 c 的买入冲击 = 列 cost_at[c] (固定段跳过): 池内 ∧ 有限 → fp16 位 (封顶), 其余 0; 可买 (eo & kEoBuy) 的格须有限
    // (cost_rows 只在可买时取), 无有效位
    const auto fold_costs = [&](size_t t, size_t row) {
      for (size_t c = 0; c < C; ++c) {
        if (cost_at[c] == kFlat)
          continue;
        const View w = view(cost_at[c], t);
        std::vector<uint16_t> &p = Y.costs[c];
        for (size_t a = 0; a < A; ++a) {
          feature_storage_t x = {};
          const bool fin = Y.gate[row + a] && cell(w, a, x) && fmeta::finite(x);
          p[row + a] = fin ? std::bit_cast<uint16_t>(static_cast<float>(x) > s.max_impact ? imp_top : x) : 0;
          assert((fin || !(Y.eo[row + a] & factor::stat::kEoBuy)) && "可买格的买入冲击须有且有限 (旧 Lob 标签库: 重算)");
        }
      }
    };
    uint8_t *gate = Y.gate.data() + d * VR * A;
    gate_day(view, ts_i, cs_i, io.dm, day, VR, A, has_data[tid], gate);
    on[tid] += gate_on(gate, VR * A);
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      fold_eo(t, row);
      fold_labels(t, row);
      fold_costs(t, row);
    }
  });
  if (!read)
    return false;
  Y.n_full = s.cap > 0.0 ? s.cap : n_full_from(on, Y.sh.T);
  return true;
}

bool load_b(const Io &io, const Chunk &k, BPlanes &B) {
  TraceN("FactorsLoadB");
  constexpr size_t K = factor::stat::kLegs;
  const FeatureTable &ft = io.ft;
  assert(io.dm.pool == kAllPool && !io.dm.member && "池篮子在 all 池评估域 (全轴; 各池成员读池列)");
  B = BPlanes{};
  B.sh = shape_of(io, k);
  const size_t A = static_cast<size_t>(B.sh.A), n = B.sh.n(), VR = static_cast<size_t>(ft.seg), NP = ft.b.pools.size();
  assert(NP == pool_count());
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
  assert(cols.refs[ts_i].pool == io.dm.rs.main && "门控 / 标签 / K 线在标签库");
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
  const bool read = read_days(io, k, std::move(cols.refs), [&](const auto &view, const Day &day, size_t d, size_t tid) {
    gate_day(view, ts_i, cs_i, io.dm, day, VR, A, has[tid], gate[tid].data());
    const std::vector<uint8_t> &on = has[tid];
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      // 池成员位: 位 kAllPool = all 截面门控; 位 p = 池列 > 0.5 (NaN = 母集外 → 不在池) ∧ 当日有成交
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

StatScope stat_scope(const FactorScope &sc, const Domain &dm, const YPlanes &y, const char *backend) {
  StatScope o;
  o.pool = sc.pool;
  o.start_date = dm.dates.front();
  o.end_date = dm.dates.back();
  o.days = static_cast<int>(dm.days.size()), o.T = o.days * y.sh.seg, o.A = y.sh.A; // 评估域全期 (分块时 y 只是一块)
  o.max_impact = sc.max_impact;
  o.n_full = y.n_full;
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
  assert(cols.refs[ts_i].pool == io.dm.rs.main && "门控在标签库");
  std::vector<std::vector<uint8_t>> has(nt, std::vector<uint8_t>(A)), gate(nt, std::vector<uint8_t>(VR * A));
  std::vector<long long> on(nt, 0);
  if (!read_days(io, k, std::move(cols.refs), [&](const auto &view, const Day &day, size_t, size_t tid) {
        gate_day(view, ts_i, cs_i, io.dm, day, VR, A, has[tid], gate[tid].data());
        on[tid] += gate_on(gate[tid].data(), VR * A);
      }))
    return false;
  out = n_full_from(on, sh.T);
  return true;
}

factor::stat::Labels labels_of(const YPlanes &y) {
  assert(y.hd.n > 0 && y.labels.size() == static_cast<size_t>(y.hd.n) && "标签未装");
  factor::stat::Labels out;
  out.hd = y.hd;
  out.cap = y.n_full;
  assert(y.eo.size() == y.sh.n() && "建仓可成交码未装");
  for (int h = 0; h < y.hd.n; ++h)
    out.l[h] = label_of(y.labels[static_cast<size_t>(h)], y.eo.data(), nullptr);
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

std::string block_key(const FeatureTable &ft, const Domain &dm, const Chunk &k, const BlockSpec &s) {
  return std::string(factor_level(ft.lv).name) + "|" + dm.key() + "|" + std::to_string(k.d0) + "-" + std::to_string(k.d1) + "|" + s.y.key();
}

namespace {

// Y 侧: 键 (或日序列) 变 → reset + 装 Y. 键中 → 不动. false = 取消 / 判废 (块已 reset)
bool ensure_yb(Block &c, const Io &io, const Chunk &k, const BlockSpec &s) {
  assert(k.d0 < k.d1 && k.d1 <= io.dm.dates.size());
  const std::string key = block_key(io.ft, io.dm, k, s);
  const auto first = io.dm.dates.begin() + static_cast<long>(k.d0), last = io.dm.dates.begin() + static_cast<long>(k.d1);
  c.ft = &io.ft;
  if (c.key == key && std::equal(c.dates.begin(), c.dates.end(), first, last))
    return true;
  c.reset();
  c.ft = &io.ft;
  if (!load_y(io, k, s.y, c.y)) {
    c.reset();
    return false;
  }
  c.key = key;
  c.dates.assign(first, last);
  c.d0 = k.d0, c.d1 = k.d1;
  c.nd = io.dm.days.size();
  return true;
}

// X 侧 (先 ensure_yb): retain → 先丢 s.x 以外的叶; 缺的叶补装. false = 取消 / 判废 (已有的叶不动)
bool ensure_x(Block &c, const Io &io, const Chunk &k, const std::vector<LeafId> &ids, bool retain) {
  assert(!c.key.empty() && "先 ensure_yb");
  if (retain)
    for (LeafId id = 0; id < c.x.v.size(); ++id)
      if (std::find(ids.begin(), ids.end(), id) == ids.end())
        c.x.v[id] = {};
  std::vector<LeafId> miss;
  for (const LeafId id : ids)
    if (!c.x.find(id))
      miss.push_back(id);
  return miss.empty() || load_x(io, k, miss, c.x);
}

} // namespace

Rc ensure_block(Block &c, const Io &io, const Chunk &k, const BlockSpec &s, bool retain) {
  assert(k.d0 <= k.s0 && k.s0 < k.s1 && k.s1 <= k.d1);
  const Rc rc = rc_of(ensure_yb(c, io, k, s) && ensure_x(c, io, k, s.x, retain), io.lr);
  if (rc == Rc::Stale)
    c.reset();
  if (rc == Rc::Ok)
    c.t_lo = static_cast<int>(k.s0 - k.d0) * io.ft.seg, c.t_back = static_cast<int>(k.d1 - k.s1) * io.ft.seg;
  return rc;
}

size_t resident_leaves(const Block &c, const std::string &key, const std::vector<LeafId> &ids, bool retain) {
  if (retain || c.key != key)
    return ids.size();
  size_t n = c.x.count();
  for (const LeafId id : ids)
    n += !c.x.find(id);
  return n;
}

std::vector<const factor::check::Plane *> leaves(const Block &c, const std::vector<std::string> &feats) {
  assert(c.ft && "先 ensure_block");
  std::vector<const factor::check::Plane *> in;
  for (const LeafId id : c.ft->x.ids(feats)) {
    in.push_back(c.x.find(id));
    assert(in.back() && "叶未装 (ensure_block)");
  }
  return in;
}

} // namespace GUI::Factors
