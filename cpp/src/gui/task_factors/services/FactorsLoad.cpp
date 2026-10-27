// FactorsLoad — 见头文件. include 因子契约头 (Stat/Contract.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsLoad.hpp"

#include "features/Method/Fundamental.hpp"         // fund::SW2021_L1_COUNT (行业 ID 上界)
#include "features/MetaFlag.hpp"                   // fmeta::valid / keep / finite
#include "features/Node/Lob/TS/Label/LabelLob.hpp" // 只取 kContCloseMin 对账尾部界 (不含字段表)
#include "features/Storage/FeatureRead.hpp"
#include "features/TimeIndex.hpp"
#include "misc/profiler.hpp"
#include "shared/Analysis.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
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

factor::stat::Holds holds_of(const std::vector<int> &keys) {
  assert(keys.size() <= static_cast<size_t>(factor::stat::kMaxHold) && "持有期数超 Stat 标签组容量 (kMaxHold)");
  factor::stat::Holds hd;
  hd.n = static_cast<int>(keys.size());
  std::copy(keys.begin(), keys.end(), hd.h);
  return hd;
}

Amts amts_of(const std::vector<int> &keys) {
  assert(keys.size() <= static_cast<size_t>(factor::stat::kMaxCost) && "金额段数超 Stat 成本容量 (kMaxCost)");
  Amts ad;
  ad.n = static_cast<int>(keys.size());
  std::copy(keys.begin(), keys.end(), ad.a);
  return ad;
}

namespace {
std::vector<int> keys_of(const std::vector<LegCol> &cols) {
  std::vector<int> keys;
  for (const LegCol &c : cols)
    keys.push_back(c.key);
  return keys;
}
} // namespace

factor::stat::Holds all_holds(const FeatureTable &ft) { return holds_of(keys_of(ft.y.labels)); }
Amts all_amts(const FeatureTable &ft) { return amts_of(keys_of(ft.y.costs)); }

YSpec y_full(const FeatureTable &ft, double max_impact) { return {all_holds(ft), all_amts(ft), static_cast<float>(max_impact)}; }

std::string YSpec::key() const {
  std::string k = "h";
  for (int i = 0; i < hd.n; ++i)
    k += "," + std::to_string(hd.h[i]);
  k += "|a";
  for (int i = 0; i < ad.n; ++i)
    k += "," + std::to_string(ad.a[i]);
  return k + "|cap" + std::to_string(max_impact) + "|n" + std::to_string(cap);
}

double cell_bytes(const XSpec &s) { return kPlaneBytes * static_cast<double>(s.codes.size()); }
double cell_bytes(const YSpec &s) { return 1.0 + kPlaneBytes * static_cast<double>(s.hd.n) + 4.0 * s.ad.n; }

const factor::check::Plane *XPlanes::find(std::string_view code) const {
  for (size_t i = 0; i < codes.size(); ++i)
    if (codes[i] == code)
      return &v[i];
  return nullptr;
}

namespace {

// 一天门控 g[n] 的在池格数 (load_y / n_full_of 同一份: 逐天逐线程攒进 on[tid])
long long gate_on(const uint8_t *g, size_t n) {
  long long k = 0;
  for (size_t i = 0; i < n; ++i)
    k += g[i] != 0;
  return k;
}
// 满仓一腿只数 (Stat 契约【持仓口径】的 cap) = 截面门控在池格数 Σon / 行数 T / kGroups (load_y / n_full_of 同一份)
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
// 格 a: 有源 → x = 值, g = 同行门控 (不门控 = 1)
bool cell(const View &w, size_t a, feature_storage_t &x, float &g) {
  const uint32_t i = w.ids ? w.ids[a] : static_cast<uint32_t>(a);
  if (!w.v || i == kAbsent)
    return false;
  x = w.v[i];
  g = w.g ? static_cast<float>(w.g[i]) : 1.f;
  return true;
}

// 张量形 (容量断言在此): days × A → T = days · seg
Shape shape_of(const FeatureTable &ft, const EvalScope &es, size_t d0, size_t d1) {
  assert(d0 < d1 && d1 <= es.days.size());
  assert(level_valid_rows(factor_level(ft.seg).lvl) == static_cast<size_t>(ft.seg));
  Shape s;
  s.days = static_cast<int>(d1 - d0);
  s.A = static_cast<int>(es.rs.axis.size());
  s.seg = ft.seg;
  s.T = s.days * s.seg;
  assert(s.A >= 2 && s.A <= factor::stat::kMaxA && "资产轴超 Stat 容量 (kMaxA)");
  assert(static_cast<long long>(s.T) * s.A < (1LL << 31));
  return s;
}

// 逐天并行读骨架 (load_x / load_y 同一份): cols → 一张读计划 (同 (池, 层) 一源, 网格行 level_asof 投影); 每天各源读进本线程暂存后
// on_day(view, day, d, tid) 散进平面 (view(c, t) = 平面列 c 在网格行 t 的源行视图). prog.begin Loading (日数), 每天 publish(1). false = 取消 / 库判废
template <class OnDay>
bool read_days(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, std::vector<ColRef> cols,
               misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog, OnDay &&on_day) {
  const size_t n_cols = cols.size();
  assert(n_cols > 0);
  prog.begin(Phase::Loading, d1 - d0);
  const ReadPlan plan = analysis::read_plan(*ft.feature, factor_level(ft.seg).lvl, analysis::Rows::AsOf, /*carry=*/true, std::move(cols));
  std::vector<std::pair<uint32_t, uint32_t>> at(n_cols); // 平面列 → (源, 源内值列)
  for (size_t si = 0; si < plan.srcs.size(); ++si)
    for (size_t k = 0; k < plan.srcs[si].slot.size(); ++k)
      at[plan.srcs[si].slot[k]] = {static_cast<uint32_t>(si), static_cast<uint32_t>(k)};
  const Readers rd = es.rs.readers(lr.ptr);
  std::vector<IoSlot> slots(static_cast<size_t>(ex.threads()));
  for (IoSlot &s : slots)
    s.prepare(plan, es.rs.sizes(), n_cols);

  ex.tasks(static_cast<int>(d1 - d0), [&](int di, int ti) {
    if (cancel.load(std::memory_order_relaxed))
      return;
    const size_t d = static_cast<size_t>(di), tid = static_cast<size_t>(ti);
    const Day &day = es.days[d0 + d];
    IoSlot &io = slots[tid];
    for (size_t si = 0; si < plan.srcs.size(); ++si)
      load_source(rd, plan.srcs[si], day, plan.srcs[si].cs.io, io.src[si]);
    if (lr.stale())
      return;
    const auto view = [&](size_t c, size_t t) {
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
    on_day(view, day, d, tid);
    prog.publish(1);
  });
  return !cancel.load(std::memory_order_relaxed) && !lr.stale();
}

// 一天的截面门控 (load_y / n_full_of 同一份): gate[t·A + a], t < VR = cs_valid ∧ 当日有数据 (∧ 子池成员); has = 本线程 [A] 暂存.
// 当日有数据 = 该资产当日至少一行有成交 (ts_valid ≠ 0). 缺 binary / 整日停牌 → 全日 0: 这种 asset-day 不进截面池门控
// (值是 carry / 0, 标签 0), valid% / Stat / CS 算子的截面域因此只看"真有行情"的格, 不与数据完整性耦合.
// 子池 (Bar 标签层): 再与当日成员相与, 池外格同样不进截面
template <class ViewFn>
void gate_day(const ViewFn &view, size_t ts_i, size_t cs_i, const EvalScope &es, const Day &day, size_t VR, size_t A, std::vector<uint8_t> &has,
              uint8_t *gate) {
  feature_storage_t x;
  float g;
  std::fill(has.begin(), has.end(), static_cast<uint8_t>(0));
  for (size_t t = 0; t < VR; ++t) {
    const View wt = view(ts_i, t);
    for (size_t a = 0; a < A; ++a)
      has[a] |= cell(wt, a, x, g) && fmeta::data_valid(static_cast<float>(x));
  }
  if (es.member) {
    const uint8_t *mem = es.member->row(day.date);
    for (size_t a = 0; a < A; ++a)
      has[a] &= mem[a];
  }
  for (size_t t = 0; t < VR; ++t) {
    const View wc = view(cs_i, t);
    for (size_t a = 0; a < A; ++a)
      gate[t * A + a] = cell(wc, a, x, g) && fmeta::data_valid(static_cast<float>(x)) && has[a];
  }
}

} // namespace

bool load_x(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, const XSpec &spec, XPlanes &X,
            misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog) {
  TraceN("FactorsLoadX");
  X = XPlanes{};
  X.sh = shape_of(ft, es, d0, d1);
  X.codes = spec.codes;
  const size_t A = static_cast<size_t>(X.sh.A), n = X.sh.n(), VR = static_cast<size_t>(ft.seg), nf = spec.codes.size();
  // ---- 列段 叶 [nf], 各按所在 (池, 层) 投影到网格行 ----
  std::vector<ColRef> cols;
  for (const std::string &f : spec.codes) {
    const FeatCol *c = ft.x.find(f);
    assert(c && c->allowed);
    cols.push_back(c->ref(es.pool));
  }
  X.v.resize(nf);
  for (factor::check::Plane &p : X.v)
    p.resize(n);
  return read_days(ft, es, lr, d0, d1, std::move(cols), ex, cancel, prog, [&](const auto &view, const Day &, size_t d, size_t) {
    feature_storage_t x;
    float g;
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      for (size_t i = 0; i < nf; ++i) {
        const View w = view(i, t);
        float *v = X.v[i].v.data() + row;
        uint8_t *m = X.v[i].m.data() + row;
        for (size_t a = 0; a < A; ++a) {
          const bool ok = cell(w, a, x, g) && fmeta::keep(g, w.vt, x);
          v[a] = ok ? static_cast<float>(x) : 0.f;
          m[a] = ok;
        }
      }
    }
  });
}

bool load_y(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, const YSpec &spec, YPlanes &Y,
            misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog) {
  TraceN("FactorsLoadY");
  constexpr size_t K = factor::stat::kLegs;
  assert((spec.ad.n == 0 || spec.max_impact > 0.f) && "装成本要封顶 (Config::max_impact)");
  Y = YPlanes{};
  Y.sh = shape_of(ft, es, d0, d1);
  Y.hd = spec.hd;
  Y.ad = spec.ad;
  factor::stat::assert_holds(Y.hd, Y.sh.seg);
  const size_t A = static_cast<size_t>(Y.sh.A), n = Y.sh.n(), VR = static_cast<size_t>(ft.seg);
  const size_t H = static_cast<size_t>(Y.hd.n), C = static_cast<size_t>(Y.ad.n);
  // ---- 列段 ts_valid | cs_valid | 标签 [H][K] | 成本 [C][K], 全是标签库网格层本层列 (同行). 可成交 / 顺延已在标签里 (features LabelHold), 不另读 ----
  std::vector<ColRef> cols;
  const auto add = [&](const Col &c) {
    cols.push_back(c.ref(es.pool));
    return cols.size() - 1;
  };
  const size_t ts_i = add(ft.y.ts), cs_i = add(ft.y.cs);
  assert(cols[ts_i].pool == es.rs.main && "门控 / 标签 / 成本在标签库");
  const size_t lab_i = cols.size();
  for (size_t i = 0; i < H; ++i)
    for (const Col &c : leg_of(ft.y.labels, Y.hd.h[i]).leg)
      add(c);
  const size_t cost_i = cols.size();
  for (size_t c = 0; c < C; ++c)
    for (const Col &l : leg_of(ft.y.costs, Y.ad.a[c]).leg)
      add(l);

  Y.gate.assign(n, 0);
  const auto alloc = [&](std::vector<LegPlane> &ps, size_t k, bool m) {
    ps.resize(k);
    for (LegPlane &p : ps) {
      for (std::vector<uint16_t> &v : p.v)
        v.assign(n, 0);
      if (m)
        p.m.assign(n, 0);
    }
  };
  alloc(Y.labels, H, true);
  alloc(Y.costs, C, false);
  // 两腿封顶 (fp16 位直接替换, 与原值同格式): 标签不截; 成本买入按 max_impact 截, 卖出原值
  constexpr float kNoCap = std::numeric_limits<float>::infinity();
  const float lab_cap[K] = {kNoCap, kNoCap}, cost_cap[K] = {spec.max_impact, kNoCap};
  std::vector<std::vector<uint8_t>> has_data(static_cast<size_t>(ex.threads()), std::vector<uint8_t>(A)); // 每线程: 当日各资产是否有成交 (∧ 在子池)
  std::vector<long long> on(static_cast<size_t>(ex.threads()), 0);                                        // 每线程: 门控在池格数 (n_full)

  const bool read = read_days(ft, es, lr, d0, d1, std::move(cols), ex, cancel, prog, [&](const auto &view, const Day &day, size_t d, size_t tid) {
    // 两腿列组 (标签 / 成本同一份折叠): 列段起点 base, 组 k 的腿 l = 列 base + K·k + l; 池内 (gate) ∧ 两腿皆有限 → fp16 位, 其余 0.
    // 标签: 有效位 = 同条件; 成本 (cost): 标签有效格须恒有限 (建仓非两向 → 特征层 NaN, 标签同 NaN), 无有效位 (cost_rows 吃标签有效位)
    const auto fold_legs = [&](size_t base, const float (&cap)[K], std::vector<LegPlane> &out, size_t t, size_t row, bool cost) {
      for (size_t k = 0; k < out.size(); ++k) {
        View w[K];
        feature_storage_t top[K];
        for (size_t l = 0; l < K; ++l) {
          w[l] = view(base + K * k + l, t);
          top[l] = static_cast<feature_storage_t>(cap[l]);
        }
        LegPlane &p = out[k];
        for (size_t a = 0; a < A; ++a) {
          feature_storage_t x[K] = {};
          float g;
          bool ok = Y.gate[row + a];
          for (size_t l = 0; l < K; ++l)
            ok = ok && cell(w[l], a, x[l], g) && fmeta::finite(x[l]);
          for (size_t l = 0; l < K; ++l)
            p.v[l][row + a] = ok ? std::bit_cast<uint16_t>(static_cast<float>(x[l]) > cap[l] ? top[l] : x[l]) : 0;
          if (cost)
            assert((ok || std::none_of(Y.labels.begin(), Y.labels.end(), [&](const LegPlane &q) { return q.m[row + a]; })) &&
                   "成本在标签有效格须有且有限 (旧 Lob 标签库: 重算)");
          else
            p.m[row + a] = ok;
        }
      }
    };
    uint8_t *gate = Y.gate.data() + d * VR * A;
    gate_day(view, ts_i, cs_i, es, day, VR, A, has_data[tid], gate);
    on[tid] += gate_on(gate, VR * A);
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      fold_legs(lab_i, lab_cap, Y.labels, t, row, false);
      fold_legs(cost_i, cost_cap, Y.costs, t, row, true);
    }
  });
  if (!read)
    return false;
  Y.n_full = spec.cap > 0.0 ? spec.cap : n_full_from(on, Y.sh.T);
  return true;
}

bool load_b(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, BPlanes &B, misc::exec::ForkJoin &ex,
            const std::atomic<bool> &cancel, Progress &prog) {
  TraceN("FactorsLoadB");
  assert(factor_level(ft.seg).lvl == L_1day && "篮子只在日层 (成员位 = 本行收盘定的池)");
  B = BPlanes{};
  B.sh = shape_of(ft, es, d0, d1);
  const size_t A = static_cast<size_t>(B.sh.A), n = B.sh.n(), VR = static_cast<size_t>(ft.seg);
  // ---- 列段 ts_valid | close adj_factor total_shares val[kPoolVals] ind_l1, 全是标签库网格层本层列 ----
  std::vector<ColRef> cols;
  const auto add = [&](const Col &col) {
    cols.push_back(col.ref(es.pool));
    return cols.size() - 1;
  };
  const size_t ts_i = add(ft.y.ts), px_i = add(ft.y.ohlc[3]), adj_i = add(ft.y.adj_factor), sh_i = add(ft.y.total_shares);
  const size_t val_i = cols.size();
  for (const Col &v : ft.y.val)
    add(v);
  const size_t ind_i = add(ft.y.ind_l1);
  assert(cols[ts_i].pool == es.rs.main && "篮子列在标签库");
  std::vector<float> vadj(n, 0.f); // 本行 adj_factor (= 次日因子, 缺 / 非正 → 0): 装完把 close 乘成后复权, 即弃
  B.close.resize(n);
  B.mcap.assign(n, 0.f);
  B.px.assign(n, 0.f);
  B.shares.resize(n);
  for (std::vector<float> &v : B.val)
    v.resize(n);
  B.ind.assign(n, 0);
  const bool read = read_days(ft, es, lr, d0, d1, std::move(cols), ex, cancel, prog, [&](const auto &view, const Day &, size_t d, size_t) {
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      // close: 本行有成交 (ts_valid 按 DATA 口径) ∧ 有限 ∧ > 0 (此处不复权, 装完乘复权因子)
      const View wt = view(ts_i, t), wp = view(px_i, t), wa = view(adj_i, t);
      for (size_t a = 0; a < A; ++a) {
        feature_storage_t s, px, f;
        float g;
        const bool ok = cell(wp, a, px, g) && cell(wt, a, s, g) && fmeta::keep(static_cast<float>(s), L2::ValidType::DATA, px) && static_cast<float>(px) > 0.f;
        B.close.v[row + a] = ok ? static_cast<float>(px) : 0.f, B.close.m[row + a] = ok;
        B.px[row + a] = B.close.v[row + a];
        vadj[row + a] = cell(wa, a, f, g) && fmeta::finite(f) && static_cast<float>(f) > 0.f ? static_cast<float>(f) : 0.f;
      }
      const View ws = view(sh_i, t), wi = view(ind_i, t);
      View wv[kPoolVals];
      for (int m = 0; m < kPoolVals; ++m)
        wv[m] = view(val_i + static_cast<size_t>(m), t);
      constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
      for (size_t a = 0; a < A; ++a) {
        feature_storage_t x;
        float g;
        const float px = B.px[row + a];
        B.shares[row + a] = cell(ws, a, x, g) ? static_cast<float>(x) : kNaN;
        if (px > 0.f && B.shares[row + a] > 0.f)
          B.mcap[row + a] = px * B.shares[row + a];
        for (int m = 0; m < kPoolVals; ++m)
          B.val[m][row + a] = cell(wv[m], a, x, g) ? static_cast<float>(x) : kNaN;
        if (cell(wi, a, x, g) && static_cast<float>(x) >= 1.f && static_cast<float>(x) < static_cast<float>(fund::SW2021_L1_COUNT))
          B.ind[row + a] = static_cast<uint8_t>(static_cast<float>(x));
      }
    }
  });
  if (!read)
    return false;
  // 后复权 close(D) = close_day(D) × D 日复权因子 (= 行 D−1 的 adj_factor); 首行无前一行 → 不作漂移锚. 只比值进收益, 首日归一无关
  ex.rows(B.sh.T, [&](int t0, int t1, int) {
    for (size_t i = static_cast<size_t>(t0) * A; i < static_cast<size_t>(t1) * A; ++i) {
      const float f = i >= A ? vadj[i - A] : 0.f;
      B.close.m[i] = B.close.m[i] && f > 0.f;
      B.close.v[i] = B.close.m[i] ? B.close.v[i] * f : 0.f;
    }
  });
  return true;
}

StatScope stat_scope(const FactorScope &sc, const EvalScope &es, const YPlanes &y, const char *backend) {
  StatScope o;
  o.pool = sc.pool;
  o.start_date = es.dates.front();
  o.end_date = es.dates.back();
  o.days = y.sh.days, o.T = y.sh.T, o.A = y.sh.A;
  o.max_impact = sc.max_impact;
  o.n_full = y.n_full;
  o.backend = backend;
  o.time = now_string();
  return o;
}

bool n_full_of(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, misc::exec::ForkJoin &ex,
               const std::atomic<bool> &cancel, Progress &prog, double &out) {
  TraceN("FactorsNFull");
  const Shape sh = shape_of(ft, es, d0, d1);
  const size_t A = static_cast<size_t>(sh.A), VR = static_cast<size_t>(ft.seg), nt = static_cast<size_t>(ex.threads());
  std::vector<ColRef> cols{ft.y.ts.ref(es.pool), ft.y.cs.ref(es.pool)};
  assert(cols[0].pool == es.rs.main && "门控在标签库");
  std::vector<std::vector<uint8_t>> has(nt, std::vector<uint8_t>(A)), gate(nt, std::vector<uint8_t>(VR * A));
  std::vector<long long> on(nt, 0);
  if (!read_days(ft, es, lr, d0, d1, std::move(cols), ex, cancel, prog, [&](const auto &view, const Day &day, size_t, size_t tid) {
        gate_day(view, 0, 1, es, day, VR, A, has[tid], gate[tid].data());
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
  for (int h = 0; h < y.hd.n; ++h)
    out.l[h] = label_of(y.labels[static_cast<size_t>(h)], nullptr);
  return out;
}

factor::stat::Labels labels_pick(const factor::stat::Labels &L, const factor::stat::Holds &hd) {
  if (hd.n == 0)
    return L;
  factor::stat::Labels out;
  out.hd = hd;
  out.cap = L.cap;
  for (int i = 0; i < hd.n; ++i) {
    const int j = factor::stat::hold_index(L.hd, hd.h[i]);
    assert(j >= 0 && "所取持有期不在标签组里");
    out.l[i] = L.l[j];
  }
  return out;
}

std::string scope_key(const FeatureTable &ft, const EvalScope &es, size_t d0, size_t d1, const YSpec &spec) {
  return std::to_string(ft.seg) + "|" + es.key() + "|" + std::to_string(d0) + "-" + std::to_string(d1) + "|" + spec.key();
}

namespace {

// 键 (或日序列) 变 → reset + 装 Y. 键中 → 不动. false = 取消 / 判废 (块已 reset)
bool ensure_y(ScopeCache &c, const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, const YSpec &spec,
              misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog) {
  assert(d0 < d1 && d1 <= es.dates.size());
  const std::string key = scope_key(ft, es, d0, d1, spec);
  const auto first = es.dates.begin() + static_cast<long>(d0), last = es.dates.begin() + static_cast<long>(d1);
  if (c.key == key && std::equal(c.dates.begin(), c.dates.end(), first, last))
    return true;
  c.reset();
  if (!load_y(ft, es, lr, d0, d1, spec, c.y, ex, cancel, prog)) {
    c.reset();
    return false;
  }
  c.key = key;
  c.dates.assign(first, last);
  c.d0 = d0, c.d1 = d1;
  c.nd = es.days.size();
  return true;
}

// 先 ensure_y. retain → 先丢 codes 以外的叶; 缺的叶补装. false = 取消 / 判废 (已有的叶不动)
bool ensure_x(ScopeCache &c, const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, const std::vector<std::string> &codes,
              bool retain, misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog) {
  assert(!c.key.empty() && "先 ensure_y");
  if (retain)
    for (size_t i = c.x.codes.size(); i-- > 0;)
      if (std::find(codes.begin(), codes.end(), c.x.codes[i]) == codes.end()) {
        c.x.codes.erase(c.x.codes.begin() + static_cast<long>(i));
        c.x.v.erase(c.x.v.begin() + static_cast<long>(i));
      }
  XSpec miss;
  for (const std::string &code : codes)
    if (!c.x.find(code))
      miss.codes.push_back(code);
  if (miss.codes.empty())
    return true;
  XPlanes add;
  if (!load_x(ft, es, lr, c.d0, c.d1, miss, add, ex, cancel, prog))
    return false;
  assert(add.sh == c.y.sh);
  for (size_t i = 0; i < add.codes.size(); ++i) {
    c.x.codes.push_back(std::move(add.codes[i]));
    c.x.v.push_back(std::move(add.v[i]));
  }
  return true;
}

} // namespace

Rc ensure_xy(ScopeCache &c, const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, const Chunk &k, const YSpec &ys,
             const std::vector<std::string> &codes, bool retain, misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog) {
  assert(k.d0 <= k.s0 && k.s0 < k.d1);
  const Rc rc = rc_of(ensure_y(c, ft, es, lr, k.d0, k.d1, ys, ex, cancel, prog) && ensure_x(c, ft, es, lr, codes, retain, ex, cancel, prog), lr);
  if (rc == Rc::Stale)
    c.reset();
  if (rc == Rc::Ok)
    c.t_lo = static_cast<int>(k.s0 - k.d0) * ft.seg;
  return rc;
}

std::vector<const factor::check::Plane *> leaves(const ScopeCache &c, const std::vector<std::string> &codes) {
  std::vector<const factor::check::Plane *> in(codes.size());
  for (size_t i = 0; i < codes.size(); ++i) {
    in[i] = c.x.find(codes[i]);
    assert(in[i] && "叶未装 (ensure_xy)");
  }
  return in;
}

} // namespace GUI::Factors
