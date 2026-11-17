// Load — 见头文件. include 因子 Stat CPU 头 (标签秩 prep_label), 依赖受控浮点: CMake MATH_PRECISE 源.
#include "gui/engine/Load.hpp"

#include "factor/GpuRun.hpp"                        // available / mem_info
#include "factor/Stat/Cpu.hpp"                      // prep_label / Workspace
#include "features/MetaFlag.hpp"                    // fmeta::valid / keep / finite
#include "features/Node/Common/Label/LabelHold.hpp" // 只取 kContCloseMin 对账尾部界
#include "features/Panel/Fund.hpp"                  // fund::SW2021_L1_COUNT
#include "features/Panel/PoolSelect.hpp"            // pool::picks
#include "features/Storage/FeatureRead.hpp"
#include "features/TimeIndex.hpp"
#include "misc/cross_platform.hpp" // memory_usage
#include "misc/date.hpp"           // months_between
#include "misc/profiler.hpp"
#include "shared/Concept.hpp" // concepts
#include "shared/ReadRequest.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

static_assert(factor::kSegMax == static_cast<int>(TRADE_MINUTES_PER_DAY), "factor::kSegMax 必须等于 L_1min 每日分钟数");
static_assert([] {
  for (size_t i = 0; i < GUI::Engine::kFactorLevelCount; ++i) {
    const GUI::Engine::FactorLevel &l = GUI::Engine::kFactorLevels[i];
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
static_assert(static_cast<size_t>(factor::stat::kLastAnchorRow) + 2 == LabelHold::kContCloseMin, "Stat 尾部界与 LabelHold 的收盘竞价起点不一致");

namespace GUI::Engine {

// ============================================================================
// 评估域 Domain 与分块计划 Plan
// ============================================================================

void open_libs(const FactorScope &sc, LibReaders &lr) {
  PerLib<FeatureLib> ready;
  for (const LibId l : lib_ids())
    if (sc.libs[l].ready())
      ready[l] = sc.libs[l];
  lr.open(ready, nullptr);
}

std::string Domain::key() const {
  std::string k = std::to_string(pool);
  for (const LibId l : lib_ids())
    if (used & lib_bit(l))
      k += "|" + rs.libs[l].dir + "#" + std::to_string(rs.libs[l].axis.hash);
  return k + "|" + std::to_string(dates.front()) + "-" + std::to_string(dates.back()) + "#" + std::to_string(dates.size());
}

LibMask used_libs(const FeatureTable &ft, PoolId pool, std::span<const LeafId> leaves) {
  const LibId lab = label_lib(pool);
  assert(ft.y.ts.ref(pool).lib == lab && ft.y.cs.ref(pool).lib == lab && "门控 / 标签 / K 线在标签库");
  LibMask m = lib_bit(lab);
  for (const LeafId id : leaves) {
    const FeatCol &c = ft.x.leaves[id];
    assert(c.allowed || c.target);
    m |= lib_bit(c.ref(pool).lib);
  }
  return m;
}

bool span_of(const FeatureTable &ft, const FactorScope &sc, std::span<const LeafId> leaves, size_t keep, Domain &dm, std::string &why) {
  assert(sc.lv == ft.lv && "作用域层与字段表层不符");
  assert(keep >= 1);
  const PoolId pool = pool_id(sc.pool);
  const LibId lab = label_lib(pool);
  const PerLib<FeatureLib> &libs = sc.libs;
  dm = Domain{};
  dm.pool = pool;
  dm.used = used_libs(ft, pool, leaves);
  for (const LibId l : lib_ids())
    if (dm.used & lib_bit(l)) {
      assert(!libs[l].dir.empty() && "请求没带该库清单 (factor_libs)");
      if (!libs[l].ready()) {
        why = lib_name(l) + " 特征库未建 (先算特征)";
        return false;
      }
      dm.rs.libs[l] = libs[l];
    }
  const FeatureLib &L0 = libs[lab];
  LibMask m_prev = 0;
  for (size_t i = 0; i < L0.dates.size(); ++i) {
    const misc::Ymd d = L0.dates[i];
    LibMask m = 0;
    for (const LibId l : lib_ids())
      if ((dm.used & lib_bit(l)) && reader::landed(libs[l], d))
        m |= lib_bit(l);
    if (m == dm.used) {
      dm.dates.push_back(d);
      dm.days.push_back(Day{d, i > 0 ? L0.dates[i - 1] : 0, m, i > 0 ? m_prev : 0});
    }
    m_prev = m;
  }
  if (dm.dates.empty()) {
    why = "所用特征库无共同落盘日";
    for (const LibId l : lib_ids())
      if (dm.used & lib_bit(l))
        why += " " + lib_name(l);
    return false;
  }
  const size_t n = dm.dates.size();
  size_t s = n, w = 0;
  if (sc.years > 0) {
    const misc::Ymd edge = dm.dates.back() - static_cast<misc::Ymd>(sc.years) * 10000;
    s = std::min(keep, static_cast<size_t>(dm.dates.end() - std::upper_bound(dm.dates.begin(), dm.dates.end(), edge)));
    w = static_cast<size_t>(std::ceil(static_cast<double>(s) * kWarmFrac));
    if (s + w > n) {
      w = std::min(n - 1, static_cast<size_t>(std::ceil(static_cast<double>(n) * kWarmFrac / (1.0 + kWarmFrac))));
      s = n - w;
    }
  } else {
    s = std::min(keep, n);
  }
  assert(s >= 1 && s + w <= n);
  const auto cut = static_cast<std::ptrdiff_t>(n - s - w);
  dm.dates.erase(dm.dates.begin(), dm.dates.begin() + cut);
  dm.days.erase(dm.days.begin(), dm.days.begin() + cut);
  dm.warm = w;
  return true;
}

bool domain_of(const FeatureTable &ft, const FactorScope &sc, std::span<const LeafId> leaves, size_t keep, Domain &dm, std::string &why) {
  TraceN("Domain");
  if (!span_of(ft, sc, leaves, keep, dm, why))
    return false;
  dm.rs.main = label_lib(dm.pool);
  dm.rs.days = dm.used;
  dm.rs.months = misc::months_between(dm.dates.front(), dm.dates.back());
  reader::map_axis(dm.rs, {});
  return true;
}

StatScope stat_scope(const FactorScope &sc, const Domain &dm, int seg, double n_full, const char *backend) {
  StatScope o;
  o.pool = sc.pool;
  o.start_date = dm.first();
  o.end_date = dm.dates.back();
  o.years = sc.years;
  o.days = static_cast<int>(dm.stat_days()), o.T = o.days * seg, o.A = static_cast<int>(dm.rs.axis.size());
  o.max_impact = sc.max_impact;
  o.n_full = n_full;
  o.backend = backend;
  o.time = now_string();
  return o;
}

double host_budget() {
  const MemoryUsage mu = memory_usage();
  return kHostFrac * static_cast<double>(mu.total_bytes) - static_cast<double>(mu.used_bytes);
}

double vram_budget() {
  size_t vfree = 0, vtotal = 0;
  factor::gpu::mem_info(&vfree, &vtotal);
  return kVramFrac * static_cast<double>(vfree);
}

int Plan::max_days() const {
  int m = 0;
  for (const Chunk &k : chunks)
    m = std::max(m, k.days());
  return m;
}

Plan plan_chunks(const Domain &dm, int seg, const Budget &b, Bands bd, double host, double vram) {
  const int days = static_cast<int>(dm.days.size()), A = static_cast<int>(dm.rs.axis.size()), warm = static_cast<int>(dm.warm);
  assert(days > warm && A > 0 && bd.back >= 0);
  const double per_day = static_cast<double>(seg) * A;
  Plan P;
  P.per_day = per_day;
  P.host = host - b.fixed;
  P.vram = b.dev > 0.0 ? vram : 0.0;
  if (bd.front < 0) {
    P.chunks = {Chunk{0, static_cast<size_t>(warm), static_cast<size_t>(days), static_cast<size_t>(days)}};
    P.D = days - warm;
    P.cache = days * per_day * b.load;
    return P;
  }
  const int band = std::max(bd.front, warm) + bd.back, stat = days - warm, least = std::min(days, 1 + band);
  const double day_host = b.host(per_day) - b.fixed, day_dev = b.vram(per_day);
  assert(P.host >= least * day_host && "内存预算装不下最小的块 (1 统计日 + 前后带)");
  assert((b.dev <= 0.0 || P.vram >= least * day_dev) && "显存预算装不下最小的块 (1 统计日 + 前后带)");
  int D = std::clamp(static_cast<int>(P.host / day_host) - band, 1, stat);
  if (b.dev > 0.0)
    D = std::min(D, std::clamp(static_cast<int>(P.vram / day_dev) - band, 1, stat));
  P.D = D;
  for (int s0 = warm; s0 < days; s0 += D) {
    const int s1 = std::min(days, s0 + D);
    const Chunk k{static_cast<size_t>(s0 == warm ? 0 : std::max(0, s0 - bd.front)), static_cast<size_t>(s0), static_cast<size_t>(s1),
                  static_cast<size_t>(std::min(days, s1 + bd.back))};
    P.chunks.push_back(k);
    P.cache += k.days() * per_day * b.load;
  }
  return P;
}

// ============================================================================
// 三侧规格与逐天并行装载: load_x ↔ load_y ↔ load_b
// ============================================================================

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

namespace {

using U16 = std::vector<uint16_t>;
template <class T>
inline constexpr double kCellBytes = 0.0;
template <>
inline constexpr double kCellBytes<YCore> = 2.0;
template <>
inline constexpr double kCellBytes<LegPlane> = kPlaneBytes;
template <>
inline constexpr double kCellBytes<U16> = 2.0;
template <>
inline constexpr double kCellBytes<Leaf> = kPlaneBytes;

long long gate_on(const uint8_t *g, size_t n) {
  long long k = 0;
  for (size_t i = 0; i < n; ++i)
    k += g[i] != 0;
  return k;
}
double n_full_from(long long on, int T) {
  const double n = static_cast<double>(on) / static_cast<double>(T) / factor::stat::kGroups;
  assert(n > 0.0 && "池内无有效资产");
  return n;
}

struct View {
  const feature_storage_t *v = nullptr, *g = nullptr;
  const uint32_t *ids = nullptr;
  L2::ValidType vt = L2::ValidType::ALL;
};
uint32_t src_of(const View &w, size_t a) { return !w.v ? kAbsent : w.ids ? w.ids[a]
                                                                         : static_cast<uint32_t>(a); }
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
uint8_t eo_cell(const View &w, size_t a, bool in) {
  feature_storage_t x = {};
  const float c = in && cell(w, a, x) && fmeta::finite(x) ? static_cast<float>(x) : 0.f;
  assert((c == 0.f || c == 1.f || c == 2.f || c == 3.f) && "lb_entry_ok 须是 kEoBuy | kEoSell");
  return static_cast<uint8_t>(c);
}
void leg_cell(const View (&w)[factor::stat::kLegs], size_t a, bool in, uint8_t eo, LegPlane &p, size_t i) {
  feature_storage_t x[factor::stat::kLegs] = {};
  bool ok = in;
  for (int l = 0; l < factor::stat::kLegs; ++l)
    ok = ok && cell(w[l], a, x[l]) && fmeta::finite(x[l]);
  for (int l = 0; l < factor::stat::kLegs; ++l)
    p.v[l][i] = ok ? std::bit_cast<uint16_t>(x[l]) : 0;
  assert((!ok || eo) && "标签有效格须有建仓可成交码");
  p.m[i] = ok;
}

struct ColList {
  PoolId pool;
  std::vector<ColRef> refs;
  size_t add(const Col &c) {
    refs.push_back(c.ref(pool));
    return refs.size() - 1;
  }
};

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

template <class OnDay>
bool read_days(const Io &io, const Chunk &k, std::vector<ColRef> cols, OnDay &&on_day) {
  const size_t n_cols = cols.size();
  assert(n_cols > 0);
  io.prog.begin(Phase::Loading, k.d1 - k.d0);
  const ReadPlan plan = reader::read_plan(*io.ft.feature, factor_level(io.ft.lv).lvl, reader::Rows::AsOf, /*carry=*/true, std::move(cols));
  std::vector<std::pair<uint32_t, uint32_t>> at(n_cols);
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

double cell_bytes(const FeatureTable &ft, const BlockSpec &s, size_t nx) {
  int books = 0;
  for (int c = 0; c < s.y.ad.n; ++c)
    books += !leg_of(ft.y.costs, s.y.ad.k[c]).flat();
  return kCellBytes<Leaf> * static_cast<double>(nx) + kCellBytes<YCore> + kCellBytes<LegPlane> * s.y.hd.n + kCellBytes<U16> * books;
}

bool load_x(const Io &io, const Chunk &k, std::span<const LeafId> ids, std::vector<Leaf> &out) {
  TraceN("FactorsLoadX");
  const FeatureTable &ft = io.ft;
  const Shape sh = shape_of(io, k);
  const size_t A = static_cast<size_t>(sh.A), n = sh.n(), VR = static_cast<size_t>(ft.seg), nf = ids.size();
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
  constexpr uint16_t kInf = 0x7C00;
  std::vector<std::vector<uint8_t>> has_data(static_cast<size_t>(io.ex.threads()), std::vector<uint8_t>(A));
  std::vector<long long> on(static_cast<size_t>(io.ex.threads()), 0);

  const bool read = read_days(io, k, std::move(cols.refs), [&](const auto &view, const Day &, size_t d, size_t tid) {
    if (!have) {
      uint8_t *gate = core.gate.data() + d * VR * A;
      gate_day(view, ts_i, cs_i, VR, A, has_data[tid], gate);
      on[tid] += gate_on(gate, VR * A);
    }
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      if (!have) {
        const View w = view(eo_i, t);
        for (size_t a = 0; a < A; ++a)
          core.eo[row + a] = eo_cell(w, a, core.gate[row + a]);
      }
      for (size_t i = 0; i < H; ++i) {
        View w[K];
        for (size_t l = 0; l < K; ++l)
          w[l] = view(lab_at[i] + l, t);
        for (size_t a = 0; a < A; ++a)
          leg_cell(w, a, g.gate[row + a], g.eo[row + a], labels[i], row + a);
      }
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
  assert(io.dm.pool == kAllPool && "池篮子在 all 池评估域 (全轴; 各池成员 = all.bar 选股表)");
  B = BPlanes{};
  B.sh = shape_of(io, k);
  const size_t A = static_cast<size_t>(B.sh.A), n = B.sh.n(), VR = static_cast<size_t>(ft.seg), NP = pool_count();
  assert(NP <= static_cast<size_t>(kPoolBits));
  assert(VR == 1 && "池篮子按日 (一行一日)");
  assert(io.dm.rs.has(kAllBar));
  const std::shared_ptr<const pool::Picks> pk = pool::picks(io.dm.rs.libs[kAllBar]);
  if (!pk)
    return false;
  assert(pk->tape->axis == io.dm.rs.axis.hash && "选股表与评估域 A 轴不符");
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
  assert(cols.refs[ts_i].lib == io.dm.rs.main && "门控 / 标签 / K 线在标签库");
  std::vector<float> vadj(n, 0.f);
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
  std::vector<std::vector<uint8_t>> has(nt, std::vector<uint8_t>(A)), gate(nt, std::vector<uint8_t>(VR * A));
  std::vector<std::vector<PoolMask>> sub(nt, std::vector<PoolMask>(A));
  const bool read = read_days(io, k, std::move(cols.refs), [&](const auto &view, const Day &day, size_t d, size_t tid) {
    gate_day(view, ts_i, cs_i, VR, A, has[tid], gate[tid].data());
    const std::vector<uint8_t> &on = has[tid];
    std::vector<PoolMask> &sm = sub[tid];
    std::fill(sm.begin(), sm.end(), PoolMask{0});
    if (day.date)
      for (PoolId p = 1; p < NP; ++p)
        for (const uint16_t a : pk->of(p, day.date))
          sm[a] |= pool_bit(p);
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      PoolMask *in = B.in.data() + row;
      for (size_t a = 0; a < A; ++a)
        in[a] = (gate[tid][t * A + a] ? pool_bit(kAllPool) : PoolMask{0}) | (on[a] ? sm[a] : PoolMask{0});
      const View we = view(eo_i, t), wl[K] = {view(lab_i, t), view(lab_i + 1, t)};
      for (size_t a = 0; a < A; ++a) {
        B.eo[row + a] = eo_cell(we, a, in[a] != 0);
        leg_cell(wl, a, in[a] != 0, B.eo[row + a], B.lab, row + a);
      }
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
  io.ex.rows(B.sh.T, [&](int t0, int t1, int) {
    for (size_t i = static_cast<size_t>(t0) * A; i < static_cast<size_t>(t1) * A; ++i) {
      const float f = vadj[i];
      B.close.m[i] = B.close.m[i] && f > 0.f;
      B.close.v[i] = B.close.m[i] ? B.close.v[i] * f : 0.f;
    }
  });
  const size_t T = static_cast<size_t>(B.sh.T);
  for (int rb = 0; rb < pool::kRebals; ++rb) {
    std::vector<uint32_t> &s = B.from[rb];
    s.assign(T, 0);
    for (size_t t = 1; t < T; ++t)
      s[t] = pool::rebal_at(static_cast<pool::Rebal>(rb), io.dm.dates[k.d0 + t - 1], io.dm.dates[k.d0 + t]) ? static_cast<uint32_t>(t) : s[t - 1];
  }
  const ConceptTable &ct = concepts();
  assert(io.dm.rs.axis.size() == A);
  B.cc_at.assign(A + 1, 0);
  for (size_t a = 0; a < A; ++a) {
    const std::span<const uint16_t> ks = ct.concepts_of(io.dm.rs.axis.global(a));
    B.cc.insert(B.cc.end(), ks.begin(), ks.end());
    B.cc_at[a + 1] = static_cast<uint32_t>(B.cc.size());
  }
  return true;
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

std::vector<const factor::check::Plane *> leaves_of(const Block &c, std::span<const LeafId> ids) {
  std::vector<const factor::check::Plane *> in;
  in.reserve(ids.size());
  for (const LeafId id : ids) {
    const Leaf *p = c.x.find(id);
    assert(p && "叶不在块上 (BlockSpec::x)");
    in.push_back(p);
  }
  return in;
}

// ============================================================================
// Store
// ============================================================================

Store::Store() : ex(misc::exec::hw()) {}

Store &Store::get() {
  static misc::Epochal<Store> s;
  return s.get();
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

template <class T, class Set>
bool Store::hit(Block &c, const std::string &key, Set &&set) {
  const std::shared_ptr<const T> p = find<T>(key);
  if (!p)
    return false;
  set(*p);
  c.pins.push_back(p);
  return true;
}

template <class T, class Set>
void Store::land(Block &c, const std::string &key, T &&v, double n, Set &&set) {
  static_assert(!std::is_reference_v<T>, "land 只收新装的平面 (右值)");
  const std::shared_ptr<const T> p = put(key, std::move(v), kCellBytes<T> * n);
  set(*p);
  c.pins.push_back(p);
}

double Store::budget() const {
  own();
  return host_budget() + bytes_ + (exec_ ? exec_->host_bytes() : 0.0);
}

double Store::vram() const {
  own();
  if (!factor::gpu::available())
    return 0.0;
  return vram_budget() + kVramFrac * (exec_ ? exec_->vram_bytes() : 0.0);
}

void Store::room(double need, double reserve) {
  own();
  double ex_b = exec_ ? exec_->host_bytes() : 0.0;
  const double cap = host_budget() + bytes_ + ex_b;
  const auto fits = [&] { return bytes_ + std::max(ex_b, reserve) + need <= cap; };
  while (!fits()) {
    auto lru = cache_.end();
    for (auto it = cache_.begin(); it != cache_.end(); ++it)
      if (it->second.p.use_count() == 1 && (lru == cache_.end() || it->second.tick < lru->second.tick))
        lru = it;
    if (lru == cache_.end())
      break;
    bytes_ -= lru->second.bytes;
    cache_.erase(lru);
  }
  if (!fits() && exec_ && ex_b > reserve) {
    exec_->drop_host();
    ex_b = 0.0;
  }
}

void Store::clear() {
  own();
  cache_.clear();
  n_full_.clear();
  bytes_ = 0.0;
  if (exec_)
    exec_->clear();
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
  c.ck = k, c.nd = io.dm.days.size(), c.warm = io.dm.warm;
  c.t_lo = static_cast<int>(k.s0 - k.d0) * ft.seg, c.t_back = static_cast<int>(k.d1 - k.s1) * ft.seg;
  c.x_gen = std::hash<std::string>{}(base + "#" + std::to_string(epoch_));
  c.gen = std::hash<std::string>{}(base + s.y.key() + "#" + std::to_string(epoch_));
  c.x.sh = sh;
  c.x.v.assign(ft.x.leaves.size(), nullptr);
  YPlanes &y = c.y;
  y.sh = sh;
  y.hd = s.y.hd, y.ad = s.y.ad, y.max_impact = s.y.max_impact;
  factor::stat::assert_holds(y.hd, sh.seg);

  const auto key = [&](char role, long long id) { return base + role + std::to_string(id); };
  const YCore *core = nullptr;
  const auto set_core = [&](const YCore &p) { core = &p, y.gate = p.gate.data(), y.eo = p.eo.data(); };
  const auto set_hold = [&y](int i) { return [&y, i](const LegPlane &p) { y.labels[i] = &p; }; };
  const auto set_amt = [&y](int i) { return [&y, i](const U16 &p) { y.costs[i] = p.data(); }; };
  const auto set_rank = [&y](int i) { return [&y, i](const U16 &p) { y.ry[i] = p.data(); }; };
  const auto set_leaf = [&c](LeafId id) { return [&c, id](const Leaf &p) { c.x.v[id] = &p; }; };

  hit<YCore>(c, key('g', 0), set_core);
  Holds mh;
  Amts ma;
  std::vector<int> mh_at, ma_at, mr;
  std::vector<LeafId> mx;
  for (int i = 0; i < y.hd.n; ++i) {
    if (!hit<LegPlane>(c, key('h', y.hd.k[i]), set_hold(i)))
      mh.k[mh.n++] = y.hd.k[i], mh_at.push_back(i);
    if (s.ry && !hit<U16>(c, key('r', y.hd.k[i]), set_rank(i)))
      mr.push_back(i);
  }
  for (int i = 0; i < y.ad.n; ++i)
    if (!leg_of(ft.y.costs, y.ad.k[i]).flat() && !hit<U16>(c, key('a', y.ad.k[i]), set_amt(i)))
      ma.k[ma.n++] = y.ad.k[i], ma_at.push_back(i);
  for (const LeafId id : s.x)
    if (!hit<Leaf>(c, key('x', id), set_leaf(id)))
      mx.push_back(id);

  const double need = n * ((core ? 0.0 : kCellBytes<YCore>)+kCellBytes<LegPlane> * mh.n + kCellBytes<U16> * static_cast<double>(ma.n + mr.size()) +
                           kCellBytes<Leaf> * static_cast<double>(mx.size()));
  room(need, reserve);
  c.fresh = need > 0.0;
  const auto end_io = [&](bool ok) {
    const Rc rc = rc_of(ok, lr);
    if (rc == Rc::Stale)
      clear();
    return rc;
  };
  if (!core || mh.n > 0 || ma.n > 0) {
    YCore got;
    std::vector<LegPlane> labs;
    std::vector<U16> cs;
    if (!load_y(io, k, core, got, mh, ma, labs, cs))
      return end_io(false);
    if (!core)
      land(c, key('g', 0), std::move(got), n, set_core);
    for (int j = 0; j < mh.n; ++j)
      land(c, key('h', mh.k[j]), std::move(labs[static_cast<size_t>(j)]), n, set_hold(mh_at[static_cast<size_t>(j)]));
    for (int j = 0; j < ma.n; ++j)
      land(c, key('a', ma.k[j]), std::move(cs[static_cast<size_t>(j)]), n, set_amt(ma_at[static_cast<size_t>(j)]));
  }
  if (!mx.empty()) {
    std::vector<Leaf> xs;
    if (!load_x(io, k, mx, xs))
      return end_io(false);
    for (size_t j = 0; j < mx.size(); ++j)
      land(c, key('x', mx[j]), std::move(xs[j]), n, set_leaf(mx[j]));
  }
  if (!mr.empty()) {
    const factor::stat::Span sp{sh.T, sh.A, factor::geo_of(sh.seg)};
    factor::stat::assert_span(sp);
    factor::cpu::stat::Workspace w;
    w.ensure(sh.A, 1, 0, ex.threads());
    for (const int i : mr) {
      U16 r(sh.n());
      factor::cpu::stat::prep_label(ex, label_of(*y.labels[i], y.eo, nullptr), sp, r.data(), w);
      land(c, key('r', y.hd.k[i]), std::move(r), n, set_rank(i));
    }
  }
  if (const auto it = n_full_.find(dom); it != n_full_.end()) {
    y.n_full = it->second;
  } else {
    const size_t nd = io.dm.days.size();
    if (k.d0 == 0 && k.d1 == nd)
      y.n_full = n_full_from(core->on, sh.T);
    else if (!n_full_of(io, Chunk{0, 0, nd, nd}, y.n_full))
      return end_io(false);
    n_full_.emplace(dom, y.n_full);
  }
  return Rc::Ok;
}

} // namespace GUI::Engine
