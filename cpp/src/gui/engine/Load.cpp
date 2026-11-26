// Load — 见头文件. include 因子 Stat CPU 头 (标签秩 prep_label), 依赖受控浮点: CMake MATH_PRECISE 源.
#include "gui/engine/Load.hpp"

#include "factor/GpuRun.hpp"     // available / mem_info
#include "factor/Stat/Cpu.hpp"   // prep_label / Workspace
#include "features/MetaFlag.hpp" // fmeta::valid / keep / finite
#include "features/Storage/FeatureRead.hpp"
#include "features/TimeIndex.hpp"
#include "misc/cross_platform.hpp" // memory_usage
#include "misc/date.hpp"           // months_between
#include "misc/profiler.hpp"
#include "shared/ReadRequest.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <functional>
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

LibMask used_libs(const FeatureTable &ft, PoolId pool, std::span<const LeafId> leaves, bool lob_cost) {
  const LibId lab = label_lib(pool);
  assert(ft.y.ts.ref(pool).lib == lab && ft.y.cs.ref(pool).lib == lab && "门控 / 标签 / K 线在标签库");
  LibMask m = lib_bit(lab);
  if (lob_cost && ft.y.amts != amt_bit(kAmtFixed))
    m |= lib_bit(ft.y.cost[1][kBuy].ref(pool).lib);
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
  dm.used = used_libs(ft, pool, leaves, sc.lob_cost);
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
    w = std::max(static_cast<size_t>(std::ceil(static_cast<double>(s) * kWarmFrac)), warm_days(ft.seg));
    if (s + w > n) {
      w = std::min(w, n / 2);
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
// 两侧规格与逐天并行装载: load_x ↔ load_y
// ============================================================================

YSpec y_spec(const FeatureTable &ft, const Domain &dm, const Holds &hd, Segs sg, double max_impact) {
  YSpec s{hd, {}, 0.f};
  if (sg == Segs::None)
    return s;
  AmtSet amts = amt_bit(kAmtFixed);
  for (int a = 1; a < kAmtCount; ++a)
    if ((ft.y.amts & amt_bit(static_cast<AmtId>(a))) && dm.uses(ft.y.cost[a][kBuy]))
      amts |= amt_bit(static_cast<AmtId>(a));
  if (sg == Segs::First) { // 有盘口段取最小的一段, 否则固定段
    const Amts all = Amts::of(amts);
    amts = amt_bit(all.n > 1 ? all.k[1] : kAmtFixed);
  }
  s.ad = Amts::of(amts);
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
inline constexpr double kCellBytes<LabPlane> = kPlaneBytes;
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
// 一格标签: 做多 (w[0]) 与名义 (w[1], 只偏移型; nom = false 则不读) 按 fmeta::lab_ok (原料带同一判据)
void lab_cell(const View (&w)[2], bool nom, size_t a, bool in, uint8_t eo, LabPlane &p, size_t i) {
  feature_storage_t x[2] = {};
  const bool got = cell(w[0], a, x[0]) && (!nom || cell(w[1], a, x[1]));
  const bool ok = got && fmeta::lab_ok(in, x[0], x[1], nom);
  p.lv[i] = ok ? std::bit_cast<uint16_t>(x[0]) : 0;
  if (nom)
    p.nv[i] = ok ? std::bit_cast<uint16_t>(x[1]) : 0;
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

double cell_bytes(const BlockSpec &s, size_t nx) {
  int books = 0;
  for (int c = 0; c < s.y.ad.n; ++c)
    books += s.y.ad.k[c] != kAmtFixed;
  return kCellBytes<Leaf> * static_cast<double>(nx) + kCellBytes<YCore> + kCellBytes<LabPlane> * s.y.hd.n + kCellBytes<U16> * kSides * books;
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

bool load_y(const Io &io, const Chunk &k, const YCore *have, YCore &core, const Holds &hd, const Amts &ad, std::vector<LabPlane> &labels,
            std::vector<std::vector<uint16_t>> &costs) {
  TraceN("FactorsLoadY");
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
  for (size_t i = 0; i < H; ++i) { // 做多 / 名义列相邻 (lab_at[i] + 腿; 锚点型只一列)
    assert(ft.y.has(hd.k[i]) && "持有期不在本层标签里");
    const LabCols &lc = ft.y.lab[hd.k[i]];
    lab_at[i] = cols.add(lc.lv);
    if (hold_offset(hd.k[i]))
      cols.add(lc.nv);
  }
  for (size_t c = 0; c < C; ++c) { // 两侧冲击列相邻 (cost_at[c] + side)
    const AmtId a = ad.k[c];
    assert(a != kAmtFixed && (ft.y.amts & amt_bit(a)) && io.dm.uses(ft.y.cost[a][kBuy]) && "只装盘口段, 要评估域含 Lob 库 (y_spec)");
    cost_at[c] = cols.add(ft.y.cost[a][kBuy]);
    cols.add(ft.y.cost[a][kSell]);
  }

  if (!have) {
    core = YCore{};
    core.gate.assign(n, 0);
    core.eo.assign(n, 0);
  }
  const YCore &g = have ? *have : core;
  assert(g.gate.size() == n && "门控与块不同形");
  labels.assign(H, LabPlane{});
  for (size_t i = 0; i < H; ++i) {
    labels[i].lv.assign(n, 0), labels[i].m.assign(n, 0);
    if (hold_offset(hd.k[i]))
      labels[i].nv.assign(n, 0);
  }
  costs.assign(C, std::vector<uint16_t>(kSides * n, 0)); // [side][n]
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
        const bool nom = hold_offset(hd.k[i]);
        const View w[2] = {view(lab_at[i], t), nom ? view(lab_at[i] + 1, t) : View{}};
        for (size_t a = 0; a < A; ++a)
          lab_cell(w, nom, a, g.gate[row + a], g.eo[row + a], labels[i], row + a);
      }
      for (size_t c = 0; c < C; ++c)
        for (size_t s = 0; s < static_cast<size_t>(kSides); ++s) {
          const View w = view(cost_at[c] + s, t);
          uint16_t *p = costs[c].data() + s * n;
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
  const std::shared_ptr<const void> *p = cache_.find(key);
  return p ? std::static_pointer_cast<const T>(*p) : nullptr;
}

template <class T>
std::shared_ptr<const T> Store::put(const std::string &key, T &&v, double bytes) {
  std::shared_ptr<const T> p = std::make_shared<const T>(std::move(v));
  cache_.put(key, p, bytes);
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
  return host_budget() + cache_.cost() + (exec_ ? exec_->host_bytes() : 0.0);
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
  const double cap = host_budget() + cache_.cost() + ex_b;
  const auto fits = [&] { return cache_.cost() + std::max(ex_b, reserve) + need <= cap; };
  cache_.trim([&](double) { return !fits(); });
  if (!fits() && exec_ && ex_b > reserve) {
    exec_->drop_host();
    ex_b = 0.0;
  }
}

void Store::clear() {
  own();
  cache_.clear();
  n_full_.clear();
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
  const auto set_hold = [&y](int i) { return [&y, i](const LabPlane &p) { y.labels[i] = &p; }; };
  const auto set_amt = [&y](int i) { return [&y, i](const U16 &p) { y.costs[i] = p.data(); }; };
  const auto set_rank = [&y](int i) { return [&y, i](const U16 &p) { y.ry[i] = p.data(); }; };
  const auto set_leaf = [&c](LeafId id) { return [&c, id](const Leaf &p) { c.x.v[id] = &p; }; };

  hit<YCore>(c, key('g', 0), set_core);
  Holds mh;
  Amts ma;
  std::vector<int> mh_at, ma_at, mr;
  std::vector<LeafId> mx;
  for (int i = 0; i < y.hd.n; ++i) {
    if (!hit<LabPlane>(c, key('h', y.hd.k[i]), set_hold(i)))
      mh.k[mh.n++] = y.hd.k[i], mh_at.push_back(i);
    if (s.ry && !hit<U16>(c, key('r', y.hd.k[i]), set_rank(i)))
      mr.push_back(i);
  }
  for (int i = 0; i < y.ad.n; ++i)
    if (y.ad.k[i] != kAmtFixed && !hit<U16>(c, key('a', y.ad.k[i]), set_amt(i)))
      ma.k[ma.n++] = y.ad.k[i], ma_at.push_back(i);
  for (const LeafId id : s.x)
    if (!hit<Leaf>(c, key('x', id), set_leaf(id)))
      mx.push_back(id);

  const double need = n * ((core ? 0.0 : kCellBytes<YCore>)+kCellBytes<LabPlane> * mh.n + kCellBytes<U16> * static_cast<double>(kSides * ma.n + mr.size()) +
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
    std::vector<LabPlane> labs;
    std::vector<U16> cs;
    if (!load_y(io, k, core, got, mh, ma, labs, cs))
      return end_io(false);
    if (!core)
      land(c, key('g', 0), std::move(got), n, set_core);
    for (int j = 0; j < mh.n; ++j)
      land(c, key('h', mh.k[j]), std::move(labs[static_cast<size_t>(j)]), n, set_hold(mh_at[static_cast<size_t>(j)]));
    for (int j = 0; j < ma.n; ++j)
      land(c, key('a', ma.k[j]), std::move(cs[static_cast<size_t>(j)]), kSides * n, set_amt(ma_at[static_cast<size_t>(j)]));
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
    factor::cpu::stat::Workspace w;
    w.ensure(sh.A, 1, ex.threads());
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
