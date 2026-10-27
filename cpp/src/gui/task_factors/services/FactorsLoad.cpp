// FactorsLoad — 见头文件. include 因子契约 / Stat 行核头 (h2f), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsLoad.hpp"

#include "factor/GpuRun.hpp"                       // mem_info (显存预算)
#include "factor/Stat/Host.hpp"                    // h2f
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
  assert(ft.y.ts.ref(pool).pool == lab && ft.y.cs.ref(pool).pool == lab && "门控 / 标签 / 成本 / OHLC 在标签库");
  const bool sub = pool != lab; // Lob 池 × Bar 标签: 截面再与上池成员
  const PerPool<FeatureLib> &libs = sc.libs;
  es = EvalScope{};
  es.pool = pool;
  es.used = pool_bit(lab);
  for (const std::string &f : feats) {
    const FeatCol *c = ft.x.find(f);
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

int Plan::max_days() const {
  int m = 0;
  for (const Chunk &k : chunks)
    m = std::max(m, k.days());
  return m;
}

Plan plan_job(const EvalScope &es, int seg, const Budget &b, int carry, const char *who) {
  const int days = static_cast<int>(es.days.size()), A = static_cast<int>(es.rs.axis.size());
  assert(days > 0 && A > 0);
  const double per_day = static_cast<double>(seg) * A;
  Plan P;
  P.host = host_budget() - b.fixed;
  if (carry < 0) {
    assert_fits(who, days * per_day * (b.load + b.work) + b.fixed, days, A, seg);
    P.chunks = {Chunk{0, 0, static_cast<size_t>(days)}};
    P.D = days;
    P.cache = days * per_day * b.load;
    return P;
  }
  assert(P.host > 0.0 && "内存预算装不下与格无关的常驻量");
  int D = std::clamp(static_cast<int>(P.host / (per_day * (b.load + b.work))) - carry, 1, days);
  if (b.dev > 0.0) {
    size_t vfree = 0, vtotal = 0;
    factor::gpu::mem_info(&vfree, &vtotal);
    P.vram = kVramFrac * static_cast<double>(vfree);
    D = std::min(D, std::clamp(static_cast<int>(P.vram / (per_day * b.dev)) - carry, 1, days));
  }
  P.D = D;
  for (int s0 = 0; s0 < days; s0 += D) {
    const Chunk k{static_cast<size_t>(std::max(0, s0 - carry)), static_cast<size_t>(s0), static_cast<size_t>(std::min(days, s0 + D))};
    P.chunks.push_back(k);
    P.cache += k.days() * per_day * b.load;
  }
  P.cache_all = P.cache + P.max_days() * per_day * b.work <= P.host;
  return P;
}

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

// 篮子 (BPlanes::index / val / turn; pool_line 只取篮子 0 等权): 日层门控 = D 收盘定的池 (收盘换仓), 故 D 的收益按 D−1 收盘的成员.
//   pool_index 出指数 (净值引擎 fold_exit, 标签; 可成交 / 顺延在标签里); pool_baskets 出估值 + 各篮子两权重的逐行收益 / 权和 (BasketRows, 只给换手漂移;
//   停牌行不更新 ⇒ 权带到复牌); pool_turn 只用篮子 0 的逐行收益 / 权和算换手.

// 篮子逐行 [Weight][kBaskets · T]: 成分收益 r = Σ权·r / Σ权 (无成分 0) / 权和 Σ权 (等权 = 成分数)
struct BasketRows {
  std::vector<double> r[kWeights], w[kWeights];
};

// 行业 ID → 篮子: 篮子 0 (池整体) 恒进, 行业 k ≥ 1 再进篮子 k
template <class Fn>
void each_basket(uint8_t ind, Fn &&fn) {
  fn(size_t{0});
  if (ind > 0)
    fn(static_cast<size_t>(ind));
}

// 资产条带 [a0, a1) 沿 t 推上一有效 close + 该 close 行的在池位 / 权 (跨日); 成分格调 fn(t, 条带内下标, r, 上一有效 close 行的 wt)
template <class Fn>
void index_strip(const factor::check::Plane &close, const float *wt, const uint8_t *in, int T, int A, int a0, int a1, Fn &&fn) {
  const size_t nA = static_cast<size_t>(a1 - a0);
  std::vector<float> prev(nA, 0.f), pw(nA, 0.f);
  std::vector<uint8_t> held(nA, 0);
  for (size_t t = 0; t < static_cast<size_t>(T); ++t)
    for (int a = a0; a < a1; ++a) {
      const size_t i = t * static_cast<size_t>(A) + static_cast<size_t>(a), j = static_cast<size_t>(a - a0);
      const float c = close.v[i];
      if (!close.m[i] || !(c > 0.f))
        continue;
      if (prev[j] > 0.f && held[j])
        fn(t, j, c / prev[j] - 1.0, pw[j]);
      prev[j] = c;
      held[j] = in[i];
      pw[j] = wt[i];
    }
}

// 篮子指数 (BPlanes::index / pool_line) = 净值引擎 fold_exit, Y 的全部持有期一遍: 行 t 一批 = 成分 (该持有期标签有效, 已含在池; ∧ 该行 ind_l1 = k)
//   的做多标签按权平均 (等权 1 / 加权 mcap; Σ权 ≤ 0 → 0) → 线 nb · hi + index_line(k, w) 逐行收益 (stride 1; nb = nk · nw). nk = 篮子数
//   (1 = 只池整体; ind 可空), nw = 权重数 (1 = 只等权; mcap 可空)
void pool_index(misc::exec::ForkJoin &ex, const YPlanes &Y, const float *mcap, const uint8_t *ind, int nk, int nw, BookLines &out) {
  static_assert(kWeightEq == 0, "只等权 (nw = 1) 的篮子线 = k");
  assert(Y.hd.n >= 1 && (nk == 1 || ind) && (nw == 1 || mcap));
  const size_t nT = static_cast<size_t>(Y.sh.T), nA = static_cast<size_t>(Y.sh.A), nb = static_cast<size_t>(nk * nw), H = static_cast<size_t>(Y.hd.n);
  std::vector<double> x(H * nb * nT, 0.0); // [hold][篮子线][T] 行 t 这一批的篮子收益
  ex.rows(Y.sh.T, [&](int t0, int t1, int) {
    std::vector<double> s(2 * H * nb); // [hold][篮子线][Σ权·r, Σ权]
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t) {
      std::fill(s.begin(), s.end(), 0.0);
      for (size_t a = 0; a < nA; ++a) {
        const size_t i = t * nA + a;
        const double om[kWeights] = {1.0, nw > 1 ? mcap[i] : 0.0};
        const uint8_t k_ind = nk > 1 ? ind[i] : uint8_t{0};
        for (size_t hi = 0; hi < H; ++hi) {
          const LegPlane &lab = Y.labels[hi];
          if (!lab.m[i])
            continue;
          const double r = factor::cpu::stat::h2f(lab.v[factor::stat::kLong][i]);
          each_basket(k_ind, [&](size_t k) {
            for (int w = 0; w < nw; ++w) {
              double *q = s.data() + 2 * (hi * nb + k * static_cast<size_t>(nw) + static_cast<size_t>(w));
              q[0] += om[w] * r, q[1] += om[w];
            }
          });
        }
      }
      for (size_t b = 0; b < H * nb; ++b)
        if (s[2 * b + 1] > 0.0)
          x[b * nT + t] = s[2 * b] / s[2 * b + 1];
    }
  });
  fold_exit(ex, Y.sh.seg, Y.sh.T, static_cast<int>(H * nb), 1, [&](int i) { return Y.hd.h[static_cast<size_t>(i) / nb]; },
            [&](int i, int t) { return x[static_cast<size_t>(i) * nT + static_cast<size_t>(t)]; }, out);
}

// 篮子估值 (BPlanes::val, 两权重) + 逐行收益 / 权和 (rows, 换手用), 两遍:
//   1) 资产条带并行, 沿 t 就地把 px 补成上一有效收盘价 (停牌带着); prv = 有效 close 格的上一有效 close 行 (−1 = 无);
//   2) 行并行, 每行一遍 A 轴攒进本行的篮子桶 (0 + 行业):
//      收益成分 = 上一有效 close 行在池, 收益 = 后复权 close / 上一有效后复权 close − 1 (load_b 由 close_day × 复权因子现算), 权 = 1 / 该行总市值 mcap, 行业取该行;
//      估值成分 = 上一行在池, 市值 = px × 上一行总股本, 估值输入 val [PoolVal] / 行业同取上一行.
//   px = 本行有成交的收盘价 (否则 0), 就地改; val = YCols::val 原值 (DY 为股息率, 分子在此还原); ind = 行业 ID (0 = 未知 / 缺)
void pool_baskets(misc::exec::ForkJoin &ex, const factor::check::Plane &close, const float *mcap, float *px, const float *sh,
                  const std::vector<float> (&val)[kPoolVals], const uint8_t *ind, const uint8_t *in, int T, int A, BPlanes &B, BasketRows &rows) {
  constexpr size_t K = BPlanes::kBaskets;
  const size_t nT = static_cast<size_t>(T), nA = static_cast<size_t>(A);
  std::vector<int32_t> prv(nT * nA, -1);
  ex.rows(A, [&](int a0, int a1, int) {
    const size_t w = static_cast<size_t>(a1 - a0);
    std::vector<int32_t> lc(w, -1);
    std::vector<float> lp(w, 0.f);
    for (size_t t = 0; t < nT; ++t)
      for (int a = a0; a < a1; ++a) {
        const size_t i = t * nA + static_cast<size_t>(a), j = static_cast<size_t>(a - a0);
        if (close.m[i] && close.v[i] > 0.f)
          prv[i] = lc[j], lc[j] = static_cast<int32_t>(t);
        if (px[i] > 0.f)
          lp[j] = px[i];
        else
          px[i] = lp[j];
      }
  });
  constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  for (int w = 0; w < kWeights; ++w)
    for (int m = 0; m < kPoolVals; ++m)
      B.val[w][m].assign(K * nT, kNaN);
  for (int w = 0; w < kWeights; ++w)
    rows.r[w].assign(K * nT, 0.0), rows.w[w].assign(K * nT, 0.0);
  ex.rows(T, [&](int t0, int t1, int) {
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t) {
      // 指数 [篮子][Weight][Σ权·r, Σ权]; 估值 [篮子][Weight][PoolVal][Σ权·收益率, Σ权] (收益率 = 分子 / 市值; 权 = 等权 1 / 加权 市值 ⇒ 整体法 Σ分子 / Σ市值)
      double x[K][kWeights][2] = {}, v[K][kWeights][kPoolVals][2] = {};
      for (size_t a = 0; a < nA; ++a) {
        const size_t i = t * nA + a;
        if (const int32_t q = prv[i]; q >= 0) {
          const size_t p = static_cast<size_t>(q) * nA + a;
          if (in[p]) {
            const double r = close.v[i] / close.v[p] - 1.0, om[kWeights] = {1.0, mcap[p]};
            each_basket(ind[p], [&](size_t k) {
              for (int w = 0; w < kWeights; ++w)
                x[k][w][0] += om[w] * r, x[k][w][1] += om[w];
            });
          }
        }
        if (t == 0 || !in[i - nA])
          continue;
        const size_t p = i - nA;
        const double mc = static_cast<double>(px[i]) * sh[p];
        if (!(mc > 0.0)) // 无价 / 缺股本 (NaN) / 非正
          continue;
        double num[kPoolVals];
        for (int m = 0; m < kPoolVals; ++m)
          num[m] = val[m][p];
        num[kValDy] *= static_cast<double>(px[p]) * sh[p];
        const double om[kWeights] = {1.0, mc};
        each_basket(ind[p], [&](size_t k) {
          for (int m = 0; m < kPoolVals; ++m)
            if (std::isfinite(num[m]))
              for (int w = 0; w < kWeights; ++w)
                v[k][w][m][0] += om[w] * (num[m] / mc), v[k][w][m][1] += om[w];
        });
      }
      for (size_t k = 0; k < K; ++k)
        for (int w = 0; w < kWeights; ++w) {
          if (x[k][w][1] > 0.0)
            rows.r[w][k * nT + t] = x[k][w][0] / x[k][w][1], rows.w[w][k * nT + t] = x[k][w][1];
          for (int m = 0; m < kPoolVals; ++m)
            if (v[k][w][m][1] > 0.0)
              B.val[w][m][k * nT + t] = static_cast<float>(v[k][w][m][0] / v[k][w][m][1]);
        }
    }
  });
}

// 篮子 0 的换手 (BPlanes::turn): 成分行 t 的权 w_t = ω / Σω (等权 ω = 1, 加权 ω = mcap) 对上一成分行漂移后的权 w̃ = w (1 + r) / (1 + R) 的
//   单边 ½ Σ|w_t − w̃|, 停牌 = 移出 / 复牌 = 移回 (与指数同成分); 首个成分行 (建仓) 不计; 沿成分行均值.
//   Σ w̃ = 1 ⇒ ½ Σ|·| = ½ (1 + Σ_成分 (|w_t − w̃| − w̃)): 资产条带并行只走成分格, 每线程逐行攒, 按 tid 序归并. rows = pool_baskets 出的 (取篮子 0 段)
void pool_turn(misc::exec::ForkJoin &ex, const factor::check::Plane &close, const float *mcap, const uint8_t *in, int T, int A, const BasketRows &rows,
               double (&turn)[kWeights]) {
  const size_t nT = static_cast<size_t>(T), nt = static_cast<size_t>(ex.threads());
  std::vector<int> prv[kWeights]; // [Weight][T] 上一成分行 (−1 = 无)
  std::vector<double> dev[kWeights];
  for (int w = 0; w < kWeights; ++w) {
    prv[w].assign(nT, -1);
    for (size_t t = 1; t < nT; ++t)
      prv[w][t] = rows.w[w][t - 1] > 0.0 ? static_cast<int>(t - 1) : prv[w][t - 1];
    dev[w].assign(nt * nT, 0.0);
  }
  ex.rows(A, [&](int a0, int a1, int tid) {
    const size_t nA = static_cast<size_t>(a1 - a0), off = static_cast<size_t>(tid) * nT;
    std::vector<double> dw[kWeights]; // 本资产在 tw 行之后漂移的权
    std::vector<int> tw[kWeights];    // 本资产上次是成分的行
    for (int w = 0; w < kWeights; ++w)
      dw[w].assign(nA, 0.0), tw[w].assign(nA, -1);
    index_strip(close, mcap, in, T, A, a0, a1, [&](size_t t, size_t j, double r, float pw) {
      const double om[kWeights] = {1.0, pw};
      for (int w = 0; w < kWeights; ++w) {
        const double W = rows.w[w][t];
        if (!(W > 0.0))
          continue;
        const double wt = om[w] / W, d = tw[w][j] == prv[w][t] ? dw[w][j] : 0.0;
        dev[w][off + t] += std::abs(wt - d) - d;
        dw[w][j] = wt * (1.0 + r) / (1.0 + rows.r[w][t]), tw[w][j] = static_cast<int>(t);
      }
    });
  });
  for (int w = 0; w < kWeights; ++w) {
    double s = 0.0;
    int n = 0;
    for (size_t t = 0; t < nT; ++t) {
      if (!(rows.w[w][t] > 0.0) || prv[w][t] < 0)
        continue;
      double x = 0.0;
      for (size_t j = 0; j < nt; ++j)
        x += dev[w][j * nT + t];
      s += 0.5 * (1.0 + x), ++n;
    }
    turn[w] = n > 0 ? s / n : 0.0;
  }
}

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
  std::vector<long long> on(static_cast<size_t>(ex.threads()), 0);                                         // 每线程: 门控在池格数 (n_full)

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

bool load_b(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, const ScopeCache &c, BPlanes &B, misc::exec::ForkJoin &ex,
            const std::atomic<bool> &cancel, Progress &prog) {
  TraceN("FactorsLoadB");
  const YPlanes &Y = c.y;
  assert(!c.key.empty() && Y.sh == shape_of(ft, es, c.d0, c.d1) && "篮子挂在本作用域块的 Y 上 (先 ensure_xy)");
  assert(factor_level(ft.seg).lvl == L_1day && "篮子只在日层 (成员位 = 本行收盘定的池)");
  assert(Y.hd.n == 1 && Y.hd.h[0] == factor::stat::hold_close(1) && "篮子的持有期只 T+1C (C(D) → C(D+1))");
  B = BPlanes{};
  const size_t A = static_cast<size_t>(Y.sh.A), n = Y.sh.n(), VR = static_cast<size_t>(ft.seg);
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
  factor::check::Plane close;                     // 本行有成交的后复权 close (估值 / 换手漂移, 跨日除权不进收益), 装完即弃
  std::vector<float> vadj(n, 0.f);                // 本行 adj_factor (= 次日因子, 缺 / 非正 → 0): 装完把 close 乘成后复权, 即弃
  std::vector<float> mcap(n, 0.f);                // 本行总市值 = close_day × total_shares (加权版的权; close 无效 / 缺股本 → 0)
  std::vector<float> vpx(n, 0.f), vsh(n);         // 本行有成交的 close_day (否则 0) / 总股本 (缺 → NaN)
  std::vector<float> vval[kPoolVals];             // [PoolVal] 估值输入 (YCols::val; 缺 → NaN), 装完算 val 即弃
  for (std::vector<float> &v : vval)
    v.resize(n);
  std::vector<uint8_t> vind(n, 0);                // 行业 ID (0 = 未知 / 缺 / 越界)
  close.resize(n);
  const bool read = read_days(ft, es, lr, c.d0, c.d1, std::move(cols), ex, cancel, prog, [&](const auto &view, const Day &, size_t d, size_t) {
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      // close: 本行有成交 (ts_valid 按 DATA 口径) ∧ 有限 ∧ > 0 (此处不复权, 装完乘复权因子)
      const View wt = view(ts_i, t), wp = view(px_i, t), wa = view(adj_i, t);
      for (size_t a = 0; a < A; ++a) {
        feature_storage_t s, px, f;
        float g;
        const bool ok = cell(wp, a, px, g) && cell(wt, a, s, g) && fmeta::keep(static_cast<float>(s), L2::ValidType::DATA, px) && static_cast<float>(px) > 0.f;
        close.v[row + a] = ok ? static_cast<float>(px) : 0.f, close.m[row + a] = ok;
        vpx[row + a] = close.v[row + a];
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
        const bool okp = vpx[row + a] > 0.f;
        const float px = vpx[row + a];
        vsh[row + a] = cell(ws, a, x, g) ? static_cast<float>(x) : kNaN;
        if (okp && vsh[row + a] > 0.f)
          mcap[row + a] = px * vsh[row + a];
        for (int m = 0; m < kPoolVals; ++m)
          vval[m][row + a] = cell(wv[m], a, x, g) ? static_cast<float>(x) : kNaN;
        if (cell(wi, a, x, g) && static_cast<float>(x) >= 1.f && static_cast<float>(x) < static_cast<float>(BPlanes::kBaskets))
          vind[row + a] = static_cast<uint8_t>(static_cast<float>(x));
      }
    }
  });
  if (!read)
    return false;
  // 后复权 close(D) = close_day(D) × D 日复权因子 (= 行 D−1 的 adj_factor); 首行无前一行 → 不作漂移锚. 只比值进收益, 首日归一无关
  ex.rows(Y.sh.T, [&](int t0, int t1, int) {
    for (size_t i = static_cast<size_t>(t0) * A; i < static_cast<size_t>(t1) * A; ++i) {
      const float f = i >= A ? vadj[i - A] : 0.f;
      close.m[i] = close.m[i] && f > 0.f;
      close.v[i] = close.m[i] ? close.v[i] * f : 0.f;
    }
  });
  vadj = {};
  pool_index(ex, Y, mcap.data(), vind.data(), static_cast<int>(BPlanes::kBaskets), kWeights, B.index);
  BasketRows rows;
  pool_baskets(ex, close, mcap.data(), vpx.data(), vsh.data(), vval, vind.data(), Y.gate.data(), Y.sh.T, Y.sh.A, B, rows);
  pool_turn(ex, close, mcap.data(), Y.gate.data(), Y.sh.T, Y.sh.A, rows, B.turn);
  return true;
}

BookLines pool_line(misc::exec::ForkJoin &ex, const YPlanes &y) {
  BookLines out;
  pool_index(ex, y, nullptr, nullptr, 1, 1, out);
  return out;
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
  c.x.sh = c.y.sh;
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

namespace {

// 叶 p 在严格域 dom 上首个越界格的值 (空 = 有效格全落在域内; 各条带首个越界格取全局最小, 与顺序扫同一格)
std::string out_of_dom(misc::exec::ForkJoin &ex, const factor::check::Plane &p, factor::Dom dom) {
  const size_t n = p.v.size();
  std::vector<size_t> first(static_cast<size_t>(ex.threads()), n);
  ex.rows(static_cast<int>(n), [&](int i0, int i1, int tid) {
    for (size_t i = static_cast<size_t>(i0); i < static_cast<size_t>(i1); ++i)
      if (p.m[i] && !factor::expr::dom_holds(dom, p.v[i])) {
        first[static_cast<size_t>(tid)] = i;
        return;
      }
  });
  const size_t i = *std::min_element(first.begin(), first.end());
  if (i == n)
    return {};
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%g", static_cast<double>(p.v[i]));
  return buf;
}

} // namespace

// 算子节点的特征叶元: parse 时放行 (值域只能查数据), 这里按 OpTable in 列的严格域 (dom_strict, 即组 id 的 INT) 逐元查数据
// (有效格全部落在值域内): CS 算子内部 max_gid 断言炸不得由数据触发. 非严格域越界格由算子自身置无效, 不在此判.
// 融合节点只装逐点 TS 算子, 无严格域元
int plan_forest(misc::exec::ForkJoin &ex, const ScopeCache &c, const factor::Dag &d, std::vector<std::string> &why, std::vector<uint8_t> &need) {
  assert(!d.roots.empty() && "森林 (build_forest) 才有根表");
  const std::vector<const factor::check::Plane *> in = leaves(c, d.feats);
  const size_t N = d.nodes.size();
  std::vector<std::string> err(N);
  for (size_t i = 0; i < N; ++i) {
    const factor::DagNode &nd = d.nodes[i];
    if (nd.op < 0 || nd.fu.n > 0)
      continue;
    const factor::expr::OpInfo &o = factor::expr::kOps[nd.op];
    assert(nd.n_in == o.arity);
    for (int s = 0; s < o.arity && err[i].empty(); ++s) {
      const factor::DagNode &g = d.nodes[static_cast<size_t>(nd.in[s])];
      if (g.op >= 0 || !factor::dom_strict(o.in[s]))
        continue;
      const std::string &code = d.feats[static_cast<size_t>(g.feat)];
      const auto [it, fresh] = c.dom.try_emplace({code, o.in[s]});
      if (fresh)
        it->second = out_of_dom(ex, *in[static_cast<size_t>(g.feat)], o.in[s]);
      if (!it->second.empty())
        err[i] = std::string(o.name) + " 第 " + std::to_string(s + 1) + " 元要求 " + factor::dom_name(o.in[s]) + ", 特征 " + code + " 含越界值 " + it->second;
    }
  }
  why.assign(d.roots.size(), {});
  need.assign(N, 0);
  int good = 0;
  for (size_t k = 0; k < d.roots.size(); ++k) {
    const std::vector<int> sub = d.subtree(d.roots[k]);
    for (const int i : sub)
      if (!err[static_cast<size_t>(i)].empty()) {
        why[k] = err[static_cast<size_t>(i)];
        break;
      }
    if (!why[k].empty())
      continue;
    ++good;
    for (const int i : sub)
      need[static_cast<size_t>(i)] = 1;
  }
  return good;
}

} // namespace GUI::Factors
