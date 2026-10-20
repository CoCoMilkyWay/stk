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

std::string YSpec::key() const {
  std::string k = "h";
  for (int i = 0; i < hd.n; ++i)
    k += "," + std::to_string(hd.h[i]);
  k += "|a";
  for (int i = 0; i < ad.n; ++i)
    k += "," + std::to_string(ad.a[i]);
  return k + "|cap" + std::to_string(max_impact) + "|i" + (index ? "1" : "0");
}

double cell_bytes(const XSpec &s) { return kPlaneBytes * static_cast<double>(s.codes.size()); }
double cell_bytes(const YSpec &s) { return 1.0 + kPlaneBytes * static_cast<double>(s.hd.n + s.ad.n + (s.index ? 1 : 0)) + (s.index ? 4.0 + 20.0 + 1.0 + 4.0 : 0.0); }

const factor::check::Plane *XPlanes::find(std::string_view code) const {
  for (size_t i = 0; i < codes.size(); ++i)
    if (codes[i] == code)
      return &v[i];
  return nullptr;
}

namespace {

// 池等权指数 (YPlanes::index): 资产条带并行, 各条带沿 t 推本条带资产的上一有效后复权 close + 该 close 行的在池位 (跨日);
// 成分格 (上一有效 close 行在池 ∧ 本行有 close) 攒进本线程. 日层门控 = D 收盘定的池 (收盘换仓), 故 D 的收益按 D−1 收盘的成员;
// 停牌行不更新 ⇒ 持仓带到复牌, 复牌跳空计入. 逐行 Σ / 数; 线程间按 tid 序归并后串行连乘.
// 加权版 (out_cap) 同成分同收益, 权 = 上一有效 close 行的 wt (流通市值): 逐行 Σ w·r / Σ w
// 换手 (turn / turn_cap, 两版各一): 成分行 t 的权 w_t (等权 1/n, 加权 w/Σw) 对上一成分行漂移后的权 w̃ = w (1 + r) / (1 + R) 的单边 ½ Σ|w_t − w̃|,
//   停牌 = 移出 / 复牌 = 移回 (与指数同成分); 首个成分行 (建仓) 不计; 沿成分行均值. Σ w̃ = 1 ⇒ ½ Σ|·| = ½ (1 + Σ_成分 (|w_t − w̃| − w̃)), 第二遍只走成分格

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

void pool_index(misc::exec::ForkJoin &ex, const factor::check::Plane &close, const float *wt, const uint8_t *in, int T, int A, std::vector<float> &out,
                std::vector<float> &out_cap, double &turn, double &turn_cap) {
  const size_t nT = static_cast<size_t>(T), nt = static_cast<size_t>(ex.threads());
  std::vector<double> sum(nt * nT, 0.0), wsum(nt * nT, 0.0), wtot(nt * nT, 0.0);
  std::vector<int> cnt(nt * nT, 0);
  ex.rows(A, [&](int a0, int a1, int tid) {
    double *s = sum.data() + static_cast<size_t>(tid) * nT, *sw = wsum.data() + static_cast<size_t>(tid) * nT, *ww = wtot.data() + static_cast<size_t>(tid) * nT;
    int *k = cnt.data() + static_cast<size_t>(tid) * nT;
    index_strip(close, wt, in, T, A, a0, a1, [&](size_t t, size_t, double r, float w) {
      s[t] += r;
      ++k[t];
      sw[t] += w * r;
      ww[t] += w;
    });
  });
  out.resize(nT);
  out_cap.resize(nT);
  std::vector<int> n(nT, 0);                               // [T] 成分数
  std::vector<double> R(nT, 0.0), W(nT, 0.0), Rc(nT, 0.0); // [T] 等权收益 / 权和 / 加权收益
  std::vector<int> pe(nT, -1), pc(nT, -1);                 // [T] 上一成分行 (−1 = 无): 等权 (n > 0) / 加权 (W > 0)
  double lv = 1.0, lc = 1.0;
  for (size_t t = 0; t < nT; ++t) {
    double s = 0.0, x = 0.0, y = 0.0;
    int k = 0;
    for (size_t j = 0; j < nt; ++j)
      s += sum[j * nT + t], k += cnt[j * nT + t], x += wsum[j * nT + t], y += wtot[j * nT + t];
    if (k)
      R[t] = s / k, lv *= 1.0 + R[t];
    if (y > 0.0)
      Rc[t] = x / y, lc *= 1.0 + Rc[t];
    n[t] = k, W[t] = y;
    if (t > 0)
      pe[t] = n[t - 1] > 0 ? static_cast<int>(t - 1) : pe[t - 1], pc[t] = W[t - 1] > 0.0 ? static_cast<int>(t - 1) : pc[t - 1];
    out[t] = static_cast<float>(lv);
    out_cap[t] = static_cast<float>(lc);
  }
  // ---- 换手: 第二遍, 每线程逐行攒 Σ_成分 (|w_t − w̃| − w̃) ----
  std::vector<double> dev(nt * nT, 0.0), dev_cap(nt * nT, 0.0);
  ex.rows(A, [&](int a0, int a1, int tid) {
    const size_t nA = static_cast<size_t>(a1 - a0);
    std::vector<double> de(nA, 0.0), dc(nA, 0.0); // 本资产在 te / tc 行之后漂移的权
    std::vector<int> te(nA, -1), tc(nA, -1);      // 本资产上次是成分的行
    double *v = dev.data() + static_cast<size_t>(tid) * nT, *vc = dev_cap.data() + static_cast<size_t>(tid) * nT;
    index_strip(close, wt, in, T, A, a0, a1, [&](size_t t, size_t j, double r, float w) {
      const double we = 1.0 / n[t], d = te[j] == pe[t] ? de[j] : 0.0;
      v[t] += std::abs(we - d) - d;
      de[j] = we * (1.0 + r) / (1.0 + R[t]), te[j] = static_cast<int>(t);
      if (W[t] > 0.0) {
        const double wc = w / W[t], dd = tc[j] == pc[t] ? dc[j] : 0.0;
        vc[t] += std::abs(wc - dd) - dd;
        dc[j] = wc * (1.0 + r) / (1.0 + Rc[t]), tc[j] = static_cast<int>(t);
      }
    });
  });
  double se = 0.0, sc = 0.0;
  int ne = 0, nc = 0;
  for (size_t t = 0; t < nT; ++t) {
    double x = 0.0, y = 0.0;
    for (size_t j = 0; j < nt; ++j)
      x += dev[j * nT + t], y += dev_cap[j * nT + t];
    if (n[t] > 0 && pe[t] >= 0)
      se += 0.5 * (1.0 + x), ++ne;
    if (W[t] > 0.0 && pc[t] >= 0)
      sc += 0.5 * (1.0 + y), ++nc;
  }
  turn = ne > 0 ? se / ne : 0.0;
  turn_cap = nc > 0 ? sc / nc : 0.0;
}

// 池 / 行业估值 + 行业流通市值加权指数 (YPlanes::val / ind_*), 两遍:
//   1) 资产条带并行, 沿 t 就地把 px 补成上一有效收盘价 (停牌带着); prv = 有效 close 格的上一有效 close 行 (−1 = 无);
//   2) 行并行, 每行一遍 A 轴攒进本行的桶 (行业 k / 池总值): 估值成分 = 上一行在池, 市值 = px × 上一行总股本, 净利 / 权益 / 股息率 / 行业同取上一行;
//      指数成分 = 上一有效 close 行在池 ∧ 其行业 = k, 权 = 该行流通市值, 收益 = close_adj / 上一有效 close_adj − 1 (同 pool_index 加权版).
//   最后逐行业沿 t 连乘. px = 本行有成交的收盘价 (否则 0), 就地改; ind = 行业 ID (0 = 未知 / 缺)
void pool_val(misc::exec::ForkJoin &ex, const factor::check::Plane &close, const float *fmc, float *px, const float *sh, const float *np,
              const float *eq, const float *dy, const uint8_t *ind, const uint8_t *in, int T, int A, YPlanes &Y) {
  constexpr size_t K = YPlanes::kInds;
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
  for (int m = 0; m < kPoolVals; ++m)
    Y.val[m].assign(nT, kNaN), Y.ind_val[m].assign(K * nT, kNaN);
  std::vector<double> ret(K * nT, 0.0); // [k][t] 行业加权收益
  ex.rows(T, [&](int t0, int t1, int) {
    for (size_t t = static_cast<size_t>(t0); t < static_cast<size_t>(t1); ++t) {
      double v[K + 1][kPoolVals][2] = {}, x[K][2] = {}; // 估值 [行业 / 池总值 (K)][PoolVal][Σ分子, Σ分母]; 指数 [行业][Σw·r, Σw]
      for (size_t a = 0; a < nA; ++a) {
        const size_t i = t * nA + a;
        if (const int32_t q = prv[i]; q >= 0) {
          const size_t p = static_cast<size_t>(q) * nA + a;
          if (in[p] && ind[p] > 0 && fmc[p] > 0.f)
            x[ind[p]][0] += fmc[p] * (close.v[i] / close.v[p] - 1.0), x[ind[p]][1] += fmc[p];
        }
        if (t == 0 || !in[i - nA])
          continue;
        const size_t p = i - nA;
        const double mc = static_cast<double>(px[i]) * sh[p];
        if (!(mc > 0.0)) // 无价 / 缺股本 (NaN) / 非正
          continue;
        const float f[kPoolVals] = {np[p], eq[p], dy[p]};
        const double num[kPoolVals] = {mc, mc, static_cast<double>(dy[p]) * px[p] * sh[p]}, den[kPoolVals] = {np[p], eq[p], mc};
        for (const size_t k : {K, static_cast<size_t>(ind[p])}) {
          if (k == 0)
            continue;
          for (int m = 0; m < kPoolVals; ++m)
            if (std::isfinite(f[m]))
              v[k][m][0] += num[m], v[k][m][1] += den[m];
        }
      }
      for (size_t k = 1; k <= K; ++k)
        for (int m = 0; m < kPoolVals; ++m)
          if (v[k][m][1] > 0.0)
            (k == K ? Y.val[m][t] : Y.ind_val[m][k * nT + t]) = static_cast<float>(v[k][m][0] / v[k][m][1]);
      for (size_t k = 1; k < K; ++k)
        if (x[k][1] > 0.0)
          ret[k * nT + t] = x[k][0] / x[k][1];
    }
  });
  Y.ind_index.assign(K * nT, 1.f);
  for (size_t k = 1; k < K; ++k) {
    double l = 1.0;
    for (size_t t = 0; t < nT; ++t)
      l *= 1.0 + ret[k * nT + t], Y.ind_index[k * nT + t] = static_cast<float>(l);
  }
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
// on_day(view, day, d, tid) 散进平面 (view(c, t) = 平面列 c 在网格行 t 的源行视图). 每天 prog.publish(1). false = 取消 / 库判废
template <class OnDay>
bool read_days(const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, std::vector<ColRef> cols,
               misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog, OnDay &&on_day) {
  const size_t n_cols = cols.size();
  assert(n_cols > 0);
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
  // ---- 列段 ts_valid | cs_valid | close_adj close_day float_shares total_shares np_ttm eq_mrq dy_ttm ind_l1 (index) | 标签 [H][K] | 成本 [C][K],
  //      全是标签库网格层本层列 (同行) ----
  std::vector<ColRef> cols;
  const auto add = [&](const Col &c) {
    cols.push_back(c.ref(es.pool));
    return cols.size() - 1;
  };
  const size_t ts_i = add(ft.y.ts), cs_i = add(ft.y.cs);
  assert((!spec.index || factor_level(ft.seg).lvl == L_1day) && "池指数只在日层 (成员位 = 本行收盘定的池)");
  const size_t close_i = spec.index ? add(ft.y.close_adj) : SIZE_MAX;
  const size_t px_i = spec.index ? add(ft.y.ohlc[3]) : SIZE_MAX, fs_i = spec.index ? add(ft.y.float_shares) : SIZE_MAX;
  const size_t sh_i = spec.index ? add(ft.y.total_shares) : SIZE_MAX, np_i = spec.index ? add(ft.y.np_ttm) : SIZE_MAX,
               eq_i = spec.index ? add(ft.y.eq_mrq) : SIZE_MAX, dy_i = spec.index ? add(ft.y.dy_ttm) : SIZE_MAX,
               ind_i = spec.index ? add(ft.y.ind_l1) : SIZE_MAX;
  assert(cols[ts_i].pool == es.rs.main && "门控 / 标签 / 成本 / OHLC 在标签库");
  const size_t lab_i = cols.size();
  for (size_t i = 0; i < H; ++i)
    for (const Col &c : leg_of(ft.y.labels, Y.hd.h[i]).leg)
      add(c);
  const size_t cost_i = cols.size();
  for (size_t c = 0; c < C; ++c)
    for (const Col &l : leg_of(ft.y.costs, Y.ad.a[c]).leg)
      add(l);

  Y.gate.assign(n, 0);
  const auto alloc = [&](std::vector<LegPlane> &ps, size_t k) {
    ps.resize(k);
    for (LegPlane &p : ps) {
      for (std::vector<uint16_t> &v : p.v)
        v.assign(n, 0);
      p.m.assign(n, 0);
    }
  };
  alloc(Y.labels, H);
  alloc(Y.costs, C);
  factor::check::Plane close;                 // 本行有成交的后复权 close (index, 跨日除权不进收益), 装完算指数即弃
  std::vector<float> fmc;                     // 本行流通市值 = close_day × float_shares (index 加权版的权; close 无效 / 缺股本 → 0)
  std::vector<float> vpx, vsh, vnp, veq, vdy; // 估值 (index): 本行有成交的 close_day (否则 0) / 总股本 / 归母净利TTM / 归母权益MRQ / 股息率TTM (缺 → NaN),
                                              //   装完算 val 即弃
  std::vector<uint8_t> vind;                  // 行业 ID (index; 0 = 未知 / 缺 / 越界)
  if (spec.index)
    close.resize(n), fmc.assign(n, 0.f), vpx.assign(n, 0.f), vsh.resize(n), vnp.resize(n), veq.resize(n), vdy.resize(n), vind.assign(n, 0);
  // 两腿封顶 (fp16 位直接替换, 与原值同格式): 标签不截; 成本买入按 max_impact 截, 卖出原值
  constexpr float kNoCap = std::numeric_limits<float>::infinity();
  const float lab_cap[K] = {kNoCap, kNoCap}, cost_cap[K] = {spec.max_impact, kNoCap};
  std::vector<std::vector<uint8_t>> has_data(static_cast<size_t>(ex.threads()), std::vector<uint8_t>(A)); // 每线程: 当日各资产是否有成交 (∧ 在子池)

  const bool read = read_days(ft, es, lr, d0, d1, std::move(cols), ex, cancel, prog, [&](const auto &view, const Day &day, size_t d, size_t tid) {
    // 两腿列组 (标签 / 成本同一份折叠): 列段起点 base, 组 k 的腿 l = 列 base + K·k + l; 池内 (gate) ∧ 两腿皆有限 → fp16 位, 其余 0 + 无效
    // (成本吃不到 = NaN → 无效)
    const auto fold_legs = [&](size_t base, const float (&cap)[K], std::vector<LegPlane> &out, size_t t, size_t row) {
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
          p.m[row + a] = ok;
        }
      }
    };
    gate_day(view, ts_i, cs_i, es, day, VR, A, has_data[tid], Y.gate.data() + d * VR * A);
    for (size_t t = 0; t < VR; ++t) {
      const size_t row = (d * VR + t) * A;
      if (spec.index) { // close: 本行有成交 (ts_valid 按 DATA 口径) ∧ 有限
        const View wt = view(ts_i, t), wl = view(close_i, t), wp = view(px_i, t), wf = view(fs_i, t);
        const View ws = view(sh_i, t), wn = view(np_i, t), we = view(eq_i, t), wd = view(dy_i, t), wi = view(ind_i, t);
        constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
        for (size_t a = 0; a < A; ++a) {
          feature_storage_t c, s, px, fs, x;
          float g;
          const bool ok = cell(wl, a, c, g) && cell(wt, a, s, g) && fmeta::keep(static_cast<float>(s), L2::ValidType::DATA, c);
          close.v[row + a] = ok ? static_cast<float>(c) : 0.f;
          close.m[row + a] = ok;
          const bool okp = ok && cell(wp, a, px, g) && static_cast<float>(px) > 0.f;
          if (okp && cell(wf, a, fs, g) && static_cast<float>(fs) > 0.f)
            fmc[row + a] = static_cast<float>(px) * static_cast<float>(fs);
          vpx[row + a] = okp ? static_cast<float>(px) : 0.f;
          vsh[row + a] = cell(ws, a, x, g) ? static_cast<float>(x) : kNaN;
          vnp[row + a] = cell(wn, a, x, g) ? static_cast<float>(x) : kNaN;
          veq[row + a] = cell(we, a, x, g) ? static_cast<float>(x) : kNaN;
          vdy[row + a] = cell(wd, a, x, g) ? static_cast<float>(x) : kNaN;
          if (cell(wi, a, x, g) && static_cast<float>(x) >= 1.f && static_cast<float>(x) < static_cast<float>(YPlanes::kInds))
            vind[row + a] = static_cast<uint8_t>(static_cast<float>(x));
        }
      }
      fold_legs(lab_i, lab_cap, Y.labels, t, row);
      fold_legs(cost_i, cost_cap, Y.costs, t, row);
    }
  });
  if (!read)
    return false;
  if (spec.index) {
    pool_index(ex, close, fmc.data(), Y.gate.data(), Y.sh.T, Y.sh.A, Y.index, Y.index_cap, Y.index_turn, Y.index_cap_turn);
    pool_val(ex, close, fmc.data(), vpx.data(), vsh.data(), vnp.data(), veq.data(), vdy.data(), vind.data(), Y.gate.data(), Y.sh.T, Y.sh.A, Y);
  }
  long long on = 0;
  for (const uint8_t g : Y.gate)
    on += g != 0;
  Y.n_full = static_cast<double>(on) / static_cast<double>(Y.sh.T) / factor::stat::kGroups;
  assert(Y.n_full > 0.0 && "池内无有效资产");
  return true;
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
  prog.begin(Phase::Loading, d1 - d0);
  if (!read_days(ft, es, lr, d0, d1, std::move(cols), ex, cancel, prog, [&](const auto &view, const Day &day, size_t, size_t tid) {
        gate_day(view, 0, 1, es, day, VR, A, has[tid], gate[tid].data());
        for (const uint8_t g : gate[tid])
          on[tid] += g != 0;
      }))
    return false;
  long long n = 0;
  for (const long long k : on)
    n += k;
  out = static_cast<double>(n) / static_cast<double>(sh.T) / factor::stat::kGroups;
  assert(out > 0.0 && "池内无有效资产");
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

std::string scope_key(const FeatureTable &ft, const EvalScope &es, size_t d0, size_t d1, const YSpec &spec) {
  return std::to_string(ft.seg) + "|" + es.key() + "|" + std::to_string(d0) + "-" + std::to_string(d1) + "|" + spec.key();
}

bool ensure_y(ScopeCache &c, const FeatureTable &ft, const EvalScope &es, const LibReaders &lr, size_t d0, size_t d1, const YSpec &spec,
              misc::exec::ForkJoin &ex, const std::atomic<bool> &cancel, Progress &prog) {
  assert(d0 < d1 && d1 <= es.dates.size());
  const std::string key = scope_key(ft, es, d0, d1, spec);
  const auto first = es.dates.begin() + static_cast<long>(d0), last = es.dates.begin() + static_cast<long>(d1);
  if (c.key == key && std::equal(c.dates.begin(), c.dates.end(), first, last))
    return true;
  c.reset();
  prog.begin(Phase::Loading, d1 - d0);
  if (!load_y(ft, es, lr, d0, d1, spec, c.y, ex, cancel, prog)) {
    c.reset();
    return false;
  }
  c.key = key;
  c.dates.assign(first, last);
  c.d0 = d0, c.d1 = d1;
  c.x.sh = c.y.sh;
  return true;
}

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
  prog.begin(Phase::Loading, c.d1 - c.d0);
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

std::vector<const factor::check::Plane *> leaves(const ScopeCache &c, const std::vector<std::string> &codes) {
  std::vector<const factor::check::Plane *> in(codes.size());
  for (size_t i = 0; i < codes.size(); ++i) {
    in[i] = c.x.find(codes[i]);
    assert(in[i] && "叶未装 (ensure_x)");
  }
  return in;
}

// 算子节点的特征叶元: parse 时放行 (值域只能查数据), 这里按 OpTable in 列的严格域 (dom_strict, 即组 id 的 INT) 逐元查数据
// (有效格全部落在值域内): CS 算子内部 max_gid 断言炸不得由数据触发. 非严格域越界格由算子自身置无效, 不在此判
void check_leaf_domains(const factor::Dag &d, int node, const std::vector<const factor::check::Plane *> &in, std::string &err) {
  const factor::DagNode &nd = d.nodes[static_cast<size_t>(node)];
  if (nd.op < 0 || nd.fu.n > 0) // 融合节点只装逐点 TS 算子, 无严格域元
    return;
  const factor::expr::OpInfo &o = factor::expr::kOps[nd.op];
  assert(nd.n_in == o.arity);
  for (int s = 0; s < o.arity; ++s) {
    const factor::DagNode &g = d.nodes[static_cast<size_t>(nd.in[s])];
    if (g.op >= 0 || !factor::dom_strict(o.in[s]))
      continue;
    const factor::check::Plane &p = *in[static_cast<size_t>(g.feat)];
    for (size_t i = 0; i < p.v.size(); ++i) {
      if (!p.m[i] || factor::expr::dom_holds(o.in[s], p.v[i]))
        continue;
      char buf[64];
      std::snprintf(buf, sizeof(buf), "%g", static_cast<double>(p.v[i]));
      err = std::string(o.name) + " 第 " + std::to_string(s + 1) + " 元要求 " + factor::dom_name(o.in[s]) + ", 特征 " +
            d.feats[static_cast<size_t>(g.feat)] + " 含越界值 " + buf;
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

void cost_tiers(misc::exec::ForkJoin &ex, const uint8_t *lc, const YPlanes &y, factor::cpu::stat::Workspace &w, std::vector<CostTier> &out) {
  const int C = y.ad.n, H = y.hd.n;
  assert(lc && y.costs.size() == static_cast<size_t>(C));
  out.assign(static_cast<size_t>(C), CostTier{});
  if (C == 0)
    return;
  std::vector<factor::stat::Cost> cost(static_cast<size_t>(C));
  for (int c = 0; c < C; ++c)
    cost[static_cast<size_t>(c)] = cost_of(y.costs[static_cast<size_t>(c)]);
  std::vector<factor::stat::CostAcc> acc(static_cast<size_t>(C) * H); // [c][hi]
  std::vector<long long> nx(static_cast<size_t>(1 + C));              // [0 = 有秩格数, 1 + c = 有秩 ∧ 成本 c 有效]
  factor::cpu::stat::cost_rows(ex, lc, y.sh.span(), y.hd, cost.data(), C, y.n_full, w, acc.data(), nx.data());
  for (int c = 0; c < C; ++c) {
    CostTier &tier = out[static_cast<size_t>(c)];
    tier.amt = y.ad.a[c];
    for (int i = 0; i < H; ++i)
      tier.acc[i] = acc[static_cast<size_t>(c) * H + i];
    tier.cover = nx[0] ? static_cast<float>(static_cast<double>(nx[static_cast<size_t>(1 + c)]) / static_cast<double>(nx[0])) : 0.f;
  }
}

void finish_root(misc::exec::ForkJoin &ex, const YPlanes &y, const RootRaw &r, factor::cpu::stat::Workspace &w, EvalStat &st, AlphaDist &adist) {
  assert(r.m && r.rx && r.lc && r.acc && r.alpha.v && r.alpha.m);
  const uint8_t *gate = y.gate.data();
  st.valid_pct = valid_pct_of(r.m, gate, y.sh.n());
  st.rank_pct = rank_pct_of(ex, r.rx, r.m, gate, y.sh.T, y.sh.A);
  st.n_hold = y.hd.n;
  factor::stat::finish_all(r.acc, y.hd, y.sh.seg, st.hold);
  alpha_dist(ex, r.alpha.v, r.alpha.m, r.rx, y.sh.T, y.sh.A, adist);
  cost_tiers(ex, r.lc, y, w, st.costs);
}

void alpha_dist(misc::exec::ForkJoin &ex, const float *v, const uint8_t *m, const uint16_t *rx, int T, int A, AlphaDist &o) {
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
  o = AlphaDist{};
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
