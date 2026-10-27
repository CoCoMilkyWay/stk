// FactorsScope — 见头文件. include 因子契约头 (FactorsTable → Stat/Contract.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsScope.hpp"

#include "factor/GpuRun.hpp" // mem_info (显存预算)
#include "features/Storage/FeatureRead.hpp"
#include "misc/cross_platform.hpp" // memory_usage
#include "misc/date.hpp"           // months_between
#include "misc/profiler.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>

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

Plan plan_chunks(const EvalScope &es, int seg, const Budget &b, int carry, const char *who) {
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

} // namespace GUI::Factors
