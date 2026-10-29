// FactorsScope — 见头文件. include 因子契约头 (FactorsTable → Stat/Contract.hpp), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsScope.hpp"

#include "factor/GpuRun.hpp"       // mem_info (显存预算)
#include "misc/cross_platform.hpp" // memory_usage
#include "misc/date.hpp"           // months_between
#include "misc/profiler.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>

namespace GUI::Factors {

void open_libs(const FactorScope &sc, LibReaders &lr, std::atomic<bool> *abort) {
  PerPool<FeatureLib> ready;
  for (const PoolId l : pool_ids())
    if (sc.libs[l].ready())
      ready[l] = sc.libs[l];
  lr.open(ready, abort);
}

std::string Domain::key() const {
  std::string k = std::to_string(pool);
  for (const PoolId l : pool_ids())
    if (used & pool_bit(l))
      k += "|" + rs.libs[l].dir + "#" + std::to_string(rs.libs[l].axis.hash);
  return k + "|" + std::to_string(dates.front()) + "-" + std::to_string(dates.back()) + "#" + std::to_string(dates.size());
}

bool domain_of(const FeatureTable &ft, const FactorScope &sc, const std::vector<LeafId> &leaves, size_t keep, Domain &dm, std::string &why) {
  TraceN("Domain");
  assert(sc.lv == ft.lv && "作用域层与字段表层不符");
  assert(keep >= 1);
  const FactorLevel &lv = factor_level(ft.lv);
  const PoolId pool = pool_id(sc.pool);
  assert(level_for_pool(lv, pool_tier(pool)) && "该池不能用这个因子层");
  const PoolId lab = lv.lab(pool);
  assert(ft.y.ts.ref(pool).pool == lab && ft.y.cs.ref(pool).pool == lab && "门控 / 标签 / 成本 / K 线在标签库");
  const bool sub = pool != lab; // Lob 池 × Bar 标签: 截面再与上池成员
  const PerPool<FeatureLib> &libs = sc.libs;
  dm = Domain{};
  dm.pool = pool;
  dm.used = pool_bit(lab);
  for (const LeafId id : leaves) {
    const FeatCol &c = ft.x.leaves[id];
    assert(c.allowed);
    dm.used |= pool_bit(c.ref(pool).pool);
  }
  for (const PoolId l : pool_ids())
    if (dm.used & pool_bit(l)) {
      assert(!libs[l].name.empty() && "请求没带该库清单 (factor_libs)");
      if (!libs[l].ready()) {
        why = std::string(pool_def(l).name) + " 池特征库未建 (先算特征)";
        return false;
      }
      dm.rs.libs[l] = libs[l];
    }
  // 评估期: 标签库日期轴上 used 各库都落了盘的日; 子池的成员读 D−1 收盘行, 还要 all 池前一日落盘
  const FeatureLib &L0 = libs[lab];
  const bool need_prev = sub;
  PoolMask m_prev = 0; // 标签库日期轴前一日的落盘位
  for (size_t i = 0; i < L0.dates.size(); ++i) {
    const misc::Ymd d = L0.dates[i];
    PoolMask m = 0;
    for (const PoolId l : pool_ids())
      if ((dm.used & pool_bit(l)) && analysis::landed(libs[l], d))
        m |= pool_bit(l);
    if (m == dm.used && (!need_prev || (i > 0 && (m_prev & pool_bit(kAllPool))))) {
      dm.dates.push_back(d);
      dm.days.push_back(Day{d, i > 0 ? L0.dates[i - 1] : 0, m, i > 0 ? m_prev : 0});
    }
    m_prev = m;
  }
  if (dm.dates.empty()) {
    why = "所用特征库无共同落盘日";
    for (const PoolId l : pool_ids())
      if (dm.used & pool_bit(l))
        why += std::string(" ") + pool_def(l).name;
    return false;
  }
  if (dm.days.size() > keep) {
    const auto cut = static_cast<std::ptrdiff_t>(dm.days.size() - keep);
    dm.dates.erase(dm.dates.begin(), dm.dates.begin() + cut);
    dm.days.erase(dm.days.begin(), dm.days.begin() + cut);
  }
  if (sub)
    dm.member = pool::Panel::build(libs[kAllPool], pool_def(pool).pool_col, dm.dates);
  dm.rs.main = lab;
  dm.rs.days = dm.used;
  dm.rs.months = misc::months_between(dm.dates.front(), dm.dates.back());
  analysis::map_axis(dm.rs, sub ? dm.member->axis().ids : std::vector<uint32_t>{});
  return true;
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

double whole_bytes(const Domain &dm, int seg, const Budget &b) {
  return static_cast<double>(dm.days.size()) * seg * static_cast<double>(dm.rs.axis.size()) * (b.load + b.work) + b.fixed;
}

double whole_dev_bytes(const Domain &dm, int seg, const Budget &b) {
  return static_cast<double>(dm.days.size()) * seg * static_cast<double>(dm.rs.axis.size()) * b.dev;
}

int Plan::max_days() const {
  int m = 0;
  for (const Chunk &k : chunks)
    m = std::max(m, k.days());
  return m;
}

Plan plan_chunks(const Domain &dm, int seg, const Budget &b, int carry) {
  const int days = static_cast<int>(dm.days.size()), A = static_cast<int>(dm.rs.axis.size());
  assert(days > 0 && A > 0);
  const double per_day = static_cast<double>(seg) * A;
  Plan P;
  P.host = host_budget() - b.fixed;
  if (b.dev > 0.0)
    P.vram = vram_budget();
  if (carry < 0) {
    P.chunks = {Chunk{0, 0, static_cast<size_t>(days)}};
    P.D = days;
    P.cache = days * per_day * b.load;
    return P;
  }
  assert(P.host > 0.0 && "内存预算装不下与格无关的常驻量");
  int D = std::clamp(static_cast<int>(P.host / (per_day * (b.load + b.work))) - carry, 1, days);
  if (b.dev > 0.0)
    D = std::min(D, std::clamp(static_cast<int>(P.vram / (per_day * b.dev)) - carry, 1, days));
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
