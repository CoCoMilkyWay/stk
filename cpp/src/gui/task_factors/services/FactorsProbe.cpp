// FactorsProbe — 见头文件. include Stat CPU 流程头 (cost_rows), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsProbe.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>

namespace GUI::Factors {

void valid_count(const uint8_t *m, const uint8_t *gate, size_t i0, size_t i1, uint64_t &on, uint64_t &all) {
  for (size_t i = i0; i < i1; ++i) {
    on += m[i] & gate[i];
    all += gate[i];
  }
}

void rank_sum(misc::exec::ForkJoin &ex, const uint16_t *rx, const uint8_t *m, const uint8_t *gate, int t0, int t1, int A, double &sum_out,
              long long &rows_out) {
  const int nt = ex.threads();
  std::vector<double> sum(static_cast<size_t>(nt), 0.0);
  std::vector<long long> rows(static_cast<size_t>(nt), 0);
  ex.rows(t1 - t0, [&](int r0, int r1, int tid) {
    std::vector<uint16_t> buf(static_cast<size_t>(A));
    double s = 0.0;
    long long nr = 0;
    for (int t = t0 + r0; t < t0 + r1; ++t) {
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
  for (int k = 0; k < nt; ++k)
    sum_out += sum[static_cast<size_t>(k)], rows_out += rows[static_cast<size_t>(k)];
}

void cost_tiers(misc::exec::ForkJoin &ex, const uint8_t *lc, const factor::stat::Span &s, const factor::stat::Labels &L, const YPlanes &y,
                factor::cpu::stat::Workspace &w, std::vector<CostTier> &out, factor::stat::CostAcc *qty, factor::stat::CostAcc *land) {
  const int C = y.ad.n, H = L.hd.n;
  assert(lc && qty && y.costs.size() == static_cast<size_t>(C));
  if (out.empty()) {
    out.assign(static_cast<size_t>(C), CostTier{});
    for (int c = 0; c < C; ++c)
      out[static_cast<size_t>(c)].amt = y.ad.a[c];
  }
  assert(out.size() == static_cast<size_t>(C));
  std::vector<factor::stat::Cost> cost(static_cast<size_t>(C));
  for (int c = 0; c < C; ++c)
    cost[static_cast<size_t>(c)] = cost_of(y.costs[static_cast<size_t>(c)]);
  std::vector<factor::stat::CostAcc> acc(static_cast<size_t>(std::max(C, 1)) * H); // [c][hi]
  factor::cpu::stat::cost_rows(ex, lc, s, L, cost.data(), C, w, acc.data(), land);
  for (int i = 0; i < H; ++i) { // 量各段同一份 (cost_row): 取段 0, 冲击清零
    factor::stat::CostAcc q = acc[static_cast<size_t>(i)];
    for (factor::stat::CostLeg &e : q.leg)
      e.imp = 0.0;
    qty[i].merge(q);
  }
  for (int c = 0; c < C; ++c)
    for (int i = 0; i < H; ++i)
      out[static_cast<size_t>(c)].acc[i].merge(acc[static_cast<size_t>(c) * H + i]);
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

} // namespace GUI::Factors
