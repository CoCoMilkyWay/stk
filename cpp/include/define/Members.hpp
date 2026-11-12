#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

// ============================================================================
// Members — 成员表 (一日的持仓, 资产主序的稀疏 CSR): 行 k = 资产 id[k] (升序, 只收有所属的), 所属 = of(k) (键升序), 各带权 w > 0
//   键 = 篮子下标 (shared/Basket.hpp; 行情价格指数 / 回测池篮子) 或权重 (pool::run_book 的账); 资产 = 调用方的轴
//   合 scatter: 各键 Σ w · x(a) / Σ w
//   两向: scale 过一日 (随价漂移) ⇄ turnover 换仓 (目标对漂移后的增仓量)
// ============================================================================
struct Members {
  struct Weight {
    uint32_t key;
    float w;
  };
  std::vector<uint32_t> id;    // [行] 资产
  std::vector<uint32_t> at{0}; // [行 + 1]
  std::vector<Weight> ws;
  std::vector<float> cap; // [行] 收盘总市值 (与权无关: 普查求和 / 加权换源的目标权); 不用的记 0

  void clear() { id.clear(), at.assign(1, 0), ws.clear(), cap.clear(); }
  // 本只入一个键 (键升序, w > 0)
  void add(uint32_t key, float w) {
    assert(w > 0 && (ws.size() == at.back() || ws.back().key < key));
    ws.push_back({key, w});
  }
  // 收一只 (资产升序): 无所属的不收
  void next(uint32_t a, float c = 0) {
    if (ws.size() == at.back())
      return;
    assert(id.empty() || id.back() < a);
    id.push_back(a), at.push_back(static_cast<uint32_t>(ws.size())), cap.push_back(c);
  }
  size_t size() const { return id.size(); }
  std::span<const Weight> of(size_t k) const { return {ws.data() + at[k], ws.data() + at[k + 1]}; }
  std::span<Weight> of(size_t k) { return {ws.data() + at[k], ws.data() + at[k + 1]}; }

  // s[键] += Σ w · x(a), n[键] += Σ w
  template <class X>
  void scatter(X &&x, std::span<double> s, std::span<double> n) const {
    for (size_t k = 0; k < size(); ++k) {
      const double v = x(id[k]);
      for (const Weight &e : of(k))
        s[e.key] += e.w * v, n[e.key] += e.w;
    }
  }
  // 过一日: 权与 cap × rel(a)
  template <class Rel>
  void scale(Rel &&rel) {
    for (size_t k = 0; k < size(); ++k) {
      const float r = rel(id[k]);
      cap[k] *= r;
      for (Weight &e : of(k))
        e.w *= r;
    }
  }
  // 换仓 (本表 = 目标, from = 漂移到换仓时的旧表): buy[键] += Σ (w / Σw − w̃ / Σw̃)⁺; 键在 from 里空 ⇒ 建仓 = 1
  void turnover(const Members &from, std::span<double> buy) const {
    std::vector<double> n(buy.size()), nf(buy.size());
    for (const Weight &e : ws)
      n[e.key] += e.w;
    for (const Weight &e : from.ws)
      nf[e.key] += e.w;
    for (size_t k = 0, j = 0; k < size(); ++k) {
      while (j < from.size() && from.id[j] < id[k])
        ++j;
      const std::span<const Weight> was = j < from.size() && from.id[j] == id[k] ? from.of(j) : std::span<const Weight>{};
      auto u = was.begin();
      for (const Weight &e : of(k)) {
        while (u != was.end() && u->key < e.key)
          ++u;
        const double w0 = u != was.end() && u->key == e.key ? u->w / nf[e.key] : 0.0;
        buy[e.key] += std::max(0.0, e.w / n[e.key] - w0);
      }
    }
  }
};
