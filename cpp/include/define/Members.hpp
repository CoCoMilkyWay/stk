#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

// ============================================================================
// Members — 成员表 (一日的持仓, 资产主序的稀疏 CSR): 行 k = 资产 id[k] (升序, 只收有所属的), 所属 = of(k) (键升序), 各带权 w > 0
//   键 = 篮子下标 (shared/Basket.hpp; 行情价格指数 / 回测池篮子) 或权重 (pool::perf 的账); 资产 = 调用方的轴
//   合 scatter: 各键对有限值 x(a) 累 Σ w · x(a) 与 Σ w
//   两向: scale 过一日 (随价漂移) ⇄ turnover 换仓 (目标对漂移后的增仓量)
// Sleeve — 篮子账 (Style 篮子账 / 回测池篮子同一核, 自持工作区零重分配): 持仓 hold 两步对仗
//   roll 换源: buy[键] += 目标 to 对 hold 的增仓量, hold ← to ⇄ pass 过一日: ret[键] = 合 rel − 1, hold 随价 × rel
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

  // s[键] += Σ w · x(a), n[键] += Σ w; x(a, v) 返回 bool 时只在真时计入
  template <class X>
  void scatter(X &&x, std::span<double> s, std::span<double> n) const {
    for (size_t k = 0; k < size(); ++k) {
      double v = 0.0;
      if constexpr (std::is_invocable_r_v<bool, X &, uint32_t, double &>) {
        if (!x(id[k], v))
          continue;
      } else {
        v = x(id[k]);
      }
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
  // 换仓 (本表 = 目标, from = 漂移到换仓时的旧表; n / nf = 调用方给的 [buy.size()] 已清零工作区):
  //   buy[键] += Σ (w / Σw − w̃ / Σw̃)⁺; 键在 from 里空 ⇒ 建仓 = 1
  void turnover(const Members &from, std::span<double> buy, std::span<double> n, std::span<double> nf) const {
    assert(n.size() == buy.size() && nf.size() == buy.size());
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

struct Sleeve {
  Members hold; // 随价漂移到上一收盘的持仓

  // 换源 (to 交出新目标, 换回旧持仓)
  void roll(Members &to, std::span<double> buy) {
    s_.assign(buy.size(), 0.0), n_.assign(buy.size(), 0.0);
    to.turnover(hold, buy, s_, n_);
    std::swap(hold, to);
  }
  // 过一日: ret 空 = 只漂移; 键无仓 → 0
  template <class Rel>
  void pass(Rel &&rel, std::span<double> ret) {
    if (!ret.empty()) {
      s_.assign(ret.size(), 0.0), n_.assign(ret.size(), 0.0);
      hold.scatter(rel, s_, n_);
      for (size_t k = 0; k < ret.size(); ++k)
        ret[k] = n_[k] > 0.0 ? s_[k] / n_[k] - 1.0 : 0.0;
    }
    hold.scale(rel);
  }

private:
  std::vector<double> s_, n_;
};
