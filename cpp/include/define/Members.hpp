#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>
#include <type_traits>
#include <vector>

// ============================================================================
// Members — 成员表 (一日的持仓, 资产主序的稀疏 CSR): 行 k = 资产 id[k] (升序, 只收有所属的), 所属 = of(k) (键升序), 各带权 w > 0
//   键 = 篮子下标 (shared/Basket.hpp; 行情价格指数 / 回测池篮子的画像); 资产 = 调用方的轴
//   合 scatter: 各键对有限值 x(a) 累 Σ w · x(a) 与 Σ w
//   过日: scale (权随价漂移); 篮子的可成交收益与换手走 factor/Stat 账核 (Pos.hpp / Turn.hpp), 不在这里
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
};
