#pragma once

// 吃单核 (写端 LabelLob 的建仓冲击 / 实盘调仓与盘口的滑点同一份): 自对手盘首档起逐档吃 amount 元, 本档成交额 = min(余额, 价 × 量);
//   全簿吃完仍有余额且 limit > 0 → 余额按 limit 价成交 (写端: 涨跌停价; 实盘不补, 给 0); 档 i 由 at(i, 价, 量) 给, 价或量 ≤ ε 的档跳过.
//   T = float (写端, 与落盘逐位一致) / double (实盘); 参考价 (中间价 / 现价) 由调用方各自定
#include <algorithm>

template <class T>
struct Swept {
  T px = 0;   // 成交均价; 一股没吃到 = 0
  T left = 0; // 吃剩的余额 (> ε = 盘口不够吃)
};

template <class T, class At>
Swept<T> sweep(int n, At &&at, T amount, T limit) {
  constexpr T eps = static_cast<T>(1e-6);
  T cost = 0, sh = 0;
  for (int i = 0; i < n && amount > eps; ++i) {
    T p = 0, q = 0;
    at(i, p, q);
    if (p < eps || q < eps)
      continue;
    const T fill = std::min(amount, p * q);
    cost += fill;
    sh += fill / p;
    amount -= fill;
  }
  Swept<T> o;
  o.left = amount;
  if (amount > eps && limit > 0) {
    cost += amount;
    sh += amount / limit;
    o.left = 0;
  }
  o.px = sh > eps ? cost / sh : 0;
  return o;
}
