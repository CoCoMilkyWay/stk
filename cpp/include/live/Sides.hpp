// 买 / 卖镜像: 方向 买 +1 / 卖 −1, 对仗容器 Sides<T> (下标 s < 0); 实盘各层与账户成交点同用
#pragma once

namespace live {

inline constexpr int kBuy = 1, kSell = -1;
inline constexpr int kSides[2] = {kBuy, kSell};
inline const char *side_name(int s) { return s > 0 ? "买入" : "卖出"; }

// 买/卖镜像对仗容器: 下标 +1 (买) → v[0], −1 (卖) → v[1]
template <class T>
struct Sides {
  T v[2]{};
  T &operator[](int s) { return v[s < 0]; }
  const T &operator[](int s) const { return v[s < 0]; }
  bool operator==(const Sides &) const = default;
};

} // namespace live
