#pragma once

// =============================================================================
// 快照档 (双向): 类型只写一份成员清单 template <class Ar> void snap(Ar &ar), Save 走一遍即写出, Load 走一遍即读回 —
//   写与读同一份清单, 次序 / 宽度不可能两边写歪. 记录不自描述: 布局变了由持有方的指纹判废 (见 FeatureSnap.hpp).
//   ar(x)       平凡可拷贝 (标量 / 定长数组 / std::array / 环形缓冲): 原样字节. 记录定长, 堆上状态先折成定长再列
//   Ar::kLoad   读向标记: 只在一个方向做的事 (写前归一化 / 读后派生) 按它分支
//   出错策略: 只 assert (读越界 = 记录与清单不符).
// =============================================================================

#include <cassert>
#include <cstddef>
#include <cstring>
#include <span>
#include <type_traits>
#include <vector>

namespace misc::snap {

class Save {
public:
  static constexpr bool kLoad = false;
  explicit Save(std::vector<std::byte> &out) : out_(out) {}

  template <class T>
  void operator()(const T &x) {
    static_assert(std::is_trivially_copyable_v<T>, "快照成员须平凡可拷贝");
    const auto *p = reinterpret_cast<const std::byte *>(&x);
    out_.insert(out_.end(), p, p + sizeof(T));
  }

private:
  std::vector<std::byte> &out_;
};

class Load {
public:
  static constexpr bool kLoad = true;
  explicit Load(std::span<const std::byte> in) : p_(in.data()), end_(in.data() + in.size()) {}

  template <class T>
  void operator()(T &x) {
    static_assert(std::is_trivially_copyable_v<T>, "快照成员须平凡可拷贝");
    take(&x, sizeof(T));
  }
  // 记录恰好读尽 (清单与记录逐字节对上)
  bool done() const { return p_ == end_; }

private:
  void take(void *dst, size_t n) {
    assert(static_cast<size_t>(end_ - p_) >= n && "快照记录短于成员清单");
    std::memcpy(dst, p_, n);
    p_ += n;
  }
  const std::byte *p_, *end_;
};

} // namespace misc::snap
