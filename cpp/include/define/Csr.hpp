#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>
#include <type_traits>
#include <vector>

// ============================================================================
// Csr — 一对多的稠密表: 行 i 的元素 = v[off[i], off[i + 1])
//   bucket: 计数排序分桶 (两遍, 无比较), 桶内保持输入序 ⇒ 输入已按次键排好, 桶内即按次键有序
// ============================================================================
template <class T>
struct Csr {
  std::vector<std::uint32_t> off{0}; // size() + 1
  std::vector<T> v;

  std::size_t size() const { return off.size() - 1; }
  std::span<const T> operator[](std::size_t i) const {
    assert(i < size() && "Csr: 行越界");
    return {v.data() + off[i], v.data() + off[i + 1]};
  }
};

// src 的每个元素 x 落行 key(x) ∈ [0, n), 值 = val(x)
template <class Src, class Key, class Val>
auto bucket(std::size_t n, const std::vector<Src> &src, Key key, Val val) {
  Csr<std::remove_cvref_t<std::invoke_result_t<Val &, const Src &>>> out;
  out.off.assign(n + 1, 0);
  for (const Src &x : src) {
    const std::size_t k = key(x);
    assert(k < n && "bucket: 键越界");
    ++out.off[k + 1];
  }
  for (std::size_t i = 0; i < n; ++i)
    out.off[i + 1] += out.off[i];
  std::vector<std::uint32_t> at(out.off.begin(), out.off.end() - 1);
  out.v.resize(src.size());
  for (const Src &x : src)
    out.v[at[key(x)]++] = val(x);
  return out;
}
