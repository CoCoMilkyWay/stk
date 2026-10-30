#pragma once

// =============================================================================
// Text — agent 成文的公共件: 字 (UTF-8 码点) / 数的写法
// =============================================================================
//   字数一律按码点算 (结果上限 kOutChars / 标题截断 / 研报片段都是字, 不是字节); 截断只落在码点边界
// =============================================================================

#include <algorithm>
#include <cstdio>
#include <string>
#include <string_view>

namespace agent::text {

// ---- 字 ----
inline bool lead(char ch) { return (static_cast<unsigned char>(ch) & 0xC0) != 0x80; } // 码点首字节
inline size_t chars(std::string_view s) { return static_cast<size_t>(std::count_if(s.begin(), s.end(), lead)); }
// 字节位 i 起前进 / 后退 n 字 (到头为止)
inline size_t fwd(std::string_view s, size_t i, size_t n) {
  for (; i < s.size() && n > 0; --n)
    for (++i; i < s.size() && !lead(s[i]);)
      ++i;
  return i;
}
inline size_t bwd(std::string_view s, size_t i, size_t n) {
  for (; i > 0 && n > 0; --n)
    for (--i; i > 0 && !lead(s[i]);)
      --i;
  return i;
}
// 前 n 字, 截了补 …
inline std::string head(std::string_view s, size_t n) {
  const size_t e = fwd(s, 0, n);
  return std::string(s.substr(0, e)) + (e < s.size() ? "…" : "");
}

// ---- 数 ----
inline std::string fmt(const char *f, double v) {
  char b[64];
  std::snprintf(b, sizeof(b), f, v);
  return b;
}
inline std::string num(double v) { return fmt("%.3g", v); } // 门槛 / 统计量

} // namespace agent::text
