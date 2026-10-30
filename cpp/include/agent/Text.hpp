#pragma once

// =============================================================================
// Text — agent 成文的公共件: 字 (UTF-8 码点) / 数的写法 / 火花线
// =============================================================================
//   字数一律按码点算 (结果上限 kOutChars / 标题截断 / 研报片段都是字, 不是字节); 截断只落在码点边界; 短句上限 (功能块说明) 按 solid
// =============================================================================

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

namespace agent::text {

// ---- 字 ----
inline bool lead(char ch) { return (static_cast<unsigned char>(ch) & 0xC0) != 0x80; } // 码点首字节
inline size_t chars(std::string_view s) { return static_cast<size_t>(std::count_if(s.begin(), s.end(), lead)); }
// 实字数: 不计空白与标点 (ASCII 非字母数字、通用标点 U+2000–206F、CJK 标点 U+3000–303F、全角形 U+FF00–FFEF)
inline size_t solid(std::string_view s) {
  size_t n = 0;
  for (size_t i = 0; i < s.size();) {
    const unsigned char c = static_cast<unsigned char>(s[i]);
    const size_t len = c < 0x80 ? 1 : c < 0xE0 ? 2 : c < 0xF0 ? 3 : 4;
    uint32_t u = len == 1 ? c : c & (0x3Fu >> (len - 1));
    for (size_t k = 1; k < len && i + k < s.size(); ++k)
      u = u << 6 | (static_cast<unsigned char>(s[i + k]) & 0x3Fu);
    i += len;
    const bool alnum = (u >= '0' && u <= '9') || (u >= 'A' && u <= 'Z') || (u >= 'a' && u <= 'z');
    n += u < 0x80 ? alnum : !((u >= 0x2000 && u <= 0x206F) || (u >= 0x3000 && u <= 0x303F) || (u >= 0xFF00 && u <= 0xFFEF));
  }
  return n;
}
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

// ---- 火花线 ----
inline constexpr size_t kSparkChars = 60; // 一条至多几字 (长序列等宽分箱取均值)
inline size_t spark_bin(size_t n) { return (n + kSparkChars - 1) / kSparkChars; }
// 有限值线性映到 ▁..█ (箱内无有限值 ·); 每字 = spark_bin(v.size()) 个值的均值
inline std::string bars(const std::vector<double> &v) {
  static constexpr const char *kBar[8] = {"▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"};
  const size_t w = std::max<size_t>(1, spark_bin(v.size()));
  std::vector<double> b;
  for (size_t i = 0; i < v.size(); i += w) {
    double s = 0.0;
    int n = 0;
    for (size_t j = i; j < std::min(v.size(), i + w); ++j)
      if (std::isfinite(v[j]))
        s += v[j], ++n;
    b.push_back(n ? s / n : NAN);
  }
  double lo = INFINITY, hi = -INFINITY;
  for (const double x : b)
    if (std::isfinite(x))
      lo = std::min(lo, x), hi = std::max(hi, x);
  std::string s;
  for (const double x : b)
    s += !std::isfinite(x) ? "·" : kBar[hi > lo ? std::min(7, static_cast<int>((x - lo) / (hi - lo) * 8.0)) : 3];
  return s;
}

} // namespace agent::text
