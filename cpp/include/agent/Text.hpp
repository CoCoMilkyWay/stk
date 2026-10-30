#pragma once

// =============================================================================
// Text — agent 成文的公共件: 字 (UTF-8 码点) / 检索词 / 时刻 / 数的写法 / 火花线
// =============================================================================
//   字数一律按码点算 (结果上限 kOutChars / 标题截断 / 研报片段都是字, 不是字节); 截断只落在码点边界; 短句上限 (功能块说明) 按 solid
//   检索 (search_leaves / search_docs / search_factors / list_notes) 同一套: 词空白隔开、ASCII 小写, 条件全中才命中, 零命中报各条件单独的命中数
// =============================================================================

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <functional>
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
    const size_t len = c < 0x80 ? 1 : c < 0xE0 ? 2
                                  : c < 0xF0   ? 3
                                               : 4;
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

// ---- 检索 ----
inline std::string lower(std::string s) { // 只动 ASCII, 字节长不变
  for (char &ch : s)
    if (ch >= 'A' && ch <= 'Z')
      ch = static_cast<char>(ch - 'A' + 'a');
  return s;
}
// 检索词: 空白隔开, 小写
inline std::vector<std::string> words(std::string_view q) {
  std::vector<std::string> ws;
  for (size_t a = q.find_first_not_of(" \t\r\n"); a != std::string_view::npos;) {
    const size_t b = std::min(q.find_first_of(" \t\r\n", a), q.size());
    ws.push_back(lower(std::string(q.substr(a, b - a))));
    a = q.find_first_not_of(" \t\r\n", b);
  }
  return ws;
}
// 具名条件的合取: 第 i 项满足全部条件才命中
struct Filter {
  struct Cond {
    std::string name; // 写法 (零命中时报)
    std::function<bool(size_t)> pass;
  };
  std::vector<Cond> conds;
  void add(std::string name, std::function<bool(size_t)> pass) { conds.push_back({std::move(name), std::move(pass)}); }
  // hay(i) (已小写) 含词 w, 一词一条
  template <class Hay>
  void add(const std::vector<std::string> &ws, const std::string &prefix, Hay hay) {
    for (const std::string &w : ws)
      add(prefix + w, [hay, w](size_t i) { return std::string_view(hay(i)).find(w) != std::string_view::npos; });
  }
  bool operator()(size_t i) const {
    return std::all_of(conds.begin(), conds.end(), [&](const Cond &c) { return c.pass(i); });
  }
  // 共 n 项时各条件单独的命中数 "名 k · 名 k"
  std::string alone(size_t n) const {
    std::string o;
    for (const Cond &c : conds) {
      size_t k = 0;
      for (size_t i = 0; i < n; ++i)
        k += c.pass(i);
      o += (o.empty() ? "" : " · ") + c.name + " " + std::to_string(k);
    }
    return o;
  }
};

// ---- 时刻 ----
// 本地时刻按 strftime 格式 f; ms 另补 ".毫秒"
inline std::string local(const char *f, bool ms = false) {
  const auto t = std::chrono::system_clock::now();
  const std::time_t s = std::chrono::system_clock::to_time_t(t);
  std::tm tm{};
  localtime_r(&s, &tm);
  char b[64];
  const size_t n = std::strftime(b, sizeof(b), f, &tm);
  if (ms)
    std::snprintf(b + n, sizeof(b) - n, ".%03d", static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count() % 1000));
  return b;
}

// ---- 数 ----
inline std::string fmt(const char *f, double v) {
  char b[64];
  std::snprintf(b, sizeof(b), f, v);
  return b;
}
inline std::string num(double v) { return std::isfinite(v) ? fmt("%.4g", v) : "—"; } // 统计量 / 门槛 / 表格数; 非有限值 —

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
