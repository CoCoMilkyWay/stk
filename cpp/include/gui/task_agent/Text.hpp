#pragma once

// =============================================================================
// Text — agent 成文的公共件: 字 (UTF-8 码点) / 串 / 检索词 / 占位 {{键}} / 时刻 / 数 / 火花线 / Markdown 表
// =============================================================================
//   字数一律按码点算 (结果上限 kOutChars / 标题截断 / 研报片段都是字, 不是字节); 截断只落在码点边界
//   检索 (search_leaves / search_docs / search_factors / list_notes) 同一套: 词空白隔开、ASCII 小写, 按子串命中 (至多 kShortWord 个字母数字的短词只认整词),
//   条件全中才命中; 全中的没有且词 ≥ 2 个时放宽为含任一词 (list_notes 不放宽), 仍零命中报各条件单独的命中数
// =============================================================================

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <ctime>
#include <functional>
#include <initializer_list>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace GUI::Agent::text {

// ---- 字与串 ----
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

inline std::string_view trim(std::string_view s) {
  const size_t b = s.find_first_not_of(" \t\r\n");
  return b == std::string_view::npos ? std::string_view() : s.substr(b, s.find_last_not_of(" \t\r\n") - b + 1);
}
// 空白压成单个空格
inline std::string flat(std::string_view s) {
  std::string o;
  for (const char ch : s) {
    const bool sp = ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t';
    if (sp && (o.empty() || o.back() == ' '))
      continue;
    o += sp ? ' ' : ch;
  }
  return o;
}
inline std::string join(std::span<const std::string> v, std::string_view sep) {
  std::string s;
  for (size_t i = 0; i < v.size(); ++i) {
    if (i)
      s += sep;
    s += v[i];
  }
  return s;
}
// 表格单元: 竖线转义, 换行压成空格
inline std::string cell(std::string_view s) {
  std::string o;
  o.reserve(s.size());
  for (const char ch : s) {
    if (ch == '|')
      o += "\\|";
    else
      o += ch == '\n' ? ' ' : ch;
  }
  return o;
}

// ---- Markdown 表 ----
struct Table {
  size_t cols = 0;
  std::string out;

  explicit Table(std::span<const std::string> hs) : cols(hs.size()) {
    assert(cols > 0);
    out = "|";
    for (const std::string &h : hs)
      out += " " + cell(h) + " |";
    out += "\n|";
    for (size_t i = 0; i < cols; ++i)
      out += "---|";
    out += "\n";
  }
  explicit Table(std::initializer_list<std::string> hs) : Table(std::span<const std::string>(hs.begin(), hs.size())) {}

  void add(std::span<const std::string> row) {
    assert(row.size() == cols && "表格行列数须一致");
    out += "|";
    for (const std::string &v : row)
      out += " " + cell(v) + " |";
    out += "\n";
  }
  void add(std::initializer_list<std::string> row) { add(std::span<const std::string>(row.begin(), row.size())); }
  const std::string &str() const { return out; }
};

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
// hay 含词 w: 至多 kShortWord 个 ASCII 字母数字的短词只认整词 (两侧不接同类字符: 字母不接字母、数字不接数字; ep 不中 repay, 30 中 lag30), 其余按子串
inline constexpr size_t kShortWord = 3;
inline bool has_word(std::string_view hay, std::string_view w) {
  const auto cls = [](char ch) { return ch >= 'a' && ch <= 'z' ? 1 : ch >= '0' && ch <= '9' ? 2
                                                                                            : 0; };
  if (w.size() > kShortWord || std::any_of(w.begin(), w.end(), [&](char ch) { return cls(ch) == 0; }))
    return hay.find(w) != std::string_view::npos;
  for (size_t p = hay.find(w); p != std::string_view::npos; p = hay.find(w, p + 1))
    if ((p == 0 || cls(hay[p - 1]) != cls(w.front())) && (p + w.size() == hay.size() || cls(hay[p + w.size()]) != cls(w.back())))
      return true;
  return false;
}
// 具名条件的合取: 第 i 项满足全部条件才命中; 检索词另可放宽为析取 (hits)
struct Filter {
  struct Cond {
    std::string name; // 写法 (零命中时报)
    std::function<bool(size_t)> pass;
    bool word = false; // 检索词 (放宽时只须含其一)
  };
  std::vector<Cond> conds;
  void add(std::string name, std::function<bool(size_t)> pass, bool word = false) { conds.push_back({std::move(name), std::move(pass), word}); }
  template <class Hay>
  void add(const std::vector<std::string> &ws, const std::string &prefix, Hay hay) {
    for (const std::string &w : ws)
      add(prefix + w, [hay, w](size_t i) { return has_word(hay(i), w); }, true);
  }
  bool operator()(size_t i) const {
    return std::all_of(conds.begin(), conds.end(), [&](const Cond &c) { return c.pass(i); });
  }
  bool loose(size_t i) const {
    return std::all_of(conds.begin(), conds.end(), [&](const Cond &c) { return c.word || c.pass(i); }) &&
           std::any_of(conds.begin(), conds.end(), [&](const Cond &c) { return c.word && c.pass(i); });
  }
  std::vector<size_t> hits(size_t n, bool &loosened) const {
    std::vector<size_t> o;
    for (size_t i = 0; i < n; ++i)
      if ((*this)(i))
        o.push_back(i);
    if (o.empty() && std::count_if(conds.begin(), conds.end(), [](const Cond &c) { return c.word; }) >= 2)
      for (size_t i = 0; i < n; ++i)
        if (loose(i))
          o.push_back(i);
    loosened = !o.empty() && !(*this)(o.front());
    return o;
  }
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

// ---- 占位 ----
template <class Val>
std::string fill(std::string_view t, Val &&val) {
  std::string out;
  for (size_t i = 0;;) {
    const size_t a = t.find("{{", i);
    if (a == std::string_view::npos) {
      out.append(t.substr(i));
      return out;
    }
    const size_t b = t.find("}}", a);
    assert(b != std::string_view::npos && "占位 {{ 未闭合");
    out.append(t.substr(i, a - i));
    out += val(t.substr(a + 2, b - a - 2));
    i = b + 2;
  }
}

// ---- 时刻 ----
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
inline std::string num(double v) { return std::isfinite(v) ? fmt("%.4g", v) : "—"; }       // 统计量 / 门槛 / 表格数; 非有限值 —
inline std::string bp(double r) { return std::isfinite(r) ? fmt("%+.1f", r * 1e4) : "—"; } // 收益 → bp (带符号); 非有限值 —
inline std::string pct(double x) { return std::isfinite(x) ? fmt("%.0f%%", x * 100.0) : "—"; }
inline std::string chg(double x) { return std::isfinite(x) ? fmt("%+.2f%%", x * 100.0) : "—"; }
inline std::string r3(double x) { return std::isfinite(x) ? fmt("%.3f", x) : "—"; }

// ---- 火花线 ----
inline constexpr size_t kSparkChars = 60; // 一条至多几字 (长序列等宽分箱取均值)
inline size_t spark_bin(size_t n) { return (n + kSparkChars - 1) / kSparkChars; }
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
inline std::string spark(const std::vector<double> &v, std::string (*f)(double) = num) {
  double lo = INFINITY, hi = -INFINITY;
  for (const double x : v)
    if (std::isfinite(x))
      lo = std::min(lo, x), hi = std::max(hi, x);
  const size_t w = spark_bin(v.size());
  return "`" + bars(v) + "`" + (lo <= hi ? "  [" + f(lo) + ", " + f(hi) + "]" : "") + (w > 1 ? " (每字 = 相邻 " + std::to_string(w) + " 个的均值)" : "");
}

// ---- 正负柱 (一个值一列一字, 共两行: 上行 |值| 的柱 (8 档, 非零至少 1 档), 下行对齐写号 + / −; 零写空, 非有限值上行写 ·) ----
//   返回 "- 题 (…):" + 两行代码块 (柱行后接 [f(最小), f(最大)]; 满格 = 两者绝对值较大者)
inline std::string vbars(const std::string &title, const std::vector<double> &v, std::string (*f)(double)) {
  static constexpr const char *kBar[8] = {"▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"};
  assert(!v.empty());
  double lo = INFINITY, hi = -INFINITY;
  for (const double x : v)
    if (std::isfinite(x))
      lo = std::min(lo, x), hi = std::max(hi, x);
  const double top = lo <= hi ? std::max(std::abs(lo), std::abs(hi)) : 0.0;
  const auto bar = [&](double x) { return kBar[std::clamp(static_cast<int>(std::ceil(x / top * 8.0)), 1, 8) - 1]; };
  std::string up, sign;
  for (const double x : v) {
    const bool ok = std::isfinite(x) && x != 0.0;
    up += !std::isfinite(x) ? "·" : ok ? bar(std::abs(x))
                                       : " ";
    sign += !ok ? " " : x > 0.0 ? "+"
                                : "-";
  }
  return "- " + title + " (上行柱高 ∝ |值|, 下行是号):\n```\n" + up + (lo <= hi ? "  [" + f(lo) + ", " + f(hi) + "]" : "") + "\n" + sign + "\n```\n";
}

} // namespace GUI::Agent::text
