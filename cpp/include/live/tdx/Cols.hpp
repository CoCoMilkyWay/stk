// 通达信适配: 应答表的列声明、解析与索引 (各表的声明并排在 Tables.hpp)
//   应答 = 各行各列的串, 列序即协议字典里该功能号的出参序;
//   kViews 各布局 {应答它的功能号, 列数 (出参数 + 1)}; kCols 行的各成员 ← 各布局的列号 (kNone = 该布局没有) 经转型 as::*,
//     各列带客户端字段名 (shim test fields 对协议字典核); check(行) 行级交叉校验; Index 各行 → 读出的样子
//   Unique<键> 键不重 → Keyed<行> / Tally 成交 → 时间序 + 按代码合计 (Fills) / Pairs 名称不重 → 名称 → 数值原文 / Plain 行原序
#pragma once

#include "live/Broker.hpp"
#include "live/Trace.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace live::tdx::cols {

using Cells = std::vector<std::string>; // 一行各列

inline constexpr int kNone = -1;

// 一种布局: 应答它的功能号, 列数 (出参数 + 1)
struct View {
  int func, cols;
};

const std::string &cell(const Cells &f, int c);
bool isclose(double a, double b, double rel, double abs);
void need_row(const char *tab, bool ok, const std::string &key, const char *what); // 行级校验不过 → Fail

template <size_t N>
struct Lit {
  char s[N];
  constexpr Lit(const char (&a)[N]) { std::copy_n(a, N, s); }
  constexpr std::string_view view() const { return {s, N - 1}; }
};

// 转型: get(表名, 一行各列, 列号) → 成员值; 不符 → Fail
namespace as {
struct text {
  static std::string get(const char *tab, const Cells &f, int c);
};
struct name { // 非空
  static std::string get(const char *tab, const Cells &f, int c);
};
struct code { // 6 位数字
  static std::string get(const char *tab, const Cells &f, int c);
};
struct clock { // HH:MM:SS
  static std::string get(const char *tab, const Cells &f, int c);
};
struct ymd { // YYYYMMDD
  static misc::Ymd get(const char *tab, const Cells &f, int c);
};
struct num {
  static double get(const char *tab, const Cells &f, int c);
};
struct num_or { // 空 = 无
  static std::optional<double> get(const char *tab, const Cells &f, int c);
};
struct flag { // 沪深标志 0 深 / 1 沪 / 60 北
  static Mkt get(const char *tab, const Cells &f, int c);
};
struct side { // 买入 / 卖出
  static int get(const char *tab, const Cells &f, int c);
};
struct sides { // 列 c / c + 1 = 买 / 卖
  static BySide get(const char *tab, const Cells &f, int c);
};
struct groups { // 自列 c 起三组「名称|数值|」
  static std::array<std::pair<std::string, std::string>, 3> get(const char *tab, const Cells &f, int c);
};
template <Lit L>
struct is { // 只核不存
  static void get(const char *tab, const Cells &f, int c) {
    const std::string &s = cell(f, c);
    if (s != L.view())
      fail(strf("%s列 %d 为「%s」, 应为「%.*s」", tab, c, s.c_str(), static_cast<int>(L.view().size()), L.view().data()));
  }
};
} // namespace as

// 一列: 成员 M (nullptr = 只核不存) ← 各布局的列号 at 经 As; field = 客户端字段字典里该列的名称
template <auto M, class As, size_t V>
struct Col {
  const char *field;
  std::array<int, V> at;

  template <class Row>
  void put(const char *tab, const Cells &f, size_t view, Row &r) const {
    assert(view < V);
    const int c = at[view];
    if (c == kNone)
      return;
    if constexpr (std::is_null_pointer_v<decltype(M)>)
      As::get(tab, f, c);
    else
      r.*M = As::get(tab, f, c);
  }
};
template <auto M, class As, class... I>
constexpr Col<M, As, sizeof...(I)> col(const char *field, I... at) {
  return {field, {static_cast<int>(at)...}};
}

// 功能 func 的应答 (各行各列) → 行: 布局按功能号认 (kViews), 列声明逐列转型, 行级校验
template <class S>
std::vector<typename S::Row> parse(int func, int cols, const std::vector<Cells> &cells) {
  const auto v = std::find_if(S::kViews.begin(), S::kViews.end(), [&](const View &x) { return x.func == func; });
  if (v == S::kViews.end())
    fail(strf("%s不由功能 %d 应答", S::kName, func));
  const size_t view = static_cast<size_t>(v - S::kViews.begin());
  if (cols >= v->cols)
    fail(strf("%s功能 %d 应答 %d 列, 布局只 %d 列", S::kName, func, cols, v->cols));
  std::vector<typename S::Row> rows;
  for (const Cells &f : cells) {
    if (static_cast<int>(f.size()) != cols)
      fail(strf("%s应答行 %zu 列, 应 %d 列", S::kName, f.size(), cols));
    typename S::Row r;
    std::apply([&](const auto &...k) { (k.put(S::kName, f, view, r), ...); }, S::kCols);
    S::check(r);
    rows.push_back(std::move(r));
  }
  return rows;
}

// ---------- 索引 ----------

inline std::string sym(const Sec &s) { return sina_sym(s.mkt, s.code); } // (市场, 代码) 键

template <auto K>
struct Unique {
  template <class Row>
  static std::string key(const Row &r) {
    if constexpr (std::is_member_object_pointer_v<decltype(K)>)
      return r.*K;
    else
      return K(r);
  }
  template <class Row>
  static Keyed<Row> build(const char *tab, std::vector<Row> rows) {
    Keyed<Row> out;
    for (Row &r : rows) {
      const std::string k = key(r);
      if (!out.at.emplace(k, out.list.size()).second)
        fail(strf("%s键 %s 有重复", tab, k.c_str()));
      out.list.push_back(std::move(r));
    }
    return out;
  }
};

struct Tally {
  static Fills build(const char *tab, std::vector<Fill> rows);
};

struct Pairs {
  template <class Row>
  static std::map<std::string, std::string> build(const char *tab, const std::vector<Row> &rows) {
    std::map<std::string, std::string> out;
    for (const Row &r : rows)
      for (const auto &[k, v] : r.kv)
        if (!k.empty() && !out.emplace(k, v).second)
          fail(strf("%s名称「%s」有重复", tab, k.c_str()));
    return out;
  }
};

struct Plain {
  template <class Row>
  static std::vector<Row> build(const char *, std::vector<Row> rows) { return rows; }
};

} // namespace live::tdx::cols
