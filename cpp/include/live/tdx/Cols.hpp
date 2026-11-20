// 通达信适配: 应答表的列声明、解析与索引 (各表声明并排在 Tables.hpp)
//   kFuncs: 应答它的功能号序列; kCols: 行的各成员 ← 各布局的列号 (kNone = 该布局没有) 经转型 as::*;
//   check(行) 行级校验; Index: 各行 → 目标索引形 (Unique / Tally / Pairs / Plain)
#pragma once

#include "live/Broker.hpp"

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

using Cells = std::vector<std::string>;

inline constexpr int kNone = -1;

const std::string &cell(const Cells &f, int c);
bool isclose(double a, double b, double rel, double abs);
void need_row(const char *tab, bool ok, const std::string &key, const char *what);

template <size_t N>
struct Lit {
  char s[N];
  constexpr Lit(const char (&a)[N]) { std::copy_n(a, N, s); }
  constexpr std::string_view view() const { return {s, N - 1}; }
};

namespace as {
struct text {
  static std::string get(const char *tab, const Cells &f, int c);
};
struct name {
  static std::string get(const char *tab, const Cells &f, int c);
};
struct code {
  static std::string get(const char *tab, const Cells &f, int c);
};
struct clock { // HH:MM:SS → 当日秒
  static int get(const char *tab, const Cells &f, int c);
};
struct ymd {
  static misc::Ymd get(const char *tab, const Cells &f, int c);
};
struct num {
  static double get(const char *tab, const Cells &f, int c);
};
struct num_or {
  static std::optional<double> get(const char *tab, const Cells &f, int c);
};
struct shares { // 整数股数 [股]
  static int64_t get(const char *tab, const Cells &f, int c);
};
struct shares_or {
  static std::optional<int64_t> get(const char *tab, const Cells &f, int c);
};
struct flag {
  static Mkt get(const char *tab, const Cells &f, int c);
};
struct side {
  static int get(const char *tab, const Cells &f, int c);
};
struct sides_i { // 列 c / c+1 = 今买 / 今卖 [股]
  static Sides<int64_t> get(const char *tab, const Cells &f, int c);
};
struct groups { // 自列 c 起三组「名称|数值|」
  static std::array<std::pair<std::string, std::string>, 3> get(const char *tab, const Cells &f, int c);
};
template <Lit L>
struct is {
  static void get(const char *tab, const Cells &f, int c) {
    const std::string &s = cell(f, c);
    if (s != L.view())
      fail(strf("%s列 %d 为「%s」, 应为「%.*s」", tab, c, s.c_str(), static_cast<int>(L.view().size()), L.view().data()));
  }
};
} // namespace as

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
    need(c >= 0 && static_cast<size_t>(c) < f.size(), strf("%s列 %d 越界 (共 %zu 列)", tab, c, f.size()));
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

template <class S>
std::vector<typename S::Row> parse(int func, int cols_n, const std::vector<Cells> &cells) {
  const auto it = std::find(S::kFuncs.begin(), S::kFuncs.end(), func);
  need(it != S::kFuncs.end(), strf("%s不由功能 %d 应答", S::kName, func));
  const size_t view = static_cast<size_t>(it - S::kFuncs.begin());
  std::vector<typename S::Row> rows;
  rows.reserve(cells.size());
  for (const Cells &f : cells) {
    need(static_cast<int>(f.size()) == cols_n, strf("%s应答行 %zu 列, 应 %d 列", S::kName, f.size(), cols_n));
    typename S::Row r;
    std::apply([&](const auto &...k) { (k.put(S::kName, f, view, r), ...); }, S::kCols);
    S::check(r);
    rows.push_back(std::move(r));
  }
  return rows;
}

// ---------- 索引 ----------

inline std::string sym(const Sec &s) { return sina_sym(s.mkt, s.code); }

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
    out.list.reserve(rows.size());
    for (Row &r : rows) {
      const std::string k = key(r);
      need(out.at.emplace(k, out.list.size()).second, strf("%s键 %s 有重复", tab, k.c_str()));
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
        if (!k.empty())
          need(out.emplace(k, v).second, strf("%s名称「%s」有重复", tab, k.c_str()));
    return out;
  }
};

struct Plain {
  template <class Row>
  static std::vector<Row> build(const char *, std::vector<Row> rows) { return rows; }
};

} // namespace live::tdx::cols
