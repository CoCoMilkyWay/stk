// 通达信适配: 客户端进程堆 (只读) 与应答表的列声明
//   堆: 定位 TdxW 进程、确保有跨进程读权限 (本机存的密码自动 sudo 放开 ptrace)、堆段的逐段读; 协议字典 (Schema) / 进程映像 (Image) /
//     shim probe 用. 账户数据不从堆里读, 一律经请求层直发 (Req.hpp)
//   表 (Tables.hpp 各一份声明): 应答 = 各行各列的串, 列序即协议字典里该功能号的出参序;
//     kViews / kFuncs  各布局的列数 (出参数 + 1) 与应答它的功能号; kCols 行的各成员 ← 各布局的列号 (kNone = 该布局没有) 经转型 as::*,
//     各列带客户端字段名 (shim test fields 对协议字典核); check(行) 行级交叉校验; Index 行 → 索引
//   Unique<键> 键不重 → Keyed<行> / Tally 成交按代码合计 → Keyed<Flow> / Pairs 名称不重 → 名称 → 数值原文 / Plain 无键 → 行原序
#pragma once

#include "live/Broker.hpp"
#include "live/tdx/Trace.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace live::tdx::mem {

using Addr = uint64_t;
using Fields = std::vector<std::string_view>;

struct Shape {
  const char *name;
};

// ---------- 堆 ----------

// 客户端进程堆: pid + 常开的 /proc/<pid>/mem (各线程共用, pread 不带状态); 堆段 = 可读写、私有、非文件映射, 排除超大段
class Heap {
public:
  explicit Heap(int pid);
  ~Heap();
  Heap(const Heap &) = delete;
  Heap &operator=(const Heap &) = delete;

  int pid() const { return pid_; }
  std::string read(Addr addr, size_t n) const; // 须整段可读
  // 逐段 (段序; near 给定时只读含其中任一地址的段): fn(段首址, 读到的字节)
  void each(const std::function<void(Addr lo, std::string_view data)> &fn, const std::vector<Addr> *near = nullptr) const;

private:
  int pid_, fd_;
};
using HeapP = std::shared_ptr<const Heap>;

int find_pid(); // 0 = 没有
HeapP attach(); // 定位进程、确保有读权限、开堆; 找不到 → Fail

std::string gbk(std::string_view raw); // GBK → UTF-8, 非法字节换 U+FFFD (0x80 即 €)
bool isclose(double a, double b, double rel, double abs);
void need_row(const Shape &t, bool ok, const std::string &key, const char *what); // 行级校验不过 → Fail

// ---------- 列 ----------

inline constexpr int kNone = -1;

std::string_view field(const Fields &f, int c);

template <size_t N>
struct Lit {
  char s[N];
  constexpr Lit(const char (&a)[N]) { std::copy_n(a, N, s); }
  constexpr std::string_view view() const { return {s, N - 1}; }
};

// 转型: get(表, 一行各列, 列号) → 成员值; 不符 → Fail
namespace as {
struct text {
  static std::string get(const Shape &t, const Fields &f, int c);
};
struct name { // 非空
  static std::string get(const Shape &t, const Fields &f, int c);
};
struct code { // 6 位数字
  static std::string get(const Shape &t, const Fields &f, int c);
};
struct clock { // HH:MM:SS
  static std::string get(const Shape &t, const Fields &f, int c);
};
struct ymd { // YYYYMMDD
  static misc::Ymd get(const Shape &t, const Fields &f, int c);
};
struct num {
  static double get(const Shape &t, const Fields &f, int c);
};
struct num_or { // 空 = 无
  static std::optional<double> get(const Shape &t, const Fields &f, int c);
};
struct flag { // 沪深标志 0 深 / 1 沪 / 60 北
  static Mkt get(const Shape &t, const Fields &f, int c);
};
struct side { // 买入 / 卖出
  static int get(const Shape &t, const Fields &f, int c);
};
struct sides { // 列 c / c + 1 = 买 / 卖
  static BySide get(const Shape &t, const Fields &f, int c);
};
struct groups { // 自列 c 起三组「名称|数值|」
  static std::array<std::pair<std::string, std::string>, 3> get(const Shape &t, const Fields &f, int c);
};
template <Lit L>
struct is { // 只核不存
  static void get(const Shape &t, const Fields &f, int c) {
    const std::string_view s = field(f, c);
    if (s != L.view())
      fail(strf("%s列 %d 为「%.*s」, 应为「%.*s」", t.name, c, static_cast<int>(s.size()), s.data(), static_cast<int>(L.view().size()), L.view().data()));
  }
};
} // namespace as

// 一列: 成员 M (nullptr = 只核不存) ← 各布局的列号 at 经 As; field = 客户端字段字典里该列的名称
template <auto M, class As, size_t V>
struct Col {
  const char *field;
  std::array<int, V> at;

  template <class Row>
  void put(const Shape &t, const Fields &f, size_t view, Row &r) const {
    assert(view < V);
    const int c = at[view];
    if (c == kNone)
      return;
    if constexpr (std::is_null_pointer_v<decltype(M)>)
      As::get(t, f, c);
    else
      r.*M = As::get(t, f, c);
  }
};
template <auto M, class As, class... I>
constexpr Col<M, As, sizeof...(I)> col(const char *field, I... at) {
  return {field, {static_cast<int>(at)...}};
}

// 功能 func 的应答 (各行各列的串) → 行: 布局按功能号认 (kFuncs), 列声明逐列转型, 行级校验
template <class S>
std::vector<typename S::Row> parse(int func, int cols, const std::vector<std::vector<std::string>> &cells) {
  const auto v = std::find(S::kFuncs.begin(), S::kFuncs.end(), func);
  if (v == S::kFuncs.end())
    fail(strf("%s不由功能 %d 应答", S::kShape.name, func));
  const size_t view = static_cast<size_t>(v - S::kFuncs.begin());
  if (cols >= S::kViews[view])
    fail(strf("%s功能 %d 应答 %d 列, 布局只 %d 列", S::kShape.name, func, cols, S::kViews[view]));
  std::vector<typename S::Row> rows;
  for (const std::vector<std::string> &c : cells) {
    if (static_cast<int>(c.size()) != cols)
      fail(strf("%s应答行 %zu 列, 应 %d 列", S::kShape.name, c.size(), cols));
    const Fields f(c.begin(), c.end());
    typename S::Row r;
    std::apply([&](const auto &...k) { (k.put(S::kShape, f, view, r), ...); }, S::kCols);
    S::check(S::kShape, r);
    rows.push_back(std::move(r));
  }
  return rows;
}

// ---------- 索引 ----------

inline std::string sym(const Sec &s) { return sina_sym(s.mkt, s.code); } // (市场, 代码) 键

template <auto K>
struct Unique {
  template <class Row>
  using Out = Keyed<Row>;

  template <class Row>
  static std::string key(const Row &r) {
    if constexpr (std::is_member_object_pointer_v<decltype(K)>)
      return r.*K;
    else
      return K(r);
  }
  template <class Row>
  static Keyed<Row> build(const Shape &t, const std::vector<Row> &rows) {
    Keyed<Row> out;
    for (const Row &r : rows) {
      const std::string k = key(r);
      if (!out.at.emplace(k, out.list.size()).second)
        fail(strf("%s键 %s 有重复", t.name, k.c_str()));
      out.list.push_back(r);
    }
    return out;
  }
};

struct Tally {
  template <class Row>
  using Out = Keyed<Flow>;

  static Keyed<Flow> build(const Shape &t, const std::vector<Fill> &rows);
};

struct Pairs {
  template <class Row>
  using Out = std::map<std::string, std::string>;

  template <class Row>
  static std::map<std::string, std::string> build(const Shape &t, const std::vector<Row> &rows) {
    std::map<std::string, std::string> out;
    for (const Row &r : rows)
      for (const auto &[k, v] : r.kv)
        if (!k.empty() && !out.emplace(k, v).second)
          fail(strf("%s名称「%s」有重复", t.name, k.c_str()));
    return out;
  }
};

struct Plain {
  template <class Row>
  using Out = std::vector<Row>;

  template <class Row>
  static std::vector<Row> build(const Shape &, const std::vector<Row> &rows) { return rows; }
};

template <class S>
using IndexOf = typename S::Index::template Out<typename S::Row>;

template <class S>
IndexOf<S> index(const std::vector<typename S::Row> &rows) {
  return S::Index::build(S::kShape, rows);
}

} // namespace live::tdx::mem
