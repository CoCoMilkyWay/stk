// 通达信适配: 读客户端进程堆里的明文表 (只服务 Tables.hpp 里的表; 调试台的探内存只借 attach / Heap::each)
//   定位 TdxW 进程、确保有跨进程读权限 (本机存的密码自动 sudo 放开 ptrace)、堆段的读与扫、
//   "连读两次一致才采信" 的完整性原语、明文表的找块 / 复读 / 解析, 以及各表共用的流程.
//
// 明文 GBK, \r\n 分行、| 分列, 首行表头 0|…|. 一张表由四条互不相干的轴拼成, 流程只此一份、只认这四条轴:
//   形状 kShape  head 表头模式 / row 数据行模式 (都不含 \r\n); empty 空表 (n = 0、无行) 算不算块; slack 复读窗口在块尾外多留的字节
//     模式 = 字面量 + 记号: {n} 行数 / {t} 标签 / {d} 不取 (≥ 1 位数字; 带位数 K 如 {t8} 即恰 K 位), {code} 6 位代码,
//       {clock} HH:MM:SS, {name} 非数字打头的非空列, * 任一列, ... 其后任意 (再跟的字面量须恰为行尾); 数据行不许有 NUL.
//     表头带 {n} 即块只取前 n 行 (后随不足 n 行 = 被覆盖了一半的旧缓冲 / 分页块, 不算), 不带即整块.
//     锚点 = 表头最长的字面段 (末段连 \r\n): 段内找锚点, 往前至多 (锚点前各记号的最宽 + 字面段) 字节找表头起点
//   列 kViews / kCols  各布局的列数; 行的各成员 ← 各布局的列号 (kNone = 该布局没有) 经转型 as::*; check(行) 行级交叉校验
//   读法 Mode  触发 × 取法:
//                        取最新一份 (没见过的)      取累积
//       被动换代          Live   活动块              Union  当日副本并集      → follow / wait
//       act 驱动          Ask    act 后新冒出的块     Whole  整块拼到齐        → arm + act + read (Chain::ask)
//     "见过的": Live 记扫到过的各块, Ask 记 act 前那一遍 (arm). 取最新一份即至多一块; 取累积即各块合并.
//   索引 Index  Unique<键> 键不重 → Keyed<行> / Tally 成交按代码合计 → Keyed<Flow> / Pairs 名称不重 → 名称 → 数值原文
// 一张表的流程: 扫 Heap::scan → 选 Mode::choose → 按址连读一致 stable → 解析校验 parse → 合并 Mode::merge → 索引 → Snap
// 客户端正改写 / 刚释放 / 进程没了 都是外部结局 → Fail
#pragma once

#include "live/Broker.hpp"
#include "live/tdx/Until.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace live::tdx::mem {

using Addr = uint64_t;
using Fields = std::vector<std::string_view>;

inline constexpr size_t kDigits = 10;   // 不定宽数字记号的最多位数 (只用于推锚点前的最宽字节数)
inline constexpr size_t kSeenCap = 256; // 活动块读法记住的块数 (客户端每重查一次多一块, 旧块一分钟内即释放)

// 命中: 块地址、表头标签、块字节、复读窗口尾 (不越段尾); 值相等即同址同内容
struct Hit {
  Addr addr = 0;
  std::string tag, body;
  Addr end = 0;
  bool operator==(const Hit &) const = default;
};

struct Shape {
  const char *name;
  std::string_view head, row;
  bool empty = false;
  size_t slack = 0;
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
  // 各形状的命中, 与 shapes 同序: 段间并行, 同锚点的形状共一遍查找, 命中按段序
  std::vector<std::vector<Hit>> scan(const std::vector<const Shape *> &shapes, const std::vector<Addr> *near = nullptr) const;

private:
  int pid_, fd_;
};
using HeapP = std::shared_ptr<const Heap>;

int find_pid(); // 0 = 没有
HeapP attach(); // 定位进程、确保有读权限、开堆; 找不到 → Fail

std::optional<std::string> reread(const Shape &t, const Heap &h, const Hit &hit); // 块已不在 (被释放 / 改作他用 / 标签变了) → 无

// 块 → 各行的列 (GBK 解码为 UTF-8、无非法字节, 首行表头其后每行一条; 列数全表一致且为 views 之一) → (UTF-8 全文, 各行列, 布局号)
struct Split {
  std::string text;
  std::vector<Fields> rows;
  size_t view = 0;
};
Split split(const Shape &t, std::string_view raw, std::span<const int> views);

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

// 转型: get(形状, 一行各列, 列号) → 成员值; 不符 → Fail
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

// 一列: 成员 M (nullptr = 只核不存) ← 各布局的列号 at 经 As
template <auto M, class As, size_t V>
struct Col {
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
constexpr Col<M, As, sizeof...(I)> col(I... at) {
  return {{static_cast<int>(at)...}};
}

template <class S>
std::vector<typename S::Row> parse(std::string_view raw) {
  const Split sp = split(S::kShape, raw, S::kViews);
  std::vector<typename S::Row> rows;
  for (const Fields &f : sp.rows) {
    typename S::Row r;
    std::apply([&](const auto &...c) { (c.put(S::kShape, f, sp.view, r), ...); }, S::kCols);
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

template <class S>
using IndexOf = typename S::Index::template Out<typename S::Row>;

template <class S>
struct Snap {
  HeapP heap;
  std::vector<Hit> mark; // 选中的各块 (body 为连读一致的那份)
  std::vector<typename S::Row> rows;
  IndexOf<S> index;
  bool operator==(const Snap &o) const { return heap == o.heap && mark == o.mark; } // 同一代
};

// ---------- 流程件 ----------

// 完整性原语: 反复调 fn() 直到相邻两次完全一致才采信, 躲开客户端正改写堆 (torn read) 的瞬间; 一致的空即 "已不在"
template <class F>
auto stable(F &&fn, const char *name, int tries = 5) {
  auto prev = fn();
  for (int i = 1; i < tries; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto cur = fn();
    if (cur == prev) {
      if (i > 1)
        trace::ev("torn", {{"tab", name}, {"n", i + 1}});
      return cur;
    }
    prev = std::move(cur);
  }
  fail(strf("%s连读 %d 次仍不一致, 内存正被改写", name, tries));
}

template <class S>
std::vector<Hit> scan_of(const Heap &h, const std::vector<Addr> *near = nullptr) {
  return h.scan({&S::kShape}, near)[0];
}
// 只扫 s 各块所在的段
template <class S>
std::vector<Hit> near_of(const Snap<S> &s) {
  std::vector<Addr> near;
  for (const Hit &h : s.mark)
    near.push_back(h.addr);
  return scan_of<S>(*s.heap, &near);
}

template <class C>
bool in(const C &hs, const Hit &h) { return std::find(hs.begin(), hs.end(), h) != hs.end(); }
inline std::vector<std::string> addrs(const std::vector<Hit> &hs) {
  std::vector<std::string> out;
  for (const Hit &h : hs)
    out.push_back(strf("%#llx", static_cast<unsigned long long>(h.addr)));
  return out;
}
inline std::vector<Hit> tagged(const std::vector<Hit> &hits, std::string_view tag) {
  std::vector<Hit> out;
  for (const Hit &h : hits)
    if (h.tag == tag)
      out.push_back(h);
  return out;
}
inline size_t bodies(const std::vector<Hit> &hs) {
  std::set<std::string_view> b;
  for (const Hit &h : hs)
    b.insert(h.body);
  return b.size();
}
template <class Row>
std::vector<Row> one(std::vector<std::vector<Row>> parts) {
  assert(parts.size() <= 1);
  return parts.empty() ? std::vector<Row>{} : std::move(parts[0]);
}

// ---------- 读法 ----------
// 各读法同一组成员: choose(命中) → 选中的块 / merge(各块的行) → 行; 被动两种另有 same(快照) 没换代 / rescan(快照) 换代后的命中,
// act 驱动两种另有 arm(act 前那一遍)

// 活动块: 客户端在用的那块表头标签为 S::kTag (一块都没有即还没查过, 不选、不算错).
// 客户端重查即另写一块, 旧块要过几秒到几十秒才释放, 其间同标签的有几块、内容不一:
// 记下扫到过的各块 (最近 kSeenCap 块), 没见过的即新冒出的、为在用的; 没有新冒出的即仍是上次选的那块; 几块内容相同取一块.
// 头一次扫就有几块不一、或新冒出的不止一种内容, 定不出 → Fail (旧块释放后自愈)
struct Live {
  static constexpr bool kWatch = true;
  std::deque<Hit> seen;
  std::optional<Hit> cur;

  template <class S>
  std::vector<Hit> choose(const std::vector<Hit> &hits) {
    const std::vector<Hit> all = tagged(hits, S::kTag);
    if (all.empty() && !hits.empty())
      fail(strf("%s共 %zu 份, 没有活动块 (标签 %s)", S::kShape.name, hits.size(), S::kTag));
    std::vector<Hit> got = all;
    if (bodies(got) > 1) {
      std::vector<Hit> fresh;
      for (const Hit &h : got)
        if (!in(seen, h))
          fresh.push_back(h);
      if (fresh.empty())
        for (const Hit &h : got)
          if (cur && h == *cur)
            fresh.push_back(h);
      got = std::move(fresh);
    }
    if (bodies(got) != 1 && !hits.empty())
      fail(strf("%s活动块 %zu 块内容不一, 定不出在用的 (等旧块释放)", S::kShape.name, all.size()));
    for (const Hit &h : all)
      if (!in(seen, h))
        seen.push_back(h);
    while (seen.size() > kSeenCap)
      seen.pop_front();
    cur = got.empty() ? std::nullopt : std::optional<Hit>(got[0]);
    got.resize(std::min<size_t>(got.size(), 1));
    return got;
  }
  template <class S>
  static std::vector<typename S::Row> merge(std::vector<std::vector<typename S::Row>> parts) { return one(std::move(parts)); }

  // 按址复读比字节 (约 0.03ms, 可高频)
  template <class S>
  static bool same(const Snap<S> &s) {
    for (const Hit &h : s.mark)
      if (reread(S::kShape, *s.heap, h) != h.body)
        return false;
    return true;
  }
  // 先扫上次所在段, 段内不恰为一块活动块再全量
  template <class S>
  static std::vector<Hit> rescan(const Snap<S> &s) {
    std::vector<Hit> near = near_of(s);
    return tagged(near, S::kTag).size() == 1 ? near : scan_of<S>(*s.heap);
  }
};

// 当日副本并集: 表头标签为 S::tag() 的各副本; 副本只增不改 (旧副本是新的子集), 按 S::uid 取并集、同号逐字段一致, 按 S::order 排
struct Union {
  static constexpr bool kWatch = true;

  template <class S>
  static std::vector<Hit> choose(const std::vector<Hit> &hits) { return tagged(hits, S::tag()); }
  template <class S>
  static std::vector<typename S::Row> merge(std::vector<std::vector<typename S::Row>> parts) {
    using Row = typename S::Row;
    std::vector<Row> uniq;
    std::map<decltype(S::uid(std::declval<const Row &>())), size_t> at;
    for (auto &p : parts)
      for (Row &r : p) {
        const auto [it, fresh] = at.emplace(S::uid(r), uniq.size());
        if (fresh)
          uniq.push_back(std::move(r));
        else if (!(uniq[it->second] == r))
          fail(strf("%s %s 两份副本不一致", S::kShape.name, r.code.c_str()));
      }
    std::stable_sort(uniq.begin(), uniq.end(), [](const Row &a, const Row &b) { return S::order(a) < S::order(b); });
    return uniq;
  }

  // 扫上次各副本所在段, 段内的都在上次的里面
  template <class S>
  static bool same(const Snap<S> &s) {
    for (const Hit &h : choose<S>(near_of(s)))
      if (!in(s.mark, h))
        return false;
    return true;
  }
  template <class S>
  static std::vector<Hit> rescan(const Snap<S> &s) { return scan_of<S>(*s.heap); }
};

// act 后新冒出的块: act 前那一遍即见过的, 之后新冒出的 (新址或新内容) 即这次的应答, 各份须逐字节一致
struct Ask {
  static constexpr bool kWatch = false;
  std::vector<Hit> seen;

  template <class S>
  void arm(std::vector<Hit> before) { seen = std::move(before); }
  template <class S>
  std::vector<Hit> choose(const std::vector<Hit> &hits) const {
    std::vector<Hit> fresh;
    for (const Hit &h : hits)
      if (!in(seen, h))
        fresh.push_back(h);
    for (const Hit &h : fresh)
      if (h.body != fresh[0].body)
        fail(strf("%s新应答 %zu 份内容不一", S::kShape.name, fresh.size()));
    fresh.resize(std::min<size_t>(fresh.size(), 1));
    return fresh;
  }
  template <class S>
  static std::vector<typename S::Row> merge(std::vector<std::vector<typename S::Row>> parts) { return one(std::move(parts)); }
};

// 整块拼到齐: 客户端收齐分页后另拼的整块 (每查全部一次一份, 旧的可能还留着), 各份须同内容; 到齐与否由调用方判
struct Whole {
  static constexpr bool kWatch = false;

  template <class S>
  void arm(std::vector<Hit>) {} // 不看 act 前: 新拼的整块与留着的旧整块同内容即同一份
  template <class S>
  static std::vector<Hit> choose(const std::vector<Hit> &hits) { return hits; }
  template <class S>
  static std::vector<typename S::Row> merge(std::vector<std::vector<typename S::Row>> parts) {
    for (const auto &p : parts)
      if (!(p == parts[0]))
        fail(strf("%s整块 %zu 份内容不一", S::kShape.name, parts.size()));
    return parts.empty() ? std::vector<typename S::Row>{} : std::move(parts[0]);
  }
};

// ---------- 表 ----------

// S: kShape / kViews / kCols / check(行) / Mode / Index; Live 另有 kTag; Union 另有 tag() / uid(行) / order(行)
template <class S>
class Table {
public:
  using Row = typename S::Row;
  using Snap = mem::Snap<S>;
  using Mode = typename S::Mode;

  Snap pick(const HeapP &heap, const std::vector<Hit> &hits) {
    std::vector<Hit> chosen;
    {
      std::lock_guard lk(mu_);
      chosen = mode_.template choose<S>(hits);
    }
    std::vector<Hit> mark;
    for (const Hit &h : chosen)
      if (auto b = stable([&] { return reread(S::kShape, *heap, h); }, S::kShape.name)) {
        Hit m = h;
        m.body = std::move(*b);
        mark.push_back(std::move(m));
      }
    if (mark.empty() && !chosen.empty())
      fail(strf("%s选中的 %zu 块在复读前全被释放, 客户端正在重查", S::kShape.name, chosen.size()));
    std::vector<std::vector<Row>> parts;
    for (const Hit &h : mark)
      parts.push_back(parse<S>(h.body));
    Snap s{heap, std::move(mark), Mode::template merge<S>(std::move(parts)), {}};
    s.index = S::Index::build(S::kShape, s.rows);
    return s;
  }
  Snap read(const HeapP &heap) { return pick(heap, scan_of<S>(*heap)); }

  // ---------- 被动换代 ----------

  // 没换代回 s 本身, 换代即重扫再选
  Snap follow(const Snap &s) {
    static_assert(Mode::kWatch);
    return Mode::template same<S>(s) ? s : pick(s.heap, Mode::template rescan<S>(s));
  }
  // 客户端重查后轮询 follow 等 s 换代即回新快照, 不等固定时长;
  // timeout 内没换代 (应答与旧的逐字节相同, 如收盘后; 或盯不到新块) 即全量读; s 没有块即无从盯, 等 blind 秒全量读
  Snap wait(const Snap &s, double timeout = 3, double blind = 1) {
    static_assert(Mode::kWatch);
    const Clock::time_point t0 = Clock::now();
    std::optional<Snap> n = poll(
        [&]() -> std::optional<Snap> {
          Snap f = follow(s);
          return f == s ? std::nullopt : std::optional<Snap>(std::move(f));
        },
        s.mark.empty() ? blind : timeout, 0.01);
    trace::ev("wait", {{"tab", S::kShape.name}, {"ms", trace::ms_since(t0)}, {"by", n ? "gen" : "full"}});
    return n ? std::move(*n) : read(s.heap);
  }

  // ---------- act 驱动 ----------

  void arm(std::vector<Hit> before) {
    static_assert(!Mode::kWatch);
    std::lock_guard lk(mu_);
    mode_.template arm<S>(std::move(before));
  }

private:
  std::mutex mu_;
  Mode mode_;
};

// 接连几问 (几张 act 驱动的表各 act 一次): 每遍扫齐各表, 等到上一问应答的那遍即下一问的 act 前, 省掉各问单另的一遍
class Chain {
public:
  Chain(HeapP heap, std::vector<const Shape *> shapes) : heap_(std::move(heap)), shapes_(std::move(shapes)), last_(heap_->scan(shapes_)) {}

  // act 让客户端重查 t 一次, 等新冒出的块; timeout 秒内没有 → Fail
  template <class S>
  Snap<S> ask(Table<S> &t, const std::function<void()> &act, double timeout = 5) {
    const size_t at = static_cast<size_t>(std::find(shapes_.begin(), shapes_.end(), &S::kShape) - shapes_.begin());
    assert(at < shapes_.size());
    t.arm(last_[at]);
    const Clock::time_point t0 = Clock::now();
    act();
    Snap<S> got = std::move(*until(
        said(strf("%s的新应答", S::kShape.name)),
        [&]() -> std::optional<Snap<S>> {
          last_ = heap_->scan(shapes_);
          Snap<S> s = t.pick(heap_, last_[at]);
          return s.mark.empty() ? std::nullopt : std::optional<Snap<S>>(std::move(s));
        },
        timeout, 0));
    trace::ev("ask", {{"tab", S::kShape.name}, {"rows", got.rows.size()}, {"addr", addrs(got.mark)}, {"ms", trace::ms_since(t0)}});
    return got;
  }

private:
  HeapP heap_;
  std::vector<const Shape *> shapes_;
  std::vector<std::vector<Hit>> last_;
};

} // namespace live::tdx::mem
