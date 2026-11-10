// 通达信适配: 读客户端进程堆里的明文表 (只服务 Tables.hpp 里的表; 调试台的探内存只借 attach / each_region)
//   定位 TdxW 进程、枚举堆段、确保有跨进程读权限 (本机存的密码自动 sudo 放开 ptrace)、
//   "连读两次一致才采信" 的完整性原语、明文表的找块 / 复读 / 解析, 以及各表共用的流程.
//
// 一张表由四条互不相干的轴拼成, 流程只此一份、只认这四条轴:
//   形状 Shape  anchor 定长锚点  back 锚点往前找 0| 起点的字节数  head / row 表头行 / 数据行 (不含 \r\n) 认不认;
//               表头带行数 n 即块只取前 n 行 (后随不足 n 行 = 被覆盖了一半的旧缓冲 / 分页块, 不算), 不带即整块;
//               empty 空表 (n = 0、无行) 算不算块  slack 复读窗口在块尾外多留的字节
//   列          kViews 各布局列数  row(列, 布局) → 行 (行级校验不过即 Fail)
//   读法 kMode  Live 活动块 / Union 副本并集 / Ask 查询前后比对 / Whole 全表; 后两种盯不了换代 (没有 follow)
//   索引        index(行) → by_code
//   find(形状, 段)        段内命中 Hit (纯形状, 不读别处)
//   pick(pid, 命中)       选取 → 按址连读一致 → 解析校验 → 合并 → 索引 → Snap
//   follow(snap)          没换代回 snap 本身, 换代即重扫再 pick
// scan 一遍扫全部堆段出各形状的命中; wait(snap) 轮询 follow 等客户端重查后的换代 (没有块可盯即等一会儿全量读).
// 读法 Ask 的表 (表内没有新旧标记) 经 ask(act) 读: act (点查询) 前后各扫一遍, 之后新冒出的命中即这次的应答.
// 客户端正改写 / 刚释放 / 进程没了 都是外部结局 → Fail
#pragma once

#include "live/Broker.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace live::tdx::mem {

using Addr = uint64_t;

// 命中: 块地址、表头标签、块字节、复读窗口尾 (不越段尾); 值相等即同址同内容
struct Hit {
  Addr addr = 0;
  std::string tag, body;
  Addr end = 0;
  bool operator==(const Hit &) const = default;
};

// 表头行 → 认得即 true, 填标签与行数 (n < 0 = 表头不带行数)
using HeadFn = bool (*)(std::string_view line, std::string_view &tag, long &n);
using RowFn = bool (*)(std::string_view line);

struct Shape {
  const char *name;
  std::string_view anchor;
  size_t back;
  HeadFn head;
  RowFn row;
  bool empty;
  size_t slack;
};

enum class Mode : uint8_t { Live,
                            Union,
                            Ask,
                            Whole };

int find_pid();                                    // 0 = 没有
int attach();                                      // 定位进程并确保有读权限; 找不到 → Fail
std::string read_at(int pid, Addr addr, size_t n); // 须整段可读
std::vector<Hit> find(const Shape &t, Addr lo, std::string_view data);
std::optional<std::string> reread(const Shape &t, int pid, const Hit &h); // 块已不在 (被释放 / 改作他用 / 标签变了) → 无
// 一遍读堆段 (near 给定时只读含其中任一地址的段): 每段 (段首址, 读到的字节) 交给 fn
void each_region(int pid, const std::function<void(Addr lo, std::string_view data)> &fn, const std::vector<Addr> *near = nullptr);
// each_region 每段交给各形状 find → 与 shapes 同序的命中
std::vector<std::vector<Hit>> scan(int pid, const std::vector<const Shape *> &shapes, const std::vector<Addr> *near = nullptr);
// 块 → 各行的列 (GBK 解码为 UTF-8、无非法字节, 首行表头其后每行一条; 列数全表一致且在 views 里) → (UTF-8 全文, 各行列, 布局号)
struct Split {
  std::string text;
  std::vector<std::vector<std::string_view>> rows;
  size_t view = 0;
};
Split split(const Shape &t, std::string_view raw, const int *views, size_t nviews);

// 列转型 (不符 → Fail)
double num(const Shape &t, std::string_view s);                   // 浮点
std::optional<double> num_or(const Shape &t, std::string_view s); // 空 = 无
long integer(const Shape &t, std::string_view s);
void check_code(const Shape &t, std::string_view code);
Mkt market_flag(const Shape &t, std::string_view code, std::string_view flag); // 0 深 / 1 沪 / 60 北
int side_text(const Shape &t, std::string_view s);                             // 买入 / 卖出
bool clock_text(std::string_view s);                                           // HH:MM:SS
bool isclose(double a, double b, double rel, double abs);

template <class S>
struct Snap {
  int pid = 0;
  std::vector<Hit> mark; // 选中的各块 (body 为连读一致的那份)
  std::vector<typename S::Row> rows;
  typename S::Index by_code;
  bool operator==(const Snap &o) const { return pid == o.pid && mark == o.mark; } // 同一代
};

inline constexpr size_t kSeenCap = 256; // 活动块读法记住的块数 (客户端每重查一次多一块, 旧块一分钟内即释放)

// 完整性原语: 反复调 fn() 直到相邻两次完全一致才采信, 躲开客户端正改写堆 (torn read) 的瞬间; 一致的空即 "已不在"
template <class F>
auto stable(F &&fn, const char *name, int tries = 5) {
  auto prev = fn();
  for (int i = 1; i < tries; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto cur = fn();
    if (cur == prev)
      return cur;
    prev = std::move(cur);
  }
  fail(strf("%s连读 %d 次仍不一致, 内存正被改写", name, tries));
}

// S: kShape / kMode / kViews / row(列, 布局) / index(行); Live 另有 kTag; Union 另有 tag() / uid(行) / order(行)
template <class S>
class Table {
public:
  using Row = typename S::Row;
  using Snap = mem::Snap<S>;

  static const Shape &shape() { return S::kShape; }

  Snap pick(int pid, const std::vector<Hit> &hits) {
    const std::vector<Hit> chosen = choose(hits);
    std::vector<Hit> mark;
    for (const Hit &h : chosen)
      if (auto b = stable([&] { return reread(shape(), pid, h); }, shape().name)) {
        Hit m = h;
        m.body = std::move(*b);
        mark.push_back(std::move(m));
      }
    if (mark.empty() && !chosen.empty())
      fail(strf("%s选中的 %zu 块在复读前全被释放, 客户端正在重查", shape().name, chosen.size()));
    std::vector<std::vector<Row>> parts;
    for (const Hit &h : mark)
      parts.push_back(parse(h.body));
    Snap s{pid, std::move(mark), merge(std::move(parts)), {}};
    s.by_code = S::index(shape(), s.rows);
    return s;
  }

  Snap read(int pid) { return pick(pid, scan(pid, {&shape()})[0]); }

  Snap follow(const Snap &s) {
    static_assert(S::kMode == Mode::Live || S::kMode == Mode::Union);
    return same(s) ? s : pick(s.pid, rescan(s));
  }

  // 客户端重查后轮询 follow 等 s 换代即回新快照, 不等固定时长;
  // timeout 内没换代 (应答与旧的逐字节相同, 如收盘后; 或盯不到新块) 即全量读; s 没有块即无从盯, 等 blind 秒全量读
  Snap wait(const Snap &s, double timeout = 3, double blind = 1) {
    const auto end = std::chrono::steady_clock::now() + std::chrono::duration<double>(s.mark.empty() ? blind : timeout);
    while (std::chrono::steady_clock::now() < end) {
      Snap n = follow(s);
      if (!(n == s))
        return n;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return read(s.pid);
  }

  // act 让客户端重查一次: 其前后各扫一遍, 之后新冒出的命中 (新址或新内容) 即这次的应答, 各份须逐字节一致
  Snap ask(int pid, const std::function<void()> &act, double timeout = 5) {
    static_assert(S::kMode == Mode::Ask);
    const std::vector<Hit> old = scan(pid, {&shape()})[0];
    act();
    const auto end = std::chrono::steady_clock::now() + std::chrono::duration<double>(timeout);
    std::vector<Hit> fresh;
    while (true) {
      std::vector<Hit> now = std::move(scan(pid, {&shape()})[0]);
      for (Hit &h : now)
        if (std::find(old.begin(), old.end(), h) == old.end())
          fresh.push_back(std::move(h));
      if (!fresh.empty())
        break;
      if (std::chrono::steady_clock::now() >= end)
        fail(strf("%gs 内没等到%s的新应答", timeout, shape().name));
    }
    for (const Hit &h : fresh)
      if (h.body != fresh[0].body)
        fail(strf("%s新应答 %zu 份内容不一", shape().name, fresh.size()));
    return pick(pid, fresh);
  }

private:
  std::vector<Row> parse(std::string_view raw) {
    const Split sp = split(shape(), raw, S::kViews.data(), S::kViews.size());
    std::vector<Row> rows;
    for (const auto &f : sp.rows)
      rows.push_back(S::row(shape(), f, sp.view));
    return rows;
  }

  // 活动块: 客户端在用的那块表头标签为 kTag (一块都没有即还没查过, 不选、不算错).
  // 客户端重查即另写一块, 旧块要过几秒到几十秒才释放, 其间同标签的有几块、内容不一:
  // 记下扫到过的各块 (最近 kSeenCap 块), 没见过的即新冒出的、为在用的; 没有新冒出的即仍是上次选的那块; 几块内容相同取一块.
  // 头一次扫就有几块不一、或新冒出的不止一种内容, 定不出 → Fail (旧块释放后自愈)
  std::vector<Hit> mine(const std::vector<Hit> &hits) const {
    std::vector<Hit> out;
    for (const Hit &h : hits)
      if (h.tag == S::kTag)
        out.push_back(h);
    return out;
  }

  static size_t bodies(const std::vector<Hit> &hs) {
    std::set<std::string_view> b;
    for (const Hit &h : hs)
      b.insert(h.body);
    return b.size();
  }

  std::vector<Hit> choose(const std::vector<Hit> &hits) {
    if constexpr (S::kMode == Mode::Live) {
      const std::vector<Hit> all = mine(hits);
      std::vector<Hit> got = all;
      if (got.empty() && !hits.empty())
        fail(strf("%s共 %zu 份, 没有活动块 (标签 %s)", shape().name, hits.size(), S::kTag));
      std::lock_guard lk(mu_);
      if (bodies(got) > 1) {
        std::vector<Hit> fresh;
        for (const Hit &h : got)
          if (std::find(seen_.begin(), seen_.end(), h) == seen_.end())
            fresh.push_back(h);
        if (fresh.empty())
          for (const Hit &h : got)
            if (cur_ && h == *cur_)
              fresh.push_back(h);
        got = std::move(fresh);
      }
      if (bodies(got) != 1 && !hits.empty())
        fail(strf("%s活动块 %zu 块内容不一, 定不出在用的 (等旧块释放)", shape().name, all.size()));
      for (const Hit &h : all)
        if (std::find(seen_.begin(), seen_.end(), h) == seen_.end())
          seen_.push_back(h);
      while (seen_.size() > kSeenCap)
        seen_.pop_front();
      cur_ = got.empty() ? std::nullopt : std::optional<Hit>(got[0]);
      got.resize(std::min<size_t>(got.size(), 1));
      return got;
    } else if constexpr (S::kMode == Mode::Union) {
      const std::string want = S::tag();
      std::vector<Hit> out;
      for (const Hit &h : hits)
        if (h.tag == want)
          out.push_back(h);
      return out;
    } else if constexpr (S::kMode == Mode::Ask) {
      return std::vector<Hit>(hits.begin(), hits.begin() + std::min<size_t>(hits.size(), 1));
    } else {
      return hits;
    }
  }

  std::vector<Row> merge(std::vector<std::vector<Row>> parts) {
    if constexpr (S::kMode == Mode::Whole) {
      for (const auto &p : parts)
        if (!(p == parts[0]))
          fail(strf("%s整块 %zu 份内容不一", shape().name, parts.size()));
      return parts.empty() ? std::vector<Row>{} : std::move(parts[0]);
    } else if constexpr (S::kMode == Mode::Union) {
      // 副本只增不改 (旧副本是新的子集): 按 uid 取并集、同号逐字段一致, 按 order 排
      std::vector<Row> uniq;
      for (auto &p : parts)
        for (Row &r : p) {
          const auto it = std::find_if(uniq.begin(), uniq.end(), [&](const Row &u) { return S::uid(u) == S::uid(r); });
          if (it == uniq.end())
            uniq.push_back(std::move(r));
          else if (!(*it == r))
            fail(strf("%s %s 两份副本不一致", shape().name, r.code.c_str()));
        }
      std::stable_sort(uniq.begin(), uniq.end(), [](const Row &a, const Row &b) { return S::order(a) < S::order(b); });
      return uniq;
    } else {
      std::vector<Row> out;
      for (auto &p : parts)
        for (Row &r : p)
          out.push_back(std::move(r));
      return out;
    }
  }

  // 没换代: Live 按址复读比字节 (约 0.03ms, 可高频); Union 扫上次各副本所在段, 段内的都在上次的里面
  bool same(const Snap &s) {
    if constexpr (S::kMode == Mode::Live) {
      for (const Hit &h : s.mark)
        if (reread(shape(), s.pid, h) != h.body)
          return false;
      return true;
    } else {
      for (const Hit &h : choose(segs(s)))
        if (std::find(s.mark.begin(), s.mark.end(), h) == s.mark.end())
          return false;
      return true;
    }
  }

  // 换代后重扫: Live 先扫上次所在段, 段内不恰为一块再全量; Union 即全量
  std::vector<Hit> rescan(const Snap &s) {
    if constexpr (S::kMode == Mode::Live) {
      std::vector<Hit> near = segs(s);
      if (mine(near).size() == 1)
        return near;
    }
    return scan(s.pid, {&shape()})[0];
  }

  std::vector<Hit> segs(const Snap &s) {
    std::vector<Addr> near;
    for (const Hit &h : s.mark)
      near.push_back(h.addr);
    return scan(s.pid, {&shape()}, &near)[0];
  }

  std::mutex mu_;
  std::deque<Hit> seen_;
  std::optional<Hit> cur_;
};

} // namespace live::tdx::mem
