#pragma once

#include "fundamental/Store.hpp"
#include "misc/parquet.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

// ============================================================================
// Scan — 读侧原语: 月度表的分段并行 map-reduce (与 sync 的 plan → fetch → put 对仗);
//   Meta 表单文件由 meta() 直读, Month 表最新一天由 snapshot() 给出, D 轴由 calendar() 给出 (文件末尾).
//   读盘一律经 Store::read.
//
//   split: 一张表的分片按 ym 升序均分成 ≤ threads 段 (段 = 连续 ym 区间), 段 s 独占累加器 segs[s]
//   fold : fold(rows, acc) 逐分片累加; 行的资产 id 与 vd 日期已整列预转
//   merge: run() 后 take() 按段序 segs[0].merge(segs[1]) ... —— Append 因而保持 (ym, 文件内) 升序
//
// 累加器 = 可合并类型 (a.merge(std::move(b))). 通用件:
//   Latest<T, K>: 按 key 取最大 —— 扫描时的 offer 与段间的 merge 是同一个操作; key == 0 = 从无
//   PerId<M>    : 每资产一个 M, 逐元素 merge
//   Append<T>   : 段间按段序拼接
//   Sink        : 无归约 —— fold 直写共享输出 (调用方保证各分片写集不相交)
//
// 调度: 全部登记表的段混在一张任务表里, 按段内字节数降序 (重段先跑, 不留长尾), 原子游标取活.
// 读盘: 只解 cols + vd + 资产键列; 键列以字典落地, 每文件只对字典项查一次 Ids, 逐行退化成数组下标.
// ============================================================================
namespace fundamental {

// ---------------------------------------------------------------------------
// 累加器
// ---------------------------------------------------------------------------
template <class T, class K = Ymd>
struct Latest {
  K key = 0;
  T v{};
  void offer(K k, const T &x) {
    if (k > key) {
      key = k;
      v = x;
    }
  }
  void merge(Latest &&o) { offer(o.key, o.v); }
  bool has() const { return key != 0; }
};

template <class M>
struct PerId {
  std::vector<M> v;
  explicit PerId(std::uint32_t n) : v(n) {}
  M &operator[](std::uint32_t id) { return v[id]; }
  const M &operator[](std::uint32_t id) const { return v[id]; }
  void merge(PerId &&o) {
    assert(o.v.size() == v.size());
    for (std::size_t i = 0; i < v.size(); ++i)
      v[i].merge(std::move(o.v[i]));
  }
};

template <class T>
struct Append {
  std::vector<T> v;
  void push(const T &x) { v.push_back(x); }
  void merge(Append &&o) { v.insert(v.end(), std::make_move_iterator(o.v.begin()), std::make_move_iterator(o.v.end())); }
};

struct Sink {
  void merge(Sink &&) {}
};

// ---------------------------------------------------------------------------
// 资产 id 空间: 代码 ("000001.SZ") → [0, size)
//   Miss::Assert: 空间外代码 = 数据错误 → assert;  Miss::Skip: 空间外代码 → kNone (fold 跳过)
// ---------------------------------------------------------------------------
class Ids {
public:
  enum class Miss { Assert,
                    Skip };
  static constexpr std::uint32_t kNone = ~0u;

  explicit Ids(Miss miss) : miss_(miss) {}
  std::uint32_t add(std::string_view code); // 重复 → assert
  std::uint32_t size() const { return static_cast<std::uint32_t>(ids_.size()); }
  std::uint32_t find(std::string_view code) const;

private:
  struct Hash {
    using is_transparent = void;
    std::size_t operator()(std::string_view s) const noexcept { return std::hash<std::string_view>{}(s); }
  };
  struct Eq {
    using is_transparent = void;
    bool operator()(std::string_view a, std::string_view b) const noexcept { return a == b; }
  };
  Miss miss_;
  std::unordered_map<std::string, std::uint32_t, Hash, Eq> ids_;
};

// ---------------------------------------------------------------------------
// 一个分片的行视图
// ---------------------------------------------------------------------------
class Rows {
public:
  std::int64_t size() const { return n_; }
  std::uint32_t id(std::int64_t i) const { return dict_ids_[key_[i]]; } // Miss::Skip 下可为 Ids::kNone
  Ymd date(std::int64_t i) const { return dates_[i]; }                  // vd; null → 0
  misc::pq::Col col(std::string_view c) const { return t_->col(c); }

private:
  friend class Scan;
  const misc::pq::TableView *t_ = nullptr;
  std::int64_t n_ = 0;
  const std::int32_t *key_ = nullptr;
  const std::uint32_t *dict_ids_ = nullptr;
  const Ymd *dates_ = nullptr;
};

// 一张表的分段累加器; run() 之后 take() 按段序归约
template <class Acc>
class Part {
public:
  Acc take() && {
    assert(!segs_.empty() && "Part: 未登记进 Scan");
    for (std::size_t s = 1; s < segs_.size(); ++s)
      segs_[0].merge(std::move(segs_[s]));
    return std::move(segs_[0]);
  }

private:
  friend class Scan;
  std::vector<Acc> segs_;
};

// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------
class Scan {
public:
  Scan(const Catalog &cat, const Ids &ids, std::size_t threads);

  // 登记一张 Month 表. proto = 每段累加器的初值; fold(const Rows &, Acc &).
  // out / fold 引用的对象须活到 run() 结束.
  template <class Acc, class Fold>
  void add(Part<Acc> &out, const Spec &s, std::vector<std::string> cols, const Acc &proto, Fold fold);

  std::size_t files() const { return files_; } // 已登记分片数 (进度分母)

  // 跑完全部登记的段 (阻塞); 每完成一个分片 done += 1
  void run(std::atomic<std::size_t> &done);

private:
  struct Scratch; // 每线程: 当前分片的表 + 键字典映射 + 日期列

  struct Job {
    std::uint64_t bytes;
    std::function<void(Scratch &, std::atomic<std::size_t> &)> body;
  };

  // 读分片 → 行视图 (vd / 键列整列预转进 x)
  Rows open(const Shard &sh, const Spec &s, const std::vector<std::string> &cols, Scratch &x) const;

  const Catalog &cat_;
  const Ids &ids_;
  std::size_t threads_;
  std::size_t files_ = 0;
  std::vector<Job> jobs_;
};

template <class Acc, class Fold>
void Scan::add(Part<Acc> &out, const Spec &s, std::vector<std::string> cols, const Acc &proto, Fold fold) {
  assert(s.shape == Shape::Month && !s.vd.empty() && "Scan: 只扫 Month 表");
  for (std::string_view c : {std::string_view(s.vd), key_col(s)})
    if (std::find(cols.begin(), cols.end(), c) == cols.end())
      cols.emplace_back(c);
  const std::vector<Shard> &all = cat_.shards(s.name);
  const std::size_t n = all.size();
  const std::size_t nseg = std::clamp<std::size_t>(n, 1, threads_);
  out.segs_.assign(nseg, proto);
  files_ += n;
  auto shared_cols = std::make_shared<const std::vector<std::string>>(std::move(cols));
  for (std::size_t seg = 0; seg < nseg; ++seg) {
    const std::size_t lo = n * seg / nseg, hi = n * (seg + 1) / nseg;
    if (lo == hi)
      continue;
    std::uint64_t bytes = 0;
    for (std::size_t i = lo; i < hi; ++i)
      bytes += all[i].bytes;
    jobs_.push_back({bytes, [this, &out, &s, &all, shared_cols, fold, seg, lo, hi](Scratch &x, std::atomic<std::size_t> &done) {
                       for (std::size_t i = lo; i < hi; ++i) {
                         const Rows r = open(all[i], s, *shared_cols, x);
                         if (r.size() > 0)
                           fold(r, out.segs_[seg]);
                         done.fetch_add(1, std::memory_order_relaxed);
                       }
                     }});
  }
}

// Meta 表 name 的 _meta 单文件, 只解 cols (分片缺失 / 非 Meta 表 → assert) —— Scan (Month) 的对偶
misc::pq::TableView meta(const Catalog &cat, std::string_view name, const std::vector<std::string> &cols);

// Month 表 name 最新那天的快照行 → fn(view, rows); 全表无行则不调 —— meta() 的对偶 (写侧 Snap::Day 的读侧)
//   自最新分片往前找首个非空片, 取其 max(vd) 那天的行. cols 须含 vd
template <class Fn>
void snapshot(const Catalog &cat, std::string_view name, const std::vector<std::string> &cols, Fn fn) {
  const Spec &s = spec(name);
  assert(s.shape == Shape::Month && "snapshot: 只读 Month 表");
  const std::vector<Shard> &shards = cat.shards(name);
  std::vector<Ymd> dates;
  for (auto it = shards.rbegin(); it != shards.rend(); ++it) {
    const misc::pq::TableView v(read(*it, cols));
    if (v.rows() == 0)
      continue; // 0 行月 (拉过为空) → 往前找
    v.col(s.vd).yyyymmdd_all(dates);
    const Ymd max_d = *std::max_element(dates.begin(), dates.end());
    std::vector<std::int64_t> rows;
    for (std::int64_t i = 0; i < v.rows(); ++i)
      if (dates[static_cast<std::size_t>(i)] == max_d)
        rows.push_back(i);
    fn(v, rows);
    return;
  }
}

// all_trading_days 的 CN 交易日 (≤ today, 升序) —— D 轴唯一来源 (AssetInfo 与 fund::Pool 共用)
std::vector<Ymd> calendar(const Catalog &cat, Ymd today);

} // namespace fundamental
