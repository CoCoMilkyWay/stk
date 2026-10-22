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
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// ============================================================================
// Scan — 读侧原语, 与写侧 Shape × Snap 对仗; 四个入口同一签名骨架 (…, Spec, cols, …fn(const Rows &)):
//   Month·全行   Scan::add  : 分段并行 map-reduce (与 sync 的 plan → fetch → put 对仗)
//   Month·最新日 snapshot() : 写侧 Snap::Day 的读侧
//   Meta         meta()     : _meta 单文件
//   Daily        daily()    : 一交易日一文件 (逐日读, 暂存由调用方跨日复用)
//   D 轴由 Store::calendar() 给出 (无资产键). 读盘一律经 Store::read, 行一律经 Rows::each.
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
//
// 检查: 基本面数据只在读侧查, 源异常只查 Bq 表 (Tu 不查). 三类, 各一个入口一种反应:
//   契约    代码 / SPECS / 调用形态 / 本地布局自洽                → 原地 assert
//   缺失    必需: Meta 分片 need() / 日历 calendar()              → assert (基本面未同步?)
//           可选: Month 表全空 snapshot() / Daily 某日无片 daily() → 不调 fn, 返回 false
//   源异常  可按行跳过: 码表漏收的在市股 Ids::miss()              → warning 行, 跳过该行; 每 Ids 每 (表, 代码) 一条
//           污染结果:   其余 expect()                             → error 行 + assert
//   行格式: "[fund] <warning|error> <表> [<日>] [<代码>] <现象>"
// ============================================================================
namespace fundamental {

// ---------------------------------------------------------------------------
// 检查
// ---------------------------------------------------------------------------
struct At {
  Ymd d = 0;                  // 0 = 无
  std::string_view code = {}; // 空 = 无
};

void error(const Spec &s, At at, std::string_view what); // error 行 + assert (expect / need 的落点)

// 源异常 (污染结果): !ok → error 行 + assert; s 须是 Bq 表
inline void expect(bool ok, const Spec &s, At at, std::string_view what) {
  if (!ok) [[unlikely]]
    error(s, at, what);
}

// 必需的 Meta 分片; 缺 → error 行 + assert
const Shard &need(const Catalog &cat, const Spec &s);

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
// 资产 id 空间: 代码 ("000001.SZ") ↔ [0, size)
//   空间外代码 → kNone (Rows::each 跳过), 经 miss 分两种:
//     当日在市 (cn_stock_static_data 快照) = 码表漏收 → warning
//     不在市 = 源数据挂在旧号 / 新三板挂牌号 / 未上市代码上 → 静默
// ---------------------------------------------------------------------------
class Ids {
public:
  static constexpr std::uint32_t kNone = ~0u;

  Ids() = default;
  // 空间 = Meta 表 s 的键列 (文件行序); 0 行 / 代码非 <码>.<交易所> 形 / 重复 → expect
  Ids(const Catalog &cat, const Spec &s);
  Ids(const Ids &) = delete; // codes_ 指向 ids_ 的节点
  Ids &operator=(const Ids &) = delete;

  std::uint32_t add(std::string_view code); // 空 / 重复 → assert
  std::uint32_t size() const { return static_cast<std::uint32_t>(codes_.size()); }
  std::uint32_t find(std::string_view code) const;
  std::string_view code(std::uint32_t id) const { return *codes_[id]; }
  // 空间外代码的处置 (Scratch::open 每字典项一次): Bq 表且当日在市 → warning; 其余静默.
  //   并发安全; 首次在市判定时载入快照
  void miss(const Spec &s, std::string_view code) const;

private:
  struct Hash {
    using is_transparent = void;
    std::size_t operator()(std::string_view s) const noexcept { return std::hash<std::string_view>{}(s); }
  };
  struct Eq {
    using is_transparent = void;
    bool operator()(std::string_view a, std::string_view b) const noexcept { return a == b; }
  };
  std::unordered_map<std::string, std::uint32_t, Hash, Eq> ids_;
  std::vector<const std::string *> codes_; // id → ids_ 的键 (unordered_map 节点地址稳定)
  mutable std::mutex miss_mu_;
  mutable bool live_loaded_ = false;
  mutable std::unordered_map<std::string, std::string, Hash, Eq> live_; // 当日在市代码 → 简称
  mutable std::unordered_set<std::string> warned_;                      // "表 代码"
};

// ---------------------------------------------------------------------------
// 一个分片的行视图 (Scratch::open 产出, 活到该 Scratch 下一次 open)
// ---------------------------------------------------------------------------
class Rows {
public:
  std::int64_t size() const { return n_; } // 分片行数 (含 each 跳过的行)
  misc::pq::Col col(std::string_view c) const { return t_->col(c); }
  const Spec &spec() const { return *s_; } // fold 内 expect 用

  // 逐行 fn(i, id, d): i = col(..) 的行下标, id ∈ Ids, d = vd (null / vd 空 → 0).
  //   跳过空间外代码 (Ids::kNone) 与 day 之外的行 (snapshot)
  template <class Fn>
  void each(Fn &&fn) const {
    for (std::int64_t i = 0; i < n_; ++i) {
      const std::uint32_t id = dict_ids_[key_[i]];
      const Ymd d = dates_[i];
      if (id != Ids::kNone && (day_ == 0 || d == day_))
        fn(i, id, d);
    }
  }

private:
  friend class Scratch;
  const Spec *s_ = nullptr;
  const misc::pq::TableView *t_ = nullptr;
  std::int64_t n_ = 0;
  const std::int32_t *key_ = nullptr;
  const std::uint32_t *dict_ids_ = nullptr;
  const Ymd *dates_ = nullptr;
  Ymd day_ = 0; // 0 = 全部日
};

// 读侧暂存 (每线程一份): 当前分片的表 + 键字典映射 + 日期列
//   open: 读分片 (只解 cols + vd + 资产键列; 键列以字典落地, 每文件只对字典项查一次 Ids) → 其上的 Rows
//   snap = Snap::Day: 只留分片内最晚一天 (snapshot); 与写侧 Snap 同义 "每个分片只留一天"
class Scratch {
public:
  Rows open(const Shard &sh, const Spec &s, const std::vector<std::string> &cols, const Ids &ids,
            Snap snap = Snap::All);

private:
  std::unique_ptr<misc::pq::TableView> t_;
  misc::pq::DictCol key_;
  std::vector<std::uint32_t> dict_ids_;
  std::vector<Ymd> dates_;
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
  struct Job {
    std::uint64_t bytes;
    std::function<void(Scratch &, std::atomic<std::size_t> &)> body;
  };

  const Catalog &cat_;
  const Ids &ids_;
  std::size_t threads_;
  std::size_t files_ = 0;
  std::vector<Job> jobs_;
};

template <class Acc, class Fold>
void Scan::add(Part<Acc> &out, const Spec &s, std::vector<std::string> cols, const Acc &proto, Fold fold) {
  assert(s.shape == Shape::Month && !s.vd.empty() && "Scan: 只扫 Month 表");
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
    jobs_.push_back({bytes, [&ids = ids_, &out, &s, &all, shared_cols, fold, seg, lo, hi](Scratch &x, std::atomic<std::size_t> &done) {
                       for (std::size_t i = lo; i < hi; ++i) {
                         const Rows r = x.open(all[i], s, *shared_cols, ids);
                         if (r.size() > 0)
                           fold(r, out.segs_[seg]);
                         done.fetch_add(1, std::memory_order_relaxed);
                       }
                     }});
  }
}

// Month 表 s 最新一天 → fn(rows); 全表无行 → 不调, 返回 false —— 写侧 Snap::Day 的读侧
//   自最新分片往前找首个非空片, 只留其 max(vd) 那天 (vd 全 null → expect)
bool snapshot(const Catalog &cat, const Ids &ids, const Spec &s, const std::vector<std::string> &cols,
              const std::function<void(const Rows &)> &fn);

// Meta 表 s 的 _meta 单文件 → fn(rows) (分片缺失 → need) —— 写侧 Shape::Meta 的读侧
void meta(const Catalog &cat, const Ids &ids, const Spec &s, const std::vector<std::string> &cols,
          const std::function<void(const Rows &)> &fn);

// Daily 表 s 的 d 日文件 → fn(rows); 分片缺失 → 不调, 返回 false —— 写侧 Shape::Daily 的读侧
//   x = 调用方暂存 (逐日调用跨日复用; 并发调用各持一份)
bool daily(const Catalog &cat, const Ids &ids, const Spec &s, Ymd d, const std::vector<std::string> &cols, Scratch &x,
           const std::function<void(const Rows &)> &fn);

} // namespace fundamental
