#pragma once

#include "features/MetaFlag.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "misc/profiler.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cassert>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

// ============================================================================
// 流式分析的三种平面 (网格行: 对应生产管线的 TS / CS 两段; 尾部行: 各列原生行) + 共用的段序 / 读计划 / IO 暂存 / 双缓冲预读
// ============================================================================
//   TsPlane   data[c][a][j][t]  资产主序: 每资产每列一段连续 segs×R (TS 扫描友好), 截面按 asset_stride 跨步取
//   CsPlane   data[j][s][c][a]  切片主序: 每 (段, 切片, 列) 一段连续 A (截面友好), 每段只取 slice_t 指定的 S 行
//   RowPlane  data[c][a][j][r]  尾部行: 各列按自己的层 (不投影), 另带开盘承接值; 行 r 落在网格行 Source::at[r] (OrderFlow)
// 两种行映射对仗: 网格行 = gather (网格行 t ← 源行 Source::row[t], level_asof); 尾部行 = scatter (源行 r → 网格行 Source::at[r]).
// 列 = ReadPlan 的 ColRef; 三平面同一契约 prepare / load_batch(…, SegPlan, s0, bs), 同一个折叠核 fold_span (只差目的步长).
// A 轴 = ReadScope::axis; 列所在库经 Readers::ids 收拢到 A 轴, 资产不在该库 / 该库这天没落盘 (Day::has) = 无源.
// 同一个 IO 契约 (每源一个 fmeta::ColumnSet: 读什么、各列怎么门控), 同一套哨兵 (fmeta):
//   门控不过 / 无源 (空日, 区间首日无承接, 资产或日不在该库) → kInvalidBits, 门控过但 NaN → kNaNBits, 其余 (含 ±inf) 原样.
//   热循环一次 v != v 两种一并跳过; 要分账 (NaN vs 门控不过) 的消费方按位区分, 分母口径由消费方自己定.
// t = 有效行 (level_valid_rows, 末哨兵行不进平面). 容量准备幂等: 尺寸对得上零分配.
// ============================================================================

// ---- 段: 平面一个 j. 日内层 1 天, L_day 连续 kDaySegDays 个交易日 (日频序列成段); 段行数 = 每日有效行 × 段天数 ----
inline constexpr size_t kDaySegDays = 255;
inline constexpr size_t seg_days(size_t lvl) { return lvl == L_day ? kDaySegDays : 1; }
inline constexpr size_t seg_rows(size_t lvl) { return level_valid_rows(lvl) * seg_days(lvl); }

// 日轴一天: 当日 + 承接日 (日轴上前一日, 0 = 轴首) + 两日各自落了盘的库 (日轴规划时一次判定, 装载时 O(1) 查位)
struct Day {
  misc::Ymd date = 0, prev = 0;
  LibMask has = 0, has_prev = 0;
};
// 段序: [段][days] 平铺; dates 0 = 尾段空位 (整天作废, has = 0); has_prev[i] = 日轴上前一日的 has
struct SegPlan {
  size_t days = 1;
  std::vector<misc::Ymd> dates, prev;
  std::vector<LibMask> has, has_prev;
  std::vector<uint16_t> month;  // months 下标
  std::vector<uint8_t> weekday; // Mon=0 .. Sun=6
  size_t size() const { return dates.size() / days; }
  Day day(size_t i) const { return {dates[i], prev[i], has[i], has_prev[i]}; }
};

// 源 = 同 (库, 层) 的一组值列: 一天一次选列读 (cs 末位带该层 ts_valid), 行映射二选一 (row gather / at scatter)
// 读端 (平面) 与写端上游读 (CoreCs 旁读 / pool::Panel 票池成员位) 同一份: 跨库跨层一律 level_asof, 不另写特例
struct Source {
  LibId lib = kAllLib; // 平面 a ← 该库轴 Readers::ids[lib][a] (nullptr = 同轴)
  size_t level = L_min;
  fmeta::ColumnSet cs;        // 值列 (+ 门控列)
  std::vector<uint32_t> slot; // [cs.n()] 值列 → 平面列
  std::vector<int32_t> row;   // 网格行: [网格每日有效行] → 源行; -1 = 前一交易日末行
  std::vector<uint32_t> at;   // 尾部行: [源层有效行] → 网格行 (行末秒 − 1 所在)
  bool cur = false;           // 读当日 (网格: row 含 ≥ 0; 尾部: 恒读)
  bool carry = false;         // 读前一交易日 (网格: row 含 -1; 尾部: 要承接且开盘 as-of 落在前日)
};
// 网格行: 网格层 grid 的 rows 个行 → level_asof(源层, grid, t)
inline void project_rows(Source &s, size_t grid, size_t rows) {
  s.row.resize(rows);
  s.at.clear();
  s.cur = s.carry = false;
  for (size_t t = 0; t < rows; ++t) {
    s.row[t] = static_cast<int32_t>(level_asof(s.level, grid, t));
    s.cur |= s.row[t] >= 0;
    s.carry |= s.row[t] < 0;
  }
}
// 尾部行: 源行 r → 网格层 grid 中含 level_end_sec(源层, r) − 1 的行; 承接 = carry 且网格开盘首行的 as-of 落在前一交易日
// (该时刻源层尚无当日行, 已知值 = 前日末行)
inline void tail_rows(Source &s, size_t grid, bool carry) {
  const size_t R = level_valid_rows(s.level);
  s.row.clear();
  s.at.resize(R);
  for (size_t r = 0; r < R; ++r)
    s.at[r] = static_cast<uint32_t>(level_row_of_sec(grid, level_end_sec(s.level, r) - 1));
  s.cur = true;
  s.carry = carry && level_asof(s.level, grid, 0) < 0;
}
// 读计划: 一组 ColRef → 网格层上的平面列 (GUI 线程构建, 见 analysis::read_plan); 源库 = 列所在库, 主库在 ReadScope
struct ReadPlan {
  size_t level = L_min;     // 网格层 (尾部行: 定 at 与开盘承接)
  std::vector<ColRef> cols; // [平面列]
  std::vector<Source> srcs;
  size_t n() const { return cols.size(); }
};
// 资产不在该库轴 (Readers::ids 的值): 平面格 = 无源
inline constexpr uint32_t kAbsent = UINT32_MAX;
// 库集各库 reader + A 轴 → 该库轴映射 (nullptr = 同轴; 库集外库 nullptr). 哪天哪库有数据归日轴 (Day::has), 不归 reader
struct Readers {
  PerLib<const FeatureRead *> rd{};
  PerLib<const uint32_t *> ids{};
  const FeatureRead &of(LibId l) const {
    assert(rd[l] && "库不在库集内");
    return *rd[l];
  }
};
// 库集 reader 组 (worker 线程按库清单开, 库集外 = 空清单): 拿到取消旗 (库判废时拉起); 清单没变 (目录 + 库轴) 的库复用
struct LibReaders {
  PerLib<FeatureLib> libs;
  PerLib<std::optional<FeatureRead>> at;
  PerLib<const FeatureRead *> ptr{};

  void open(const PerLib<FeatureLib> &want, std::atomic<bool> *abort) {
    for (const LibId l : lib_ids()) {
      const FeatureLib &w = want[l];
      if (w.name.empty()) {
        at[l].reset();
        libs[l] = FeatureLib{};
        ptr[l] = nullptr;
        continue;
      }
      assert(w.ready() && "库集含未建库 (请求方按 months 为空放弃)");
      if (!at[l] || libs[l].dir != w.dir || libs[l].axis.hash != w.axis.hash || libs[l].axis.size() != w.axis.size()) {
        libs[l] = w;
        at[l].emplace(libs[l], abort);
      }
      ptr[l] = &*at[l];
    }
  }
  bool stale() const {
    return std::ranges::any_of(at.v, [](const std::optional<FeatureRead> &r) { return r && r->stale(); });
  }
  void revive() { // 重读库 (重算清库 / 判废后新库落地), 判废解除
    for (std::optional<FeatureRead> &r : at.v)
      if (r)
        r->revive();
  }
};

// IO 暂存 (按 io 池 tid): 每源 当日 + 承接日 选列张量 + 选列表 (稳态零分配)
struct IoSlot {
  struct Buf {
    FeatureRead::DayColumns cur, prev;
    bool has_cur = false, has_prev = false;
  };
  std::vector<Buf> src; // [plan.srcs]
  std::vector<size_t> cols;

  // As: 各库 A 轴长 (ReadScope::sizes); max_cols: 单次读的值列上限 (分块读的平面传块宽)
  void prepare(const ReadPlan &plan, const PerLib<size_t> &As, size_t max_cols) {
    src.resize(plan.srcs.size());
    for (size_t i = 0; i < src.size(); ++i) {
      const Source &s = plan.srcs[i];
      const size_t n = std::min(s.cs.n(), max_cols) + s.cs.gated();
      const size_t SA = As[s.lib];
      if (s.cur)
        src[i].cur.preallocate(SA, s.level, n);
      if (s.carry)
        src[i].prev.preallocate(SA, s.level, n);
    }
    cols.reserve(max_cols + 1);
  }
};

// 源一天 (当日 + 承接日) 选列读进暂存; io = 本次读列 (值列子集 [+ 门控列]); 源库这天没落盘 (Day 位) = 无源
inline void load_source(const Readers &rd, const Source &s, const Day &d, const std::vector<size_t> &io, IoSlot::Buf &b) {
  const FeatureRead &reader = rd.of(s.lib);
  const LibMask bit = lib_bit(s.lib);
  assert(!(d.has & bit) || d.date != 0);
  assert(!(d.has_prev & bit) || d.prev != 0);
  b.has_cur = s.cur && (d.has & bit);
  b.has_prev = s.carry && (d.has_prev & bit);
  if (b.has_cur)
    reader.load_day_columns(d.date, io, b.cur);
  if (b.has_prev)
    reader.load_day_columns(d.prev, io, b.prev);
}
// 暂存 dc 第 k 列第 r 行首格 (dc nullptr = 无源 → nullptr)
inline const feature_storage_t *col_row(const FeatureRead::DayColumns *dc, size_t k, size_t r) {
  return dc ? dc->col(k) + r * dc->A : nullptr;
}
// 网格行 t → 源暂存 + 源行 r (nullptr = 无源)
inline const FeatureRead::DayColumns *source_row(const Source &s, const IoSlot::Buf &b, size_t t, size_t &r) {
  const int32_t x = s.row[t];
  if (x >= 0) {
    r = static_cast<size_t>(x);
    return b.has_cur ? &b.cur : nullptr;
  }
  r = level_valid_rows(s.level) - 1;
  return b.has_prev ? &b.prev : nullptr;
}

// 一格原值 → 折叠哨兵 (g = 同层门控同一格, nullptr = 不门控)
inline feature_storage_t fold_cell(feature_storage_t raw, const feature_storage_t *g, L2::ValidType vt) {
  if (g && !fmeta::valid(static_cast<float>(*g), vt))
    return std::bit_cast<feature_storage_t>(fmeta::kInvalidBits);
  return fmeta::nan(raw) ? std::bit_cast<feature_storage_t>(fmeta::kNaNBits) : raw;
}
// 源行的第 x 格 (源库轴下标; kAbsent = 资产不在该库 → 无源) → 折叠哨兵
inline feature_storage_t fold_at(const feature_storage_t *src, const feature_storage_t *g, uint32_t x, L2::ValidType vt) {
  return x == kAbsent ? std::bit_cast<feature_storage_t>(fmeta::kInvalidBits) : fold_cell(src[x], g ? g + x : nullptr, vt);
}
// 折叠核 (三平面同一份, 只差目的步长): 源一行 → 平面 [a0, a1) 格 dst[a · stride] (平面 a ← 源 ids[a], nullptr = 同轴).
// val nullptr = 无源 (整段哨兵); g = 同行门控 (nullptr = 不门控)
inline void fold_span(const feature_storage_t *val, const feature_storage_t *g, L2::ValidType vt, const uint32_t *ids,
                      size_t a0, size_t a1, feature_storage_t *dst, size_t stride) {
  if (!val) {
    for (size_t a = a0; a < a1; ++a)
      dst[a * stride] = std::bit_cast<feature_storage_t>(fmeta::kInvalidBits);
    return;
  }
  for (size_t a = a0; a < a1; ++a)
    dst[a * stride] = fold_at(val, g, ids ? ids[a] : static_cast<uint32_t>(a), vt);
}

// 单段一任务的批装载 (TsPlane / RowPlane 共用; CsPlane 按 段 × 列块 自己拆): 批 [s0, s0 + bs) 第 j 段 = 第 s0 + j 段,
// io 池跑, 暂存按 tid; 只写本平面 (可与另一平面上的计算并发)
template <class Plane>
struct SegBatch {
  void load_batch(misc::exec::ForkJoin &io, std::vector<IoSlot> &slots, const Readers &rd, const ReadPlan &plan,
                  const SegPlan &sp, size_t s0, size_t bs, const std::atomic<bool> &cancel) {
    Plane &pl = static_cast<Plane &>(*this);
    assert(bs <= pl.segs && s0 + bs <= sp.size() && slots.size() >= static_cast<size_t>(io.threads()));
    io.tasks(static_cast<int>(bs), [&](int j, int tid) {
      if (!cancel.load(std::memory_order_relaxed))
        pl.load_seg(rd, plan, sp, s0 + static_cast<size_t>(j), static_cast<size_t>(j), slots[tid]);
    });
  }
};

// ============================================================================
// TsPlane: 段批 TS 视图 (Dist / Transform 分批流式, Preview 单段)
// ============================================================================
struct TsPlane : SegBatch<TsPlane> {
  static constexpr size_t kTransposeBlock = 64; // 转置按 64 资产一块: 块内写驻留在 64 条 cache line 上

  size_t A = 0, level = 0, R = 0, segs = 0, n_cols = 0;
  std::vector<feature_storage_t> data; // [n_cols][A][segs][R]

  size_t asset_stride() const { return segs * R; }

  // 资产 a 在平面列 c 的批内第 j 段序列 (R 个)
  const feature_storage_t *series(size_t c, size_t a, size_t j) const {
    assert(c < n_cols && a < A && j < segs);
    return data.data() + ((c * A + a) * segs + j) * R;
  }
  // (c, j, t) 截面列首元素; 资产 a 在 + a * asset_stride()
  const feature_storage_t *column(size_t c, size_t j, size_t t) const {
    assert(c < n_cols && j < segs && t < R);
    return data.data() + (c * A * segs + j) * R + t;
  }

  void prepare(size_t A_, size_t level_, size_t segs_, size_t n_cols_) {
    TraceN("TsPlanePrepare");
    assert(A_ > 0 && segs_ > 0 && n_cols_ > 0);
    A = A_;
    level = level_;
    R = seg_rows(level_);
    segs = segs_;
    n_cols = n_cols_;
    data.resize(n_cols * A * segs * R);
  }

  // 第 seg 段 → 批内第 j 段: 逐日逐源读进 sl, 行投影 + 按各列 vt 折叠哨兵 + 转置
  void load_seg(const Readers &rd, const ReadPlan &plan, const SegPlan &sp, size_t seg, size_t j, IoSlot &sl) {
    assert(j < segs && plan.n() == n_cols && plan.level == level && sp.days == seg_days(level));
    const size_t rpd = level_valid_rows(level), stride = asset_stride();
    for (size_t d = 0; d < sp.days; ++d) {
      const Day day = sp.day(seg * sp.days + d);
      for (size_t si = 0; si < plan.srcs.size(); ++si) {
        const Source &s = plan.srcs[si];
        IoSlot::Buf &b = sl.src[si];
        {
          TraceN("LoadDay");
          load_source(rd, s, day, s.cs.io, b);
        }
        TraceN("TransposeDay");
        const size_t nv = s.cs.n();
        const uint32_t *ids = rd.ids[s.lib];
        for (size_t k = 0; k < nv; ++k) {
          feature_storage_t *base = data.data() + (s.slot[k] * A * segs + j) * R + d * rpd; // + a * stride + t
          for (size_t a0 = 0; a0 < A; a0 += kTransposeBlock) {
            const size_t a1 = std::min(a0 + kTransposeBlock, A);
            for (size_t t = 0; t < rpd; ++t) {
              size_t r = 0;
              const FeatureRead::DayColumns *dc = source_row(s, b, t, r);
              fold_span(col_row(dc, k, r), s.cs.gated() ? col_row(dc, nv, r) : nullptr, s.cs.vts[k], ids, a0, a1, base + t, stride);
            }
          }
        }
      }
    }
  }

  void clear() {
    // 必须 move 赋空容器: `= {}` 走 initializer_list 重载, 只清元素不还内存
    data = std::vector<feature_storage_t>{};
    A = level = R = segs = n_cols = 0;
  }
};

// ============================================================================
// CsPlane: 段批 CS 视图 (Corr / CorrLag / CorrPair) —— 每段只抽 slice_t 指定的 S 行
// ============================================================================
// 行号 g = (j·S + s)·n_cols + c  ↔  data + g·A  (切片主序: 同一 (j, s) 的全部列相邻)
struct CsPlane {
  static constexpr size_t kBlockCols = 16; // IO 一任务读的列数 (暂存 16·T·A·2B)

  size_t A = 0, level = 0, R = 0, segs = 0, S = 0, n_cols = 0;
  std::vector<size_t> slice_t;         // [segs][S] 段内行号 (>= R = 越界, 整片作废); 调用方在 load 前填
  std::vector<feature_storage_t> data; // [segs][S][n_cols][A]

  const feature_storage_t *slice(size_t j, size_t s, size_t c) const {
    assert(j < segs && s < S && c < n_cols);
    return data.data() + ((j * S + s) * n_cols + c) * A;
  }

  void prepare(size_t A_, size_t level_, size_t segs_, size_t S_, size_t n_cols_) {
    TraceN("CsPlanePrepare");
    assert(A_ > 0 && segs_ > 0 && S_ > 0 && n_cols_ > 0);
    A = A_;
    level = level_;
    R = seg_rows(level_);
    segs = segs_;
    S = S_;
    n_cols = n_cols_;
    slice_t.assign(segs * S, R);
    data.resize(segs * S * n_cols * A);
  }

  // 一批 [s0, s0 + bs): (段 × 源列块) 一任务 —— 块内逐日读 (只读有切片落点的日, 列块 + 该源门控), 抽行即折叠 (暂存按 io tid)
  void load_batch(misc::exec::ForkJoin &io, std::vector<IoSlot> &slots, const Readers &rd, const ReadPlan &plan,
                  const SegPlan &sp, size_t s0, size_t bs, const std::atomic<bool> &cancel) {
    TraceN("CsPlaneLoad");
    assert(bs <= segs && s0 + bs <= sp.size() && plan.n() == n_cols && plan.level == level && sp.days == seg_days(level));
    assert(slots.size() >= static_cast<size_t>(io.threads()));
    struct Blk {
      size_t si, k0, k1;
    };
    std::vector<Blk> blks;
    for (size_t si = 0; si < plan.srcs.size(); ++si)
      for (size_t k0 = 0; k0 < plan.srcs[si].cs.n(); k0 += kBlockCols)
        blks.push_back({si, k0, std::min(k0 + kBlockCols, plan.srcs[si].cs.n())});
    const auto invalid = std::bit_cast<feature_storage_t>(fmeta::kInvalidBits);
    const size_t rpd = level_valid_rows(level), n_blk = blks.size();
    io.tasks(static_cast<int>(bs * n_blk), [&](int task, int tid) {
      if (cancel.load(std::memory_order_relaxed))
        return;
      const size_t j = static_cast<size_t>(task) / n_blk;
      const Blk &bk = blks[static_cast<size_t>(task) % n_blk];
      const Source &src = plan.srcs[bk.si];
      const size_t *st = slice_t.data() + j * S;
      for (size_t s = 0; s < S; ++s)
        if (st[s] >= R)
          for (size_t k = bk.k0; k < bk.k1; ++k)
            std::fill_n(data.data() + ((j * S + s) * n_cols + src.slot[k]) * A, A, invalid);
      IoSlot &sl = slots[tid];
      IoSlot::Buf &b = sl.src[bk.si];
      sl.cols.assign(src.cs.io.begin() + bk.k0, src.cs.io.begin() + bk.k1);
      if (src.cs.gated())
        sl.cols.push_back(src.cs.gate());
      for (size_t d = 0; d < sp.days; ++d) {
        if (std::none_of(st, st + S, [&](size_t t) { return t < R && t / rpd == d; }))
          continue;
        load_source(rd, src, sp.day((s0 + j) * sp.days + d), sl.cols, b);
        for (size_t s = 0; s < S; ++s) {
          if (st[s] >= R || st[s] / rpd != d)
            continue;
          size_t r = 0;
          const FeatureRead::DayColumns *dc = source_row(src, b, st[s] % rpd, r);
          const feature_storage_t *g = src.cs.gated() ? col_row(dc, bk.k1 - bk.k0, r) : nullptr;
          for (size_t k = bk.k0; k < bk.k1; ++k)
            fold_span(col_row(dc, k - bk.k0, r), g, src.cs.vts[k], rd.ids[src.lib], 0, A,
                      data.data() + ((j * S + s) * n_cols + src.slot[k]) * A, 1);
        }
      }
    });
  }

  void clear() {
    slice_t = std::vector<size_t>{};
    data = std::vector<feature_storage_t>{};
    A = level = R = segs = S = n_cols = 0;
  }
};

// ============================================================================
// RowPlane: 段批尾部行视图 (OrderFlow: 各列按自己的层画, 行 r 落在网格行 Source::at[r]; 段 = 1 天)
// ============================================================================
struct RowPlane : SegBatch<RowPlane> {
  size_t A = 0, segs = 0, n_cols = 0;
  std::vector<size_t> rows, off;       // [n_cols] 列 c 的层有效行数 / 在 data 中的起点
  std::vector<feature_storage_t> data; // 列 c: [A][segs][rows[c]]
  std::vector<feature_storage_t> open; // [n_cols][A][segs] 开盘承接值 (前日末行; 不承接 = kInvalidBits)

  const feature_storage_t *series(size_t c, size_t a, size_t j) const {
    assert(c < n_cols && a < A && j < segs);
    return data.data() + off[c] + (a * segs + j) * rows[c];
  }
  feature_storage_t carry(size_t c, size_t a, size_t j) const {
    assert(c < n_cols && a < A && j < segs);
    return open[(c * A + a) * segs + j];
  }

  void prepare(size_t A_, const ReadPlan &plan, size_t segs_) {
    TraceN("RowPlanePrepare");
    assert(A_ > 0 && segs_ > 0 && plan.n() > 0);
    A = A_;
    segs = segs_;
    n_cols = plan.n();
    rows.resize(n_cols);
    off.resize(n_cols);
    size_t n = 0;
    for (size_t c = 0; c < n_cols; ++c) {
      rows[c] = level_valid_rows(plan.cols[c].level);
      off[c] = n;
      n += A * segs * rows[c];
    }
    data.resize(n);
    open.resize(n_cols * A * segs);
  }

  // 第 seg 段 → 批内第 j 段: 逐源读当日 (+ 承接日) 进 sl, 原生行按各列 vt 折叠哨兵
  void load_seg(const Readers &rd, const ReadPlan &plan, const SegPlan &sp, size_t seg, size_t j, IoSlot &sl) {
    assert(j < segs && plan.n() == n_cols && sp.days == 1 && sl.src.size() == plan.srcs.size());
    const Day day = sp.day(seg);
    for (size_t si = 0; si < plan.srcs.size(); ++si) {
      const Source &s = plan.srcs[si];
      assert(!s.at.empty() && "RowPlane 只收尾部行源 (analysis::Rows::Tail)");
      IoSlot::Buf &b = sl.src[si];
      {
        TraceN("LoadDay");
        load_source(rd, s, day, s.cs.io, b);
      }
      const size_t nv = s.cs.n(), R = level_valid_rows(s.level);
      const uint32_t *ids = rd.ids[s.lib];
      const FeatureRead::DayColumns *cur = b.has_cur ? &b.cur : nullptr, *prev = b.has_prev ? &b.prev : nullptr;
      for (size_t k = 0; k < nv; ++k) {
        const size_t c = s.slot[k];
        const L2::ValidType vt = s.cs.vts[k];
        feature_storage_t *base = data.data() + off[c] + j * R; // + (a * segs) * R + r
        for (size_t r = 0; r < R; ++r)
          fold_span(col_row(cur, k, r), s.cs.gated() ? col_row(cur, nv, r) : nullptr, vt, ids, 0, A, base + r, segs * R);
        fold_span(col_row(prev, k, R - 1), s.cs.gated() ? col_row(prev, nv, R - 1) : nullptr, vt, ids, 0, A,
                  open.data() + c * A * segs + j, segs);
      }
    }
  }

  void clear() {
    rows = off = std::vector<size_t>{};
    data = open = std::vector<feature_storage_t>{};
    A = segs = n_cols = 0;
  }
};

// ============================================================================
// Prefetch<Plane>: 双缓冲预读 —— 算 front 的同时后台线程把下一批读进 back
// ============================================================================
// 两个池互不重入: 计算池由 worker 线程 dispatch, io 池由预读线程 dispatch.
// 循环契约 (issue 与 wait 之间不许 return: 预读线程引用着调用方栈) 只在 stream 里写一份:
//   now(load 0);
//   for b: { if (有下一批) issue(load b+1);  compute(front());  wait();  if (cancel) return false;  publish;  flip(); }
template <class Plane>
struct Prefetch {
  Plane buf[2];
  size_t cur = 0;
  std::unique_ptr<misc::exec::ForkJoin> io;
  std::vector<IoSlot> slots; // [io 线程数]
  std::thread th;

  Plane &front() { return buf[cur]; }
  Plane &back() { return buf[cur ^ 1]; }

  // 幂等: 建 io 池 + 暂存 (As / max_cols 见 IoSlot::prepare); 两个平面的尺寸由调用方各自 prepare
  void prepare(size_t n_io, const ReadPlan &plan, const PerLib<size_t> &As, size_t max_cols) {
    assert(!th.joinable());
    misc::exec::ensure(io, static_cast<int>(n_io));
    slots.resize(n_io);
    for (IoSlot &s : slots)
      s.prepare(plan, As, max_cols);
    cur = 0;
  }
  // fn(Plane &, ForkJoin &io, std::vector<IoSlot> &)
  template <class Fn>
  void now(Fn &&fn) {
    assert(!th.joinable());
    fn(front(), *io, slots);
  }
  template <class Fn>
  void issue(Fn &&fn) {
    assert(!th.joinable());
    th = std::thread([this, fn = std::forward<Fn>(fn)]() mutable { fn(back(), *io, slots); });
  }
  void wait() {
    if (th.joinable())
      th.join();
  }
  void flip() {
    assert(!th.joinable());
    cur ^= 1;
  }
  // 流式批骨架 (Dist / Transform / Corr / OrderFlow Kline 同一份): n 段每批 bs 段, 算本批的同时预读下一批; 取消 → false (半批不发布)
  //   load(b0, bd) → fn(Plane &, ForkJoin &io, std::vector<IoSlot> &);  compute(const Plane &, b0, bd);  publish(b0, bd)
  template <class Load, class Compute, class Publish>
  bool stream(size_t n, size_t bs, const std::atomic<bool> &cancel, Load &&load, Compute &&compute, Publish &&publish) {
    assert(bs > 0);
    if (n == 0)
      return true; // 空日轴: 直接 Done
    now(load(size_t{0}, std::min(bs, n)));
    for (size_t b0 = 0; b0 < n; b0 += bs) {
      const size_t bd = std::min(bs, n - b0);
      if (b0 + bd < n)
        issue(load(b0 + bd, std::min(bs, n - b0 - bd)));
      compute(static_cast<const Plane &>(front()), b0, bd);
      wait();
      if (cancel.load(std::memory_order_relaxed))
        return false;
      publish(b0, bd);
      flip();
    }
    return true;
  }
  void clear() {
    wait();
    buf[0].clear();
    buf[1].clear();
    io.reset();
    slots = std::vector<IoSlot>{};
    cur = 0;
  }
  ~Prefetch() { wait(); }
};
