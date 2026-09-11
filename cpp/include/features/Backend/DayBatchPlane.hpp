#pragma once

#include "features/Backend/FeatureRead.hpp"
#include "features/MetaFlag.hpp" // fmeta::valid
#include "misc/profiler.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdint>
#include <string>
#include <vector>

// ============================================================================
// DayBatchPlane: 天批平面 —— 分批流式分析 (Dist / Transform) 共用的 IO 骨架
// ============================================================================
// 天是流式维度: 每批 days 个天, 抢单天并行 load_day_columns [T][列][A] → 转置进资产主序批平面
//   data[c][a][j][t]   c = 值列, a = 资产, j = 批内天, t = 有效行 (level_valid_rows, 末哨兵行不进平面)
// 每资产每列一段连续 days×VR (TS 扫描友好); 某 (j, t) 的截面列则按 asset_stride 跨步取 (CS gather).
//
// valid 门控 (可选 _meta 列, 恒为 columns 末列) 不过 与 真 NaN 一并折叠成统一哨兵 kInvalidBits (f16 qNaN):
// 二者的分账在 IO 就地做完 (这里还看得见 valid 列), 热扫描一次 v != v 就能跳过, 也不怕哨兵撞上数据里的 NaN.
// 真 NaN 只记值列 0 (被分析的特征列); 附加列 (中性化上下文等) 同一门控, 不记账.
//
// 容量准备幂等: 尺寸对得上零分配 (prewarm 与 build 共用).
// ============================================================================
struct DayBatchPlane {
  static constexpr uint16_t kInvalidBits = 0x7E00u;
  static constexpr size_t kTransposeBlock = 64; // 转置按 64 资产一块: 块内写驻留在 64 条 cache line 上

  size_t A = 0, level = 0, VR = 0, days = 0, n_cols = 0;
  std::vector<feature_storage_t> data;          // [n_cols][A][days][VR]
  std::vector<FeatureRead::DayColumns> staging; // [n_io] 每 IO 线程一份 [T][列][A] 暂存
  std::vector<uint64_t> nan_seen;               // [n_io] 本批真 NaN 数 (值列 0), 批末调用方收走并清零

  size_t asset_stride() const { return days * VR; }

  // 资产 a 在值列 c 的批内第 j 天序列 (VR 个)
  const feature_storage_t *series(size_t c, size_t a, size_t j) const {
    assert(c < n_cols && a < A && j < days);
    return data.data() + ((c * A + a) * days + j) * VR;
  }
  // (c, j, t) 截面列首元素; 资产 a 在 + a * asset_stride()
  const feature_storage_t *column(size_t c, size_t j, size_t t) const {
    assert(c < n_cols && j < days && t < VR);
    return data.data() + (c * A * days + j) * VR + t;
  }

  // 幂等容量准备. n_io = 参与 IO 的线程数 (≤ days 才有意义), max_columns = 值列 + 可选 valid 列 的上界
  void prepare(size_t A_, size_t level_, size_t days_, size_t n_cols_, size_t n_io, size_t max_columns) {
    TraceN("PlanePrepare");
    assert(A_ > 0 && days_ > 0 && n_cols_ > 0 && n_io > 0);
    A = A_;
    level = level_;
    VR = level_valid_rows(level_);
    days = days_;
    n_cols = n_cols_;
    data.resize(n_cols * A * days * VR);
    staging.resize(n_io);
    for (auto &s : staging)
      s.preallocate(A, level, max_columns);
    nan_seen.assign(n_io, 0);
  }

  // 一天: 读 columns (值列... [+ valid 列]) → 转置进批内第 j 天. tid 选 staging/nan_seen 槽 (每线程独占).
  // has_valid: columns 末列是 _meta 门控列, 按 valid_type 判 (编码见 Meta.hpp); 否则全部视为有效.
  void load_day(const FeatureRead &reader, const std::string &date, const std::vector<size_t> &columns,
                bool has_valid, L2::ValidType valid_type, size_t j, size_t tid) {
    assert(j < days && tid < staging.size());
    assert(columns.size() == n_cols + (has_valid ? 1 : 0));
    FeatureRead::DayColumns &st = staging[tid];
    {
      TraceN("LoadDay");
      reader.load_day_columns(date, columns, st);
    }
    TraceN("TransposeDay");
    const size_t n_total = columns.size();
    const auto invalid = std::bit_cast<feature_storage_t>(kInvalidBits);
    const size_t stride = asset_stride();
    uint64_t nans = 0;
    for (size_t c = 0; c < n_cols; ++c) {
      feature_storage_t *base = data.data() + (c * A * days + j) * VR; // + a * stride + t
      for (size_t a0 = 0; a0 < A; a0 += kTransposeBlock) {
        const size_t a1 = std::min(a0 + kTransposeBlock, A);
        for (size_t t = 0; t < VR; ++t) {
          const feature_storage_t *val = st.data.data() + (t * n_total + c) * A;
          const feature_storage_t *valid = st.data.data() + (t * n_total + n_cols) * A; // has_valid 才读
          for (size_t a = a0; a < a1; ++a) {
            const bool ok = !has_valid || fmeta::valid(static_cast<float>(valid[a]), valid_type);
            const feature_storage_t raw = val[a];
            const bool is_nan = ok && !(raw == raw);
            nans += (c == 0) & is_nan;
            base[a * stride + t] = (ok && !is_nan) ? raw : invalid;
          }
        }
      }
    }
    nan_seen[tid] += nans;
  }

  // 批末: 收走全部 IO 线程的真 NaN 账目并清零
  uint64_t take_nan_seen() {
    uint64_t total = 0;
    for (uint64_t &n : nan_seen) {
      total += n;
      n = 0;
    }
    return total;
  }

  void clear() {
    // 必须 move 赋空容器: `= {}` 走 initializer_list 重载, 只清元素不还内存
    data = std::vector<feature_storage_t>{};
    staging = std::vector<FeatureRead::DayColumns>{};
    nan_seen = std::vector<uint64_t>{};
    A = level = VR = days = n_cols = 0;
  }
};
