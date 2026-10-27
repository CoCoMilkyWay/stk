#pragma once
// fmeta — ts_valid / cs_valid 列编码/解码唯一事实源 (稳定头:
// 消费端 (DayBatchPlane / Preview / CoreCs / OrderFlowService) 只需
// 判定函数, 不依赖节点文件 —— 节点改动不牵连它们重编). 写入端状态机 MetaTracker
// 与落盘列声明在 Meta.hpp.
//
//   ts_valid 单槽三态编码: 0 = 该行无事件 (L_sec) / 无成交 (L_1min); 非 0 = 有 (data 有效); 负 = 盘口有更新 (depth 有效)
//   幅值: L_sec = 当时 micro price (量加权中间价, 元; 开盘竞价盘口未发布时退化为最近事件价); L_1min = 1 (纯标志)
//   依据: 盘口更新必来自事件 → depth ⟹ data, 三态刚好用 零/符号 编进一个 Float16 槽, 价格精度无损
//   L_sec 事件稀疏, 行只在事件秒写, 特征列 DATA / DEPTH 按它门控; L_1min 稠密, 255 行每行都写 (无成交分钟各节点归零 / 延续),
//   特征列 ALL 不门控, ts_valid 只是"本分钟有成交"的信息位 (标签分钟型 exit 只在这些分钟推进).
//   cs_valid = 当日在池 ? 1 : 0 (整日常量, 与事件无关; 判定用 data_valid), 决定谁进截面
//   (CoreCs gather / 因子层 CS 节点 / Stat), 不与 ts_valid 相与 (见 Meta.hpp)

#include "codec/L2_DataType.hpp" // L2::ValidType (fmeta::valid)
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace fmeta {
inline constexpr bool data_valid(float v) { return v != 0.0f; }
inline constexpr bool depth_valid(float v) { return v < 0.0f; }
inline constexpr float price(float v) { return v < 0.0f ? -v : v; }
inline constexpr float pack(bool depth, float mag) { return depth ? -mag : mag; }
// 某特征列按其 valid_type 的门控判定 (消费端读同层 ts_valid 后调用; cs_valid 是 0/1, 只用 data_valid)
inline constexpr bool valid(float v, L2::ValidType vt) {
  return vt == L2::ValidType::ALL || (vt == L2::ValidType::DEPTH ? depth_valid(v) : data_valid(v));
}

// 落盘 f16 值判定: 按位测, 不走浮点比较 —— -ffast-math TU 里 isnan/isfinite/x!=x 会被折成常量
// 消费端两个哨兵 (都是 f16 qNaN, 热循环一次 v != v 一并跳过; 要分账时按位区分):
inline constexpr uint16_t kInvalidBits = 0x7E00u; // 门控不过
inline constexpr uint16_t kNaNBits = 0x7E01u;     // 门控过但值为 NaN (任意 NaN 位型归一到这里)
inline bool nan(_Float16 x) { return (std::bit_cast<uint16_t>(x) & 0x7FFFu) > 0x7C00u; }
inline bool finite(_Float16 x) { return (std::bit_cast<uint16_t>(x) & 0x7C00u) != 0x7C00u; }
inline bool is_nan_sentinel(_Float16 x) { return std::bit_cast<uint16_t>(x) == kNaNBits; }
// 特征值可进统计: 门控 (gate = 同层 ts_valid) 通过 且 有限
inline bool keep(float gate, L2::ValidType vt, _Float16 x) { return valid(gate, vt) && finite(x); }

// 消费端列描述 (流式分析 / 平面 IO 唯一一份): 值列 + 各自门控语义. 任一值列需门控才带同层 ts_valid (读列表末位);
// 上下文列 (mcap / ind / lim 等) 就是 vt = ALL 的普通值列
struct ColumnSet {
  std::vector<size_t> io;         // 读列表: 值列 [n] (+ 门控列)
  std::vector<L2::ValidType> vts; // [n] 各值列的门控语义
  size_t n() const { return vts.size(); }
  bool gated() const { return io.size() > vts.size(); }
  size_t gate() const {
    assert(gated());
    return io.back();
  }
};
} // namespace fmeta
