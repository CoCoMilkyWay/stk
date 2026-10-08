#pragma once
// fmeta — ts_valid / cs_valid 列编码/解码唯一事实源 (从 Operator/TS/Meta/Meta.hpp 拆出的稳定头:
// 消费端 (DayBatchPlane / FeaturePreview / CoreCrosssection / OrderFlowService) 只需
// 判定函数, 不依赖算子文件 —— 算子改动不再牵连它们重编). 写入端状态机 MetaTracker
// 与落盘列声明仍在 Meta.hpp.
//
//   ts_valid 单槽三态编码: 0 = 该行无事件; 非 0 = 有事件写入 (data 有效); 负 = 盘口有更新 (depth 有效)
//   幅值: L0 = 当时 micro price (量加权中间价, 元; 开盘竞价盘口未发布时退化为最近事件价); L1 = 1 (纯标志)
//   依据: 盘口更新必来自事件 → depth ⟹ data, 三态刚好用 零/符号 编进一个 Float16 槽, 价格精度无损
//   cs_valid = 当日在池 ? 1 : 0 (整日常量, 与事件无关; 判定用 data_valid). 分工: ts_valid 门控逐资产分析 / 因子 TS 输入;
//   cs_valid 决定谁进截面 (CoreCrosssection gather / 因子层 CS 节点 / Stat), 不与 ts_valid 相与 (见 Meta.hpp)

#include "codec/L2_DataType.hpp" // L2::ValidType (fmeta::valid)

namespace fmeta {
inline constexpr bool data_valid(float v) { return v != 0.0f; }
inline constexpr bool depth_valid(float v) { return v < 0.0f; }
inline constexpr float price(float v) { return v < 0.0f ? -v : v; }
inline constexpr float pack(bool depth, float mag) { return depth ? -mag : mag; }
// 某特征列按其 valid_type 的门控判定 (消费端读同层 ts_valid 后调用; cs_valid 是 0/1, 只用 data_valid)
inline constexpr bool valid(float v, L2::ValidType vt) {
  return vt == L2::ValidType::ALL || (vt == L2::ValidType::DEPTH ? depth_valid(v) : data_valid(v));
}
} // namespace fmeta
