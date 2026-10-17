#pragma once

// ============================================================================
// FEATURE LEVELS — 特征存储的"字段表无关"稳定层 (从 FeatureStoreConfig.hpp 拆出)
// ============================================================================
// 读端 (FeatureRead / GUI / shared) 只需要这里的结构体 + 运行时层表访问, 不 include
// 字段表 (FieldsGenerated) —— 增删改特征不触发它们重编, 只重编 FeatureMeta.cpp + 写端.
//   level_info(lvl)   运行时层表 (定义在 src/features/FeatureMeta.cpp, 指向编译期 LEVELS)
//   编译期 LEVELS[] (写端定址折叠用) 在 FeatureStoreConfig.hpp, 仅写端 include.
// 日文件格式 / 落盘编码在 FeatureDay.hpp.
// ============================================================================

#include "codec/L2_DataType.hpp"       // L2::ValidType (FieldInfo.valid)
#include "features/FeaturesDefine.hpp" // FeatureDataType / ALL_LEVELS (层数)
#include "features/Method/TS.hpp"      // ts::TfId (FieldInfo.ts_tf)
#include <cstddef>
#include <cstdint>

// 存储类型: _Float16 (内存/磁盘减半, F16C/AVX-512 FP16/NEON 硬件转换, ±65504, ~3.3 位有效数字)
using feature_storage_t = _Float16;

// 每列 {code, valid, kind, ts_tf} (由字段表 SRC 列推出); 列下标 = 存储列
struct FieldInfo {
  const char *code;
  L2::ValidType valid;
  FeatureDataType kind;
  ts::TfId ts_tf; // OP 行落盘前套用的元素变换 (改变落盘值 → 进指纹); 非 OP 行 None
};

// 层表项: 运行时按层下标取 rows / field_count / fingerprint
struct LevelInfo {
  const char *level_name;  // "L0" / "L1": 文件名 features_<name>.bin
  size_t rows;             // T
  size_t field_count;      // F
  const FieldInfo *fields; // [field_count]
  uint64_t fingerprint;    // 字段表指纹
  const char *psd;         // 该层特征的推荐频谱 (GUI 元数据)
};

// 层数 (ALL_LEVELS 项数, 与字段表无关)
#define LEVEL_COUNT_ONE(...) +1
inline constexpr size_t LEVEL_COUNT = 0 ALL_LEVELS(LEVEL_COUNT_ONE);
#undef LEVEL_COUNT_ONE

// 运行时层表访问 (定义在 src/features/FeatureMeta.cpp — 唯一依赖字段表的元数据 TU)
const LevelInfo &level_info(size_t lvl);

// 有效行数 = 落盘行数 - 1: 末行是哨兵 (label lookahead 的落点, 不是真实时间).
// rows 用于缓冲区 / stride; 消费端迭代时间轴一律用本函数, 不要用 rows.
inline size_t level_valid_rows(size_t lvl) { return level_info(lvl).rows - 1; }
