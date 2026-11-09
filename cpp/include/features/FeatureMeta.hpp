#pragma once

#include "features/FeaturesDefine.hpp"        // FeatureDataType / L2::ValidType
#include "features/Method/CS.hpp"             // CS 行 Tf / Method
#include "features/Method/TS.hpp"             // TS 行 Tf / Method
#include "features/Storage/FeatureLevels.hpp" // TierId / LEVEL_COUNT
#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

// ============================================================================
// Feature Metadata Structure (for UI display)
// ============================================================================

struct FeatureMetadata {
  const char *code;          // tick_ret_z
  L2::ValidType valid_type;  // ALL/DATA/DEPTH (由 SRC / 节点 flush 域推出)
  FeatureDataType data_type; // TS/CS/LB/META (由 SRC 列推出)
  const char *cat_l1;        // Node/TS/<dir> or Node/CS/<dir>
  const char *file;          // 定义该列的节点文件 (include/ 下相对路径, 如 features/Node/Bar/TS/Basic/Kline.hpp)
  const char *cat_l2;        // FIELDS row token (AUTO = GUI 值域自动探测; 其他 = 人工覆盖), generated into FeatureCategoryL2_ALL
  ts::TfId ts_tf;            // 由 SRC 推出: TS(..., Tf, Method) → 时序归一化 (Tf 已落盘套用, Method 占位); 非 TS 行 None
  ts::MethodId ts_method;
  cs::TfId cs_tf; // 由 SRC 推出: CS(..., Tf, Method) → 截面归一化; 非 CS 行 None
  cs::MethodId cs_method;
  const char *formula;     // "(r-μ)/σ, W=50"
  const char *name_en;     // "Tick Return Z-score"
  const char *name_cn;     // "微小对数收益"
  const char *description; // "滚动窗口标准化..."
  uint8_t level;           // 0=L_sec, 1=L_1min
};

// ============================================================================
// 字段表元数据的运行时入口 (定义在 src/features/FeatureMeta.cpp — 唯一依赖生成字段表
// 的元数据 TU; 本头保持稳定, 增删改特征不重编消费者, 只重编 FeatureMeta.cpp)
// ============================================================================
namespace feature_meta {
std::span<const FeatureMetadata> features(TierId tier, size_t level); // 该档该层字段表 (行序 = 列序; 档缺的层为空)
std::span<const char *const> categories_l1(TierId tier);              // Node/<Tier>/{TS,CS}/<分类> 目录名 (排序)
std::span<const char *const> categories_l2(TierId tier);              // 字段表二级分类 token (排序)
// 每层每字段的直接依赖 (字段 code, 分号分隔), 与 features(tier, level) 平行
//   TS 字段 = 上游节点产出的字段 (单口引用只取该口); CS 字段 = 源字段
std::array<std::vector<std::string>, LEVEL_COUNT> deps(TierId tier);
} // namespace feature_meta
