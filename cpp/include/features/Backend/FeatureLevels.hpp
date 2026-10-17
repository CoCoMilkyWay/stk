#pragma once

// ============================================================================
// FEATURE LEVELS — 特征存储的"字段表无关"稳定层 (从 FeatureStoreConfig.hpp 拆出)
// ============================================================================
// 读端 (FeatureRead / GUI / shared) 只需要这里的结构体 + 运行时层表访问, 不 include
// 字段表 (FieldsGenerated) —— 增删改特征不触发它们重编, 只重编 FeatureMeta.cpp + 写端.
//   level_info(lvl)   运行时层表 (定义在 src/features/FeatureMeta.cpp, 指向编译期 LEVELS)
//   编译期 LEVELS[] (写端定址折叠用) 在 FeatureStoreConfig.hpp, 仅写端 include.
// ============================================================================

// 落盘编码两实现 (同 API 对仗), 选型见文件末尾 FeatureCodec
#include "SparseCodec.hpp"
#include "ZstdCodec.hpp"               // IWYU pragma: keep
#include "codec/L2_DataType.hpp"       // L2::ValidType (FieldInfo.valid)
#include "features/FeaturesDefine.hpp" // FeatureDataType / ALL_LEVELS (层数)
#include "features/Method/TS.hpp"      // ts::TfId (FieldInfo.ts_tf)
#include "misc/date.hpp"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

// 存储类型: _Float16 (内存/磁盘减半, F16C/AVX-512 FP16/NEON 硬件转换, ±65504, ~3.3 位有效数字)
using feature_storage_t = _Float16;

// 每列 {code, width, valid, kind, ts_tf} (由字段表 SRC 列推出)
struct FieldInfo {
  const char *code;
  size_t width;
  L2::ValidType valid;
  FeatureDataType kind;
  ts::TfId ts_tf; // OP 行落盘前套用的元素变换 (改变落盘值 → 进指纹); 非 OP 行 None
};

// 层表项: 运行时按层下标取 rows / width / offsets / fingerprint
struct LevelInfo {
  const char *level_name;  // "L0" / "L1": 文件名 features_<name>.bin
  size_t rows;             // T
  size_t width;            // F_total
  size_t field_count;      // 列数 (≤ width)
  const FieldInfo *fields; // [field_count]
  const size_t *offsets;   // [field_count] 列下标 → 行内偏移
  uint64_t fingerprint;    // 字段表指纹
  const char *psd;         // 该层特征的推荐频谱 (GUI 元数据)
  bool xor_delta;          // true: 落盘前沿 T 轴 XOR 差分 (无损预变换, 提升 zstd 收益, 见 ALL_LEVELS)
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

// ============================================================================
// 特征日文件 — <base>/YYYY/MM/DD/features_<LVL>.bin, 一天一层一个
// ============================================================================
//
//   [FeatureFileHeader][ColRef × F][列载荷 × F]
//
//   头 + 目录定宽 (F = 该层存储列数 width, 编译期固定): 写端先空出, 逐列追加载荷,
//   收工回填头 + 目录, .tmp → rename 原子发布. 读端一次拿头 + 目录, 按列 pread.
//   列载荷 = 一列 [T][A] (xor_delta 层先沿 T 差分) 经 FeatureCodec 编码; 目录下标
//   = 存储列 (宽 1 字段即字段 offsets[f]).
//   A / axis_hash = universe 子轴 (列 → 资产映射靠子轴顺序, 不在文件里);
//   fingerprint = 字段表指纹. 三者与读端期望不符 = 旧库, 读端判废.
//
// 与 orders 日文件 (shared/OrdersDay.hpp) 同形: 头 + 载荷 + 目录. 这里单写者顺序
// 追加且 F 已知, 目录放前部; orders 多 worker 并发追加, 目录只能放尾部.
struct FeatureFileHeader {
  static constexpr uint64_t kMagic = 0x0000315941445446; // 'F','T','D','A','Y','1' 小端

  uint64_t magic;
  uint64_t T;
  uint64_t F;
  uint64_t A;
  uint64_t axis_hash;
  uint64_t fingerprint;
};
static_assert(sizeof(FeatureFileHeader) == 48, "特征日文件头必须定宽无填充");

// 一列载荷在日文件里的位置
struct ColRef {
  uint64_t offset;
  uint64_t size;
};
static_assert(sizeof(ColRef) == 16, "特征日文件目录项必须紧凑无填充");

inline std::string feature_day_dir(const std::string &base, misc::Ymd date) {
  return base + "/" + misc::slash(date);
}
inline std::string feature_file(const std::string &day_dir, size_t lvl) {
  return day_dir + "/features_" + level_info(lvl).level_name + ".bin";
}

// ============================================================================
// T 轴 XOR 差分 (LEVELS[lvl].xor_delta 层的落盘预变换, 无损, 就地)
//   编码 (写端, 压缩前): row[t] ^= row[t-1], t 从高往低; row = 一列载荷的一个时间行
//   (A 个). 解码 (读端, 解压后): 前缀 XOR, t 从低往高.
//   fp16 按位当 uint16 处理, 往返恒等 (含 NaN/Inf 位型).
// ============================================================================
static_assert(sizeof(feature_storage_t) == sizeof(uint16_t));
inline void xor_delta_encode(feature_storage_t *data, size_t T, size_t row_elems) {
  auto *u = reinterpret_cast<uint16_t *>(data);
  for (size_t t = T - 1; t >= 1; --t)
    for (size_t i = 0; i < row_elems; ++i)
      u[t * row_elems + i] ^= u[(t - 1) * row_elems + i];
}
inline void xor_delta_decode(feature_storage_t *data, size_t T, size_t row_elems) {
  auto *u = reinterpret_cast<uint16_t *>(data);
  for (size_t t = 1; t < T; ++t)
    for (size_t i = 0; i < row_elems; ++i)
      u[t * row_elems + i] ^= u[(t - 1) * row_elems + i];
}

// ============================================================================
// 落盘编码选型: 两种实现同一 API (bound / encode / decode), 载荷不落自述信息,
// 读端按此别名解码 —— 换选型 = 换别名 + 重算特征库 (解码断言会拦住旧文件)
//   SparseCodec  位图+非零字面: 全天 ~45%, 内存带宽级, IO 单核无压力 (默认)
//   ZstdCodec    zstd 熵压缩:   全天 ~29%, 单核 ~185 MB/s 顶不住落盘节奏
//   CODEC_ENABLED = false: 裸写直读 (写读两端 if constexpr 免掉中转拷贝)
// ============================================================================
using FeatureCodec = SparseCodec;
inline constexpr bool CODEC_ENABLED = true;
