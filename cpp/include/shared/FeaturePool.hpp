#pragma once

// 特征库 = 名 + 档 + A 轴 + 目录 + 日期轴. 池名即目录名 (<feature_dir>/<name>/), 档由名定:
//   all        Bar 档 (bar1m + 基本面), 全轴, 交易日 [PIPELINE_START_DATE, end_date]
//   <pool>     Lob 档 (逐笔 → 订单簿), 池轴, 回测日历; <pool> = all 池的 pool_<pool> 列 (feature_pools)
// 写端 (ComputeService) 规划后落清单 <dir>/pool.json (save_pool); 读端 (FeatureRead / ReadScope / 截面旁读) 只读清单 (load_pool),
// 两端永远同一 (档, 轴, 日期轴). 无清单 = 未建池 (空轴, ready() == false).
//
// 计算 / 落盘单位 = 池 (TS + CS 同池一趟), 刻意不拆:
//   TS 行将来可以吃池内 CS 的输出 (截面 → 时序), TS / CS 剥不干净 → 不把 TS 拆成跨池共享的一份, 也不跨池去重 TS.
//   Lob 只在池内算 (池轴 = 本池并集); 一般只算一个池, 不为"多池一起算"做并集轴 / 共享优化.

#include "features/Storage/FeatureLevels.hpp" // TierId
#include "misc/date.hpp"
#include "shared/AssetAxis.hpp" // PoolAxis
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

struct Config;

// 池表 (全部池唯一一份, 定义在 FeatureMeta.cpp): [0] = all (Bar 档), 其余 = 池 (Lob 档, 由 Bar 字段表 Pool 类 pool_* 列展开, 字段表序)
struct PoolDef {
  const char *name;    // 池名 = 目录名 (池 = 列名去 pool_ 前缀)
  const char *name_cn; // 中文名 (池 = pool_<name> 列的 name_cn; all = 全市场), UI 下拉跟在池名后
  TierId tier;
  size_t pool_col; // 池: Bar 池 L_1day 的 pool_<name> 列下标; all: SIZE_MAX
};
std::span<const PoolDef> feature_pools(); // 下标 = PoolId
const PoolDef &pool_def(PoolId pool);
const PoolDef &pool_def(std::string_view name); // 不是池名 → assert
PoolId pool_id(std::string_view name);          // 不是池名 → assert

struct FeaturePool {
  TierId tier = TierId::Lob;
  std::string name;             // 池名 (= 目录名)
  PoolAxis axis;                // A 轴: 张量列 / 文件列序 / 头指纹 (空 = 未建池)
  std::string dir;              // 池目录 (日文件落在其下)
  std::vector<misc::Ymd> dates; // 日期轴 (升序; 已落盘的是其子集, 见 FeatureRead::has_date)
  bool ready() const { return axis.size() > 0; }
};
FeaturePool load_pool(const Config &cfg, std::string_view name); // 读清单; 无清单 → 未建池 (tier / name / dir 仍有效)
void save_pool(const FeaturePool &pool);                         // 写清单 (tmp + rename)
std::string default_pool(const Config &cfg, TierId tier);        // 该档首个已建池, 都没建 → 该档首个池
