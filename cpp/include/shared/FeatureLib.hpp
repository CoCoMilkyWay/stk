#pragma once

// 特征库 = 名 + 档 + A 轴 + 目录 + 日期轴. 库名即目录名 (<feature_dir>/<name>/), 档由名定:
//   all        Bar 档 (bar1m + 基本面), 全轴, 交易日 [PIPELINE_START_DATE, end_date]
//   <pool>     Lob 档 (逐笔 → LOB), 票池并集子轴, 回测日历; <pool> = all 库票池列 pool_<pool> (feature_pools)
// 写端 (ComputeService) 规划后落清单 <dir>/lib.json (save_lib); 读端 (FeatureRead / ReadScope / 截面旁读) 只读清单 (load_lib),
// 两端永远同一 (档, 轴, 日期轴). 无清单 = 未建库 (空轴, ready() == false).

#include "features/Storage/FeatureLevels.hpp" // TierId
#include "misc/date.hpp"
#include "shared/AssetAxis.hpp" // UniverseAxis
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

struct Config;

// 票池: 库名 (列名去 pool_ 前缀) + all 库 L1 列下标. 定义在 FeatureMeta.cpp (由 Bar 字段表 Pool 类 pool_* 列展开)
struct PoolDef {
  const char *name;
  size_t col;
};
std::span<const PoolDef> feature_pools();
const PoolDef &pool_def(std::string_view name); // 不是票池名 → assert

TierId lib_tier(std::string_view name); // all → Bar, 票池名 → Lob, 其余 → assert

struct FeatureLib {
  TierId tier = TierId::Lob;
  std::string name;             // 库名 (= 目录名)
  UniverseAxis uni;             // A 轴: 张量列 / 文件列序 / 头指纹 (空 = 未建库)
  std::string dir;              // 库目录 (日文件落在其下)
  std::vector<misc::Ymd> dates; // 日期轴 (升序; 已落盘的是其子集, 见 FeatureRead::has_date)
  bool ready() const { return uni.size() > 0; }
};
FeatureLib load_lib(const Config &cfg, std::string_view name); // 读清单; 无清单 → 未建库 (tier / name / dir 仍有效)
void save_lib(const FeatureLib &lib);                          // 写清单 (tmp + rename)
std::string default_pool(const Config &cfg);                   // 首个已建库的票池, 都没建 → 首个票池
