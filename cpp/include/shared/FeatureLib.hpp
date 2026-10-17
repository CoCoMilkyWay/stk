#pragma once

// 特征库 = 名 + 档 + A 轴 + 目录 + 日期轴. 库名即目录名 (<feature_dir>/<name>/), 档由名定:
//   all        Bar 档 (bar1m + 基本面), 全轴, 交易日 [PIPELINE_START_DATE, end_date]
//   <pool>     Lob 档 (逐笔 → 订单簿), 池轴, 回测日历; <pool> = Bar 库票池列 pool_<pool> (feature_libs)
// 写端 (ComputeService) 规划后落清单 <dir>/lib.json (save_lib); 读端 (FeatureRead / ReadScope / 截面旁读) 只读清单 (load_lib),
// 两端永远同一 (档, 轴, 日期轴). 无清单 = 未建库 (空轴, ready() == false).

#include "features/Storage/FeatureLevels.hpp" // TierId
#include "misc/date.hpp"
#include "shared/AssetAxis.hpp" // LibAxis
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

struct Config;

// 库表 (全部库唯一一份, 定义在 FeatureMeta.cpp): [0] = all (Bar 档), 其余 = 票池 (Lob 档, 由 Bar 字段表 Pool 类 pool_* 列展开, 字段表序)
struct LibDef {
  const char *name; // 库名 = 目录名 (票池 = 列名去 pool_ 前缀)
  TierId tier;
  size_t pool_col; // 票池: Bar 库 L_day 的 pool_<name> 列下标; all: SIZE_MAX
};
std::span<const LibDef> feature_libs();
const LibDef &lib_def(std::string_view name); // 不是库名 → assert

struct FeatureLib {
  TierId tier = TierId::Lob;
  std::string name;             // 库名 (= 目录名)
  LibAxis axis;                 // A 轴: 张量列 / 文件列序 / 头指纹 (空 = 未建库)
  std::string dir;              // 库目录 (日文件落在其下)
  std::vector<misc::Ymd> dates; // 日期轴 (升序; 已落盘的是其子集, 见 FeatureRead::has_date)
  bool ready() const { return axis.size() > 0; }
};
FeatureLib load_lib(const Config &cfg, std::string_view name); // 读清单; 无清单 → 未建库 (tier / name / dir 仍有效)
void save_lib(const FeatureLib &lib);                          // 写清单 (tmp + rename)
std::string default_lib(const Config &cfg, TierId tier);       // 该档首个已建库, 都没建 → 该档首个库
