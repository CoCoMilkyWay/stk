#pragma once

// 特征库 = 池 × 档 + A 轴 + 目录 + 日期轴. 目录 <feature_dir>/<池>/<档> (lib_dir); 每池两档对称, 各自一库:
//   Bar (bar1m + 基本面)  交易日 [PIPELINE_START_DATE, 末个 bar1m 日]
//   Lob (逐笔 → 订单簿)   回测日历, 截到 min(末个已编码日, 同池 Bar 库末日)
//   池轴 / 逐日成员: all = 全轴, 成员 = 当日活跃 (Fund live); 子池 = all Bar 库池列 pool_<池> 逐日成员之并 (shared/Pool.hpp),
//   子池库都依赖 all Bar 库先算到其末日. 末日随数据走, 各库首末日只在清单里 (重算即前移).
// 写端 (ComputeService) 规划后落清单 <dir>/lib.json (save_lib); 读端 (FeatureRead / ReadScope / 截面旁读) 只读清单 (load_lib),
// 两端永远同一 (档, 轴, 日期轴). 无清单 = 未建库 (空轴, ready() == false).
//
// 计算 / 落盘单位 = 库 (TS + CS 同库一趟), 刻意不拆:
//   CS 读同库 TS 张量行, 池成员 (cs_valid) 又决定截面的参与者 → 截面恒在池内排, 不把 TS 拆成跨池共享的一份, 也不跨池去重 TS.
//   一般只算一个库, 不为"多库一起算"做并集轴 / 共享优化.

#include "features/Storage/FeatureLevels.hpp" // TierId / PoolId / LibId
#include "misc/date.hpp"
#include "shared/AssetAxis.hpp" // PoolAxis
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

struct Config;

// 池表 (全部池唯一一份, 定义在 FeatureMeta.cpp): [0] = all, 其余 = 子池 (由 Bar 字段表 Pool 类 pool_* 列展开, 字段表序)
struct PoolDef {
  const char *name;    // 池名 = 目录名 (子池 = 列名去 pool_ 前缀)
  const char *name_cn; // 中文名 (子池 = pool_<name> 列的 name_cn; all = 全市场), UI 下拉跟在池名后
  size_t pool_col;     // 子池: all Bar 库 L_1day 的 pool_<name> 列下标; all: SIZE_MAX
};
std::span<const PoolDef> feature_pools(); // 下标 = PoolId
const PoolDef &pool_def(PoolId pool);
const PoolDef &pool_def(std::string_view name); // 不是池名 → assert
PoolId pool_id(std::string_view name);          // 不是池名 → assert
std::string lib_name(LibId lib);                // "<池>.<档>" (日志 / UI)

struct FeatureLib {
  LibId id = kAllBar;
  std::string dir;              // 库目录 (日文件落在其下)
  PoolAxis axis;                // A 轴: 张量列 / 文件列序 / 头指纹 (空 = 未建库)
  std::vector<misc::Ymd> dates; // 日期轴 (升序; 已落盘的是其子集, 见 FeatureRead::has_date)
  // 轴外 (全局 id 升序): 续算沿用清单轴, 之后本该在轴上的资产 (新上市 / 新进池) 不进库, 记在这里供 UI 提示;
  // 全量重算即并入轴 (清空). 只在非空时落清单
  std::vector<uint32_t> outside;
  TierId tier() const { return lib_tier(id); }
  PoolId pool() const { return lib_pool(id); }
  bool ready() const { return axis.size() > 0; }
};
// 库目录 <feature_dir>/<池>/<档>: 各库并存, 重算一个不清别的库; 写读两端唯一的路径入口
std::string lib_dir(const Config &cfg, LibId id);
FeatureLib load_lib(const Config &cfg, LibId id); // 读清单; 无清单 → 未建库 (id / dir 仍有效)
void save_lib(const FeatureLib &lib);             // 写清单 (tmp + rename)
