#pragma once

// 特征库 = 池 × 档 + A 轴 + 目录 + 日期轴. 目录 <feature_dir>/<池>/<档> (lib_dir); 每池两档对称, 各自一库:
//   Bar (bar1m + 基本面)  交易日 [PIPELINE_START_DATE, 末个 bar1m 日]
//   Lob (逐笔 → 订单簿)   回测日历, 截到 min(末个已编码日, 同池 Bar 库末日)
//   A 轴归池 (shared/Pool.hpp): 同池各库同一 A 轴, 由定轴库 (同池定轴档 tier_base 的库 = Bar) 定, 其余档的库沿用它的清单轴.
//   逐日成员: all = 当日活跃 (Fund live); 子池 = all Bar 库池列 pool_<池> (D−1 收盘行).
//   库依赖 (lib_deps) 须先算到本库末日. 末日随数据走, 各库首末日只在清单里 (重算即前移).
// 写端 (ComputeService) 规划后落清单 <dir>/lib.json (save_lib); 读端 (FeatureRead / ReadScope / 截面旁读) 只读清单 (load_lib),
// 两端永远同一 (档, 轴, 日期轴). 无清单 = 未建库 (空轴, ready() == false).
//
// 计算 / 落盘单位 = 库 (TS + CS 同库一趟), 刻意不拆:
//   实盘逐分钟 TS → CS 相互触发, 回测走同一条 asset-day 路径才能与实盘逐位一致; 拆开 TS / CS 即两条路径, 一致性无从保证.
//   CS 读同库 TS 张量行, 池成员 (cs_valid) 又决定截面的参与者 → 截面恒在池内排.
//   子池 TS 各自重算, 不与 all 共享 / 去重: 子池 TS 计算量小, 换来每库自洽.
//   每池 bar / lob 同一 A 轴; all 轴 = 全部出现过的标的 (cs_valid = 在市), 子池轴 = 进过池的标的. 不做跨池并集轴.
//   一般只算一个库, 不为"多库一起算"做共享优化.

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
  // 轴外 (全局 id 升序): 池的自然轴里不在本库轴上的资产 (定轴库续算冻结轴之后的新上市 / 新进池) 不进库, 记在这里供 UI 提示;
  // 全量重算定轴库即并入轴. 只在非空时落清单
  std::vector<uint32_t> outside;
  TierId tier() const { return lib_tier(id); }
  PoolId pool() const { return lib_pool(id); }
  bool ready() const { return axis.size() > 0; }
};
// 定轴库: 同池定轴档的库 (自身 = 本库定轴)
inline LibId axis_lib(LibId l) { return lib_id(lib_pool(l), tier_base(lib_tier(l))); }
// 库依赖 (写端规划 / 断言唯一一份): 本库日期轴截到 lib 末日; prev = 还读 lib 的前一交易日 (本库首日须晚于 lib 首日)
//   定轴库 (A 轴 + 截面旁读) + 子池的 all Bar 库 (成员 = D−1 收盘行池列)
struct LibDep {
  LibId lib;
  bool prev;
};
inline std::vector<LibDep> lib_deps(LibId l) {
  std::vector<LibDep> deps;
  if (axis_lib(l) != l)
    deps.push_back({axis_lib(l), false});
  if (lib_pool(l) != kAllPool)
    deps.push_back({kAllBar, true});
  return deps;
}
// 库目录 <feature_dir>/<池>/<档>: 各库并存, 重算一个不清别的库; 写读两端唯一的路径入口
std::string lib_dir(const Config &cfg, LibId id);
FeatureLib load_lib(const Config &cfg, LibId id); // 读清单; 无清单 → 未建库 (id / dir 仍有效)
void save_lib(const FeatureLib &lib);             // 写清单 (tmp + rename)
