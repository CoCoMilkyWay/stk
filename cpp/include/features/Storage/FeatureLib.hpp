#pragma once

// 特征库 = 池 × 档 + A 轴 + 目录 + 日期轴. 目录 output/features/<池>/<档> (lib_dir); 每池两档对称, 各自一库:
//   Bar (bar1m + 基本面)  交易日 [fundamental::kSyncStart, min(末个 bar1m 日, fund::last_day)]
//   Lob (逐笔 → 订单簿)   回测日历 ≤ 末个已编码日、止于首个待编码日之前, 落在同池 Bar 库日期轴上的日
//   源缺尾日 → 库停在源齐处, 数据入盘后再追加 (追加不改已落盘行)
//   库树 (lib_parent): 每库至多一个上游, 本库日期轴 ⊆ 上游日期轴, 上游落盘齐才可算. 末日随数据走, 各库首末日只在清单里 (重算即前移).
//   A 轴 + 逐日成员每库一份 (features/Panel/Pool.hpp), 池由定轴库 (同池定轴档 tier_base 的库 = Bar) 定:
//     定轴库  A 轴 = 池的自然轴 (续算 = 旧清单轴); 成员 all = 当日活跃 (Fund live), 子池 = 上游 all Bar 库的选股表 (features/Panel/PoolSelect.hpp Picks)
//     随轴库  上游 = 同池定轴库: 成员 = 它同日的 cs_valid, A 轴 = 本库日期段内进过池的 (⊆ 它的清单轴); 截面旁读它
//   A 轴一经建库即冻结, 续算只记轴外; 何时改轴由用户定 (全量重算该库).
// 写端 (ComputeService) 规划后落清单 <dir>/lib.json (save_lib); 读端 (FeatureRead / ReadScope / 截面旁读) 只读清单 (load_lib),
// 两端永远同一 (档, 轴, 日期轴). 无清单 = 未建库 (空轴, ready() == false).
//
// 计算 / 落盘单位 = 库 (TS + CS 同库一趟), 刻意不拆:
//   实盘逐分钟 TS → CS 相互触发, 回测走同一条 asset-day 路径才能与实盘逐位一致; 拆开 TS / CS 即两条路径, 一致性无从保证.
//   CS 读同库 TS 张量行, 池成员 (cs_valid) 又决定截面的参与者 → 截面恒在池内排.
//   子池 TS 各自重算, 不与 all 共享 / 去重: 子池 TS 计算量小, 换来每库自洽.
//   all bar 轴 = 全部出现过的标的 (cs_valid = 在市), 其余库轴 = 本库日期段内进过池的标的. 不做跨池并集轴.
//   一般只算一个库, 不为"多库一起算"做共享优化.

#include "disk/Disk.hpp"
#include "features/Panel/PoolSpec.hpp"        // pool::Spec
#include "features/Storage/FeatureLevels.hpp" // TierId / PoolId / LibId
#include "fundamental/AssetAxis.hpp"          // PoolAxis
#include "misc/date.hpp"
#include <algorithm>
#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

inline constexpr const char *kAllPoolName = "all"; // 全市场池的池名 (= 目录名)
inline constexpr const char *kAllPoolTip = "all = 全市场: 当日活跃 (在市 ∧ 行业已知, 含 ST / 次新 / 北交所)";

// 池表 (全部池唯一一份, 每进程代读一次: 子池表一落地即进程内重启): [0] = all, 其余 = 子池表 (pool::load_pools, 表序)
struct PoolDef {
  std::string name;    // 池名 = 目录名 (子池 = 规范串 pool::code)
  std::string name_cn; // 显示名 (all = 全市场; 子池 = pool::aliases)
  pool::Spec spec;     // 子池参数 (all 不用; 悬停 = 逐维拆解 pool::detail)
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
  // 轴外 (全局 id 升序): 本库自然轴里不在本库轴上的资产 (冻结轴之后的新上市 / 新进池) 不进库, 记在这里供 UI 提示;
  // 全量重算本库即并入轴. 只在非空时落清单
  std::vector<uint32_t> outside;
  TierId tier() const { return lib_tier(id); }
  PoolId pool() const { return lib_pool(id); }
  bool ready() const { return axis.size() > 0; }
};
// 定轴库: 同池定轴档的库 (自身 = 本库定轴; 随轴库轴 ⊆ 它)
inline LibId axis_lib(LibId l) { return lib_id(lib_pool(l), tier_base(lib_tier(l))); }
// 库树的边 (写端规划 / 断言唯一一份): 本库每日都在上游日期轴上; prev = 还读上游的前一交易日 (本库日不许是上游首日)
//   随轴库 → 同池定轴库 (A 轴 + 成员 + 截面旁读); 子池定轴库 → all 同档库 (成员 = 其选股表, D 之前调仓行选出); all 定轴库 = 根
struct LibParent {
  LibId lib;
  bool prev;
};
inline std::optional<LibParent> lib_parent(LibId l) {
  if (axis_lib(l) != l)
    return LibParent{axis_lib(l), false};
  if (lib_pool(l) != kAllPool)
    return LibParent{lib_id(kAllPool, lib_tier(l)), true};
  return std::nullopt;
}
// 本库日 d 可算于上游日期轴 up (升序, lib_parent 的 prev 一并判)
inline bool parent_covers(std::span<const misc::Ymd> up, bool prev, misc::Ymd d) {
  const auto it = std::lower_bound(up.begin(), up.end(), d);
  return it != up.end() && *it == d && (!prev || it != up.begin());
}
// 规划序 (上游在前): 池序 (all 先) × 档的依赖序 (TIER_ORDER); 规划 / 库表行 / 自动续算同序
inline std::vector<LibId> lib_order() {
  std::vector<LibId> o;
  for (const PoolId p : pool_ids())
    for (const TierId t : TIER_ORDER)
      o.push_back(lib_id(p, t));
  return o;
}
// 库目录 output/features/<池>/<档>: 各库并存, 重算一个不清别的库. 库内节点 (disk::Lib 之下) 的键都从 lib_key 起
disk::Key lib_key(LibId id);
std::string lib_dir(LibId id);
std::string lib_day_file(LibId id, misc::Ymd d, size_t lvl); // <库>/YYYY/MM/DD/features_<LVL>.bin
FeatureLib load_lib(LibId id);                               // 读清单; 无清单 → 未建库 (id / dir 仍有效)
void save_lib(const FeatureLib &lib);                        // 写清单 (原子替换)
