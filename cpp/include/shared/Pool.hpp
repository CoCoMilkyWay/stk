#pragma once
// pool — 池日频面板: 本库 A 轴 + 逐日成员 (PIT) + 轴外; 两档库 Meta 的 cs_valid 数据源, 每池同一形状. 与 fund::Panel (基本面日频面板) 对仗.
//
//   同池各库同一份, 由定轴库定 (库树见 shared/FeatureLib.hpp lib_parent):
//   定轴库 (Bar)
//     逐日成员 (第 D 日, 只是 (资产, 日) 的 bool; 各层同一成员, 整日常量, 含 L_1day):
//       all   = 当日活跃 (fund::Panel::live, D 日盘前态; 与 Fund live 口同一份)
//       子池  = 上游 all Bar 库 D 的前一交易日 (其日期轴上的前一日) L_1day 行 (收盘) 的 pool_<pool> 列 > 0.5
//               (池是 Bar 档截面列 Node/Bar/CS/Pool/Pool.hpp, 盘后列: 盘中 as-of 恒 -1)
//               L_1day 也不读 D 收盘行: 池键母集按次日 (D+1) 停牌 / ST 判 (盘后列), D 收盘建仓时尚不可知 (前视)
//     自然轴 = 池本该有的 A 轴 (全局 id 升序): all = 全部标的 (asset_axis 全轴); 子池 = dates 内进过池的
//     axis() = 旧清单轴 (续算冻结) 或自然轴 (全量). 张量列 / 落盘 / 读端都按它; 轴上资产在区间内全程算 TS, 区间内日后进池不用补历史
//     outside() = 旧清单轴外 ∪ 自然轴 \ axis() (UI 提示, 要纳入就全量重算定轴库)
//   随轴库 (Lob)
//     axis() / outside() = 上游 (同池定轴库) 清单; 逐日成员 = 上游 D 的 L_1day 行 cs_valid (即上游那份面板写下的成员位)
//   上游一律经读端 Source 同一投影读 (level_asof): 盘后池列 → D−1 收盘行, 盘中 cs_valid → D 行
//   in_pool(sub, D) → Core{Lob,Bar}::member → MetaTracker → cs_valid
#include "features/Storage/FeatureLevels.hpp" // LibId
#include "misc/date.hpp"
#include "shared/AssetAxis.hpp" // PoolAxis

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

struct Config;
struct FeatureLib;
namespace fund {
class Panel;
}

namespace pool {

class Panel {
public:
  // 库 lib 在 dates (管线日期轴) 上的面板; old = 旧清单 (续算) 或未建库 (全量). fund 须已 build 覆盖 dates (all 定轴库的成员)
  // 上游未就绪 / D (子池定轴库: D−1) 不在上游日期轴或未落盘 / 上游判废 → assert; 自然轴为空 → assert; 随轴库续算的旧清单轴 ≠ 上游清单轴 → assert
  static Panel build(LibId lib, const Config &cfg, const fund::Panel &fund, std::span<const misc::Ymd> dates, const FeatureLib &old);

  const PoolAxis &axis() const { return axis_; }
  const std::vector<uint32_t> &outside() const { return outside_; } // 轴外 (全局 id 升序)
  const std::vector<misc::Ymd> &dates() const { return dates_; }    // = build 的 dates
  // 第 D 日成员位 [axis().size()] / 单格. D 不在 dates() → assert
  const uint8_t *row(misc::Ymd d) const;
  bool in_pool(size_t sub, misc::Ymd d) const;

private:
  std::vector<misc::Ymd> dates_;
  PoolAxis axis_;
  std::vector<uint8_t> member_; // [date_i * axis_.size() + sub]
  std::vector<uint32_t> outside_;
};

} // namespace pool
