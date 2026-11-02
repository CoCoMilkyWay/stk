#pragma once
// pool — 池日频面板: 本库 A 轴 + 逐日成员 (PIT) + 轴外; 两档库 Meta 的 cs_valid 数据源, 每池同一形状. 与 fund::Panel (基本面日频面板) 对仗.
//
//   逐日成员 (第 D 日, 只是 (资产, 日) 的 bool; 各层同一成员, 整日常量, 含 L_1day):
//     all   = 当日活跃 (fund::Panel::live, D 日盘前态; 与 Fund live 口同一份)
//     子池  = all Bar 库 D 的前一交易日 (其日期轴上的前一日) L_1day 行 (收盘) 的 pool_<pool> 列 > 0.5
//             (池是 Bar 档截面列 Node/Bar/CS/Pool/Pool.hpp, 盘后列: 盘中 as-of 恒 -1, 经读端 Source 同一投影读出)
//             L_1day 也不读 D 收盘行: 池键母集按次日 (D+1) 停牌 / ST 判 (盘后列), D 收盘建仓时尚不可知 (前视)
//   自然轴 = 池本该有的 A 轴 (全局 id 升序): all = 全部标的 (asset_axis 全轴); 子池 = dates 内进过池的
//   axis() = frozen (定轴库清单轴: 同池随轴库沿用 / 定轴库续算冻结) 或自然轴 (定轴库全量). 张量列 / 落盘 / 读端都按它;
//            轴上资产在区间内全程算 TS, 区间内日后进池不用补历史
//   outside() = 自然轴 \ axis() (UI 提示, 要纳入就全量重算定轴库)
//   in_pool(sub, D) → Core{Lob,Bar}::member → MetaTracker → cs_valid
#include "features/Storage/FeatureLevels.hpp" // PoolId
#include "misc/date.hpp"
#include "shared/AssetAxis.hpp" // PoolAxis

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

struct Config;
namespace fund {
class Panel;
}

namespace pool {

class Panel {
public:
  // 池 pool 在 dates 上的逐日成员 (fund 须已 build, 覆盖 dates); frozen = 冻结轴 (nullptr = 自然轴).
  // 子池: all Bar 库未就绪 / 非盘后列 / D 不在其日期轴 / D−1 不在轴上或未落盘 → assert; 自然轴为空 → assert
  static Panel build(PoolId pool, const Config &cfg, const fund::Panel &fund, std::span<const misc::Ymd> dates, const PoolAxis *frozen);

  const PoolAxis &axis() const { return axis_; }
  const std::vector<uint32_t> &outside() const { return outside_; } // 自然轴里不在 axis() 上的 (全局 id 升序)
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
