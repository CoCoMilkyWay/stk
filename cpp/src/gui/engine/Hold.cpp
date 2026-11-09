// Hold — 见头文件
#include "gui/engine/Hold.hpp"

#include "factor/Stat/Contract.hpp"

namespace GUI::Engine {

HoldPoints hold_points(int hold, FactorLevelId lv) {
  namespace st = factor::stat;
  const FactorLevel &L = factor_level(lv);
  const bool at_close = st::close_entry(0, L.geo()); // 首行就按收盘价建仓 = 每行都是 (行即整日)
  const std::string px = "次分钟VWAP";
  const std::string close_px = "收盘撮合价", open_px = "开盘撮合价";
  const std::string at = at_close ? "C(T)" : "C(" + std::string(L.name) + ")";
  const HoldPoint open{at, at_close ? close_px : px};
  if (st::hold_intraday(hold))
    return {open, {at + "+" + std::to_string(hold) + "m", px}};
  if (hold == st::kHoldClose)
    return {open, {"C(T)", close_px}};
  const std::string n = std::to_string(st::hold_days(hold));
  return {open, st::hold_is_open(hold) ? HoldPoint{"O(T+" + n + ")", open_px} : HoldPoint{"C(T+" + n + ")", close_px}};
}

std::string hold_desc(int hold, FactorLevelId lv) {
  const HoldPoints p = hold_points(hold, lv);
  return "开仓: " + p.open.at + " @ " + p.open.px + ", 清仓: " + p.close.at + " @ " + p.close.px;
}

} // namespace GUI::Engine
