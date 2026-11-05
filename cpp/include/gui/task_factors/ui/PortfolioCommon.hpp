// Factors→Portfolio 页的内部件 (TabPortfolio / PortfolioPlot / PortfolioSheet 三处共用): 页表 / 行色 / 估值口表 / 选中 + 图与右表的入口
//   TabPortfolio = 头 / 页签 / 布局 / 三级缓存; PortfolioPlot = 主图 (NAV ↔ VAL) + 区间条; PortfolioSheet = 右表 (NAV / VAL / FLOW 同一骨架)
//   图与右表只读 ui 的缓存末级 (ui.der.val / ui.view.val), 写 ui 的交互态 (选中 / 展开 / 区间 / 显隐)
#pragma once

#include "gui/task_factors/ui/TabPortfolio.hpp"
#include "imgui.h"
#include "shared/AssetAxis.hpp" // asset_axis (成分股代码)

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <numbers>
#include <string>
#include <vector>

namespace GUI::Factors {

inline constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
inline constexpr ImVec4 kHead(0.4f, 0.8f, 1.0f, 1.0f);

// 页表 [PortfolioPage]: 页签名 / 页签悬停 / 右表类别条的组数 (前 groups 个 RowGroup) / 行选中方式 / 右表 ID
struct PageDef {
  const char *name, *tip;
  int groups, pick;
  const char *table;
};
inline constexpr PageDef kPageDefs[kPages] = {
    {"NAV",
     "NAV = 资金曲线 (Net Asset Value, 净值)\n"
     "• 橙线 = 本组合 G20 多头: 各因子独立回测 (分批 1/h̄, 全日平均满仓; 能成交才成交) 的逐行增量等权聚合\n"
     "• 池指数线 = 各池等权指数 (all 池拆 全A.等 / 全A.加); 行业线 = 申万一级 / 概念线 = 东财题材概念 (全A 内切, 总市值加权)\n"
     "• 单因子线 = 各自的 G20 多头净值\n"
     "• 每条成对: 可成交 (篮子 = 标签指数, 收盘封板不建仓 / 平仓顺延; 因子 / 策略 = 扣换手冲击) + 理想 (篮子 = 后复权收盘价指数; 因子 / 策略 = 无冲击; 同色淡线)\n"
     "• 默认不画 (空图); 右侧类别条选一组 (指数 / 行业 / 概念 / 因子 / 策略), 点行选中 = 画该行两条, 主图例默认只显理想, 可成交点图例显; 显隐变了自动重新 fit\n"
     "• 日内 (右侧开关, 默认开): 主图上方画选中的指数 / 行业 / 概念的日内行情 (多行 = 同主图线色叠加; 一行 = 蜡烛 + 现价十字)\n"
     "• 只显示一条线: 视野内末点画现价十字 (同日内栏; 右壁标区间涨跌, 底壁标日期)\n"
     "• 底部区间条选日段: 各线在区间起点归一 = 1, 年化按区间复利",
     kRowGroups, kPickMany, "##pf_nav_tbl"},
    {"VAL",
     "VAL = 估值 (Valuation)\n"
     "• 右侧单选一个池 / 申万一级行业 / 概念 / 成分股 (▸ 展开行): 主图 = 它的指数 (成分股 = 后复权收盘) + 估值 Band\n"
     "  (全A.加 / 行业 / 概念 = 总市值加权, 全A.等 / 其余池 = 等权; 成分股 = 本只)\n"
     "• EP 盈利收益率 (1/PE) / BP 账面市值比 (1/PB) / SP 营收市值比 (1/PS) / CP 现金流收益率 (1/PCF) / DY 股息率\n"
     "  (收益率口径, 与指数同权重: 加权 = 整体法, 等权 = 个股均值, 成分股 = 本只分子 / 总市值; 高 = 便宜, 亏损照计为负, 过 0 连续)\n"
     "• 主图左上图例矩阵: 行 = 口, 列 = 当前收益率 / 倍数 / N 年分位 / Band / 等值线; 点色块开关, 点行头 / 列头开关整行 / 整列\n"
     "• Band = 把过去 N 年的估值分位换算成价格线: 指数落在哪档 = 当前估值在历史里的位置 (绿 = 便宜, 红 = 贵)\n"
     "• 等值线 = 绝对估值: 倍数 (PE / PB / PS / PCF / P/D = 1 / 收益率) 的价格线, 档 = 该标的全史倍数区间 (两头各剔 2% 极端) 20 档等分;\n"
     "  默认关, 悬停一条 = 高亮 + 看该档倍数 / 当日倍数 / 指数到该线还差多少\n"
     "• 线尾刻度 = 指数末点 +10% / -5% / -10% / -20% / -30% 的点位: 对照 Band 线看还要涨跌多少到哪档; 末点另画现价十字 (同 NAV)\n"
     "• 区间 / log 与 NAV 页共用",
     kBasketGroups, kPickOne, "##pf_val_tbl"},
    {"FLOW",
     "FLOW = 资金流 (Money Flow): 指数 / 行业 / 概念的日内净流入, 当日随新浪快照实时动\n"
     "• 净流入 = Σ 成分 Σ 分钟 ± 该分钟额 (分钟涨跌法): 收 > 上一分钟收 (首分钟对昨收) 记 +, < 记 −, 平随上一分钟; 成分之和不加权, 分不出大单\n"
     "• 左图 = 右表选中行 (与 NAV 页同一份选中) 自日轴首日开盘起逐分钟累计的净流入, 跨日接续; 档位 日内 / 2日 … 5日 叠在图左上角\n"
     "• y 口径 (图左上角切): 占比 = 累计净流入 / 近 5 日日均成交额 (默认; 分母盘前已定, 开盘即稳, 跨篮子可比) / 金额 = 累计净流入 (亿元)\n"
     "• 开收盘 (图左上角开关, 默认关): 计入首根 09:31 (开盘集合竞价并入) / 末根 15:00 (收盘集合竞价); 关 = 两根的额与净流入都不计 (图与右表同一开关)\n"
     "• 右表: 日轴各日的净流入占当日成交额 + 档内合计; 过滤条与 NAV / VAL 页同一份\n"
     "• 历史日 = all.bar 库 L_1min (其后至基本面末日 = bar1m) 合成, 当日 = 新浪快照合成; 不看区间条",
     kBasketGroups, kPickMany, "##pf_flow_tbl"},
};

// 线 ID 后缀 [PairLine]: 线的 ImGui ID = 行 id + 后缀 (主图例显隐 / 行刚选中回默认显隐同一份)
inline constexpr const char *kPairId[kPairLines] = {"", "_ideal"};
inline std::string line_id(const PortfolioRow &r, int l) { return r.id + kPairId[l]; }

// VAL 估值口表 [PoolVal]: 名 / 倍数名 (1 / 收益率, 等值线) / 百分比小数位 (五口同为收益率, 显示 %)
struct ValMeta {
  const char *name, *inv;
  int prec;
};
inline constexpr ValMeta kVal[kPoolVals] = {{"EP", "PE", 2}, {"BP", "PB", 1}, {"SP", "PS", 1}, {"CP", "PCF", 2}, {"DY", "P/D", 2}};
// 便宜 → 贵色: u ∈ [0, 1] (0 = 最便宜 / 名次最好 绿 → 1 = 最贵 / 最差 红) 按色相渐变 (VAL band / 右表名次底色同一份)
inline ImVec4 band_col(double u, float a) {
  ImVec4 c(0.f, 0.f, 0.f, a);
  ImGui::ColorConvertHSVtoRGB(0.33f * static_cast<float>(1.0 - u), 0.8f, 0.9f, c.x, c.y, c.z);
  return c;
}

// OKLCH (感知均匀: 同 L / C 的色环上两色的感知距离 = OKLab 弦长, 小角度 ∝ 色相差) → sRGB; 分量截到 [0, 1] 只吸收舍入
inline ImVec4 oklch(float L, float C, float hue) {
  const float h = 2.0f * std::numbers::pi_v<float> * hue, a = C * std::cos(h), b = C * std::sin(h);
  const float l = std::pow(L + 0.3963377774f * a + 0.2158037573f * b, 3.0f), m = std::pow(L - 0.1055613458f * a - 0.0638541728f * b, 3.0f),
              s = std::pow(L - 0.0894841775f * a - 1.2914855480f * b, 3.0f);
  const auto gamma = [](float x) {
    x = std::clamp(x, 0.0f, 1.0f);
    return x <= 0.0031308f ? 12.92f * x : 1.055f * std::pow(x, 1.0f / 2.4f) - 0.055f;
  };
  return {gamma(4.0767416621f * l - 3.3077115913f * m + 0.2309699292f * s), gamma(-1.2684380046f * l + 2.6097574011f * m - 0.3413193965f * s),
          gamma(-0.0041960863f * l - 0.7034186147f * m + 1.7076147010f * s), 1.0f};
}
// 组 g 行 r 线 l 的色 (主图 / 右表 / 日内同一份): 策略 = kStratCol, 其余 = 行色相 (PortfolioRow::hue) 的定亮定艳色, 未定色 = 同亮度灰; 理想列 = 同色淡
inline constexpr ImVec4 kStratCol(1.0f, 0.55f, 0.15f, 1.0f);
inline constexpr float kIdealAlpha = 0.5f;
inline constexpr float kRowL = 0.75f, kRowC = 0.125f; // 行色的 OKLCH 亮度 / 艳度: 该亮度下整环都在 sRGB 色域内的艳度上限附近 (环不被截变形)
inline ImVec4 row_color(const PortfolioDerived &d, int g, size_t r, int l) {
  const float hue = d.rows[g][r].hue;
  ImVec4 c = g == kGroupStrat ? kStratCol : std::isnan(hue) ? oklch(kRowL, 0.0f, 0.0f)
                                                            : oklch(kRowL, kRowC, hue);
  c.w = l == kIdeal ? kIdealAlpha : 1.0f;
  return c;
}
// 成分股的色 (叠加线 / 点中的成分行名同一份): 色相按全局 id 黄金角散开, 亮度 / 艳度同行色
inline ImVec4 stock_color(uint32_t id) {
  const double h = static_cast<double>(id) * 0.6180339887498949;
  return oklch(kRowL, kRowC, static_cast<float>(h - std::floor(h)));
}
// 成分股的名: 简称 (无档案 / 无简称 → 代码)
inline std::string stock_name(const StockInfos &infos, uint32_t id) {
  if (id < infos.size() && infos[id].known && !infos[id].name.empty())
    return infos[id].name;
  return asset_axis().code(id);
}

// 页 page 的右表行 / 成分行选中 (按页表的选中方式; NAV / FLOW 同一份): 多选页的成分 = 叠加的个股 (pick.stocks), 单选页的成分 = VAL 标的 (stock_key)
inline bool picked(const PortfolioUIState &ui, int page, const PortfolioRow &r) { return ui.pick.rows[kPageDefs[page].pick].contains(r.id); }
inline bool kid_picked(const PortfolioUIState &ui, int page, uint32_t id) {
  return kPageDefs[page].pick == kPickOne ? ui.pick.rows[kPickOne].contains(stock_key(id)) : ui.pick.stocks.contains(id);
}

// ---- 图 (PortfolioPlot.cpp) ----
// 主图 (size 贴边占满): nav → NAV 资金曲线 (右表选中行), 否则 VAL 标的 pin 的价格 + 估值 band; 内容 / 显隐变 → 重新 fit; 线尾小字 / 刻度 / 现价十字
void RenderPortfolioPlot(PortfolioUIState &ui, const PortfolioPin &pin, bool nav, const ImVec2 &size, const StockInfos &infos);
// 区间条 (size): 全段策略线 (nav) / VAL 标的价格线上拖选 ui.range
void RenderPortfolioRange(PortfolioUIState &ui, const PortfolioPin &pin, bool nav, const ImVec2 &size);

// ---- 右表 (PortfolioSheet.cpp) ----
// 页 page 的右图例 (类别条 + 表): NAV / VAL / FLOW; flow_days = FLOW 日轴 (其余页不看); infos = 成分行名 / 近况 / 估值
void RenderPortfolioSheet(PortfolioUIState &ui, int page, const std::vector<misc::Ymd> &flow_days, const StockInfos &infos);
// 展开行的成分在新浪当日的根 (ui.cons): 成分集 / 行情日 · 快照时刻变了才重取
void SyncPortfolioCons(PortfolioUIState &ui, const Database::QuoteService &q);

} // namespace GUI::Factors
