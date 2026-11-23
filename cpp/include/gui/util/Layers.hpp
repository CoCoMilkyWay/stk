#pragma once

// 分层对数累计图 (Alpha→Inspect 左上 / agent eval_factor 同一份) 及其公共件 (分层色 / 紧凑图例 / 可缩放视野 / 日末横轴读数 / 空图占位):
//   一个持有期的分层 / LS 日收益 (Table.hpp kNavExcess 线序) → derive_layers (前缀对数和 + 夏普 / 年换手) → roll_layers (按窗口折年)
//   → plot_layers (1×2 子图共 y 轴: 左 3/4 时序 + 可拖光标, 右 1/4 光标日截面柱)

#include "gui/engine/Table.hpp"
#include "imgui.h"
#include "implot.h"
#include "misc/date.hpp"

#include <vector>

namespace GUI::Layer {

// 线表: 簇 i 与 LS 线 i 同号 (簇 0 = 20 组, 簇 1 + j = 第 j 级尾部两端)
inline constexpr int kLayerLines = Engine::kNavLayers;
inline constexpr int kLsLines = Engine::kNavLs;

inline constexpr ImVec4 kColBottom(0.00f, 0.20f, 0.70f, 1.f), kColTop(0.85f, 0.15f, 0.10f, 1.f), kColLS(1.f, 1.f, 1.f, 1.f);
inline constexpr ImVec4 kColCursor(1.f, 0.5f, 0.f, 1.f); // 光标 (分层时序 / K 线 / 资产轴), 对仗 OrderFlow 的锚
inline constexpr double kCursorPad = 0.02;               // 时序图默认 x 视野右侧留白 (占全段): 默认末日光标不贴边
inline constexpr float kHiScale = 2.f;                   // 图例 hover 高亮倍数 (同 ImPlot 线宽 ITEM_HIGHLIGHT_LINE_SCALE; 线以外的件自己乘)
inline constexpr const char *kYTickFmt = "%7.2f";        // 竖轴刻度定宽 (等宽字体 7 字符): 数字位数变不改绘图区左边界

// 分层色: t ∈ [0, 1] (0 = G1 深蓝, 0.5 = 中性灰, 1 = G20 深红); 线性底坡 + 两侧反向 tanh 台阶, 边上两组近全色, 往里渐淡到中心灰 (单调不翻色)
ImVec4 lerp_color(float t);
ImVec4 group_color(int k);

// 紧凑图例 (几张图 legend box 等大): 构造 push, 析构 pop, 包住 BeginPlot / EndPlot
struct CompactLegend {
  CompactLegend();
  ~CompactLegend();
};
// 可手动缩放的视野: 目标 [lo, hi] 变了才强设一次, 否则留着用户的缩放 / 平移; BeginPlot 后 Setup 阶段调
void follow_limits(ImAxis ax, double lo, double hi);
// 日末横轴读数: x = i (第 i − 1 日收; 0 = 起点) → YYMMDD (user_data = std::vector<misc::Ymd>)
int date_formatter(double v, char *buf, int size, void *ud);
void empty_plot(const char *title, const ImVec2 &size, const char *hint);

struct LayerDerived {
  int days = 0;
  std::vector<misc::Ymd> dates;                       // [days]
  std::vector<float> x_day;                           // [days + 1] 0..days (日末)
  std::vector<double> lay_cum[kLayerLines];           // [days + 1] 前 c 日 Σ ln(1 + 日收益) (爆仓日起 NaN)
  std::vector<double> ls_cum[kLsLines];               // 同上
  int win = 0;                                        // lay_ann / ls_ann 现按的窗口 (交易日); 0 = 未铺
  std::vector<float> lay_ann[kLayerLines];            // [days + 1] (cum[c] − cum[max(0, c − win)]) × kDaysPerYear / win × 100
  std::vector<float> ls_ann[kLsLines];                // 同上
  float ls_sharpe[kLsLines] = {};                     // 日收益年化 Sharpe
  float ls_turn[kLsLines] = {};                       // 年换手 (倍)
  float lay_lo[kLsLines] = {}, lay_hi[kLsLines] = {}; // 各簇 (按 win) 的 min / max (含 0)
  float ls_lo[kLsLines] = {}, ls_hi[kLsLines] = {};
};
// lay[i] / ls[i] = [dates.size()] 日收益 (已按权重方向), turn[i] = LS 线 i 的年换手
void derive_layers(LayerDerived &d, std::vector<misc::Ymd> dates, const float *const *lay, const float *const *ls, const float *turn);
void roll_layers(LayerDerived &d, int win); // win ∈ [1, days]; 变了才重铺
void plot_layers(const LayerDerived &d, bool absolute, int &cursor_day, const ImVec2 &size);

} // namespace GUI::Layer
