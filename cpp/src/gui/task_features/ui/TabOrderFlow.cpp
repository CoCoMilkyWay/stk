// TabOrderFlow Implementation - OrderFlow Visualization
// 渲染面 (数据面/线程模型见 shared/OrderFlow.hpp):
//   1. 帧首 ack: Sec / Pick 背槽发布 → 翻 front
//   2. 期望态: 三路 Want (MinKey / SecKey / PickKey) 键变 → gen++ → Request
//   3. 渲染: Min 画已发布前缀 (gen 配对), Sec 画 front 槽 (新代在途时旧槽照画 + Loading 提示)
#include "gui/task_features/ui/TabOrderFlow.hpp"
#include "features/Method/Fundamental.hpp"
#include "gui/task_database/models/SharedTypes.hpp" // BoardType / GetBoardType (板块口径与 TABLE 同源)
#include "gui/task_features/services/OrderFlowService.hpp"
#include "gui/task_features/ui/TabFeature.hpp" // EffectiveCat2Snapshot (legend 标注)
#include "gui/util/AssetFilter.hpp"            // 筛选控件 (与 TABLE 共用)
#include "shared/FeatureLib.hpp"               // pool_def (legend 池名)
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "implot.h"
#include "implot_internal.h"
#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <iterator>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Features {

// ============================================================================
// Formatters
// ============================================================================

// 图1 X (交易秒下标) → HH:MM:SS (缩放自适应: 默认刻度 + 逐值换算)
static int SecTimeFormatter(double value, char *buff, int size, void * /*user_data*/) {
  const size_t tick_idx = std::min(static_cast<size_t>(std::max(0.0, value)), OrderFlowConst::SEC_CAPACITY);
  const ClockTime ct = L_sec_to_Clock(tick_idx); // 15300 → 15:00:00 (右边界)
  return std::snprintf(buff, size, "%02d:%02d:%02d", ct.hour, ct.minute, ct.second);
}

// 图2 X (day_idx * MIN_CAPACITY + minute) → YY/MM/DD HH:MM (user_data = Min, gen 配对期间 dates 稳定)
static int MinTimeFormatter(double value, char *buff, int size, void *user_data) {
  const auto &mn = *static_cast<const OrderFlow::Min *>(user_data);
  if (mn.dates.empty() || value < 0)
    return std::snprintf(buff, size, " ");
  const size_t d = std::min(mn.day_idx_from_x(value), mn.dates.size() - 1);
  const size_t m = std::min(static_cast<size_t>(value) % OrderFlowConst::MIN_CAPACITY, TRADE_MINUTES_PER_DAY);
  const ClockTime ct = L_1min_to_Clock(m); // 255 → 15:00 (日右边界)
  const misc::Ymd date = mn.dates[d];
  return std::snprintf(buff, size, "%02d/%02d/%02d %02d:%02d", date / 10000 % 100, date / 100 % 100, date % 100,
                       ct.hour, ct.minute);
}

// 图1/图2 Y (价格) → 定宽 (右对齐补空格, 等宽字体下宽度恒定): 刻度文字宽度决定
// 左侧留白, 换资产时价位数量级一变, plot 区域边界跟着挪 → X 像素映射看起来在抖
static int PriceFormatter(double value, char *buff, int size, void * /*user_data*/) {
  return std::snprintf(buff, size, "%8.2f", value);
}

// 图1右 Y (价格) → 相对锚点 mid/参考价的百分比 (user_data = 基准价 double*)
static int DepthPercentFormatter(double value, char *buff, int size, void *user_data) {
  const double base = *static_cast<const double *>(user_data);
  return std::snprintf(buff, size, "%+.1f%%", (value / base - 1.0) * 100.0);
}

static void FormatTimeHMS(char *buf, size_t size, uint8_t hour, uint8_t minute, uint8_t second) {
  std::snprintf(buf, size, "%02d:%02d:%02d", hour, minute, second);
}

static void FormatDateFull(char *buf, size_t size, misc::Ymd date) {
  std::snprintf(buf, size, "%s", misc::dash(date).c_str());
}

static void FormatDateShort(char *buf, size_t size, misc::Ymd date) {
  std::snprintf(buf, size, "%02d/%02d/%02d", date / 10000 % 100, date / 100 % 100, date % 100);
}

// 选中特征列 (跨池跨层): 多选集合 (ColRef 序) 中不细于图网格 min_level 的列, 截断 MAX_FEATURES.
// 各列读自己的池, 资产不在该池 / 该池缺这天 = 无点
static std::vector<ColRef> CollectFeats(const Feature::Selection &sel, size_t min_level) {
  std::vector<ColRef> v;
  for (const ColRef &c : sel.picked) {
    if (v.size() >= OrderFlowConst::MAX_FEATURES)
      break;
    if (c.level >= min_level)
      v.push_back(c);
  }
  return v;
}

// 一条 overlay 线的有效 Cat2 + 预览抽样全局值域 (与 FEATURE 表同口径; 无预览 = "?", val_min > val_max)
struct FeatCat {
  const char *cat2 = "?";
  float val_min = 1.0f, val_max = 0.0f;
};
// cols → [i] FeatCat: 每个 (档, 层) 取一次快照
static void CollectFeatCats(SharedData &data, const std::vector<ColRef> &cols, std::vector<FeatCat> &out) {
  static Cat2Snapshot s_snap; // GUI 单线程, 帧内复用
  out.assign(cols.size(), FeatCat{});
  std::vector<bool> done(cols.size(), false);
  for (size_t i = 0; i < cols.size(); ++i) {
    if (done[i])
      continue;
    EffectiveCat2Snapshot(data, cols[i].tier(), cols[i].level, s_snap);
    for (size_t j = i; j < cols.size(); ++j) {
      if (done[j] || cols[j].tier() != cols[i].tier() || cols[j].level != cols[i].level)
        continue;
      done[j] = true;
      const size_t c = cols[j].col;
      assert(c < s_snap.cat2.size());
      out[j] = FeatCat{s_snap.cat2[c], s_snap.val_min[c], s_snap.val_max[c]};
    }
  }
}

// legend = "中文名 (cat2) 池 层"; cat2 = 有效 Cat2 (探测 or 人工覆盖, 无预览 = "?"); 同列不同池靠池名区分
static const char *FeatName(const Feature &feature, const ColRef &c, const FeatCat &cat, char *buf, size_t size) {
  std::snprintf(buf, size, "%s (%s) %s %s", feature.meta(c).name_cn, cat.cat2, pool_def(c.pool).name, level_name(c.level));
  return buf;
}

// ============================================================================
// Feature Overlay Axes (仅特征信号; 两图自身的轴行为不受影响)
//   price     → Y1: 与主图价格共轴 (NoFit, 不参与主图自动缩放)
//   rank/flag → Y2: 固定 = 值域两头各留 10% 余量 (贴边线不压轴框); rank 值域 [0,1] → [-0.1, 1.1],
//                flag 值域 = 该特征预览抽样的全局 min/max (换资产/换日不跳; 无预览退回该线自身范围),
//                整数跨度至少按 1 算; 多线同场取并集
//   ratio/ret → Y3: 强制 0 轴居中, limit = max(|min|,|max|) → [-limit, +limit] (0 有语义: 差分/收益)
//   ratio_pos/ratio_neg/raw/? → Y3: 同类共轴, 范围 = 各线 min/max 并集 (自动缩放)
//   Y3 上 center 与 scale 混场 → 取并集后整体对称于 0 (center 主导)
//   Y2 与 Y3 同时在场 → 右轴语义冲突, 两轴隐去刻度 (线照常画)
// ============================================================================

enum FeatAxisKind { FEAT_AXIS_PRICE = 0,
                    FEAT_AXIS_RANK,
                    FEAT_AXIS_FLAG,
                    FEAT_AXIS_SCALE,
                    FEAT_AXIS_CENTER };

static FeatAxisKind FeatAxisOf(const FeatCat &cat) {
  const char *c = cat.cat2;
  if (std::strcmp(c, "price") == 0)
    return FEAT_AXIS_PRICE;
  if (std::strcmp(c, "rank") == 0)
    return FEAT_AXIS_RANK;
  if (std::strcmp(c, "flag") == 0)
    return FEAT_AXIS_FLAG;
  if (std::strcmp(c, "ratio") == 0 || std::strcmp(c, "ret") == 0)
    return FEAT_AXIS_CENTER;
  return FEAT_AXIS_SCALE; // ratio_pos / ratio_neg / raw / ?
}

static ImAxis FeatYAxis(FeatAxisKind kind) {
  return kind == FEAT_AXIS_PRICE ? ImAxis_Y1 : ((kind == FEAT_AXIS_SCALE || kind == FEAT_AXIS_CENTER) ? ImAxis_Y3 : ImAxis_Y2);
}

// Y2 (rank/flag) 值域并集累加: rank 固定 [0,1]; flag 取该特征全局 min/max (预览快照),
// 无预览 (快照 min > max) 退回该线自身 y_min/y_max
static void FeatFixedRange(FeatAxisKind kind, const FeatCat &cat,
                           float y_min, float y_max, float &fixed_min, float &fixed_max) {
  if (kind == FEAT_AXIS_RANK) {
    y_min = 0.0f;
    y_max = 1.0f;
  } else if (cat.val_min <= cat.val_max) {
    y_min = cat.val_min;
    y_max = cat.val_max;
  }
  fixed_min = std::min(fixed_min, y_min);
  fixed_max = std::max(fixed_max, y_max);
}

// Setup 阶段: 按在场类别开右轴 (必须在任何绘制调用之前)
// has_center: Y3 上有 ratio/ret (强制 0 轴居中); 与 scale 共轴时取并集后整体对称于 0
static void SetupFeatAxes(bool has_fixed, float fixed_min, float fixed_max,
                          bool has_scale, float scale_min, float scale_max, bool has_center) {
  const ImPlotAxisFlags flags = ImPlotAxisFlags_AuxDefault | ImPlotAxisFlags_Opposite |
                                ((has_fixed && has_scale) ? ImPlotAxisFlags_NoDecorations : 0);
  if (has_fixed) {
    ImPlot::SetupAxis(ImAxis_Y2, nullptr, flags);
    // 两头各留 10% 余量; 跨度下限 1 (flag 整数, 恒值线跨度 0 也给一格)
    const float margin = 0.1f * std::max(fixed_max - fixed_min, 1.0f);
    ImPlot::SetupAxisLimits(ImAxis_Y2, fixed_min - margin, fixed_max + margin, ImPlotCond_Always);
  }
  if (has_scale) {
    ImPlot::SetupAxis(ImAxis_Y3, nullptr, flags);
    if (scale_min <= scale_max) {
      if (has_center) {
        // 0 轴居中: limit 取并集绝对值上确界, 两头对称
        const float limit = std::max(std::fabs(scale_min), std::fabs(scale_max));
        ImPlot::SetupAxisLimits(ImAxis_Y3, -limit, limit, ImPlotCond_Always);
      } else {
        ImPlot::SetupAxisLimits(ImAxis_Y3, scale_min, scale_max, ImPlotCond_Always);
      }
    }
  }
}

// Y3 居中时画 0 轴横线 (灰, 1px). 须在 BeginPlot 内、SetupFeatAxes 之后调用
static void DrawFeatZeroLine() {
  const double zero = 0.0;
  ImPlot::SetAxes(ImAxis_X1, ImAxis_Y3);
  ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
  ImPlot::PlotInfLines("##feat_zero", &zero, 1, ImPlotInfLinesFlags_Horizontal);
  ImPlot::SetAxes(ImAxis_X1, ImAxis_Y1);
}

// 两图 overlay 同一套绘制: 在画线 = cats (与 ov 线 i 对齐; 未就绪传空), 线 i 可画点数 = ov.count(i) (0 = 不在场)
// Setup 阶段 (任何绘制之前): 按在场类别开右轴, Y3 居中则画 0 轴
static void SetupOverlayAxes(const OrderFlow::Overlay &ov, const std::vector<FeatCat> &cats) {
  bool feat_fixed = false, feat_scale = false, feat_center = false;
  float fixed_min = (std::numeric_limits<float>::max)(), scale_min = fixed_min;
  float fixed_max = std::numeric_limits<float>::lowest(), scale_max = fixed_max;
  for (size_t i = 0; i < cats.size(); ++i) {
    if (ov.count(i) == 0)
      continue;
    const FeatAxisKind kind = FeatAxisOf(cats[i]);
    if (kind == FEAT_AXIS_RANK || kind == FEAT_AXIS_FLAG) {
      feat_fixed = true;
      FeatFixedRange(kind, cats[i], ov.y_lo(i), ov.y_hi(i), fixed_min, fixed_max);
    } else if (kind == FEAT_AXIS_SCALE || kind == FEAT_AXIS_CENTER) {
      feat_scale = true;
      feat_center |= (kind == FEAT_AXIS_CENTER);
      scale_min = std::min(scale_min, ov.y_lo(i));
      scale_max = std::max(scale_max, ov.y_hi(i));
    }
  }
  SetupFeatAxes(feat_fixed, fixed_min, fixed_max, feat_scale, scale_min, scale_max, feat_center);
  if (feat_center)
    DrawFeatZeroLine();
}
// 线色 = 列在选中集里的序位 (不用 ImPlot 的 item 序 / 线下标: 两图各截各的网格, 同一列才能两图同色); 尾挂列 (未选中, 即 vwap) = 紫
static ImVec4 FeatColor(const Feature &feature, const ColRef &c) {
  const auto &picked = feature.selection.picked;
  const auto it = picked.find(c);
  return it == picked.end() ? ImVec4(0.9f, 0.5f, 1.0f, 0.9f) : ImPlot::GetColormapColor(static_cast<int>(std::distance(picked.begin(), it)));
}
// 画线 (legend = 中文名 (cat2) 池 层); price 类共用主图 Y1, NoFit 保证主图自动 / 双击缩放口径不被特征撑大
static void PlotOverlay(const Feature &feature, const OrderFlow::Overlay &ov, const std::vector<FeatCat> &cats) {
  for (size_t i = 0; i < cats.size(); ++i) {
    const size_t n = ov.count(i);
    if (n == 0)
      continue;
    char label[128];
    const FeatAxisKind kind = FeatAxisOf(cats[i]);
    ImPlot::SetAxes(ImAxis_X1, FeatYAxis(kind));
    ImPlot::SetNextLineStyle(FeatColor(feature, ov.refs[i]));
    ImPlot::PlotStairs(FeatName(feature, ov.refs[i], cats[i], label, sizeof(label)), ov.line[i].x.data(), ov.line[i].y.data(),
                       static_cast<int>(n), kind == FEAT_AXIS_PRICE ? ImPlotItemFlags_NoFit : 0);
    ImPlot::SetAxes(ImAxis_X1, ImAxis_Y1);
  }
}

// ============================================================================
// 买卖两侧绘制属性 (下标 = OrderFlow::Side): 图1 一档价线 / 图1右 累计曲线
// ============================================================================

struct SideStyle {
  const char *best, *cum;
  ImVec4 best_col, cum_col;
};
static const OrderFlow::PerSide<SideStyle> kSideStyle = {{
    {"Best Bid", "BidΣ", ImVec4(0.3f, 0.8f, 0.3f, 0.7f), ImVec4(0.3f, 0.9f, 0.3f, 0.9f)},
    {"Best Ask", "AskΣ", ImVec4(0.8f, 0.3f, 0.3f, 0.7f), ImVec4(0.95f, 0.35f, 0.35f, 0.9f)},
}};
static constexpr OrderFlow::Side kSides[] = {OrderFlow::Bid, OrderFlow::Ask};

// ============================================================================
// Candlestick Renderer
// ============================================================================

static void PlotCandlestick(const char *label_id, const double *xs, const std::array<std::vector<double>, 4> &ohlc, int count,
                            double half_width) {
  if (count <= 0)
    return;
  const double *opens = ohlc[OrderFlow::Min::kOpen].data(), *highs = ohlc[OrderFlow::Min::kHigh].data();
  const double *lows = ohlc[OrderFlow::Min::kLow].data(), *closes = ohlc[OrderFlow::Min::kClose].data();

  ImDrawList *draw_list = ImPlot::GetPlotDrawList();

  if (ImPlot::BeginItem(label_id)) {
    ImPlot::GetCurrentItem()->Color = IM_COL32(128, 128, 128, 255);

    if (ImPlot::FitThisFrame()) {
      for (int i = 0; i < count; ++i) {
        ImPlot::FitPoint(ImPlotPoint(xs[i], lows[i]));
        ImPlot::FitPoint(ImPlotPoint(xs[i], highs[i]));
      }
    }

    // 视野裁剪: 只画可视 X 区间 (数组按 x 升序)
    const ImPlotRect limits = ImPlot::GetPlotLimits();
    const double *lo = std::lower_bound(xs, xs + count, limits.X.Min - 2.0 * half_width);
    const double *hi = std::upper_bound(xs, xs + count, limits.X.Max + 2.0 * half_width);

    for (int i = static_cast<int>(lo - xs); i < static_cast<int>(hi - xs); ++i) {
      const double o = opens[i], h = highs[i], l = lows[i], c = closes[i];
      const ImVec2 open_pos = ImPlot::PlotToPixels(xs[i] - half_width, o);
      const ImVec2 close_pos = ImPlot::PlotToPixels(xs[i] + half_width, c);
      const ImVec2 low_pos = ImPlot::PlotToPixels(xs[i], l);
      const ImVec2 high_pos = ImPlot::PlotToPixels(xs[i], h);
      const ImU32 color = c >= o ? IM_COL32(0, 200, 0, 255) : IM_COL32(200, 0, 0, 255);

      draw_list->AddLine(low_pos, high_pos, color);

      ImVec2 body_top = open_pos;
      ImVec2 body_bottom = close_pos;
      constexpr float min_body_height = OrderFlowConst::MIN_CANDLESTICK_BODY_HEIGHT;
      if (std::abs(body_bottom.y - body_top.y) < min_body_height) {
        const float mid_y = (body_top.y + body_bottom.y) * 0.5f;
        body_top.y = mid_y - min_body_height * 0.5f;
        body_bottom.y = mid_y + min_body_height * 0.5f;
      }
      draw_list->AddRectFilled(body_top, body_bottom, color);
    }

    ImPlot::EndItem();
  }
}

// ============================================================================
// Depth Panel Renderer (图1右: 锚点秒纵向深度图, 全簿 = 热力图截面)
//   Y = 价格, 每帧同步图1视野 (严格对齐), 刻度显示相对基准价百分比
//   逐价位净额密堆横条 (像素空间, 100W cap; hover 高亮 + 数值) 与
//   买卖累计曲线 (X 轴, 万元) 解耦, 各自看各自细节;
//   竞价交叉簿三态: 纯买 / 纯卖 / 重合区 (过渡色带), 预撮合参考价横线
// ============================================================================

static void RenderDepthPanel(OrderFlow &of, const OrderFlow::Sec &sc, size_t row) {
  assert(row < sc.size());
  const size_t t = static_cast<size_t>(sc.x[row]);
  const double ref = sc.ref[row];

  // 截面缓存: (槽 gen, 锚点秒) 变了才重建
  auto &prof = of.profile;
  const bool rebuilt = prof.key.stale(sc.gen, t);
  if (rebuilt)
    prof.build(sc, t);
  if (prof.price.empty()) {
    ImGui::TextDisabled("No book at anchor");
    return;
  }

  // 基准价 (Y 轴百分比 / 横线): 竞价交叉秒 = 预撮合参考价, 其余 = 中间价
  double mark = ref > 0.0 ? ref : sc.mid[row];

  if (ImPlot::BeginPlot("##DepthProfile", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    const ImPlotCond cond = rebuilt ? ImPlotCond_Always : ImPlotCond_Once;
    // Y 与图1 视野每帧同步 (价格严格对齐, Lock 不可单独缩放); 刻度显示相对基准价百分比
    ImPlot::SetupAxes(nullptr, nullptr, 0, ImPlotAxisFlags_Opposite | ImPlotAxisFlags_Lock);
    ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, std::max(prof.cum_max * 1.05, 1.0), cond);
    const auto &ui = of.ui;
    if (ui.sec_y_min < ui.sec_y_max)
      ImPlot::SetupAxisLimits(ImAxis_Y1, ui.sec_y_min, ui.sec_y_max, ImPlotCond_Always);
    else
      ImPlot::SetupAxisLimits(ImAxis_Y1, sc.y_lo, sc.y_hi, cond);
    ImPlot::SetupAxisFormat(ImAxis_Y1, DepthPercentFormatter, &mark);

    ImPlot::PushPlotClipRect();
    ImDrawList *draw_list = ImPlot::GetPlotDrawList();
    const ImPlotRect limits = ImPlot::GetPlotLimits();

    // 重合区带 (竞价交叉: 买卖价位在 [edge[Ask], edge[Bid]] 混排) — 过渡色打底
    const auto &edge = prof.edge;
    if (edge[OrderFlow::Ask] > 0 && edge[OrderFlow::Bid] > 0 && edge[OrderFlow::Ask] < edge[OrderFlow::Bid]) {
      const ImVec2 p0 = ImPlot::PlotToPixels(limits.X.Min, edge[OrderFlow::Bid]);
      const ImVec2 p1 = ImPlot::PlotToPixels(limits.X.Max, edge[OrderFlow::Ask]);
      draw_list->AddRectFilled(p0, p1, IM_COL32(230, 180, 60, 40));
    }

    // 逐价位净额横条: 密堆矩形 (价位高 = 1 tick, 相邻价位无缝), 像素空间自左向右,
    // 100W cap 满宽 —— 与 X 轴 (累计曲线) 解耦, 缩放曲线不影响柱子
    const ImVec2 plot_pos = ImPlot::GetPlotPos();
    const float plot_w = ImPlot::GetPlotSize().x;
    constexpr double HALF_TICK = OrderFlowConst::TICK_SIZE * 0.5;

    // Hover: 鼠标价格落在哪个价位 (价升序二分)
    int hovered = -1;
    if (ImPlot::IsPlotHovered()) {
      const double mp = ImPlot::GetPlotMousePos().y;
      const auto it = std::lower_bound(prof.price.begin(), prof.price.end(), mp - HALF_TICK);
      if (it != prof.price.end() && std::abs(*it - mp) <= HALF_TICK)
        hovered = static_cast<int>(it - prof.price.begin());
    }

    for (size_t i = 0; i < prof.price.size(); ++i) {
      const double p = prof.price[i];
      if (p + HALF_TICK < limits.Y.Min || p - HALF_TICK > limits.Y.Max)
        continue;
      const double a = prof.amount[i];
      const float ratio = std::min(1.0f, static_cast<float>(std::abs(a)) / OrderFlowConst::DEPTH_BAR_MAX_AMOUNT);
      const bool hov = static_cast<int>(i) == hovered;
      const ImU32 col = a > 0 ? IM_COL32(60, 200, 60, hov ? 230 : 140)
                              : IM_COL32(220, 70, 70, hov ? 230 : 140);
      const ImVec2 r0(plot_pos.x, ImPlot::PlotToPixels(0.0, p + HALF_TICK).y);
      const ImVec2 r1(plot_pos.x + ratio * plot_w, ImPlot::PlotToPixels(0.0, p - HALF_TICK).y);
      draw_list->AddRectFilled(r0, r1, col);
      if (hov)
        draw_list->AddRect(r0, r1, IM_COL32(255, 255, 255, 255));
    }

    // 角标信息 (图内左上角; 图外不放文本, 保证与图1 plot 区域像素级对齐)
    {
      char buf[64], date_buf[16], time_buf[16];
      const ClockTime ct = L_sec_to_Clock(t);
      FormatDateFull(date_buf, sizeof(date_buf), sc.date);
      FormatTimeHMS(time_buf, sizeof(time_buf), ct.hour, ct.minute, ct.second);
      const float lh = ImGui::GetTextLineHeight();
      const ImVec2 tp(plot_pos.x + 6.0f, plot_pos.y + 4.0f);
      std::snprintf(buf, sizeof(buf), "%s %s", date_buf, time_buf);
      draw_list->AddText(tp, IM_COL32(255, 255, 255, 210), buf);
      if (ref > 0.0) { // 竞价交叉秒: 预撮合三元组
        std::snprintf(buf, sizeof(buf), "预撮合 %.2f元", ref);
        draw_list->AddText(ImVec2(tp.x, tp.y + lh), IM_COL32(255, 190, 50, 255), buf);
        std::snprintf(buf, sizeof(buf), "匹配%.0f万 失衡%+.0f万",
                      amount_to_wan(static_cast<float>(sc.matched[row])), amount_to_wan(static_cast<float>(sc.imbalance[row])));
        draw_list->AddText(ImVec2(tp.x, tp.y + 2.0f * lh), IM_COL32(255, 190, 50, 255), buf);
      } else {
        std::snprintf(buf, sizeof(buf), "中间价 %.2f元", sc.mid[row]);
        draw_list->AddText(ImVec2(tp.x, tp.y + lh), IM_COL32(255, 255, 0, 255), buf);
      }
    }
    ImPlot::PopPlotClipRect();

    if (hovered >= 0) {
      const double p = prof.price[static_cast<size_t>(hovered)];
      const double a = prof.amount[static_cast<size_t>(hovered)];
      ImGui::SetTooltip("%.2f元 (%+.2f%%)\n%s %.1f万", p, (p / mark - 1.0) * 100.0,
                        a > 0 ? "买" : "卖", amount_to_wan(static_cast<float>(std::abs(a))));
    }

    // 累计曲线: 买自最高买价向下, 卖自最低卖价向上 (交叉簿两线在重合区交叠)
    for (const OrderFlow::Side s : kSides) {
      const OrderFlow::Profile::Curve &c = prof.cum[s];
      if (c.x.empty())
        continue;
      ImPlot::SetNextLineStyle(kSideStyle[s].cum_col, 2.0f);
      ImPlot::PlotLine(kSideStyle[s].cum, c.x.data(), c.y.data(), static_cast<int>(c.x.size()));
    }

    // 参考价 / 中间价横线 (标注绝对价, 轴刻度已是百分比)
    const ImVec4 mark_col = ref > 0.0 ? ImVec4(1.0f, 0.75f, 0.2f, 1.0f) : ImVec4(1.0f, 1.0f, 1.0f, 0.7f);
    ImPlot::SetNextLineStyle(mark_col, ref > 0.0 ? 2.0f : 1.0f);
    ImPlot::PlotInfLines("##mark", &mark, 1, ImPlotInfLinesFlags_Horizontal);
    ImPlot::Annotation(limits.X.Max, mark, mark_col, ImVec2(-5, -5), true, "%.2f", mark);

    ImPlot::EndPlot();
  }
}

// ============================================================================
// Heatmap (图1 底层): 价位价升序 → Y 视野二分取价位区间; 价位内矩形 t 升序 → X 视野二分起画.
//   hover = 鼠标价 ± 1 tick 内的价位里, 覆盖鼠标秒且矩形价区间含鼠标价的着色矩形
// ============================================================================

static void PlotHeat(const OrderFlow::Sec &sc, const std::vector<uint32_t> &color) {
  using Heat = OrderFlow::Sec::Heat;
  constexpr double TICK = OrderFlowConst::TICK_SIZE;
  ImPlot::PushPlotClipRect();
  ImDrawList *draw_list = ImPlot::GetPlotDrawList();
  const ImPlotRect limits = ImPlot::GetPlotLimits();

  const size_t t_min = static_cast<size_t>(std::max(0.0, limits.X.Min));
  const auto [l0, l1] = sc.heat.span(limits.Y.Min - TICK, limits.Y.Max + TICK);
  for (size_t li = l0; li < l1; ++li) {
    const Heat::Level &l = sc.heat.levels[li];
    auto it = std::upper_bound(l.rects.begin(), l.rects.end(), t_min, [](size_t v, const Heat::Rect &r) { return v < r.t1; });
    for (; it != l.rects.end() && it->t0 <= limits.X.Max; ++it) {
      const uint32_t col = color[l.off + static_cast<size_t>(it - l.rects.begin())];
      if (col == 0)
        continue;
      const auto [y0, y1] = Heat::rect_y(l.price, it->amount);
      draw_list->AddRectFilled(ImPlot::PlotToPixels(it->t0, y1), ImPlot::PlotToPixels(it->t1, y0), col);
    }
  }

  const Heat::Level *hl = nullptr;
  const Heat::Rect *hr = nullptr;
  if (ImPlot::IsPlotHovered()) {
    const ImPlotPoint mp = ImPlot::GetPlotMousePos();
    const size_t t = static_cast<size_t>(std::max(0.0, mp.x));
    const auto [h0, h1] = sc.heat.span(mp.y - TICK, mp.y + TICK);
    for (size_t li = h0; li < h1 && !hr; ++li) {
      const Heat::Level &l = sc.heat.levels[li];
      const Heat::Rect *r = l.at(t);
      if (!r || color[l.off + static_cast<size_t>(r - l.rects.data())] == 0)
        continue;
      const auto [y0, y1] = Heat::rect_y(l.price, r->amount);
      if (mp.y >= y0 && mp.y <= y1) {
        hl = &l;
        hr = r;
      }
    }
  }
  if (hr) {
    const auto [y0, y1] = Heat::rect_y(hl->price, hr->amount);
    const ImVec2 p_min = ImPlot::PlotToPixels(hr->t0, y1), p_max = ImPlot::PlotToPixels(hr->t1, y0);
    draw_list->AddRectFilled(p_min, p_max, color[hl->off + static_cast<size_t>(hr - hl->rects.data())] | IM_COL32_A_MASK);
    draw_list->AddRect(p_min, p_max, IM_COL32(255, 255, 255, 255), 0.0f, 0, 2.0f);
  }
  ImPlot::PopPlotClipRect();

  if (hr) {
    const ClockTime t0 = L_sec_to_Clock(hr->t0);
    const ClockTime t1 = L_sec_to_Clock(hr->t1); // 开区间右界 → 边界时刻
    ImGui::SetTooltip("%.2f万元\n价格: %.2f\n%02d:%02d:%02d - %02d:%02d:%02d", amount_to_wan(static_cast<float>(hr->amount)), hl->price,
                      t0.hour, t0.minute, t0.second, t1.hour, t1.minute, t1.second);
  }
}

// ============================================================================
// L_sec Plot Renderer (图1: front Sec 槽)
// ============================================================================

static void RenderSecPlot(SharedData &data) {
  OrderFlow &of = data.orderflow;
  auto &ui = of.ui;
  const OrderFlow::Sec &sc = of.sec.front_slot();

  if (sc.asset_idx == SIZE_MAX) {
    ImGui::TextDisabled("Waiting for depth replay...");
    return;
  }
  const OrderFlow::Overlay &ov = sc.feat; // front 槽实际渲染的列 (换列在途时与旧槽数据配对)
  if (!sc.has_data && ov.size() == 0) {
    ImGui::TextDisabled("No orders data for this (day, asset)");
    return;
  }
  static std::vector<FeatCat> cats; // GUI 单线程, 帧内复用
  CollectFeatCats(data, ov.refs, cats);

  // 新槽 (gen 变了) → 重置视图 + 热力图阈值回到当日自动初值 (之后尊重用户拖动)
  const bool slot_changed = (ui.sec_last_gen != sc.gen);
  ui.sec_last_gen = sc.gen;
  if (slot_changed)
    ui.log_amount_threshold = sc.auto_log_threshold;

  if (ImPlot::BeginPlot("##SecPrice", ImVec2(-1, -1))) {
    const ImPlotCond cond = slot_changed ? ImPlotCond_Always : ImPlotCond_Once;

    ImPlot::SetupAxes(nullptr, nullptr, 0, 0);
    // X 是固定域 (全天交易秒), 只在首帧设一次 → 换资产/换日不动视野 (双击才复位)
    ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, static_cast<double>(OrderFlowConst::SEC_CAPACITY), ImPlotCond_Once);
    ImPlot::SetupAxisLimits(ImAxis_Y1, sc.y_lo, sc.y_hi, cond);
    ImPlot::SetupAxisFormat(ImAxis_X1, SecTimeFormatter);
    ImPlot::SetupAxisFormat(ImAxis_Y1, PriceFormatter); // 定宽: 换资产不挪 plot 左边界

    // 特征 overlay 的右轴 (按 Cat2 分流; price 类直接借主图 Y1, 不开轴)
    SetupOverlayAxes(ov, cats);

    // 当前 Y 视野快照 → 右侧深度面板每帧同步 (两图价格轴严格对齐)
    {
      const ImPlotRect lr = ImPlot::GetPlotLimits();
      ui.sec_y_min = lr.Y.Min;
      ui.sec_y_max = lr.Y.Max;
    }

    // ------------------------------------------------------------------
    // Heatmap (GUI 侧着色缓存: 槽 gen + 阈值变了才重建)
    // ------------------------------------------------------------------
    if (ui.show_heatmap && sc.heat.rect_count > 0) {
      if (of.heat_color.key.stale(sc.gen, ui.log_amount_threshold))
        of.heat_color.build(sc, ui.log_amount_threshold);
      PlotHeat(sc, of.heat_color.color);
    }

    // ------------------------------------------------------------------
    // 盘口线: best bid / ask + spread 填充 + mid
    // ------------------------------------------------------------------
    if (sc.size() > 0) {
      // 盘口线全部 NoFit: 双击 fit 只认下方手动 FitPoint (= 初始视野口径),
      // 竞价段预撮合 mid 摸涨跌停不会撑大 fit
      const int n = static_cast<int>(sc.size());
      for (const OrderFlow::Side s : kSides) {
        ImPlot::PushStyleColor(ImPlotCol_Line, kSideStyle[s].best_col);
        ImPlot::PlotStairs(kSideStyle[s].best, sc.x.data(), sc.best[s].data(), n, ImPlotItemFlags_NoFit);
        ImPlot::PopStyleColor();
      }

      ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(1.0f, 1.0f, 0.0f, 0.6f));
      ImPlot::PlotShaded("Spread", sc.x.data(), sc.best[OrderFlow::Bid].data(), sc.best[OrderFlow::Ask].data(), n,
                         ImPlotItemFlags_NoFit);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(1.0f, 1.0f, 1.0f, 0.9f));
      ImPlot::PlotStairs("Mid Price", sc.x.data(), sc.mid.data(), n, ImPlotItemFlags_NoFit);
      ImPlot::PopStyleColor();

      // 双击复位 = 新资产初始渲染的口径 (X 全天 + Y 带 margin); 否则 ImPlot 默认
      // fit 会贴紧数据边缘, 与初始视图不一致且难操作. 必须在有 item 之后 (SetupLock 已发生)
      if (ImPlot::FitThisFrame()) {
        ImPlot::FitPointX(0.0);
        ImPlot::FitPointX(static_cast<double>(OrderFlowConst::SEC_CAPACITY));
        ImPlot::FitPointY(sc.y_lo);
        ImPlot::FitPointY(sc.y_hi);
      }
    }

    // ------------------------------------------------------------------
    // 特征 overlay (多选 + 尾挂 vwap; 轴按 Cat2 分流, legend = 中文名 (cat2) 池 层)
    // ------------------------------------------------------------------
    PlotOverlay(data.feature, ov, cats);

    // ------------------------------------------------------------------
    // Anchor: 锚在秒级时间 (跨日/换资产稳定); DragLineX + 双击, 落点吸附有效秒
    // ------------------------------------------------------------------
    if (sc.size() > 0) {
      const auto snap = [&](double gx) { return static_cast<size_t>(sc.x[sc.row_at(gx)]); };
      if (ui.sec_anchor_tick == SIZE_MAX)
        ui.sec_anchor_tick = static_cast<size_t>(sc.x.back()); // 首次: 默认末个有效秒 (收盘)

      double anchor_x = static_cast<double>(ui.sec_anchor_tick);
      const bool drag_changed = ImPlot::DragLineX(0, &anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f);
      const bool drag_active = ImGui::IsItemActive();
      if (drag_changed) // 拖动中跟手 (原样), 释放时吸附有效秒
        ui.sec_anchor_tick = drag_active
                                 ? static_cast<size_t>(std::clamp(anchor_x, 0.0, static_cast<double>(OrderFlowConst::SEC_CAPACITY - 1)))
                                 : snap(anchor_x);

      if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0))
        ui.sec_anchor_tick = snap(ImPlot::GetPlotMousePos().x);

      // 当日该秒无数据 → 就近吸附取值 (锚点本身不动)
      const size_t row = sc.row_at(static_cast<double>(ui.sec_anchor_tick));
      const ClockTime ct = L_sec_to_Clock(static_cast<size_t>(sc.x[row]));
      char time_buf[16];
      FormatTimeHMS(time_buf, sizeof(time_buf), ct.hour, ct.minute, ct.second);
      ImPlot::Annotation(sc.x[row], sc.mid[row], ImVec4(1, 0.5f, 0, 1), ImVec2(5, -15), false, "%s", time_buf);
    }

    ImPlot::EndPlot();
  }
}

// ============================================================================
// Event Lanes (图2 时间轴事件标注, 数据见 shared/AssetEvent.hpp / OrderFlow::EventMarks)
//   一组一行, 自底向上贴 X 轴 (隐藏组不占行); 单点 / 区间同一画法: 衬底圆 + 彩色单字落在首日中点,
//   区间另在本行画细色带至末日. 同行重叠按重要程度叠画 (kEventRank 大者在上); hover 列出本行鼠标处 (字形 / 色带) 的全部事件
// ============================================================================

static constexpr ImU32 kEventColor[] = {
    IM_COL32(90, 220, 120, 255),  // List
    IM_COL32(200, 90, 90, 255),   // Delist
    IM_COL32(170, 170, 170, 255), // Suspend
    IM_COL32(255, 140, 0, 255),   // Risk
    IM_COL32(120, 180, 255, 255), // Rename
    IM_COL32(0, 200, 200, 255),   // Industry
    IM_COL32(255, 215, 0, 255),   // ExDiv
    IM_COL32(220, 180, 120, 255), // Allot
    IM_COL32(180, 130, 255, 255), // Capital
    IM_COL32(0, 200, 0, 255),     // LimitUp (与 K 线同色系: 涨绿)
    IM_COL32(220, 40, 40, 255),   // LimitDn (跌红)
    IM_COL32(255, 90, 200, 255),  // Dragon
    IM_COL32(230, 230, 230, 255), // Report
    IM_COL32(100, 200, 255, 255), // Forecast
    IM_COL32(180, 255, 100, 255), // Express
    IM_COL32(255, 170, 140, 255), // Holder
};
static_assert(std::size(kEventColor) == kEventKinds, "kEventColor 与 ASSET_EVENT_KINDS 逐行对齐");

// 重要程度: 同行 (同组) 内比较, 大者后画压在上面; 同级按时间序
static constexpr uint8_t kEventRank[] = {
    4, // List
    5, // Delist
    2, // Suspend
    3, // Risk
    0, // Rename
    1, // Industry
    1, // ExDiv
    2, // Allot
    0, // Capital
    0, // LimitUp
    0, // LimitDn
    1, // Dragon
    1, // Report
    3, // Forecast
    2, // Express
    0, // Holder
};
static_assert(std::size(kEventRank) == kEventKinds, "kEventRank 与 ASSET_EVENT_KINDS 逐行对齐");

static ImU32 EventColor(EventKind k) { return kEventColor[static_cast<size_t>(k)]; }
static uint8_t EventRank(EventKind k) { return kEventRank[static_cast<size_t>(k)]; }

// 区间色带: Risk 按等级 v1 分色 (1=ST 2=*ST 3=退市整理期), 其余同种类色
static constexpr ImU32 kRiskBandColor[] = {
    IM_COL32(255, 140, 0, 255), // ST
    IM_COL32(255, 40, 40, 255), // *ST
    IM_COL32(170, 0, 120, 255), // 退市整理期
};
static ImU32 EventBandColor(const AssetEvent &e) {
  if (e.kind != EventKind::Risk)
    return EventColor(e.kind);
  const size_t level = static_cast<size_t>(e.v[1]);
  assert(level >= 1 && level <= std::size(kRiskBandColor) && "Risk 等级须为 1 / 2 / 3");
  return kRiskBandColor[level - 1];
}

// hover 明细 (日期之后的载荷; 无载荷 = 空串). 源单位 → 展示单位只在这里换
// evs = 选中资产全部事件 (d0 升序), i = 本事件下标 (改名回溯曾用名用)
static void EventDetail(const std::vector<AssetEvent> &evs, size_t i, const AssetEvents &src, char *buf, size_t n) {
  const AssetEvent &e = evs[i];
  const auto &v = e.v;
  const char *t = src.texts[e.text].c_str();
  switch (e.kind) {
  case EventKind::List:
    if (e.text)
      std::snprintf(buf, n, "上市简称 %s", t);
    else
      buf[0] = '\0';
    return;
  case EventKind::Delist:
  case EventKind::LimitUp:
  case EventKind::LimitDn:
    buf[0] = '\0';
    return;
  case EventKind::Suspend:
    std::snprintf(buf, n, "%.0f 个交易日  %s", v[0], t);
    return;
  case EventKind::Risk:
    std::snprintf(buf, n, "%s  %.0f 个交易日", v[1] == 1.0f ? "ST" : (v[1] == 2.0f ? "*ST" : "退市整理期"), v[0]);
    return;
  case EventKind::Rename: { // 曾用名 = 前一条改名的新名, 首次改名 = 上市简称 (无 = ?)
    uint32_t prev = 0;
    for (size_t j = i; j-- > 0;)
      if (evs[j].kind == EventKind::Rename || evs[j].kind == EventKind::List) {
        prev = evs[j].text;
        break;
      }
    std::snprintf(buf, n, "%s → %s", prev ? src.texts[prev].c_str() : "?", t);
    return;
  }
  case EventKind::Industry:
    std::snprintf(buf, n, "申万一级: %s", t);
    return;
  case EventKind::ExDiv: { // 每 10 股 (A 股惯例); 缺项不列
    char pay[32] = "", bonus[32] = "", conv[32] = "";
    if (v[0] > 0.0f)
      std::snprintf(pay, sizeof(pay), " 派%.2f元", v[0] * 10.0f);
    if (v[1] > 0.0f)
      std::snprintf(bonus, sizeof(bonus), " 送%.2g", v[1] * 10.0f);
    if (v[2] > 0.0f)
      std::snprintf(conv, sizeof(conv), " 转%.2g", v[2] * 10.0f);
    std::snprintf(buf, n, "每10股%s%s%s (税前)", pay, bonus, conv);
    return;
  }
  case EventKind::Allot:
    std::snprintf(buf, n, "每10股配%.2g股  配股价 %.2f元", v[1] * 10.0f, v[0]);
    return;
  case EventKind::Capital:
    std::snprintf(buf, n, "总股本 %.2f亿股  流通A %.2f亿股  原因码 %s", v[0] / 1e8f, v[1] / 1e8f, t);
    return;
  case EventKind::Dragon:
    std::snprintf(buf, n, "净买 %+.0f万  涨跌 %+.2f%%  %s", v[0] / 1e4f, v[1], t);
    return;
  case EventKind::Report:
    std::snprintf(buf, n, "报告期 %s", misc::dash(e.ref).c_str());
    return;
  case EventKind::Forecast:
    if (std::isnan(v[0]) || std::isnan(v[1]))
      std::snprintf(buf, n, "报告期 %s  %s", misc::dash(e.ref).c_str(), t);
    else
      std::snprintf(buf, n, "报告期 %s  %s  净利变动 %+.0f%% ~ %+.0f%%", misc::dash(e.ref).c_str(), t, v[0], v[1]);
    return;
  case EventKind::Express:
    if (v[2] > 0.0f)
      std::snprintf(buf, n, "报告期 %s  营收 %.2f亿  净利 %.2f亿  同比 %+.1f%%", misc::dash(e.ref).c_str(), v[0] / 1e8f,
                    v[1] / 1e8f, (v[1] / v[2] - 1.0f) * 100.0f);
    else
      std::snprintf(buf, n, "报告期 %s  营收 %.2f亿  净利 %.2f亿", misc::dash(e.ref).c_str(), v[0] / 1e8f, v[1] / 1e8f);
    return;
  case EventKind::Holder:
    if (std::isnan(v[1]))
      std::snprintf(buf, n, "截止 %s  %.0f户", misc::dash(e.ref).c_str(), v[0]);
    else
      std::snprintf(buf, n, "截止 %s  %.0f户  环比 %+.1f%%", misc::dash(e.ref).c_str(), v[0], v[1] * 100.0f);
    return;
  }
  assert(false && "EventDetail: 未知事件种类");
}

static void PlotEventLanes(const OrderFlow::EventMarks &em, const AssetEvents &src, unsigned groups) {
  if (!em.events)
    return;
  const std::vector<AssetEvent> &evs = *em.events;
  const float r = ImGui::GetFontSize() * 0.6f; // 衬底圆半径
  const float lane_h = 2.0f * r + 2.0f;
  const double half_day = 0.5 * static_cast<double>(OrderFlowConst::MIN_CAPACITY);
  const ImVec2 pos = ImPlot::GetPlotPos(), size = ImPlot::GetPlotSize();
  const ImVec2 mouse = ImGui::GetMousePos();
  const bool hovered = ImPlot::IsPlotHovered();
  struct Vis {
    uint32_t ev;
    float px0, px1, cx; // 色带两端 / 字形中心 (像素)
  };
  static std::vector<uint32_t> hits; // GUI 单线程, 帧内复用
  static std::vector<Vis> vis;
  hits.clear();

  ImPlot::PushPlotClipRect();
  ImDrawList *dl = ImPlot::GetPlotDrawList();
  float y1 = pos.y + size.y; // 当前行下沿
  for (size_t g = kEventGroups; g-- > 0;) {
    if (!(groups >> g & 1u))
      continue;
    const float y0 = y1 - lane_h, yc = 0.5f * (y0 + y1);
    const bool in_lane = hovered && mouse.y >= y0 && mouse.y < y1;
    dl->AddRectFilled(ImVec2(pos.x, y0), ImVec2(pos.x + size.x, y1), IM_COL32(0, 0, 0, 90));

    // 可见事件 (hover 命中按时间序收集)
    vis.clear();
    for (const OrderFlow::EventMarks::Mark &m : em.lane[g]) {
      const bool span = event_def(evs[m.ev].kind).shape == EventShape::Span;
      const float px0 = ImPlot::PlotToPixels(m.x0, 0.0).x, px1 = ImPlot::PlotToPixels(m.x1, 0.0).x;
      const float cx = ImPlot::PlotToPixels(m.x0 + half_day, 0.0).x;
      const float right = span ? std::max(cx + r, px1) : cx + r;
      if (right < pos.x || std::min(px0, cx - r) > pos.x + size.x)
        continue;
      if (in_lane && mouse.x >= cx - r && mouse.x <= right)
        hits.push_back(m.ev);
      vis.push_back({m.ev, px0, px1, cx});
    }
    // 叠画序: 重要程度升序 (大者后画在上), 同级保持时间序
    std::stable_sort(vis.begin(), vis.end(),
                     [&](const Vis &a, const Vis &b) { return EventRank(evs[a.ev].kind) < EventRank(evs[b.ev].kind); });

    // 色带先画, 字形压在上面
    for (const Vis &v : vis) {
      const AssetEvent &e = evs[v.ev];
      if (event_def(e.kind).shape != EventShape::Span)
        continue;
      const ImU32 band = (EventBandColor(e) & ~IM_COL32_A_MASK) | (140u << IM_COL32_A_SHIFT);
      dl->AddRectFilled(ImVec2(v.px0, yc - 2.0f), ImVec2(std::max(v.px1, v.px0 + 1.0f), yc + 2.0f), band);
    }

    for (const Vis &v : vis) {
      const AssetEvent &e = evs[v.ev];
      const EventKindDef &def = event_def(e.kind);
      const float cx = v.cx;
      const ImU32 col = EventColor(e.kind);
      dl->AddCircleFilled(ImVec2(cx, yc), r, IM_COL32(24, 24, 28, 235));
      dl->AddCircle(ImVec2(cx, yc), r, col, 0, 1.5f);
      const ImVec2 ts = ImGui::CalcTextSize(def.glyph);
      dl->AddText(ImVec2(cx - 0.5f * ts.x, yc - 0.5f * ts.y), col, def.glyph);
    }
    y1 = y0;
  }
  ImPlot::PopPlotClipRect();

  if (hits.empty())
    return;
  constexpr size_t kMaxLines = 12;
  char buf[256];
  ImGui::BeginTooltip();
  for (size_t i = 0; i < std::min(hits.size(), kMaxLines); ++i) {
    const AssetEvent &e = evs[hits[i]];
    const EventKindDef &def = event_def(e.kind);
    ImGui::TextColored(ImGui::ColorConvertU32ToFloat4(EventColor(e.kind)), "%s", def.glyph);
    ImGui::SameLine();
    const std::string day = def.shape == EventShape::Span ? misc::dash(e.d0) + " ~ " + misc::dash(e.d1) : misc::dash(e.d0);
    ImGui::Text("%s  %s", def.name, day.c_str());
    EventDetail(evs, hits[i], src, buf, sizeof(buf));
    if (buf[0]) {
      ImGui::SameLine();
      ImGui::TextDisabled("%s", buf);
    }
  }
  if (hits.size() > kMaxLines)
    ImGui::TextDisabled("… 另 %zu 条", hits.size() - kMaxLines);
  ImGui::EndTooltip();
}

// ============================================================================
// L_1min Plot Renderer (图2: Min 已发布前缀)
// ============================================================================

static void RenderMinPlot(SharedData &data, float height) {
  OrderFlow &of = data.orderflow;
  auto &ui = of.ui;
  auto &mn = of.min;

  // 发布代追上请求代 = 数组前缀可读 (未就绪: 计数归零, 旧代计数不可用于本帧)
  const OrderFlow::Min::Prefix pre = mn.prefix(ui.min.gen);

  // 换代在途 (拖动资产): 照画空图, 轴与视野原样留着 —— 撤掉整张图会闪.
  // 只有 dates 正在重扫 (rescan / 首帧) 时连 X 域和刻度都读不出来, 才出提示
  if (!pre.ready && (ui.min_rescan_pending || ui.min_x_max <= 0.0)) {
    ImGui::TextDisabled("Preparing K-line stream...");
    return;
  }
  if (pre.ready) {
    if (mn.dates.empty()) {
      ImGui::TextDisabled("No feature dates found");
      return;
    }
    ui.min_x_max = static_cast<double>(mn.dates.size() * OrderFlowConst::MIN_CAPACITY);
  }
  // overlay 列 = 已发布代实际渲染的列 (refs 在本代发布期间稳定; 未就绪 = 不画)
  const OrderFlow::Overlay &ov = mn.feat;
  static const std::vector<ColRef> kNone;
  static std::vector<FeatCat> cats; // GUI 单线程, 帧内复用
  CollectFeatCats(data, pre.ready ? ov.refs : kNone, cats);

  // Y1: 发布范围变化 (换代 / 流式追加) 或上帧 X 视野变了 → 按可见 X 区间内的 K 线贴合 (RangeFit); 只动 Y 轴不重贴
  if ((pre.points > 0 && ui.min_last_pub_days != pre.days) || ui.min_fit_y)
    ImPlot::SetNextAxisToFit(ImAxis_Y1);
  ui.min_fit_y = false;

  if (ImPlot::BeginPlot("##KLine", ImVec2(-1, height))) {
    ImPlot::SetupAxes(nullptr, nullptr, 0, ImPlotAxisFlags_RangeFit);
    // X: 固定域 (0 ~ 全部特征日), 只在域本身变了 (重扫日期) 才复位 → 换资产 / 流式追加都不动视野
    const bool x_domain_changed = (ui.min_x_applied != ui.min_x_max);
    ui.min_x_applied = ui.min_x_max;
    ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, std::max(ui.min_x_max, 1.0),
                            x_domain_changed ? ImPlotCond_Always : ImPlotCond_Once);
    ImPlot::SetupAxisFormat(ImAxis_X1, MinTimeFormatter, &mn); // YY/MM/DD HH:MM (非重扫期 dates 稳定)
    ImPlot::SetupAxisFormat(ImAxis_Y1, PriceFormatter);        // 定宽: 换资产不挪 plot 左边界
    if (pre.ready)
      ui.min_last_pub_days = pre.days;

    // 特征 overlay 的右轴 (按 Cat2 分流; price 类直接借主图 Y1, 不开轴)
    SetupOverlayAxes(ov, cats);

    {
      const ImPlotRange xr = ImPlot::GetPlotLimits().X;
      ui.min_fit_y = xr.Min != ui.min_view_x_min || xr.Max != ui.min_view_x_max;
      ui.min_view_x_min = xr.Min;
      ui.min_view_x_max = xr.Max;
    }

    // ------------------------------------------------------------------
    // K线 (已发布前缀) + 特征 overlay (多选, legend = 中文名 (cat2) 池 层)
    // ------------------------------------------------------------------

    // 缺失日竖条: 已发布前缀里该资产无 data_valid 分钟的 day_idx → 淡黄色竖 stripe
    // (alpha 0.1) 打底, 画在 K 线之前以免遮挡. 缓存键 (Min 请求代, 已发布日数), 流式
    // 追加 / 换资产 / 重扫日期都使键变 → 自动失效. 二分判定每日有无 K 线点.
    // 只在单池日轴时画 (多池叠加 = 并集日轴, 各池自己画自己有的日, 缺日不标)
    if (pre.ready && pre.days > 0 && std::has_single_bit(ui.min.key->pools)) {
      auto &miss = of.missing_days;
      if (miss.key.stale(ui.min.gen, pre.days))
        miss.build(mn, pre.days, pre.points);

      if (!miss.days.empty()) {
        ImPlot::PushPlotClipRect();
        ImDrawList *draw_list = ImPlot::GetPlotDrawList();
        const ImPlotRect limits = ImPlot::GetPlotLimits();
        constexpr ImU32 col = IM_COL32(255, 240, 170, 26); // 淡黄, alpha 0.1
        for (size_t d : miss.days) {
          const double x0 = static_cast<double>(d * OrderFlowConst::MIN_CAPACITY);
          const double x1 = x0 + static_cast<double>(OrderFlowConst::MIN_CAPACITY);
          const ImVec2 p0 = ImPlot::PlotToPixels(x0, limits.Y.Min);
          const ImVec2 p1 = ImPlot::PlotToPixels(x1, limits.Y.Max);
          draw_list->AddRectFilled(p0, p1, col);
        }
        ImPlot::PopPlotClipRect();
      }
    }

    if (pre.points > 0)
      PlotCandlestick("OHLC", mn.x.data(), mn.ohlc, static_cast<int>(pre.points), mn.half_width);
    // 双击复位 X = 全日轴 (与初始视野同口径); K 线 FitPoint 只到本资产有 K 线的日
    if (ImPlot::FitThisFrame()) {
      ImPlot::FitPointX(0.0);
      ImPlot::FitPointX(std::max(ui.min_x_max, 1.0));
    }

    PlotOverlay(data.feature, ov, cats);

    // 事件标注: (Min 请求代, 事件构建代) 变了才重建 (dates 只在 ready 时可读)
    if (pre.ready) {
      const AssetEvents &events = data.assetinfo.get_events();
      if (of.event_marks.key.stale(ui.min.gen, events.seq)) {
        const AssetItem &item = data.asset.items[ui.min.key->asset];
        std::string key = item.exchange;
        std::transform(key.begin(), key.end(), key.begin(), ::tolower);
        of.event_marks.build(mn.dates, events.find(key + "." + item.asset_code));
      }
      PlotEventLanes(of.event_marks, events, ui.event_groups);
    }

    // ------------------------------------------------------------------
    // Anchor: 吸附日起点 → 驱动图1 (day, asset) 请求
    // ------------------------------------------------------------------
    {
      double anchor_x = ui.min_anchor_x;
      const bool drag_changed = ImPlot::DragLineX(0, &anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f);
      const bool drag_active = ImGui::IsItemActive();

      if (drag_changed) {
        if (drag_active) { // 拖动中跟手, 不动 anchor_date (释放才吸附日起点 → 触发图1 重放)
          ui.min_anchor_x = std::max(0.0, anchor_x);
        } else {
          ui.min_anchor_x = mn.snap_to_day_start(anchor_x);
          ui.min_anchor_date = mn.date_from_x(ui.min_anchor_x);
        }
      }
      if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0)) {
        ui.min_anchor_x = mn.snap_to_day_start(ImPlot::GetPlotMousePos().x);
        ui.min_anchor_date = mn.date_from_x(ui.min_anchor_x);
      }

      if (ui.min_anchor_date && pre.points > 0) {
        const auto end = mn.x.begin() + static_cast<long>(pre.points);
        const auto it = std::lower_bound(mn.x.begin(), end, ui.min_anchor_x);
        double anchor_y = 0;
        if (it != end)
          anchor_y = mn.ohlc[OrderFlow::Min::kClose][static_cast<size_t>(it - mn.x.begin())];

        char date_buf[16];
        FormatDateShort(date_buf, sizeof(date_buf), ui.min_anchor_date);
        ImPlot::Annotation(ui.min_anchor_x, anchor_y, ImVec4(1, 0.5f, 0, 1),
                           ImVec2(5, -15), false, "%s", date_buf);
      }
    }

    ImPlot::EndPlot();
  }
}

// ============================================================================
// Asset Filter + Selector (筛选口径搬自 DATABASE/TABLE, 数据换成锚点日的逐日 PIT)
// ============================================================================

static void RenderAssetFilterBar(SharedData &data, OrderFlow &of) {
  auto &pick = of.pick;

  // ST/Listed/Board 与 DATABASE/TABLE 共用控件 (gui/util/AssetFilter.hpp);
  // risk_warn 有 3=退市整理期, 所以 ST 要全 4 个价位
  bool changed = GUI::Filter::CommonFilters<int>("of", true, pick.st_filter,
                                                 pick.listed_filter, pick.board_filter);

  // 行业维自绘: 这里的键是数字 SW2021 一级 ID (0 = 未知), 与 TABLE 的字符串
  // 行业码不同源, 表在 fund::SW2021_L1_NAMES
  static const std::vector<std::pair<int, std::string>> industry_items = [] {
    std::vector<std::pair<int, std::string>> v;
    for (size_t i = 0; i < fund::SW2021_L1_COUNT; ++i)
      v.emplace_back(static_cast<int>(i), std::string(fund::SW2021_L1_NAMES[i]));
    return v;
  }();
  ImGui::SameLine();
  changed |= GUI::Filter::MultiSelectCombo("Industry##ofInd", 140.0f, industry_items,
                                           pick.industry_filter);

  if (changed)
    ++pick.filter_epoch;

  // 候选数 / 锚点日
  const OrderFlow::Pick::Slot &slot = pick.slots.front_slot();
  ImGui::SameLine();
  if (pick.slots.ready(of.ui.pick.gen) && !slot.meta.empty()) {
    ImGui::TextColored(ImVec4(0.3f, 0.8f, 0.3f, 1.0f), "%zu/%zu",
                       pick.candidates.size(), data.asset.items.size());
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s 通过筛选的资产数 / 总数\n"
                        "• 状态取自前一交易日 L_1day 行 (ST / 上市 / 行业)\n"
                        "• 候选范围 = 主池轴 (无选中特征 = all, 否则 = 选中特征所在池)\n"
                        "• 日轴首日无前一交易日状态, 不列入",
                        misc::dash(slot.date).c_str());
  } else {
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.0f, 1.0f), "loading...");
  }
}

// 资产选择 = 候选序位拖动条 (无下拉菜单): 条面覆盖显示 "代码-市场-名称[ST] 序位/总数",
// 与筛选栏同行右侧; 滚轮在条上 ±1 个价位微调 (拖动条一像素可能跨多个资产)
static void RenderAssetSlider(SharedData &data, OrderFlow &of) {
  auto &ui = of.ui;
  auto &pick = of.pick;
  const size_t asset_idx = static_cast<size_t>(ui.selected_asset_idx);

  ImGui::SameLine();
  const int n = static_cast<int>(pick.candidates.size());
  if (n == 0) {
    ImGui::TextDisabled("无符合筛选的资产");
    return;
  }

  // 当前选中是否在候选内 (切日期/改筛选后可能落选; 保留选中, 仅标注)
  const auto it = std::find(pick.candidates.begin(), pick.candidates.end(), asset_idx);
  const bool in_candidates = (it != pick.candidates.end());
  int pos = in_candidates ? static_cast<int>(it - pick.candidates.begin()) : 0;

  // candidates 可能建于上一代槽 (新代在途), ST 标记按当前 front 槽尽力显示
  const auto &meta_now = pick.slots.front_slot().meta;
  const uint8_t rw = asset_idx < meta_now.size() ? meta_now[asset_idx].risk_warn : 0;
  const auto &asset = data.asset.items[asset_idx];
  char overlay[256];
  std::snprintf(overlay, sizeof(overlay), "%s-%s-%s%s  %d/%d",
                asset.asset_code.c_str(), asset.exchange.c_str(), asset.asset_name.c_str(),
                in_candidates ? (rw == 2 ? " *ST" : (rw == 1 ? " ST" : (rw == 3 ? " 退整" : "")))
                              : " (不符筛选)",
                in_candidates ? pos + 1 : 0, n);

  ImGui::SetNextItemWidth(std::max(ImGui::GetContentRegionAvail().x - 4.0f, 160.0f));
  if (!in_candidates)
    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.0f, 0.8f, 0.0f, 1.0f));
  bool changed = ImGui::SliderInt("##asset", &pos, 0, n - 1, overlay, ImGuiSliderFlags_AlwaysClamp);
  if (!in_candidates)
    ImGui::PopStyleColor();

  const bool hovered = ImGui::IsItemHovered();
  if (hovered) {
    const float wheel = ImGui::GetIO().MouseWheel;
    if (wheel != 0.0f) {
      pos = std::clamp(pos + (wheel > 0.0f ? 1 : -1), 0, n - 1);
      changed = true;
    }
    ImGui::SetTooltip("切资产\n"
                      "• 拖动: 按候选序 (市场 → 代码)\n"
                      "• 滚轮: ±1");
  }
  if (changed)
    ui.selected_asset_idx = static_cast<int>(pick.candidates[static_cast<size_t>(pos)]);
}

// ============================================================================
// Status Bar
// ============================================================================

static void RenderStatusBar(const OrderFlow &of) {
  const auto &ui = of.ui;
  const auto &mn = of.min;

  // Min 流式进度
  const OrderFlow::Min::Prefix pre = mn.prefix(ui.min.gen);
  ImGui::SameLine();
  if (pre.ready && !mn.dates.empty()) {
    const bool streaming = pre.days < mn.dates.size();
    ImGui::TextColored(streaming ? ImVec4(1.0f, 0.8f, 0.0f, 1.0f) : ImVec4(0.3f, 0.8f, 0.3f, 1.0f),
                       "[L_1min: %zu/%zu days, %zu pts]", pre.days, mn.dates.size(), pre.points);
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("K线流式加载 (从前往后逐日)\n"
                        "• 已发布日数 / 总日数\n"
                        "• 有效分钟数");
  } else {
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.0f, 1.0f), "[L_1min: rebuilding]");
  }

  ImGui::SameLine();
  if (ui.min_anchor_date) {
    char date_buf[16];
    FormatDateShort(date_buf, sizeof(date_buf), ui.min_anchor_date);
    ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "Anchor: %s", date_buf);
  }

  // Sec 重放状态
  ImGui::SameLine();
  const OrderFlow::Sec &sc = of.sec.front_slot();
  if (!of.sec.ready(ui.sec.gen)) {
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.0f, 1.0f), "[L_sec: replaying...]");
  } else if (sc.has_data) {
    ImGui::TextColored(ImVec4(0.3f, 0.6f, 0.9f, 1.0f), "[L_sec: %zu snaps / %zu rects / %zu orders]",
                       sc.size(), sc.heat.rect_count, sc.order_count);
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("L_sec 当日盘口重放\n"
                        "• 秒级盘口快照数 (订单簿逐笔重放)\n"
                        "• 热力图合并矩形数\n"
                        "• 当日逐笔条数");
  } else if (sc.asset_idx != SIZE_MAX) {
    ImGui::TextColored(ImVec4(0.6f, 0.6f, 0.6f, 1.0f), "[L_sec: no data]");
  }
}

// ============================================================================
// Heatmap Controls
// ============================================================================

static void RenderHeatmapControls(OrderFlow &of) {
  auto &ui = of.ui;

  ImGui::Checkbox("Heatmap", &ui.show_heatmap);

  if (ui.show_heatmap) {
    ImGui::SameLine();
    ImGui::SetNextItemWidth(150);
    ImGui::SliderFloat("Threshold", &ui.log_amount_threshold,
                       OrderFlowConst::HEATMAP_LOG_THR_MIN, OrderFlowConst::HEATMAP_LOG_THR_MAX, "%.1f");

    if (ImGui::IsItemHovered()) {
      ImGui::SetNextWindowSize(ImVec2(350, 0), ImGuiCond_Always);
      ImGui::BeginTooltip();
      ImGui::Text("Log10(金额) 下限阈值");
      ImGui::Separator();
      ImGui::Text("• 3.0 = 1千元 (显示所有 >= 1千的价位)");
      ImGui::Text("• 4.0 = 1万元");
      ImGui::Text("• 5.0 = 10万元");
      ImGui::Text("• 6.0 = 100万元");
      ImGui::Text("• 7.0 = 1000万元 (仅显示大额价位)");
      ImGui::Separator();
      ImGui::TextWrapped("• 着色: [阈值, 1000万] 映射到 [透明, 完全实色]");
      ImGui::TextWrapped("• 自动初值: 换日 / 换资产自动取当日初值");
      ImGui::TextWrapped("    ◦ 按当日全簿 (价位·秒) 加权金额分位取");
      ImGui::TextWrapped("    ◦ 使着色面积占有量面积恒定 → 各日色块浓度观感一致");
      ImGui::EndTooltip();
    }
  }
}

// ============================================================================
// Event Controls (图2 事件行开关, 一组一个; hover = 本组字形图例)
// ============================================================================

static void RenderEventControls(OrderFlow &of) {
  for (size_t g = 0; g < kEventGroups; ++g) {
    char label[32];
    std::snprintf(label, sizeof(label), "%s##ofEv", kEventGroupNames[g]);
    ImGui::SameLine();
    ImGui::CheckboxFlags(label, &of.ui.event_groups, 1u << g);
    if (!ImGui::IsItemHovered())
      continue;
    ImGui::BeginTooltip();
    ImGui::TextDisabled("图2 时间轴事件行");
    for (size_t i = 0; i < kEventKinds; ++i) {
      const EventKindDef &def = kEventKindDefs[i];
      if (static_cast<size_t>(def.group) != g)
        continue;
      ImGui::TextColored(ImGui::ColorConvertU32ToFloat4(kEventColor[i]), "%s", def.glyph);
      ImGui::SameLine();
      ImGui::Text("%s%s", def.name, def.shape == EventShape::Span ? " (区间)" : "");
    }
    ImGui::EndTooltip();
  }
}

// ============================================================================
// Main Orchestration
// ============================================================================

void RenderTabOrderFlow(OrderFlowService *service, SharedData &data) {
  if (!service) {
    ImGui::TextDisabled("OrderFlowService not initialized");
    return;
  }

  // Configure input mapping: Left-click for box select, Right-click for pan
  static bool input_map_configured = false;
  if (!input_map_configured) {
    ImPlot::MapInputReverse();
    input_map_configured = true;
  }

  auto &of = data.orderflow;
  auto &ui = of.ui;
  const size_t num_assets = data.asset.items.size();
  if (num_assets == 0) {
    ImGui::TextDisabled("No assets");
    return;
  }

  service->Start(data); // 幂等: 首帧启动 worker

  // ==========================================================================
  // 帧首: 背槽发布 ack → 翻 front (先翻面后清 pending, worker 等 pending 清)
  // ==========================================================================
  of.sec.ack();
  of.pick.slots.ack();

  // ==========================================================================
  // 期望态检测 → 请求 (gen++ 即"停止消费旧代", 发布追上后自动恢复渲染)
  // ==========================================================================
  const auto &sel = data.feature.selection;
  const size_t asset_idx = static_cast<size_t>(ui.selected_asset_idx);

  // 主池 = 选中列所在的唯一池 (无选中 = all): 图1 VWAP / 候选轴; 选中跨池 → 不画, 提示重选 (先于消费 needs_rescan)
  PoolMask picked_pools = 0;
  for (const ColRef &c : sel.picked)
    picked_pools |= pool_bit(c.pool);
  if (std::popcount(picked_pools) > 1) {
    std::string names;
    for (const PoolId l : pool_ids())
      if (picked_pools & pool_bit(l))
        names += (names.empty() ? "" : " / ") + std::string(pool_def(l).name);
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.0f, 1.0f), "选中特征跨多个池 (%s), 请在 Feature 页只选一个池的特征", names.c_str());
    return;
  }
  // 主池变了 = SecKey / PickKey 变了 → 图1 / Pick 自然重发
  const PoolId main_pool = picked_pools ? static_cast<PoolId>(std::countr_zero(picked_pools)) : kAllPool;
  const bool rescan = of.needs_rescan.exchange(false, std::memory_order_relaxed);

  // 两图同一组选中列 (跨层跨档), 各截到图的网格 (图2 = L_1min, 图1 = L_sec; 细于网格的列不进), 各点尾部对齐.
  // 图2 日轴 = 池集 (all 日 K 底图 + overlay 各池) 落盘日的并集: 池集变了 = 日轴变了 → 重扫
  const std::vector<ColRef> min_feats = CollectFeats(sel, L_1min), sec_feats = CollectFeats(sel, L_sec);
  PoolMask min_pools = pool_bit(kAllPool);
  for (const ColRef &c : min_feats)
    min_pools |= pool_bit(c.pool);
  const bool repool = !ui.min.key || ui.min.key->pools != min_pools;
  if (ui.min.set({asset_idx, min_feats, min_pools}, rescan)) {
    ui.min_last_pub_days = SIZE_MAX; // 换代: 重置轴管理
    // dates 会被重扫 (或首次建) → 期间不可读; 粘到新代落定 (重扫请求可能被后一请求覆盖, 后者须照带)
    ui.min_rescan_pending |= rescan || repool || ui.min_x_max <= 0.0;
    if (rescan) { // 库重算 / 日轴可能变了: 锚点日先在新日轴上核对, 落定后图1 / Pick 重发
      ui.sec.reset();
      ui.pick.reset();
    }
    service->Request(data, ui.min.gen, *ui.min.key, ui.min_rescan_pending);
  }

  // Min 发布状态 (anchor 默认值需要 dates)
  const bool min_ready = of.min.prefix(ui.min.gen).ready;
  if (min_ready && ui.min_rescan_pending) {
    ui.min_rescan_pending = false; // dates 已随新代落定
    // 锚点日须在新日轴上: 在 = 按新轴重定位, 不在 = 回末日
    const auto &dates = of.min.dates;
    const auto it = std::lower_bound(dates.begin(), dates.end(), ui.min_anchor_date);
    if (it != dates.end() && *it == ui.min_anchor_date)
      ui.min_anchor_x = static_cast<double>(static_cast<size_t>(it - dates.begin()) * OrderFlowConst::MIN_CAPACITY);
    else
      ui.min_anchor_date = 0;
  }

  // 默认锚点 = 末日 (轴首无承接日, Pick 状态读不到)
  if (!ui.min_anchor_date && min_ready && !of.min.dates.empty()) {
    ui.min_anchor_x = static_cast<double>((of.min.dates.size() - 1) * OrderFlowConst::MIN_CAPACITY);
    ui.min_anchor_date = of.min.dates.back();
  }

  // 图1 / Pick 只取锚点日 (日轴重扫在途时不发: 锚点日待核对)
  const bool anchored = ui.min_anchor_date && !ui.min_rescan_pending;

  // 图1 特征 = 同一组选中列 (≥ L_sec)
  if (anchored && ui.sec.set({main_pool, ui.min_anchor_date, asset_idx, sec_feats}))
    service->Request(data, ui.sec.gen, *ui.sec.key);

  // ==========================================================================
  // Pick: 锚点日的资产筛选状态 (先定日期, 再 apply filter)
  // ==========================================================================
  auto &pick = of.pick;

  // 静态显示序 (市场 → 代码) + 板块表: 建一次
  static std::vector<int> asset_boards;
  if (pick.display_order.size() != num_assets) {
    std::vector<std::pair<std::string, std::string>> exch_code(num_assets);
    asset_boards.resize(num_assets);
    for (size_t i = 0; i < num_assets; ++i) {
      exch_code[i] = {data.asset.items[i].exchange, data.asset.items[i].asset_code};
      asset_boards[i] = static_cast<int>(GUI::Database::GetBoardType(data.asset.items[i].asset_code));
    }
    pick.build_display_order(exch_code);
    pick.key.reset(); // 强制重建候选
  }

  if (anchored && ui.pick.set({main_pool, ui.min_anchor_date}))
    service->Request(data, ui.pick.gen, *ui.pick.key);

  // 候选重建 (槽换代 或 筛选条件变)
  {
    const OrderFlow::Pick::Slot &slot = pick.slots.front_slot();
    if (pick.slots.ready(ui.pick.gen) && slot.meta.size() == num_assets && pick.key.stale(slot.gen, pick.filter_epoch))
      pick.rebuild_candidates(slot, asset_boards);
  }

  // 初始资产: 候选里代码编号最小的 (默认筛选已排除退市/未上市; 仅首次自动选)
  if (!ui.asset_initialized && !pick.candidates.empty()) {
    size_t best = pick.candidates.front();
    for (size_t i : pick.candidates) {
      if (data.asset.items[i].asset_code < data.asset.items[best].asset_code)
        best = i;
    }
    ui.selected_asset_idx = static_cast<int>(best);
    ui.asset_initialized = true;
  }

  // ==========================================================================
  // LAYOUT
  // ==========================================================================
  const float content_height = ImGui::GetContentRegionAvail().y;
  const float content_width = ImGui::GetContentRegionAvail().x;
  const float top_view_height = content_height * OrderFlowConst::TOP_VIEW_RATIO;
  const float bottom_view_height = content_height * (1.0f - OrderFlowConst::TOP_VIEW_RATIO) - 5.0f;
  const float chart_width = content_width - OrderFlowConst::DEPTH_PANEL_WIDTH - 10.0f;

  // ==========================================================================
  // TOP: L_sec 秒级盘口 + 深度面板
  // ==========================================================================
  ImGui::BeginChild("TopSection", ImVec2(0, top_view_height), false);
  ImGui::BeginChild("SecChart", ImVec2(chart_width, -1), false);
  RenderSecPlot(data);
  ImGui::EndChild();

  ImGui::SameLine();
  // 无边框: 与 SecChart 同 padding, 两图 plot 区域上下像素级对齐
  ImGui::BeginChild("DepthPanel", ImVec2(OrderFlowConst::DEPTH_PANEL_WIDTH, -1), false);
  {
    const OrderFlow::Sec &sc = of.sec.front_slot();
    if (sc.has_data && ui.sec_anchor_tick != SIZE_MAX) {
      RenderDepthPanel(of, sc, sc.row_at(static_cast<double>(ui.sec_anchor_tick)));
    } else {
      ImGui::TextDisabled("No L_sec data");
    }
  }
  ImGui::EndChild();
  ImGui::EndChild(); // TopSection

  // ==========================================================================
  // BOTTOM: K线 + 控件
  // ==========================================================================
  ImGui::BeginChild("BottomSection", ImVec2(0, bottom_view_height), true);

  RenderAssetFilterBar(data, of);
  RenderAssetSlider(data, of); // 同行右侧 (筛选控件之后剩余宽度)
  RenderHeatmapControls(of);
  RenderEventControls(of);
  RenderStatusBar(of);

  const float min_height = ImGui::GetContentRegionAvail().y;
  RenderMinPlot(data, min_height);

  ImGui::EndChild(); // BottomSection
}

} // namespace GUI::Features
