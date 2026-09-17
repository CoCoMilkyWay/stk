// TabOrderFlow Implementation - OrderFlow Visualization
// 渲染面 (数据面/线程模型见 shared/OrderFlow.hpp):
//   1. 帧首 ack: depth 背槽发布 → 翻 front
//   2. 期望态检测: asset / anchor date / 选中特征 变了 → gen++ → Request*
//   3. 渲染: Kline 画已发布前缀 (gen 配对), Depth 画 front 槽 (新代在途时旧槽照画 + Loading 提示)
#include "gui/task_features/ui/TabOrderFlow.hpp"
#include "features/Method/Fundamental.hpp"
#include "gui/task_database/models/SharedTypes.hpp" // BoardType / GetBoardType (板块口径与 TABLE 同源)
#include "gui/task_features/services/OrderFlowService.hpp"
#include "gui/task_features/ui/TabFeature.hpp" // EffectiveCat2Snapshot (legend 标注)
#include "gui/util/AssetFilter.hpp"            // 筛选控件 (与 TABLE 共用)
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "implot.h"
#include "implot_internal.h"
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Features {

// ============================================================================
// Formatters
// ============================================================================

// 图1 X (交易秒下标) → HH:MM:SS (缩放自适应: 默认刻度 + 逐值换算)
static int L0TimeFormatter(double value, char *buff, int size, void * /*user_data*/) {
  const size_t tick_idx = std::min(static_cast<size_t>(std::max(0.0, value)), OrderFlowConst::L0_CAPACITY);
  const ClockTime ct = L0_to_Clock(tick_idx); // 15300 → 15:00:00 (右边界)
  return std::snprintf(buff, size, "%02d:%02d:%02d", ct.hour, ct.minute, ct.second);
}

// 图2 X (day_idx * L1_CAPACITY + minute) → YY/MM/DD HH:MM (user_data = Kline, gen 配对期间 dates 稳定)
static int L1TimeFormatter(double value, char *buff, int size, void *user_data) {
  const auto &k = *static_cast<const OrderFlow::Kline *>(user_data);
  if (k.dates.empty() || value < 0)
    return std::snprintf(buff, size, " ");
  const size_t d = std::min(k.day_idx_from_x(value), k.dates.size() - 1);
  const size_t m = std::min(static_cast<size_t>(value) % OrderFlowConst::L1_CAPACITY, TRADE_MINUTES_PER_DAY);
  const ClockTime ct = L1_to_Clock(m); // 255 → 15:00 (日右边界)
  const std::string &date = k.dates[d];
  if (date.size() != 8)
    return std::snprintf(buff, size, "%s %02d:%02d", date.c_str(), ct.hour, ct.minute);
  return std::snprintf(buff, size, "%s/%s/%s %02d:%02d", date.substr(2, 2).c_str(),
                       date.substr(4, 2).c_str(), date.substr(6, 2).c_str(), ct.hour, ct.minute);
}

// 图1/图2 Y (价格) → 定宽 (右对齐补空格, 等宽字体下宽度恒定): 刻度文字宽度决定
// 左侧留白, 换标的时价位数量级一变, plot 区域边界跟着挪 → X 像素映射看起来在抖
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

static void FormatDateFull(char *buf, size_t size, const std::string &date) {
  if (date.size() == 8) {
    std::snprintf(buf, size, "%s-%s-%s",
                  date.substr(0, 4).c_str(), date.substr(4, 2).c_str(), date.substr(6, 2).c_str());
  } else {
    std::snprintf(buf, size, "%s", date.c_str());
  }
}

static void FormatDateShort(char *buf, size_t size, const std::string &date) {
  if (date.size() == 8) {
    std::snprintf(buf, size, "%s/%s/%s",
                  date.substr(2, 2).c_str(), date.substr(4, 2).c_str(), date.substr(6, 2).c_str());
  } else {
    std::snprintf(buf, size, "%s", date.c_str());
  }
}

// 选中特征列 (当前层): 多选集合, 截断 MAX_FEATURES
static std::vector<int> CollectFeats(const Feature::Selection &sel, int level) {
  std::vector<int> v;
  if (sel.selected_level != level)
    return v;
  for (int f : sel.selected_features) {
    if (v.size() >= OrderFlowConst::MAX_FEATURES)
      break;
    v.push_back(f);
  }
  return v;
}

// legend = "中文名 (cat2)"; cat2 = 该层有效 Cat2 快照 (探测 or 人工覆盖, 无预览 = "?")
static const char *FeatName(const Feature &feature, const std::vector<const char *> &cat2,
                            int level, int idx, char *buf, size_t size) {
  const auto &metas = feature.metadata.features[level];
  const bool ok = idx >= 0 && static_cast<size_t>(idx) < metas.size();
  std::snprintf(buf, size, "%s (%s)", ok ? metas[idx].name_cn : "?",
                ok && static_cast<size_t>(idx) < cat2.size() ? cat2[static_cast<size_t>(idx)] : "?");
  return buf;
}

// ============================================================================
// Feature Overlay Axes (仅特征信号; 两图自身的轴行为不受影响)
//   price     → Y1: 与主图价格共轴 (NoFit, 不参与主图自动缩放)
//   rank      → Y2: 固定 [-0.1, 1.1] (值域 [0,1] 两头留余量, 贴边线不压轴框)
//   ratio*/raw/? → Y3: 同类共轴, 范围 = 各线 min/max 并集 (自动缩放)
//   rank 与 ratio/raw 同时在场 → 右轴语义冲突, 两轴隐去刻度 (线照常画)
// ============================================================================

enum FeatAxisKind { FEAT_AXIS_PRICE = 0,
                    FEAT_AXIS_RANK,
                    FEAT_AXIS_SCALE };

static FeatAxisKind FeatAxisOf(const std::vector<const char *> &cat2, int idx) {
  const char *c = (idx >= 0 && static_cast<size_t>(idx) < cat2.size()) ? cat2[static_cast<size_t>(idx)] : "?";
  if (std::strcmp(c, "price") == 0)
    return FEAT_AXIS_PRICE;
  if (std::strcmp(c, "rank") == 0)
    return FEAT_AXIS_RANK;
  return FEAT_AXIS_SCALE; // ratio / ratio_pos / ratio_neg / raw / ?
}

static ImAxis FeatYAxis(FeatAxisKind kind) {
  return kind == FEAT_AXIS_PRICE ? ImAxis_Y1 : (kind == FEAT_AXIS_RANK ? ImAxis_Y2 : ImAxis_Y3);
}

// Setup 阶段: 按在场类别开右轴 (必须在任何绘制调用之前)
static void SetupFeatAxes(bool has_rank, bool has_scale, float scale_min, float scale_max) {
  const ImPlotAxisFlags flags = ImPlotAxisFlags_AuxDefault | ImPlotAxisFlags_Opposite |
                                ((has_rank && has_scale) ? ImPlotAxisFlags_NoDecorations : 0);
  if (has_rank) {
    ImPlot::SetupAxis(ImAxis_Y2, nullptr, flags);
    ImPlot::SetupAxisLimits(ImAxis_Y2, -0.1, 1.1, ImPlotCond_Always); // [0,1] 两头各留点余量
  }
  if (has_scale) {
    ImPlot::SetupAxis(ImAxis_Y3, nullptr, flags);
    if (scale_min <= scale_max)
      ImPlot::SetupAxisLimits(ImAxis_Y3, scale_min, scale_max, ImPlotCond_Always);
  }
}

// ============================================================================
// Candlestick Renderer
// ============================================================================

static void PlotCandlestick(const char *label_id, const double *xs, const double *opens,
                            const double *highs, const double *lows, const double *closes, int count) {
  if (count <= 0)
    return;

  ImDrawList *draw_list = ImPlot::GetPlotDrawList();
  constexpr double half_width = OrderFlowConst::CANDLESTICK_HALF_WIDTH;

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
    const double *lo = std::lower_bound(xs, xs + count, limits.X.Min - 1.0);
    const double *hi = std::upper_bound(xs, xs + count, limits.X.Max + 1.0);

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
//   逐档净额密堆横条 (像素空间, 100W cap; hover 高亮 + 数值) 与
//   买卖累计曲线 (X 轴, 万元) 解耦, 各自看各自细节;
//   竞价交叉簿三态: 纯买 / 纯卖 / 重合区 (过渡色带), 预撮合参考价横线
// ============================================================================

static void RenderDepthPanel(OrderFlow &of, const OrderFlow::Depth &dp, size_t plot_idx) {
  const OrderFlow::Depth::Snapshot snap = dp.query_depth(plot_idx);
  if (!snap.valid) {
    ImGui::TextDisabled("No valid data");
    return;
  }

  // 截面缓存: (槽 gen, 锚点秒) 变了才重建
  auto &prof = of.depth_profile;
  const bool rebuilt = !prof.matches(dp.gen, snap.tick_idx);
  if (rebuilt)
    prof.build(dp, snap.tick_idx);
  if (prof.price.empty()) {
    ImGui::TextDisabled("No book at anchor");
    return;
  }

  // 基准价 (Y 轴百分比 / 横线): 竞价交叉秒 = 预撮合参考价, 其余 = 中间价
  double mark = snap.ref_price > 0.0f ? static_cast<double>(snap.ref_price)
                                      : static_cast<double>(snap.mid_price);

  if (ImPlot::BeginPlot("##DepthProfile", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    const ImPlotCond cond = rebuilt ? ImPlotCond_Always : ImPlotCond_Once;
    // Y 与图1 视野每帧同步 (价格严格对齐, Lock 不可单独缩放); 刻度显示相对基准价百分比
    ImPlot::SetupAxes(nullptr, nullptr, 0, ImPlotAxisFlags_Opposite | ImPlotAxisFlags_Lock);
    ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, std::max(prof.cum_max * 1.05, 1.0), cond);
    const auto &ui = of.ui;
    if (ui.l0_y_min < ui.l0_y_max)
      ImPlot::SetupAxisLimits(ImAxis_Y1, ui.l0_y_min, ui.l0_y_max, ImPlotCond_Always);
    else
      ImPlot::SetupAxisLimits(ImAxis_Y1, dp.plot.y_min_with_margin, dp.plot.y_max_with_margin, cond);
    ImPlot::SetupAxisFormat(ImAxis_Y1, DepthPercentFormatter, &mark);

    ImPlot::PushPlotClipRect();
    ImDrawList *draw_list = ImPlot::GetPlotDrawList();
    const ImPlotRect limits = ImPlot::GetPlotLimits();

    // 重合区带 (竞价交叉: 买卖档在 [ask_low, bid_top] 混排) — 过渡色打底
    if (prof.ask_low > 0 && prof.bid_top > 0 && prof.ask_low < prof.bid_top) {
      const ImVec2 p0 = ImPlot::PlotToPixels(limits.X.Min, prof.bid_top);
      const ImVec2 p1 = ImPlot::PlotToPixels(limits.X.Max, prof.ask_low);
      draw_list->AddRectFilled(p0, p1, IM_COL32(230, 180, 60, 40));
    }

    // 逐档净额横条: 密堆矩形 (档高 = 1 tick, 相邻档无缝), 像素空间自左向右,
    // 100W cap 满宽 —— 与 X 轴 (累计曲线) 解耦, 缩放曲线不影响柱子
    const ImVec2 plot_pos = ImPlot::GetPlotPos();
    const float plot_w = ImPlot::GetPlotSize().x;
    constexpr double HALF_TICK = OrderFlowConst::TICK_SIZE * 0.5;

    // Hover: 鼠标价格落在哪个档 (价升序二分)
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
      FormatDateFull(date_buf, sizeof(date_buf), dp.date);
      FormatTimeHMS(time_buf, sizeof(time_buf), snap.time.hour, snap.time.minute, snap.time.second);
      const float lh = ImGui::GetTextLineHeight();
      const ImVec2 tp(plot_pos.x + 6.0f, plot_pos.y + 4.0f);
      std::snprintf(buf, sizeof(buf), "%s %s", date_buf, time_buf);
      draw_list->AddText(tp, IM_COL32(255, 255, 255, 210), buf);
      if (snap.ref_price > 0.0f) { // 竞价交叉秒: 预撮合三元组
        std::snprintf(buf, sizeof(buf), "预撮合 %.2f元", snap.ref_price);
        draw_list->AddText(ImVec2(tp.x, tp.y + lh), IM_COL32(255, 190, 50, 255), buf);
        std::snprintf(buf, sizeof(buf), "匹配%.0f万 失衡%+.0f万",
                      amount_to_wan(snap.matched_amount), amount_to_wan(snap.imbalance_amount));
        draw_list->AddText(ImVec2(tp.x, tp.y + 2.0f * lh), IM_COL32(255, 190, 50, 255), buf);
      } else {
        std::snprintf(buf, sizeof(buf), "中间价 %.2f元", snap.mid_price);
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
    if (!prof.bid_cum_x.empty()) {
      ImPlot::SetNextLineStyle(ImVec4(0.3f, 0.9f, 0.3f, 0.9f), 2.0f);
      ImPlot::PlotLine("BidΣ", prof.bid_cum_x.data(), prof.bid_cum_y.data(),
                       static_cast<int>(prof.bid_cum_x.size()));
    }
    if (!prof.ask_cum_x.empty()) {
      ImPlot::SetNextLineStyle(ImVec4(0.95f, 0.35f, 0.35f, 0.9f), 2.0f);
      ImPlot::PlotLine("AskΣ", prof.ask_cum_x.data(), prof.ask_cum_y.data(),
                       static_cast<int>(prof.ask_cum_x.size()));
    }

    // 参考价 / 中间价横线 (标注绝对价, 轴刻度已是百分比)
    ImPlot::SetNextLineStyle(snap.ref_price > 0.0f ? ImVec4(1.0f, 0.75f, 0.2f, 1.0f)
                                                   : ImVec4(1.0f, 1.0f, 1.0f, 0.7f),
                             snap.ref_price > 0.0f ? 2.0f : 1.0f);
    ImPlot::PlotInfLines("##mark", &mark, 1, ImPlotInfLinesFlags_Horizontal);
    ImPlot::Annotation(limits.X.Max, mark,
                       snap.ref_price > 0.0f ? ImVec4(1.0f, 0.75f, 0.2f, 1.0f) : ImVec4(1, 1, 1, 0.7f),
                       ImVec2(-5, -5), true, "%.2f", mark);

    ImPlot::EndPlot();
  }
}

// ============================================================================
// L0 Plot Renderer (图1: front Depth 槽)
// ============================================================================

static void RenderL0Plot(OrderFlow &of, const Feature &feature, const std::vector<const char *> &cat2) {
  auto &ui = of.ui;
  const OrderFlow::Depth &dp = of.depth_front_slot();

  if (dp.asset_idx == SIZE_MAX) {
    ImGui::TextDisabled("Waiting for depth replay...");
    return;
  }
  if (!dp.has_data && dp.n_feat == 0) {
    ImGui::TextDisabled("No orders data for this (day, asset)");
    return;
  }

  // 新槽 (gen 变了) → 重置视图 + 热力图阈值回到当日自动初值 (之后尊重用户拖动)
  const bool slot_changed = (ui.l0_last_gen != dp.gen);
  ui.l0_last_gen = dp.gen;
  if (slot_changed)
    ui.log_amount_threshold = dp.auto_log_threshold;

  if (ImPlot::BeginPlot("##L0Price", ImVec2(-1, -1))) {
    const ImPlotCond cond = slot_changed ? ImPlotCond_Always : ImPlotCond_Once;

    ImPlot::SetupAxes(nullptr, nullptr, 0, 0);
    // X 是固定域 (全天交易秒), 只在首帧设一次 → 换标的/换日不动视野 (双击才复位)
    ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, static_cast<double>(OrderFlowConst::L0_CAPACITY), ImPlotCond_Once);
    ImPlot::SetupAxisLimits(ImAxis_Y1, dp.plot.y_min_with_margin, dp.plot.y_max_with_margin, cond);
    ImPlot::SetupAxisFormat(ImAxis_X1, L0TimeFormatter);
    ImPlot::SetupAxisFormat(ImAxis_Y1, PriceFormatter); // 定宽: 换标的不挪 plot 左边界

    // 特征 overlay 的右轴 (按 Cat2 分流; price 类直接借主图 Y1, 不开轴)
    bool feat_rank = false, feat_scale = false;
    float scale_min = (std::numeric_limits<float>::max)();
    float scale_max = std::numeric_limits<float>::lowest();
    for (size_t i = 0; i < dp.n_feat && i < ui.depth_feats.size(); ++i) {
      if (dp.feat[i].x.empty())
        continue;
      const FeatAxisKind kind = FeatAxisOf(cat2, ui.depth_feats[i]);
      if (kind == FEAT_AXIS_RANK) {
        feat_rank = true;
      } else if (kind == FEAT_AXIS_SCALE) {
        feat_scale = true;
        scale_min = std::min(scale_min, dp.feat_y_min[i]);
        scale_max = std::max(scale_max, dp.feat_y_max[i]);
      }
    }
    SetupFeatAxes(feat_rank, feat_scale, scale_min, scale_max);

    // 当前 Y 视野快照 → 右侧深度面板每帧同步 (两图价格轴严格对齐)
    {
      const ImPlotRect lr = ImPlot::GetPlotLimits();
      ui.l0_y_min = lr.Y.Min;
      ui.l0_y_max = lr.Y.Max;
    }

    // ------------------------------------------------------------------
    // Heatmap (GUI 侧着色缓存: 槽 gen + 阈值变了才重建)
    // ------------------------------------------------------------------
    if (ui.show_heatmap && dp.merged.rect_count > 0) {
      if (!of.heatmap_colored.matches(dp.gen, ui.log_amount_threshold))
        of.heatmap_colored.build(dp, ui.log_amount_threshold);

      ImPlot::PushPlotClipRect();
      ImDrawList *draw_list = ImPlot::GetPlotDrawList();
      const ImPlotRect limits = ImPlot::GetPlotLimits();

      // Hover 检测 (仅悬停帧, O(N))
      int hovered_idx = -1;
      if (ImPlot::IsPlotHovered()) {
        const ImPlotPoint mouse_pos = ImPlot::GetPlotMousePos();
        for (size_t i = 0; i < of.heatmap_colored.rects.size(); ++i) {
          const auto &rect = of.heatmap_colored.rects[i];
          if (mouse_pos.x >= rect.x1 && mouse_pos.x <= rect.x2 &&
              mouse_pos.y >= rect.y2 && mouse_pos.y <= rect.y1) {
            hovered_idx = static_cast<int>(i);
            break;
          }
        }
      }

      // 渲染 (视野裁剪: 秒级矩形量比分钟频大 60×)
      for (size_t i = 0; i < of.heatmap_colored.rects.size(); ++i) {
        const auto &rect = of.heatmap_colored.rects[i];
        if (rect.x2 < limits.X.Min || rect.x1 > limits.X.Max ||
            rect.y1 < limits.Y.Min || rect.y2 > limits.Y.Max)
          continue;
        const ImVec2 p_min = ImPlot::PlotToPixels(rect.x1, rect.y1);
        const ImVec2 p_max = ImPlot::PlotToPixels(rect.x2, rect.y2);

        if (static_cast<int>(i) == hovered_idx) {
          const uint8_t r = (rect.color >> 0) & 0xFF;
          const uint8_t g = (rect.color >> 8) & 0xFF;
          const uint8_t b = (rect.color >> 16) & 0xFF;
          draw_list->AddRectFilled(p_min, p_max, IM_COL32(r, g, b, 255));
          draw_list->AddRect(p_min, p_max, IM_COL32(255, 255, 255, 255), 0.0f, 0, 2.0f);
        } else {
          draw_list->AddRectFilled(p_min, p_max, rect.color);
        }
      }
      ImPlot::PopPlotClipRect();

      if (hovered_idx >= 0 && static_cast<size_t>(hovered_idx) < of.heatmap_colored.metadata.size()) {
        const auto &meta = of.heatmap_colored.metadata[hovered_idx];
        const ClockTime t0 = L0_to_Clock(meta.tick_start);
        const ClockTime t1 = L0_to_Clock(meta.tick_end); // 开区间右界 → 边界时刻
        ImGui::SetTooltip("%.2f万元\n价格: %.2f\n%02d:%02d:%02d - %02d:%02d:%02d",
                          amount_to_wan(static_cast<float>(meta.amount_rmb)), meta.price,
                          t0.hour, t0.minute, t0.second, t1.hour, t1.minute, t1.second);
      }
    }

    // ------------------------------------------------------------------
    // 盘口线: best bid / ask + spread 填充 + mid
    // ------------------------------------------------------------------
    if (!dp.plot.x.empty()) {
      // 盘口线全部 NoFit: 双击 fit 只认下方手动 FitPoint (= 初始视野口径),
      // 竞价段预撮合 mid 摸涨跌停不会撑大 fit
      const int n = static_cast<int>(dp.plot.x.size());
      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.3f, 0.8f, 0.3f, 0.7f));
      ImPlot::PlotStairs("Best Bid", dp.plot.x.data(), dp.plot.best_bid.data(), n, ImPlotItemFlags_NoFit);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.8f, 0.3f, 0.3f, 0.7f));
      ImPlot::PlotStairs("Best Ask", dp.plot.x.data(), dp.plot.best_ask.data(), n, ImPlotItemFlags_NoFit);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(1.0f, 1.0f, 0.0f, 0.6f));
      ImPlot::PlotShaded("Spread", dp.plot.x.data(), dp.plot.best_bid.data(), dp.plot.best_ask.data(), n,
                         ImPlotItemFlags_NoFit);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(1.0f, 1.0f, 1.0f, 0.9f));
      ImPlot::PlotStairs("Mid Price", dp.plot.x.data(), dp.plot.mid_price.data(), n, ImPlotItemFlags_NoFit);
      ImPlot::PopStyleColor();

      // 双击复位 = 新标的初始渲染的口径 (X 全天 + Y 带 margin); 否则 ImPlot 默认
      // fit 会贴紧数据边缘, 与初始视图不一致且难操作. 必须在有 item 之后 (SetupLock 已发生)
      if (ImPlot::FitThisFrame()) {
        ImPlot::FitPointX(0.0);
        ImPlot::FitPointX(static_cast<double>(OrderFlowConst::L0_CAPACITY));
        ImPlot::FitPointY(dp.plot.y_min_with_margin);
        ImPlot::FitPointY(dp.plot.y_max_with_margin);
      }
    }

    // ------------------------------------------------------------------
    // 特征 overlay (多选; 轴按 Cat2 分流, legend = 中文名 (cat2), 颜色 ImPlot 自动分配)
    // ------------------------------------------------------------------
    for (size_t i = 0; i < dp.n_feat && i < ui.depth_feats.size(); ++i) {
      const auto &fl = dp.feat[i];
      if (fl.x.empty())
        continue;
      char label[128];
      const FeatAxisKind kind = FeatAxisOf(cat2, ui.depth_feats[i]);
      ImPlot::SetAxes(ImAxis_X1, FeatYAxis(kind));
      // 颜色按选中槽位取 (不用 ImPlot 的 item 序自动分配): 两图 item 序不同, 同一
      // 特征才能在图1 图2 同色
      ImPlot::SetNextLineStyle(ImPlot::GetColormapColor(static_cast<int>(i)));
      // price 类共用主图 Y1: NoFit 保证图1 自动/双击缩放口径不被特征撑大
      ImPlot::PlotStairs(FeatName(feature, cat2, dp.feat_level, ui.depth_feats[i], label, sizeof(label)),
                         fl.x.data(), fl.y.data(), static_cast<int>(fl.x.size()),
                         kind == FEAT_AXIS_PRICE ? ImPlotItemFlags_NoFit : 0);
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y1);
    }

    // ------------------------------------------------------------------
    // Anchor: 锚在秒级时间 (跨日/换资产稳定); DragLineX + 双击, 落点吸附有效秒
    // ------------------------------------------------------------------
    if (!dp.plot.x.empty()) {
      if (ui.l0_anchor_tick == SIZE_MAX)
        ui.l0_anchor_tick = dp.ticks.front().tick_idx; // 首次: 默认首个有效秒

      double anchor_x = static_cast<double>(ui.l0_anchor_tick);
      const bool drag_changed = ImPlot::DragLineX(0, &anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f);
      const bool drag_active = ImGui::IsItemActive();
      if (drag_changed) // 拖动中跟手 (原样), 释放时吸附有效秒
        ui.l0_anchor_tick = drag_active
                                ? static_cast<size_t>(std::clamp(anchor_x, 0.0, static_cast<double>(OrderFlowConst::L0_CAPACITY - 1)))
                                : dp.ticks[dp.snap_to_valid_plot_idx(anchor_x)].tick_idx;

      if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0))
        ui.l0_anchor_tick = dp.ticks[dp.snap_to_valid_plot_idx(ImPlot::GetPlotMousePos().x)].tick_idx;

      // 当日该秒无数据 → 就近吸附取值 (锚点本身不动)
      const size_t plot_idx = dp.snap_to_valid_plot_idx(static_cast<double>(ui.l0_anchor_tick));
      if (plot_idx < dp.plot.x.size()) {
        const auto depth = dp.query_depth(plot_idx);
        if (depth.valid) {
          char time_buf[16];
          FormatTimeHMS(time_buf, sizeof(time_buf), depth.time.hour, depth.time.minute, depth.time.second);
          ImPlot::Annotation(dp.plot.x[plot_idx], dp.plot.mid_price[plot_idx],
                             ImVec4(1, 0.5f, 0, 1), ImVec2(5, -15), false, "%s", time_buf);
        }
      }
    }

    ImPlot::EndPlot();
  }
}

// ============================================================================
// L1 Plot Renderer (图2: Kline 已发布前缀)
// ============================================================================

static void RenderL1Plot(OrderFlow &of, const Feature &feature, const std::vector<const char *> &cat2, float height) {
  auto &ui = of.ui;
  auto &k = of.kline;

  uint32_t pub_gen;
  size_t pub_days = 0, pub_points = 0;
  OrderFlow::Kline::unpack(k.pub.load(std::memory_order_acquire), pub_gen, pub_days, pub_points);
  const bool ready = (pub_gen == (ui.kline_gen & 0xFFFF)); // 发布代追上请求代 = 数组前缀可读

  // 换代在途 (拖动标的): 照画空图, 轴与视野原样留着 —— 撤掉整张图会闪.
  // 只有 dates 正在重扫 (rescan / 首帧) 时连 X 域和刻度都读不出来, 才出提示
  if (!ready && (ui.kline_rescan_pending || ui.l1_x_max <= 0.0)) {
    ImGui::TextDisabled("Preparing K-line stream...");
    return;
  }
  if (ready) {
    if (k.dates.empty()) {
      ImGui::TextDisabled("No feature dates found");
      return;
    }
    ui.l1_x_max = static_cast<double>(k.dates.size() * OrderFlowConst::L1_CAPACITY);
  } else {
    pub_days = pub_points = 0; // 旧代计数不可用于本帧 (数组正在重建)
  }

  if (ImPlot::BeginPlot("##KLine", ImVec2(-1, height))) {
    ImPlot::SetupAxes(nullptr, nullptr, 0, 0);
    // X: 固定域 (0 ~ 全部特征日), 只在域本身变了 (重扫日期) 才复位 → 换标的 / 流式追加都不动视野
    const bool x_domain_changed = (ui.l1_x_applied != ui.l1_x_max);
    ui.l1_x_applied = ui.l1_x_max;
    ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, std::max(ui.l1_x_max, 1.0),
                            x_domain_changed ? ImPlotCond_Always : ImPlotCond_Once);
    ImPlot::SetupAxisFormat(ImAxis_X1, L1TimeFormatter, &k); // YY/MM/DD HH:MM (非重扫期 dates 稳定)
    ImPlot::SetupAxisFormat(ImAxis_Y1, PriceFormatter);      // 定宽: 换标的不挪 plot 左边界
    // Y1: 发布范围变化 (换代 / 流式追加) 时跟随; 稳定后不再打扰
    if (pub_points > 0 && ui.l1_last_pub_days != pub_days) {
      ImPlot::SetupAxisLimits(ImAxis_Y1, k.y_min.load(std::memory_order_relaxed),
                              k.y_max.load(std::memory_order_relaxed), ImPlotCond_Always);
    }
    if (ready)
      ui.l1_last_pub_days = pub_days;

    // 特征 overlay 的右轴 (按 Cat2 分流; price 类直接借主图 Y1, 不开轴)
    const size_t nf = ready ? std::min(k.n_feat, ui.kline_feats.size()) : 0;
    std::array<size_t, OrderFlowConst::MAX_FEATURES> feat_counts{};
    {
      bool feat_rank = false, feat_scale = false;
      float scale_min = (std::numeric_limits<float>::max)();
      float scale_max = std::numeric_limits<float>::lowest();
      for (size_t i = 0; i < nf; ++i) {
        feat_counts[i] = k.feat_n[i].load(std::memory_order_acquire);
        if (feat_counts[i] == 0)
          continue;
        const FeatAxisKind kind = FeatAxisOf(cat2, ui.kline_feats[i]);
        if (kind == FEAT_AXIS_RANK) {
          feat_rank = true;
        } else if (kind == FEAT_AXIS_SCALE) {
          feat_scale = true;
          scale_min = std::min(scale_min, k.feat_y_min[i].load(std::memory_order_relaxed));
          scale_max = std::max(scale_max, k.feat_y_max[i].load(std::memory_order_relaxed));
        }
      }
      SetupFeatAxes(feat_rank, feat_scale, scale_min, scale_max);
    }

    // ------------------------------------------------------------------
    // K线 (已发布前缀) + 特征 overlay (多选, legend = 中文名 (cat2))
    // ------------------------------------------------------------------
    if (pub_points > 0) {
      PlotCandlestick("OHLC", k.x.data(), k.open.data(), k.high.data(),
                      k.low.data(), k.close.data(), static_cast<int>(pub_points));
    }

    for (size_t i = 0; i < nf; ++i) {
      if (feat_counts[i] == 0)
        continue;
      char label[128];
      const FeatAxisKind kind = FeatAxisOf(cat2, ui.kline_feats[i]);
      ImPlot::SetAxes(ImAxis_X1, FeatYAxis(kind));
      // 颜色按选中槽位取, 与图1 同一口径 (选中层 = L1 时两图特征列表逐项相同 → 同色)
      ImPlot::SetNextLineStyle(ImPlot::GetColormapColor(static_cast<int>(i)));
      // price 类共用主图 Y1: NoFit 保证 K线自身的缩放口径不被特征撑大
      ImPlot::PlotStairs(FeatName(feature, cat2, 1, ui.kline_feats[i], label, sizeof(label)),
                         k.feat[i].x.data(), k.feat[i].y.data(), static_cast<int>(feat_counts[i]),
                         kind == FEAT_AXIS_PRICE ? ImPlotItemFlags_NoFit : 0);
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y1);
    }

    // ------------------------------------------------------------------
    // Anchor: 吸附日起点 → 驱动图1 (day, asset) 请求
    // ------------------------------------------------------------------
    {
      double anchor_x = ui.l1_anchor_x;
      const bool drag_changed = ImPlot::DragLineX(0, &anchor_x, ImVec4(1, 0.5f, 0, 1), 2.0f);
      const bool drag_active = ImGui::IsItemActive();

      if (drag_changed) {
        if (drag_active) { // 拖动中跟手, 不动 anchor_date (释放才吸附日起点 → 触发图1 重放)
          ui.l1_anchor_x = std::max(0.0, anchor_x);
        } else {
          ui.l1_anchor_x = k.snap_to_day_start(anchor_x);
          ui.l1_anchor_date = k.date_from_x(ui.l1_anchor_x);
        }
      }
      if (ImPlot::IsPlotHovered() && ImGui::IsMouseDoubleClicked(0)) {
        ui.l1_anchor_x = k.snap_to_day_start(ImPlot::GetPlotMousePos().x);
        ui.l1_anchor_date = k.date_from_x(ui.l1_anchor_x);
      }

      if (!ui.l1_anchor_date.empty() && pub_points > 0) {
        const auto it = std::lower_bound(k.x.begin(), k.x.begin() + static_cast<long>(pub_points), ui.l1_anchor_x);
        double anchor_y = 0;
        if (it != k.x.begin() + static_cast<long>(pub_points))
          anchor_y = k.close[static_cast<size_t>(it - k.x.begin())];

        char date_buf[16];
        FormatDateShort(date_buf, sizeof(date_buf), ui.l1_anchor_date);
        ImPlot::Annotation(ui.l1_anchor_x, anchor_y, ImVec4(1, 0.5f, 0, 1),
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
  auto &uni = of.universe;

  // ST/Listed/Board 与 DATABASE/TABLE 共用控件 (gui/util/AssetFilter.hpp);
  // risk_warn 有 3=退市整理期, 所以 ST 要全 4 档
  bool changed = GUI::Filter::CommonFilters<int>("of", true, uni.st_filter,
                                                 uni.listed_filter, uni.board_filter);

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
                                           uni.industry_filter);

  if (changed)
    ++uni.filter_epoch;

  // 候选数 / 锚点日
  const OrderFlow::Universe::Slot &slot = uni.front_slot();
  ImGui::SameLine();
  if (slot.gen == uni.gen && !slot.meta.empty()) {
    ImGui::TextColored(ImVec4(0.3f, 0.8f, 0.3f, 1.0f), "%zu/%zu",
                       uni.candidates.size(), data.asset.items.size());
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("%s 通过筛选的标的数 / 总数\n"
                        "状态取自当日 L1 落盘列 (ST / 上市 / 行业);\n"
                        "当日无有效分钟的标的不列入 (无盘口可看)",
                        slot.date.c_str());
  } else {
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.0f, 1.0f), "loading...");
  }
}

// 标的选择 = 候选序位拖动条 (无下拉菜单): 条面覆盖显示 "代码-市场-名称[ST] 序位/总数",
// 与筛选栏同行右侧; 滚轮在条上 ±1 档微调 (拖动条一像素可能跨多个标的)
static void RenderAssetSlider(SharedData &data, OrderFlow &of) {
  auto &ui = of.ui;
  auto &uni = of.universe;
  const size_t asset_idx = static_cast<size_t>(ui.selected_asset_idx);

  ImGui::SameLine();
  const int n = static_cast<int>(uni.candidates.size());
  if (n == 0) {
    ImGui::TextDisabled("无符合筛选的标的");
    return;
  }

  // 当前选中是否在候选内 (切日期/改筛选后可能落选; 保留选中, 仅标注)
  const auto it = std::find(uni.candidates.begin(), uni.candidates.end(), asset_idx);
  const bool in_candidates = (it != uni.candidates.end());
  int pos = in_candidates ? static_cast<int>(it - uni.candidates.begin()) : 0;

  // candidates 可能建于上一代槽 (新代在途), ST 标记按当前 front 槽尽力显示
  const auto &meta_now = uni.front_slot().meta;
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
    ImGui::SetTooltip("拖动切标的 (候选序: 市场 → 代码), 滚轮 ±1");
  }
  if (changed)
    ui.selected_asset_idx = static_cast<int>(uni.candidates[static_cast<size_t>(pos)]);
}

// ============================================================================
// Status Bar
// ============================================================================

static void RenderStatusBar(const OrderFlow &of) {
  const auto &ui = of.ui;
  const auto &k = of.kline;

  // Kline 流式进度
  uint32_t pub_gen;
  size_t pub_days, pub_points;
  OrderFlow::Kline::unpack(k.pub.load(std::memory_order_acquire), pub_gen, pub_days, pub_points);

  ImGui::SameLine();
  if (pub_gen == (ui.kline_gen & 0xFFFF) && !k.dates.empty()) {
    const bool streaming = pub_days < k.dates.size();
    ImGui::TextColored(streaming ? ImVec4(1.0f, 0.8f, 0.0f, 1.0f) : ImVec4(0.3f, 0.8f, 0.3f, 1.0f),
                       "[L1: %zu/%zu days, %zu pts]", pub_days, k.dates.size(), pub_points);
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("K线流式加载 (从前往后逐日): 已发布日数 / 总日数, 有效分钟数");
  } else {
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.0f, 1.0f), "[L1: rebuilding]");
  }

  ImGui::SameLine();
  if (!ui.l1_anchor_date.empty()) {
    char date_buf[16];
    FormatDateShort(date_buf, sizeof(date_buf), ui.l1_anchor_date);
    ImGui::TextColored(ImVec4(1.0f, 0.5f, 0.0f, 1.0f), "Anchor: %s", date_buf);
  }

  // Depth 重放状态
  ImGui::SameLine();
  const OrderFlow::Depth &dp = of.depth_front_slot();
  if (dp.gen != ui.depth_gen) {
    ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.0f, 1.0f), "[L0: replaying...]");
  } else if (dp.has_data) {
    ImGui::TextColored(ImVec4(0.3f, 0.6f, 0.9f, 1.0f), "[L0: %zu snaps / %zu rects / %zu orders]",
                       dp.ticks.size(), dp.merged.rect_count, dp.order_count);
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("秒级盘口快照数 (LOB 逐笔重放) / 热力图合并矩形数 / 当日逐笔条数");
  } else if (dp.asset_idx != SIZE_MAX) {
    ImGui::TextColored(ImVec4(0.6f, 0.6f, 0.6f, 1.0f), "[L0: no data]");
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
      ImGui::Text("Log10(金额) 下限阈值 (换日/换标的自动取当日初值)");
      ImGui::Separator();
      ImGui::Text("3.0 = 1千元 (显示所有 >= 1千的档位)");
      ImGui::Text("4.0 = 1万元");
      ImGui::Text("5.0 = 10万元");
      ImGui::Text("6.0 = 100万元");
      ImGui::Text("7.0 = 1000万元 (仅显示大额档位)");
      ImGui::Separator();
      ImGui::TextWrapped("范围: [阈值, 1000万] 映射到 [透明, 完全实色]");
      ImGui::TextWrapped("自动初值: 按当日全簿 (档·秒) 加权金额分位取, "
                         "使着色面积占有量面积恒定 → 各日色块浓度观感一致");
      ImGui::EndTooltip();
    }
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
  if (of.depth_pending.load(std::memory_order_acquire)) {
    of.depth_front.store(1 - of.depth_front.load(std::memory_order_relaxed), std::memory_order_release);
    of.depth_pending.store(false, std::memory_order_release);
  }
  if (of.universe.pending.load(std::memory_order_acquire)) {
    of.universe.front.store(1 - of.universe.front.load(std::memory_order_relaxed), std::memory_order_release);
    of.universe.pending.store(false, std::memory_order_release);
  }

  // ==========================================================================
  // 期望态检测 → 请求 (gen++ 即"停止消费旧代", 发布追上后自动恢复渲染)
  // ==========================================================================
  const bool rescan = of.needs_rescan.exchange(false, std::memory_order_relaxed);
  const auto &sel = data.feature.selection;
  const size_t asset_idx = static_cast<size_t>(ui.selected_asset_idx);

  std::vector<int> l1_feats = CollectFeats(sel, 1);
  if (rescan || ui.kline_asset != asset_idx || ui.kline_feats != l1_feats) {
    ui.kline_asset = asset_idx;
    ui.kline_feats = l1_feats;
    ++ui.kline_gen;
    ui.l1_last_pub_days = SIZE_MAX;                         // 换代: 重置轴管理
    ui.kline_rescan_pending = rescan || ui.l1_x_max <= 0.0; // dates 会被重扫 (或首次建) → 期间不可读
    service->RequestKline(ui.kline_gen, asset_idx, std::move(l1_feats), rescan);
  }

  // Kline 发布状态 (anchor 默认值需要 dates)
  uint32_t pub_gen;
  size_t pub_days, pub_points;
  OrderFlow::Kline::unpack(of.kline.pub.load(std::memory_order_acquire), pub_gen, pub_days, pub_points);
  const bool kline_ready = (pub_gen == (ui.kline_gen & 0xFFFF));
  if (kline_ready)
    ui.kline_rescan_pending = false; // dates 已随新代落定

  if (ui.l1_anchor_date.empty() && kline_ready && !of.kline.dates.empty()) {
    ui.l1_anchor_x = 0;
    ui.l1_anchor_date = of.kline.dates.front();
  }

  // 图1 特征 = 当前选中层的指标集 (L0/L1 皆可): 选 L1 时与图2 同源, 只取锚点日的日内段
  std::vector<int> depth_feats = CollectFeats(sel, sel.selected_level);
  if (!ui.l1_anchor_date.empty() &&
      (rescan || ui.depth_date != ui.l1_anchor_date || ui.depth_asset != asset_idx ||
       ui.depth_feats != depth_feats || ui.depth_feat_level != sel.selected_level)) {
    ui.depth_date = ui.l1_anchor_date;
    ui.depth_asset = asset_idx;
    ui.depth_feats = depth_feats;
    ui.depth_feat_level = sel.selected_level;
    ++ui.depth_gen;
    service->RequestDepth(ui.depth_gen, ui.l1_anchor_date, asset_idx, sel.selected_level, std::move(depth_feats));
  }

  // ==========================================================================
  // Universe: 锚点日的资产筛选状态 (先定日期, 再 apply filter)
  // ==========================================================================
  auto &uni = of.universe;

  // 静态显示序 (市场 → 代码) + 板块表: 建一次
  static std::vector<int> asset_boards;
  if (uni.display_order.size() != num_assets) {
    std::vector<std::pair<std::string, std::string>> exch_code(num_assets);
    asset_boards.resize(num_assets);
    for (size_t i = 0; i < num_assets; ++i) {
      exch_code[i] = {data.asset.items[i].exchange, data.asset.items[i].asset_code};
      asset_boards[i] = static_cast<int>(GUI::Database::GetBoardType(data.asset.items[i].asset_code));
    }
    uni.build_display_order(exch_code);
    uni.cached_gen = UINT32_MAX; // 强制重建候选
  }

  if (!ui.l1_anchor_date.empty() && (rescan || uni.req_date != ui.l1_anchor_date)) {
    uni.req_date = ui.l1_anchor_date;
    ++uni.gen;
    service->RequestUniverse(uni.gen, ui.l1_anchor_date);
  }

  // 候选重建 (槽换代 或 筛选条件变)
  {
    const OrderFlow::Universe::Slot &slot = uni.front_slot();
    if (slot.gen == uni.gen && slot.meta.size() == num_assets && !uni.matches(slot.gen))
      uni.rebuild_candidates(slot, asset_boards);
  }

  // 初始标的: 候选里代码编号最小的 (默认筛选已排除退市/未上市; 仅首次自动选)
  if (!ui.asset_initialized && !uni.candidates.empty()) {
    size_t best = uni.candidates.front();
    for (size_t i : uni.candidates) {
      if (data.asset.items[i].asset_code < data.asset.items[best].asset_code)
        best = i;
    }
    ui.selected_asset_idx = static_cast<int>(best);
    ui.asset_initialized = true;
  }

  // Cat2 有效值快照 (与 FEATURE 表同口径): 两图 legend 括号标注.
  // 图1 用 front 槽实际渲染的特征层 (换层在途时与旧槽数据配对)
  static std::vector<const char *> s_cat2_depth, s_cat2_l1;
  EffectiveCat2Snapshot(data, 1, s_cat2_l1);
  EffectiveCat2Snapshot(data, static_cast<size_t>(of.depth_front_slot().feat_level), s_cat2_depth);

  // ==========================================================================
  // LAYOUT
  // ==========================================================================
  const float content_height = ImGui::GetContentRegionAvail().y;
  const float content_width = ImGui::GetContentRegionAvail().x;
  const float top_view_height = content_height * OrderFlowConst::TOP_VIEW_RATIO;
  const float bottom_view_height = content_height * (1.0f - OrderFlowConst::TOP_VIEW_RATIO) - 5.0f;
  const float chart_width = content_width - OrderFlowConst::DEPTH_PANEL_WIDTH - 10.0f;

  // ==========================================================================
  // TOP: L0 秒级盘口 + 深度面板
  // ==========================================================================
  ImGui::BeginChild("TopSection", ImVec2(0, top_view_height), false);
  ImGui::BeginChild("L0Chart", ImVec2(chart_width, -1), false);
  RenderL0Plot(of, data.feature, s_cat2_depth);
  ImGui::EndChild();

  ImGui::SameLine();
  // 无边框: 与 L0Chart 同 padding, 两图 plot 区域上下像素级对齐
  ImGui::BeginChild("DepthPanel", ImVec2(OrderFlowConst::DEPTH_PANEL_WIDTH, -1), false);
  {
    const OrderFlow::Depth &dp = of.depth_front_slot();
    if (dp.has_data && ui.l0_anchor_tick != SIZE_MAX) {
      const size_t plot_idx = dp.snap_to_valid_plot_idx(static_cast<double>(ui.l0_anchor_tick));
      RenderDepthPanel(of, dp, plot_idx);
    } else {
      ImGui::TextDisabled("No L0 data");
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
  RenderStatusBar(of);

  const float kline_height = ImGui::GetContentRegionAvail().y;
  RenderL1Plot(of, data.feature, s_cat2_l1, kline_height);

  ImGui::EndChild(); // BottomSection
}

} // namespace GUI::Features
