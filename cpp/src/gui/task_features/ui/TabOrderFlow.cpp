// TabOrderFlow Implementation - OrderFlow Visualization
// 渲染面 (数据面/线程模型见 shared/OrderFlow.hpp):
//   1. 帧首 ack: depth 背槽发布 → 翻 front
//   2. 期望态检测: asset / anchor date / 选中特征 变了 → gen++ → Request*
//   3. 渲染: Kline 画已发布前缀 (gen 配对), Depth 画 front 槽 (新代在途时旧槽照画 + Loading 提示)
#include "gui/task_features/ui/TabOrderFlow.hpp"
#include "features/Method/Fundamental.hpp"
#include "gui/task_database/models/SharedTypes.hpp" // BoardType / GetBoardType (板块口径与 TABLE 同源)
#include "gui/task_features/services/OrderFlowService.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "implot.h"
#include "implot_internal.h"
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <limits>
#include <set>
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

// 选中特征列 (当前层): primary + secondary, 截断 MAX_FEATURES
static std::vector<int> CollectFeats(const Feature::Selection &sel, int level) {
  std::vector<int> v;
  if (sel.selected_level != level)
    return v;
  if (sel.primary_feature_idx >= 0)
    v.push_back(sel.primary_feature_idx);
  for (int f : sel.secondary_features) {
    if (v.size() >= OrderFlowConst::MAX_FEATURES)
      break;
    if (f != sel.primary_feature_idx)
      v.push_back(f);
  }
  return v;
}

static const char *FeatName(const Feature &feature, int level, int idx) {
  const auto &metas = feature.metadata.features[level];
  return (idx >= 0 && static_cast<size_t>(idx) < metas.size()) ? metas[idx].name_cn : "?";
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
// Depth Panel Renderer
// ============================================================================

static void RenderDepthPanel(const OrderFlow::Depth::Snapshot &depth, const std::string &date, float panel_width) {
  if (!depth.valid) {
    ImGui::TextDisabled("No valid data");
    return;
  }

  char date_buf[16], time_buf[16];
  FormatDateFull(date_buf, sizeof(date_buf), date);
  FormatTimeHMS(time_buf, sizeof(time_buf), depth.time.hour, depth.time.minute, depth.time.second);

  ImGui::PushFont(ImGui::GetIO().Fonts->Fonts[0]);
  ImGui::SetWindowFontScale(0.75f);
  ImGui::PushStyleVar(ImGuiStyleVar_ItemSpacing, ImVec2(0, 0));
  ImGui::PushStyleVar(ImGuiStyleVar_FramePadding, ImVec2(1, 0));

  ImGui::Text("%s %s", date_buf, time_buf);
  ImGui::Separator();

  const float bar_max_width = panel_width - 100.0f;

  // NOTE: volume is SIGNED (bid > 0, ask < 0); NaN 档位 (哨兵) 直接跳过
  auto render_level = [&](float price, float volume, bool is_bid) {
    if (price != price) { // NaN: 无此档
      ImGui::TextDisabled("      --");
      return;
    }
    const float amount = volume_to_amount(volume, price); // Preserves sign
    const float abs_amount = std::abs(amount);
    const float ratio = std::min(1.0f, abs_amount / OrderFlowConst::DEPTH_BAR_MAX_AMOUNT);
    const float amount_in_wan = amount_to_wan(amount);

    ImVec4 bar_color;
    const bool expected_sign = is_bid ? (amount > 0) : (amount < 0);
    if (expected_sign) {
      const float intensity = std::min(1.0f, abs_amount / OrderFlowConst::DEPTH_BAR_MAX_AMOUNT);
      if (is_bid) {
        bar_color = ImVec4(0.3f * (1.0f - intensity * 0.5f), 0.8f, 0.3f * (1.0f - intensity * 0.5f), 0.8f);
      } else {
        bar_color = ImVec4(0.8f, 0.3f * (1.0f - intensity * 0.5f), 0.3f * (1.0f - intensity * 0.5f), 0.8f);
      }
    } else {
      bar_color = ImVec4(0.9f, 0.9f, 0.3f, 0.8f); // 符号异常: 黄色告警
    }

    const float bar_height = 5.0f;
    const float text_height = ImGui::GetTextLineHeight();
    const float y_offset = (text_height - bar_height) * 0.5f;

    const float cursor_y = ImGui::GetCursorPosY();
    ImGui::SetCursorPosY(cursor_y + y_offset);

    ImGui::PushStyleColor(ImGuiCol_PlotHistogram, bar_color);
    ImGui::ProgressBar(ratio, ImVec2(bar_max_width, bar_height), "");
    ImGui::PopStyleColor();

    ImGui::SameLine();
    ImGui::SetCursorPosY(cursor_y);
    ImGui::Text("%6.2f元 %+7.2f万", price, amount_in_wan);
  };

  // Ask side (red) - 10 levels, from top (ask10) to bottom (ask1)
  for (int i = 9; i >= 0; --i) {
    render_level((*depth.ask_price)[i], (*depth.ask_volume)[i], false);
  }

  ImGui::TextColored(ImVec4(1.0f, 1.0f, 0.0f, 1.0f), "%.2f元", depth.mid_price);

  // Bid side (green) - 10 levels, from top (bid1) to bottom (bid10)
  for (int i = 0; i < 10; ++i) {
    render_level((*depth.bid_price)[i], (*depth.bid_volume)[i], true);
  }

  ImGui::SetWindowFontScale(1.0f);
  ImGui::PopStyleVar(2);
  ImGui::PopFont();
}

// ============================================================================
// L0 Plot Renderer (图1: front Depth 槽)
// ============================================================================

static void RenderL0Plot(OrderFlow &of, const Feature &feature) {
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

  // 新槽 (gen 变了) → 重置视图
  const bool slot_changed = (ui.l0_last_gen != dp.gen);
  ui.l0_last_gen = dp.gen;

  if (ImPlot::BeginPlot("##L0Price", ImVec2(-1, -1))) {
    const ImPlotCond cond = slot_changed ? ImPlotCond_Always : ImPlotCond_Once;

    ImPlot::SetupAxes(nullptr, nullptr, 0, 0);
    ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, static_cast<double>(OrderFlowConst::L0_CAPACITY), cond);
    ImPlot::SetupAxisLimits(ImAxis_Y1, dp.plot.y_min_with_margin, dp.plot.y_max_with_margin, cond);
    ImPlot::SetupAxisFormat(ImAxis_X1, L0TimeFormatter);

    // Y2: 特征 overlay (全部特征共轴, 范围 = 并集)
    if (dp.n_feat > 0) {
      ImPlot::SetupAxis(ImAxis_Y2, nullptr, ImPlotAxisFlags_AuxDefault | ImPlotAxisFlags_Opposite);
      float y2_min = (std::numeric_limits<float>::max)();
      float y2_max = std::numeric_limits<float>::lowest();
      for (size_t i = 0; i < dp.n_feat; ++i) {
        if (dp.feat[i].x.empty())
          continue;
        y2_min = std::min(y2_min, dp.feat_y_min[i]);
        y2_max = std::max(y2_max, dp.feat_y_max[i]);
      }
      if (y2_min <= y2_max)
        ImPlot::SetupAxisLimits(ImAxis_Y2, y2_min, y2_max, ImPlotCond_Always);
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
      const int n = static_cast<int>(dp.plot.x.size());
      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.3f, 0.8f, 0.3f, 0.7f));
      ImPlot::PlotStairs("Best Bid", dp.plot.x.data(), dp.plot.best_bid.data(), n);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(0.8f, 0.3f, 0.3f, 0.7f));
      ImPlot::PlotStairs("Best Ask", dp.plot.x.data(), dp.plot.best_ask.data(), n);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Fill, ImVec4(1.0f, 1.0f, 0.0f, 0.6f));
      ImPlot::PlotShaded("Spread", dp.plot.x.data(), dp.plot.best_bid.data(), dp.plot.best_ask.data(), n);
      ImPlot::PopStyleColor();

      ImPlot::PushStyleColor(ImPlotCol_Line, ImVec4(1.0f, 1.0f, 1.0f, 0.9f));
      ImPlot::PlotStairs("Mid Price", dp.plot.x.data(), dp.plot.mid_price.data(), n);
      ImPlot::PopStyleColor();
    }

    // ------------------------------------------------------------------
    // 特征 overlay (Y2, 多选; legend = 中文名, 颜色 ImPlot 自动分配)
    // ------------------------------------------------------------------
    for (size_t i = 0; i < dp.n_feat && i < ui.depth_feats.size(); ++i) {
      const auto &fl = dp.feat[i];
      if (fl.x.empty())
        continue;
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y2);
      ImPlot::PlotStairs(FeatName(feature, 0, ui.depth_feats[i]),
                         fl.x.data(), fl.y.data(), static_cast<int>(fl.x.size()));
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

static void RenderL1Plot(OrderFlow &of, const Feature &feature, float height) {
  auto &ui = of.ui;
  auto &k = of.kline;

  uint32_t pub_gen;
  size_t pub_days, pub_points;
  OrderFlow::Kline::unpack(k.pub.load(std::memory_order_acquire), pub_gen, pub_days, pub_points);

  if (pub_gen != (ui.kline_gen & 0xFFFF)) {
    ImGui::TextDisabled("Preparing K-line stream...");
    return;
  }
  if (k.dates.empty()) {
    ImGui::TextDisabled("No feature dates found");
    return;
  }

  const bool gen_changed = (ui.l1_last_pub_days == SIZE_MAX);

  if (ImPlot::BeginPlot("##KLine", ImVec2(-1, height))) {
    const double x_max = static_cast<double>(k.dates.size() * OrderFlowConst::L1_CAPACITY);

    ImPlot::SetupAxes(nullptr, nullptr, 0, 0);
    // X: 全区间固定 (dates 数已知), 只在换代时重置 → 流式追加不打扰用户视野
    ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, std::max(x_max, 1.0), gen_changed ? ImPlotCond_Always : ImPlotCond_Once);
    ImPlot::SetupAxisFormat(ImAxis_X1, L1TimeFormatter, &k); // YY/MM/DD HH:MM, 缩放自适应
    // Y1: 发布范围变化 (流式追加) 时跟随; 稳定后不再打扰
    if (pub_points > 0 && ui.l1_last_pub_days != pub_days) {
      ImPlot::SetupAxisLimits(ImAxis_Y1, k.y_min.load(std::memory_order_relaxed),
                              k.y_max.load(std::memory_order_relaxed), ImPlotCond_Always);
    }
    ui.l1_last_pub_days = pub_days;

    // Y2: 特征 overlay (共轴, 范围 = 并集)
    const size_t nf = std::min(k.n_feat, ui.kline_feats.size());
    std::array<size_t, OrderFlowConst::MAX_FEATURES> feat_counts{};
    bool any_feat = false;
    {
      float y2_min = (std::numeric_limits<float>::max)();
      float y2_max = std::numeric_limits<float>::lowest();
      for (size_t i = 0; i < nf; ++i) {
        feat_counts[i] = k.feat_n[i].load(std::memory_order_acquire);
        if (feat_counts[i] == 0)
          continue;
        any_feat = true;
        y2_min = std::min(y2_min, k.feat_y_min[i].load(std::memory_order_relaxed));
        y2_max = std::max(y2_max, k.feat_y_max[i].load(std::memory_order_relaxed));
      }
      if (any_feat) {
        ImPlot::SetupAxis(ImAxis_Y2, nullptr, ImPlotAxisFlags_AuxDefault | ImPlotAxisFlags_Opposite);
        if (y2_min <= y2_max)
          ImPlot::SetupAxisLimits(ImAxis_Y2, y2_min, y2_max, ImPlotCond_Always);
      }
    }

    // ------------------------------------------------------------------
    // K线 (已发布前缀) + 特征 overlay (多选, legend = 中文名)
    // ------------------------------------------------------------------
    if (pub_points > 0) {
      PlotCandlestick("OHLC", k.x.data(), k.open.data(), k.high.data(),
                      k.low.data(), k.close.data(), static_cast<int>(pub_points));
    }

    for (size_t i = 0; i < nf; ++i) {
      if (feat_counts[i] == 0)
        continue;
      ImPlot::SetAxes(ImAxis_X1, ImAxis_Y2);
      ImPlot::PlotStairs(FeatName(feature, 1, ui.kline_feats[i]),
                         k.feat[i].x.data(), k.feat[i].y.data(), static_cast<int>(feat_counts[i]));
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

// 多选下拉 (空集 = 不筛; 对仗 TabTable::RenderMultiSelectCombo)
static bool RenderFilterCombo(const char *label, float width,
                              const std::vector<std::pair<int, std::string>> &items,
                              std::set<int> &selected) {
  std::string preview;
  if (selected.empty()) {
    preview = "All";
  } else {
    for (const auto &[value, text] : items) {
      if (selected.count(value)) {
        if (!preview.empty())
          preview += ", ";
        preview += text;
      }
    }
    if (preview.size() > 24)
      preview = std::to_string(selected.size()) + " selected";
  }

  bool changed = false;
  ImGui::SetNextItemWidth(width);
  if (ImGui::BeginCombo(label, preview.c_str())) {
    for (const auto &[value, text] : items) {
      bool is_selected = selected.count(value) != 0;
      if (ImGui::Checkbox(text.c_str(), &is_selected)) {
        if (is_selected)
          selected.insert(value);
        else
          selected.erase(value);
        changed = true;
      }
    }
    ImGui::Separator();
    if (ImGui::SmallButton("All")) {
      selected.clear();
      changed = true;
    }
    ImGui::EndCombo();
  }
  return changed;
}

static void RenderAssetFilterBar(SharedData &data, OrderFlow &of) {
  auto &uni = of.universe;
  bool changed = false;

  static const std::vector<std::pair<int, std::string>> st_items = {
      {0, "正常"}, {1, "ST"}, {2, "*ST"}, {3, "退市整理"}};
  changed |= RenderFilterCombo("ST##ofSt", 100.0f, st_items, uni.st_filter);

  static const std::vector<std::pair<int, std::string>> listed_items = {{0, "在市"}, {1, "退市"}};
  ImGui::SameLine();
  changed |= RenderFilterCombo("Listed##ofListed", 100.0f, listed_items, uni.listed_filter);

  using GUI::Database::BoardType;
  static const std::vector<std::pair<int, std::string>> board_items = {
      {static_cast<int>(BoardType::Unknown), "Unknown"},
      {static_cast<int>(BoardType::SH_Main), "沪主板"},
      {static_cast<int>(BoardType::SZ_Main), "深主板"},
      {static_cast<int>(BoardType::STAR), "科创板"},
      {static_cast<int>(BoardType::ChiNext), "创业板"},
      {static_cast<int>(BoardType::BSE), "北交所"}};
  ImGui::SameLine();
  changed |= RenderFilterCombo("Board##ofBoard", 120.0f, board_items, uni.board_filter);

  // 行业: SW2021 一级 (0 = 未知), 表在 fund::SW2021_L1_NAMES
  static const std::vector<std::pair<int, std::string>> industry_items = [] {
    std::vector<std::pair<int, std::string>> v;
    for (size_t i = 0; i < fund::SW2021_L1_COUNT; ++i)
      v.emplace_back(static_cast<int>(i), std::string(fund::SW2021_L1_NAMES[i]));
    return v;
  }();
  ImGui::SameLine();
  changed |= RenderFilterCombo("Industry##ofInd", 140.0f, industry_items, uni.industry_filter);

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

static void RenderAssetSelector(SharedData &data, OrderFlow &of) {
  const size_t num_assets = data.asset.items.size();
  if (num_assets == 0)
    return;

  auto &ui = of.ui;
  auto &uni = of.universe;
  const size_t asset_idx = static_cast<size_t>(ui.selected_asset_idx);

  // 当前选中是否在候选内 (切日期/改筛选后可能落选; 保留选中, 仅标注)
  const bool cur_in_candidates =
      std::find(uni.candidates.begin(), uni.candidates.end(), asset_idx) != uni.candidates.end();

  ImGui::Text("Asset:");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(220);

  const auto &current_asset = data.asset.items[asset_idx];
  char preview_buf[256];
  std::snprintf(preview_buf, sizeof(preview_buf), "%s-%s-%s%s",
                current_asset.asset_code.c_str(), current_asset.exchange.c_str(),
                current_asset.asset_name.c_str(), cur_in_candidates ? "" : " (不符筛选)");

  if (!cur_in_candidates)
    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.0f, 0.8f, 0.0f, 1.0f));
  const bool combo_open = ImGui::BeginCombo("##asset", preview_buf);
  if (!cur_in_candidates)
    ImGui::PopStyleColor();

  if (combo_open) {
    if (uni.candidates.empty())
      ImGui::TextDisabled("无符合筛选的标的");

    // candidates 可能建于上一代槽 (新代在途), ST 标记按当前 front 槽尽力显示
    const auto &meta_now = uni.front_slot().meta;

    // candidates 已是 市场 → 代码 序
    ImGuiListClipper clipper;
    clipper.Begin(static_cast<int>(uni.candidates.size()));
    while (clipper.Step()) {
      for (int row = clipper.DisplayStart; row < clipper.DisplayEnd; ++row) {
        const size_t i = uni.candidates[static_cast<size_t>(row)];
        const auto &asset = data.asset.items[i];
        const uint8_t rw = i < meta_now.size() ? meta_now[i].risk_warn : 0;

        char label[256];
        std::snprintf(label, sizeof(label), "%s-%s-%s%s##a%zu",
                      asset.asset_code.c_str(), asset.exchange.c_str(),
                      asset.asset_name.c_str(),
                      rw == 2 ? " *ST" : (rw == 1 ? " ST" : (rw == 3 ? " 退整" : "")),
                      i);

        const bool is_selected = (asset_idx == i);
        if (ImGui::Selectable(label, is_selected))
          ui.selected_asset_idx = static_cast<int>(i);
        if (is_selected)
          ImGui::SetItemDefaultFocus();
      }
    }
    ImGui::EndCombo();
  }
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
    ImGui::SliderFloat("Threshold", &ui.log_amount_threshold, 3.0f, 7.0f, "%.1f");

    if (ImGui::IsItemHovered()) {
      ImGui::SetNextWindowSize(ImVec2(350, 0), ImGuiCond_Always);
      ImGui::BeginTooltip();
      ImGui::Text("Log10(金额) 下限阈值");
      ImGui::Separator();
      ImGui::Text("3.0 = 1千元 (显示所有 >= 1千的档位)");
      ImGui::Text("4.0 = 1万元");
      ImGui::Text("5.0 = 10万元");
      ImGui::Text("6.0 = 100万元");
      ImGui::Text("7.0 = 1000万元 (仅显示大额档位)");
      ImGui::Separator();
      ImGui::TextWrapped("范围: [阈值, 1000万] 映射到 [透明, 完全实色]");
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
    ui.l1_last_pub_days = SIZE_MAX; // 换代: 重置轴管理
    service->RequestKline(ui.kline_gen, asset_idx, std::move(l1_feats), rescan);
  }

  // Kline 发布状态 (anchor 默认值需要 dates)
  uint32_t pub_gen;
  size_t pub_days, pub_points;
  OrderFlow::Kline::unpack(of.kline.pub.load(std::memory_order_acquire), pub_gen, pub_days, pub_points);
  const bool kline_ready = (pub_gen == (ui.kline_gen & 0xFFFF));

  if (ui.l1_anchor_date.empty() && kline_ready && !of.kline.dates.empty()) {
    ui.l1_anchor_x = 0;
    ui.l1_anchor_date = of.kline.dates.front();
  }

  std::vector<int> l0_feats = CollectFeats(sel, 0);
  if (!ui.l1_anchor_date.empty() &&
      (rescan || ui.depth_date != ui.l1_anchor_date || ui.depth_asset != asset_idx || ui.depth_feats != l0_feats)) {
    ui.depth_date = ui.l1_anchor_date;
    ui.depth_asset = asset_idx;
    ui.depth_feats = l0_feats;
    ++ui.depth_gen;
    service->RequestDepth(ui.depth_gen, ui.l1_anchor_date, asset_idx, std::move(l0_feats));
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
  RenderL0Plot(of, data.feature);
  ImGui::EndChild();

  ImGui::SameLine();
  ImGui::BeginChild("DepthPanel", ImVec2(OrderFlowConst::DEPTH_PANEL_WIDTH, -1), true);
  {
    const OrderFlow::Depth &dp = of.depth_front_slot();
    if (dp.has_data && ui.l0_anchor_tick != SIZE_MAX) {
      const size_t plot_idx = dp.snap_to_valid_plot_idx(static_cast<double>(ui.l0_anchor_tick));
      RenderDepthPanel(dp.query_depth(plot_idx), dp.date, OrderFlowConst::DEPTH_PANEL_WIDTH);
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
  RenderAssetSelector(data, of);
  ImGui::SameLine();
  RenderHeatmapControls(of);
  RenderStatusBar(of);

  const float kline_height = ImGui::GetContentRegionAvail().y;
  RenderL1Plot(of, data.feature, kline_height);

  ImGui::EndChild(); // BottomSection
}

} // namespace GUI::Features
