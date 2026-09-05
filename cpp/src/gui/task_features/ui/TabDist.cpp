#include "gui/task_features/ui/TabDist.hpp"
#include "gui/task_features/services/DistService.hpp"
#include "shared/Asset.hpp"
#include "shared/Config.hpp"
#include "shared/Dist.hpp"
#include "shared/Feature.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "implot.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <map>

namespace GUI::Features {

// ============================================================================
// Helpers
// ============================================================================

static const char *StatusText(Dist::Status s) {
  switch (s) {
  case Dist::Status::Idle:
    return "Idle";
  case Dist::Status::Building:
    return "Building...";
  case Dist::Status::Done:
    return "Done";
  case Dist::Status::Cancelled:
    return "Cancelled";
  }
  return "?";
}

static ImVec4 StatusColor(Dist::Status s) {
  switch (s) {
  case Dist::Status::Idle:
    return ImVec4(0.5f, 0.5f, 0.5f, 1.0f); // 灰色
  case Dist::Status::Building:
    return ImVec4(0.2f, 0.7f, 1.0f, 1.0f); // 蓝色
  case Dist::Status::Done:
    return ImVec4(0.2f, 0.8f, 0.4f, 1.0f); // 绿色
  case Dist::Status::Cancelled:
    return ImVec4(0.9f, 0.6f, 0.2f, 1.0f); // 橙色
  }
  return ImVec4(1.0f, 1.0f, 1.0f, 1.0f);
}

// Calculate distance from point to line segment (normalized coords)
static double point_to_segment_dist_sq(double px, double py, double x1, double y1,
                                       double x2, double y2) {
  double dx = x2 - x1;
  double dy = y2 - y1;
  double len_sq = dx * dx + dy * dy;
  if (len_sq < 1e-10)
    return (px - x1) * (px - x1) + (py - y1) * (py - y1);

  double t = ((px - x1) * dx + (py - y1) * dy) / len_sq;
  t = std::max(0.0, std::min(1.0, t));
  double closest_x = x1 + t * dx;
  double closest_y = y1 + t * dy;
  return (px - closest_x) * (px - closest_x) + (py - closest_y) * (py - closest_y);
}

// Hover 命中阈值 (绘图区归一化坐标的距离平方)
constexpr double kHoverDistSq = 0.001;

// 折线到鼠标的最近段距离平方 (归一化坐标).
// PDF 的 x 网格等距 → x 方向超出命中半径的段不可能入选, 只扫鼠标 x 窗口内的段
// (资产截面 5000 条线 × 127 段, 逐段全扫每帧 ~64 万次, 裁剪后 ~2 段/线)
static double nearest_seg_dist_sq(const float *x, const float *y, size_t n,
                                  const ImPlotPoint &mouse, const ImPlotRect &limits) {
  if (n < 2)
    return 1e9;
  const double x_range = limits.X.Max - limits.X.Min;
  const double y_range = limits.Y.Max - limits.Y.Min;
  const double nmx = (mouse.x - limits.X.Min) / x_range;
  const double nmy = (mouse.y - limits.Y.Min) / y_range;

  size_t j0 = 0, j1 = n - 1; // 扫段 [j0, j1)
  const double dx = (x[n - 1] - x[0]) / static_cast<double>(n - 1);
  if (dx > 0) {
    const double wx = std::sqrt(kHoverDistSq) * x_range; // 命中半径换回 value 域
    const double lo = (mouse.x - wx - x[0]) / dx;
    const double hi = (mouse.x + wx - x[0]) / dx;
    if (hi < 0.0 || lo > static_cast<double>(n - 1))
      return 1e9;
    j0 = lo <= 0.0 ? 0 : static_cast<size_t>(lo);
    j1 = hi >= static_cast<double>(n - 1) ? n - 1 : static_cast<size_t>(hi) + 1;
  }

  double best = 1e9;
  for (size_t j = j0; j < j1; ++j) {
    const double nx1 = (x[j] - limits.X.Min) / x_range;
    const double ny1 = (y[j] - limits.Y.Min) / y_range;
    const double nx2 = (x[j + 1] - limits.X.Min) / x_range;
    const double ny2 = (y[j + 1] - limits.Y.Min) / y_range;
    best = std::min(best, point_to_segment_dist_sq(nmx, nmy, nx1, ny1, nx2, ny2));
  }
  return best;
}

// ============================================================================
// 行业色: 一个行业一个颜色 (资产表静态, 缓存一次; 基本面未就绪时下帧重试)
// ============================================================================

static void EnsureIndustryCache(DistUIState &ui, const Asset &asset, const AssetInfo &assetinfo) {
  if (ui.industry_idx.size() == asset.items.size() && !ui.industry_names.empty())
    return;
  ui.industry_idx.assign(asset.items.size(), -1);
  ui.industry_names.clear();
  std::map<std::string, int> name_to_idx;
  for (size_t a = 0; a < asset.items.size(); ++a) {
    const auto &item = asset.items[a];
    std::string ex = item.exchange;
    std::transform(ex.begin(), ex.end(), ex.begin(), ::tolower);
    const StockInfo *si = assetinfo.find_stock_info(ex + "." + item.asset_code);
    if (!si || si->ind_name.empty())
      continue;
    auto [it, inserted] = name_to_idx.try_emplace(si->ind_name, static_cast<int>(ui.industry_names.size()));
    if (inserted)
      ui.industry_names.push_back(si->ind_name);
    ui.industry_idx[a] = it->second;
  }
}

static ImVec4 IndustryColor(const DistUIState &ui, size_t asset_idx) {
  const int idx = asset_idx < ui.industry_idx.size() ? ui.industry_idx[asset_idx] : -1;
  if (idx < 0)
    return ImVec4(0.5f, 0.5f, 0.5f, 1.0f); // 未知行业: 灰
  const int n = static_cast<int>(ui.industry_names.size());
  const float t = n > 1 ? static_cast<float>(idx) / static_cast<float>(n - 1) : 0.5f;
  return ImPlot::SampleColormap(t, ImPlotColormap_Jet);
}

// ============================================================================
// Integrity Panel
// ============================================================================

// Color helpers for integrity display
static ImVec4 GetMinMaxColor(float val) {
  // Red if outside [-100, 100]
  if (val > 100.0f || val < -100.0f) {
    return ImVec4(1.0f, 0.3f, 0.3f, 1.0f); // 红色
  }
  return ImVec4(0.2f, 0.8f, 0.4f, 1.0f); // 绿色
}

static ImVec4 GetZeroPctColor(float pct) {
  // 20%以上红色, 10%以上黄色
  if (pct >= 10.0f) {
    return ImVec4(1.0f, 0.3f, 0.3f, 1.0f); // 红色
  } else if (pct >= 5.0f) {
    return ImVec4(1.0f, 0.9f, 0.3f, 1.0f); // 黄色
  }
  return ImVec4(0.2f, 0.8f, 0.4f, 1.0f); // 绿色
}

static ImVec4 GetNanInfPctColor(float pct) {
  // 1%以上红色, 非零黄色
  if (pct >= 1.0f) {
    return ImVec4(1.0f, 0.3f, 0.3f, 1.0f); // 红色
  } else if (pct > 0.0f) {
    return ImVec4(1.0f, 0.9f, 0.3f, 1.0f); // 黄色
  }
  return ImVec4(0.2f, 0.8f, 0.4f, 1.0f); // 绿色
}

static void RenderIntegrity(const Dist::Integrity &integrity) {
  float zero_pct = integrity.zero_pct();
  float nan_pct = integrity.nan_pct();
  float inf_pct = integrity.inf_pct();

  // Zero with color
  ImGui::Text("Zero: %zu (", integrity.n_zero);
  ImGui::SameLine(0, 0);
  ImGui::TextColored(GetZeroPctColor(zero_pct), "%.1f%%", zero_pct);
  ImGui::SameLine(0, 0);
  ImGui::Text(")");
  ImGui::SameLine();

  // NaN with color
  ImGui::Text("NaN: %zu (", integrity.n_nan);
  ImGui::SameLine(0, 0);
  ImGui::TextColored(GetNanInfPctColor(nan_pct), "%.1f%%", nan_pct);
  ImGui::SameLine(0, 0);
  ImGui::Text(")");
  ImGui::SameLine();

  // +Inf with color
  ImGui::Text("+Inf: %zu (", integrity.n_pos_inf);
  ImGui::SameLine(0, 0);
  ImGui::TextColored(GetNanInfPctColor(inf_pct), "%.1f%%", inf_pct);
  ImGui::SameLine(0, 0);
  ImGui::Text(")");
  ImGui::SameLine();

  // -Inf with color (same inf_pct as +Inf)
  ImGui::Text("-Inf: %zu (", integrity.n_neg_inf);
  ImGui::SameLine(0, 0);
  ImGui::TextColored(GetNanInfPctColor(inf_pct), "%.1f%%", inf_pct);
  ImGui::SameLine(0, 0);
  ImGui::Text(")");
  ImGui::SameLine();

  // Min/Max with color (无有效样本时 min/max 恒为 ±inf, 显示 --)
  if (integrity.n_valid == 0) {
    ImGui::Text("Min: -- Max: --");
    return;
  }
  ImGui::Text("Min: ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(GetMinMaxColor(integrity.val_min), "%.2f", integrity.val_min);
  ImGui::SameLine();
  ImGui::Text("Max: ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(GetMinMaxColor(integrity.val_max), "%.2f", integrity.val_max);
}

// ============================================================================
// Window Control Panel
// Row 1: Compute | Cancel | Status (n/m) | By selector
// Row 2: Month slider (always visible, from config date range)
// ============================================================================

// Format month key "YYYYMM" -> "YYYY/MM"
static std::string format_month(const std::string &m) {
  if (m.size() >= 6)
    return m.substr(0, 4) + "/" + m.substr(4, 2);
  return m;
}

// Get slider label: "YYYY/MM (n_samples)" or "YYYY/MM" if no data yet
static std::string get_month_label(const Dist &dist, int idx,
                                   const std::vector<std::string> &months) {
  if (idx < 0 || idx >= static_cast<int>(months.size()))
    return "";
  std::string label = format_month(months[idx]);
  // Add n_samples if data available (随资产流增长)
  if (idx < static_cast<int>(dist.months.size()) && dist.months[idx].kll.totalCount() > 0) {
    label += " (" + std::to_string(dist.months[idx].kll.totalCount()) + ")";
  }
  return label;
}

static void RenderWindowControl(DistService *service, SharedData &data,
                                DistUIState &ui) {
  auto &dist = data.dist;
  const Dist::Status status = dist.status.load(std::memory_order_acquire);

  // Row 1: Compute | Cancel | Status | By selector
  const bool is_l1 = (data.feature.selection.selected_level == 1);
  bool can_compute = status != Dist::Status::Building &&
                     data.feature.selection.primary_feature_idx >= 0 && is_l1;
  ImGui::BeginDisabled(!can_compute);
  if (ImGui::Button("Compute")) {
    service->RequestCompute(data);
  }
  ImGui::EndDisabled();
  if (!is_l1) {
    ImGui::SameLine();
    ImGui::TextDisabled("(仅 L1)");
  }

  ImGui::SameLine();
  if (ImGui::Button("Cancel")) {
    service->RequestCancel();
  }

  ImGui::SameLine();
  ImGui::Text("Status: ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(StatusColor(status), "%s", StatusText(status));
  ImGui::SameLine(0, 0);
  // 进度: 分批流式, 天是唯一流式维度 (每批扫全部资产, 全资产逐批收敛)
  ImGui::Text(" (天 %zu/%zu)", dist.days_loaded.load(), dist.days_total.load());

  // Row 2: Month slider (config 区间月份表, 缓存; 枚举逻辑与 DistService 共用)
  const std::string months_key = data.config.start_date + "|" + data.config.end_date;
  if (ui.months_key != months_key) {
    ui.months_key = months_key;
    ui.months = dist_enumerate_months(data.config.start_date, data.config.end_date);
  }
  if (!ui.months.empty()) {
    int n_months = static_cast<int>(ui.months.size());
    ui.focus_month_idx = std::clamp(ui.focus_month_idx, 0, n_months - 1);

    std::string label = get_month_label(dist, ui.focus_month_idx, ui.months);
    ImGui::SetNextItemWidth(-1);
    ImGui::SliderInt("##FocusMonth", &ui.focus_month_idx, 0, n_months - 1, label.c_str());
  }
}

// ============================================================================
// Asset Color (图4 顶部散点 + PDF 折线)
// 模式: 0=行业(Jet), 1=市值, 2=PE, 3=PB, 4=PS, 5=PCF, 6=股息率(Viridis)
// 连续值按 5/95 分位 winsorize 后线性映射到 colormap, 离群值饱和
// ============================================================================

// 取连续值字段的字符串 → float; 缺失/解析失败 → NaN
static float AssetColorValue(const StockInfo &si, int mode) {
  const std::string *field = nullptr;
  switch (mode) {
  case 1:
    field = &si.mcap;
    break;
  case 2:
    field = &si.peTTM;
    break;
  case 3:
    field = &si.pbMRQ;
    break;
  case 4:
    field = &si.psTTM;
    break;
  case 5:
    field = &si.pcfNcfTTM;
    break;
  case 6:
    field = &si.dy1y;
    break;
  default:
    return std::nanf("");
  }
  if (field->empty())
    return std::nanf("");
  try {
    return std::stof(*field);
  } catch (...) {
    return std::nanf("");
  }
}

// 构建连续值缓存 (资产表静态, 每模式一次): [A] + 5/95 分位范围
static void EnsureColorCache(DistUIState &ui, const Asset &asset, const AssetInfo &assetinfo) {
  if (ui.color_cache_mode == ui.color_mode && !ui.color_values.empty())
    return;
  ui.color_cache_mode = ui.color_mode;
  ui.color_values.assign(asset.items.size(), std::nanf(""));
  std::vector<float> valid;
  valid.reserve(asset.items.size());
  for (size_t a = 0; a < asset.items.size(); ++a) {
    const auto &item = asset.items[a];
    std::string ex = item.exchange;
    std::transform(ex.begin(), ex.end(), ex.begin(), ::tolower);
    const StockInfo *si = assetinfo.find_stock_info(ex + "." + item.asset_code);
    float v = si ? AssetColorValue(*si, ui.color_mode) : std::nanf("");
    ui.color_values[a] = v;
    if (!std::isnan(v))
      valid.push_back(v);
  }
  if (valid.size() >= 2) {
    std::sort(valid.begin(), valid.end());
    size_t n = valid.size();
    ui.color_lo = valid[static_cast<size_t>(0.05f * n)];
    ui.color_hi = valid[static_cast<size_t>(0.95f * n)];
    if (ui.color_hi <= ui.color_lo)
      ui.color_hi = ui.color_lo + 1e-6f;
  } else {
    ui.color_lo = 0.0f;
    ui.color_hi = 1.0f;
  }
}

// 统一取色: 行业 → Jet; 连续值 → Viridis (5/95 winsorize)
static ImVec4 AssetColor(const DistUIState &ui, size_t asset_idx) {
  if (ui.color_mode == 0)
    return IndustryColor(ui, asset_idx);
  if (asset_idx >= ui.color_values.size())
    return ImVec4(0.5f, 0.5f, 0.5f, 1.0f);
  float v = ui.color_values[asset_idx];
  if (std::isnan(v))
    return ImVec4(0.5f, 0.5f, 0.5f, 1.0f);
  float t = (ui.color_hi > ui.color_lo)
                ? std::clamp((v - ui.color_lo) / (ui.color_hi - ui.color_lo), 0.0f, 1.0f)
                : 0.5f;
  return ImPlot::SampleColormap(t, ImPlotColormap_Viridis);
}

// ============================================================================
// Color Mode Selector (Left Column, hover 详情上方)
// ============================================================================

static const char *kColorModeNames[] = {"行业", "市值", "PE", "PB",
                                        "PS", "PCF", "股息率"};

static void RenderColorModeSelector(DistUIState &ui, const Asset &asset,
                                    const AssetInfo &assetinfo) {
  ImGui::PushFont(ImGui::GetIO().Fonts->Fonts[0]);
  ImGui::TextUnformatted("[染色]");
  ImGui::PopFont();
  ImGui::SameLine();
  ImGui::SetNextItemWidth(-1);
  ImGui::Combo("##ColorMode", &ui.color_mode, kColorModeNames,
               IM_ARRAYSIZE(kColorModeNames));
  EnsureColorCache(ui, asset, assetinfo);
  // 连续模式显示当前 winsorize 范围
  if (ui.color_mode != 0 && ui.color_hi > ui.color_lo) {
    ImGui::TextDisabled("范围 [%.3g, %.3g]", ui.color_lo, ui.color_hi);
  }
}

// ============================================================================
// PDF Panels - Multiple views with hover tooltips
// ============================================================================

// Common PDF data structure (零拷贝: 指向 KLL 内部重建缓存, 帧内有效)
struct PDFData {
  KLLcache::LinePtr line{nullptr, nullptr, 0};
  int month_idx = -1; // Original month index for focus comparison
};

// Generic PDF rendering with hover detection
template <typename GetTooltipFunc>
static int RenderPDFPlot(const char *plot_id, const std::vector<PDFData> &pdfs,
                         int focus_idx, bool need_autofit, GetTooltipFunc get_tooltip) {
  int hovered_idx = -1;
  double min_dist_sq = 1e9;

  // Trigger autofit if requested
  if (need_autofit) {
    ImPlot::SetNextAxesToFit();
  }

  if (ImPlot::BeginPlot(plot_id, ImVec2(-1, -1))) {
    ImPlot::SetupAxes(nullptr, nullptr,
                      ImPlotAxisFlags_NoLabel | ImPlotAxisFlags_NoTickLabels,
                      ImPlotAxisFlags_NoLabel | ImPlotAxisFlags_NoTickLabels);

    int n_items = static_cast<int>(pdfs.size());

    // Compute max distance for normalization (only for month plots with focus)
    int max_dist = 0;
    if (focus_idx >= 0) {
      for (int i = 0; i < n_items; ++i) {
        if (pdfs[i].line.n == 0 || pdfs[i].month_idx < 0)
          continue;
        int dist = std::abs(pdfs[i].month_idx - focus_idx);
        max_dist = std::max(max_dist, dist);
      }
    }

    // Draw all lines with color based on distance to focus
    ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 2.0f);
    for (int i = 0; i < n_items; ++i) {
      if (pdfs[i].line.n == 0)
        continue;

      float t; // colormap parameter: 0=dark, 1=bright
      if (focus_idx >= 0 && pdfs[i].month_idx >= 0 && max_dist > 0) {
        // Month plot with focus: closer to focus = brighter
        int dist = std::abs(pdfs[i].month_idx - focus_idx);
        t = 1.0f - static_cast<float>(dist) / static_cast<float>(max_dist);
      } else {
        // No focus (weekday/hour) or single month: uniform distribution
        t = static_cast<float>(i) / static_cast<float>(n_items);
      }

      ImVec4 color = ImPlot::SampleColormap(t, ImPlotColormap_Hot);
      ImPlot::SetNextLineStyle(color, 1.0f);
      ImPlot::PlotLine("##pdf", pdfs[i].line.x, pdfs[i].line.y, static_cast<int>(pdfs[i].line.n));
    }
    ImPlot::PopStyleVar();

    // Detect hover (x 窗口裁剪, 只扫鼠标附近的段)
    if (ImPlot::IsPlotHovered()) {
      ImPlotPoint mouse = ImPlot::GetPlotMousePos();
      ImPlotRect limits = ImPlot::GetPlotLimits();
      for (int i = 0; i < n_items; ++i) {
        double d_sq = nearest_seg_dist_sq(pdfs[i].line.x, pdfs[i].line.y, pdfs[i].line.n, mouse, limits);
        if (d_sq < min_dist_sq) {
          min_dist_sq = d_sq;
          hovered_idx = i;
        }
      }
    }

    // Note: no visual hover highlight; tooltip is enough and won't fight the focus line.

    // Click detection for dimension selection
    if (ImPlot::IsPlotHovered() && ImGui::IsMouseClicked(0)) {
      hovered_idx = -2; // Signal that plot was clicked
    }

    ImPlot::EndPlot();
  }

  // Tooltip
  if (hovered_idx >= 0 && min_dist_sq < kHoverDistSq) {
    ImGui::BeginTooltip();
    get_tooltip(hovered_idx);
    ImGui::EndTooltip();
  }

  return hovered_idx;
}

static void RenderPDFByMonth(const Dist &dist, int focus_month_idx, bool need_autofit,
                             int selected_dimension, int &clicked_dimension) {
  ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(2, 2));
  ImGui::BeginChild("PDFByMonth", ImVec2(0, 0), false);
  ImGui::PopStyleVar();
  ImGui::Text("PDF密度(月度漂移)");
  ImGui::Separator();

  if (dist.months.empty()) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  // 流式: 月 sketch 随资产完成度增长, count 够了就画
  int n_months = static_cast<int>(dist.months.size());
  std::vector<PDFData> pdfs(n_months);

  for (int m = 0; m < n_months; ++m) {
    const auto &mc = dist.months[m];
    if (mc.kll.totalCount() >= 10) {
      pdfs[m].line = mc.kll.exportPDF();
      pdfs[m].month_idx = m; // Save original month index
    }
  }

  int hovered = RenderPDFPlot("##PDFMonth", pdfs, focus_month_idx, need_autofit, [&](int idx) {
    const auto &kll = dist.months[idx].kll;
    ImGui::Text("%s", dist.months[idx].month.c_str());
    ImGui::Text("n=%llu", static_cast<unsigned long long>(kll.totalCount()));
    ImGui::Text("mean=%.4f std=%.4f", kll.mean(), std::sqrt(kll.var()));
    ImGui::Text("skew=%.4f kurt=%.4f", kll.skew(), kll.kurt());
  });

  // Click detection
  if (hovered == -2) {
    clicked_dimension = 0; // MONTH
  }

  // Highlight border if selected
  if (selected_dimension == 0) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetItemRectMin();
    ImVec2 p_max = ImGui::GetItemRectMax();
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 3.0f);
  }

  ImGui::EndChild();
}

static void RenderPDFByWeekday(const Dist &dist, bool need_autofit,
                               int selected_dimension, int &clicked_dimension) {
  ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(2, 2));
  ImGui::BeginChild("PDFByWeekday", ImVec2(0, 0), false);
  ImGui::PopStyleVar();
  ImGui::Text("PDF密度(周内偏移)");
  ImGui::Separator();

  if (dist.by_weekday.size() != 7) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  const auto &global_weekday = dist.by_weekday;

  const char *wd_names[] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};
  std::vector<PDFData> pdfs(7);

  for (size_t wd = 0; wd < 7; ++wd) {
    if (global_weekday[wd].totalCount() >= 10) {
      pdfs[wd].line = global_weekday[wd].exportPDF();
    }
  }

  int hovered = RenderPDFPlot("##PDFWeekday", pdfs, -1, need_autofit, [&](int idx) {
    const auto &kll = global_weekday[idx];
    ImGui::Text("%s", wd_names[idx]);
    ImGui::Text("n=%llu", static_cast<unsigned long long>(kll.totalCount()));
    ImGui::Text("mean=%.4f std=%.4f", kll.mean(), std::sqrt(kll.var()));
    ImGui::Text("skew=%.4f kurt=%.4f", kll.skew(), kll.kurt());
  });

  // Click detection
  if (hovered == -2) {
    clicked_dimension = 1; // WEEKDAY
  }

  // Highlight border if selected
  if (selected_dimension == 1) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetItemRectMin();
    ImVec2 p_max = ImGui::GetItemRectMax();
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 3.0f);
  }

  ImGui::EndChild();
}

static void RenderPDFByHour(const Dist &dist, bool need_autofit,
                            int selected_dimension, int &clicked_dimension) {
  ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(2, 2));
  ImGui::BeginChild("PDFByHour", ImVec2(0, 0), false);
  ImGui::PopStyleVar();
  ImGui::Text("PDF密度(日内偏移)");
  ImGui::Separator();

  if (dist.by_hour.size() != 24) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  const auto &global_hour = dist.by_hour;

  std::vector<PDFData> pdfs(24);

  for (size_t h = 0; h < 24; ++h) {
    if (global_hour[h].totalCount() >= 10) {
      pdfs[h].line = global_hour[h].exportPDF();
    }
  }

  int hovered = RenderPDFPlot("##PDFHour", pdfs, -1, need_autofit, [&](int idx) {
    const auto &kll = global_hour[idx];
    ImGui::Text("Hour %d", idx);
    ImGui::Text("n=%llu", static_cast<unsigned long long>(kll.totalCount()));
    ImGui::Text("mean=%.4f std=%.4f", kll.mean(), std::sqrt(kll.var()));
    ImGui::Text("skew=%.4f kurt=%.4f", kll.skew(), kll.kurt());
  });

  // Click detection
  if (hovered == -2) {
    clicked_dimension = 2; // HOUR
  }

  // Highlight border if selected
  if (selected_dimension == 2) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetItemRectMin();
    ImVec2 p_max = ImGui::GetItemRectMax();
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 3.0f);
  }

  ImGui::EndChild();
}

// ============================================================================
// Assets PDF Plot
// ============================================================================

static void RenderAssetsPDF(const Dist &dist, const Asset &asset, const AssetInfo &assetinfo,
                            DistUIState &ui, int &clicked_dimension, int &hovered_line_out) {
  // Title with tooltip
  ImGui::Text("资产截面(分布密度 + 分位数偏移)");
  ImGui::SameLine();
  ImGui::TextDisabled("(?)");
  if (ImGui::IsItemHovered()) {
    ImGui::BeginTooltip();
    ImGui::PushTextWrapPos(350.0f);
    ImGui::TextUnformatted(
        "分位数一致性尤为重要:\n"
        "不同资产的分位数取值(ICDF)应该尽量靠近, 以保证因子组合阶段分位数的截面一致性\n\n"
        "F_i: CDF; Q_i: 逆CDF; X_i: Feature i\n\n"
        "偏移散点(强调全局差异): 均值校准(最优中心化)的 Wasserstein-L2 偏移距离(积分),\n"
        "相对上一批末的全局分位在发布侧算好, 随构建逐批收敛");
    ImGui::TextColored(ImVec4(0.7f, 0.9f, 1.0f, 1.0f),
                       "    W2(F_i, F_μ) = || (Q_i - E[X_i]) - (Q_μ - E[X_μ]) ||_2 = || ΔW2_i ||_2");
    ImGui::Text("\n颜色 = 左栏 [染色] 选项 (行业/市值/估值/股息率); W2 散点/hover 为全资产,\n"
                "PDF 细线只画固定随机子集 (纯顶点预算, 即全市场无偏抽样)");
    ImGui::PopTextWrapPos();
    ImGui::EndTooltip();
  }
  ImGui::Separator();

  // 发布快照: worker 批末算好整条线 (PDF/矩/W2), UI 零计算零重建只画
  EnsureIndustryCache(ui, asset, assetinfo);
  EnsureColorCache(ui, asset, assetinfo);

  auto &line_indices = ui.line_indices;
  line_indices.clear();
  line_indices.reserve(dist.lines.size());
  for (size_t i = 0; i < dist.lines.size(); ++i) {
    if (dist.lines[i].n_pts > 0)
      line_indices.push_back(i);
  }
  const size_t n_valid = line_indices.size();

  if (n_valid == 0) {
    ImGui::Text("No data (need assets with n >= %zu)", kMinAssetSamples);
    return;
  }

  // W2 偏移散点: 发布侧算好原始值, 每帧只做 max 归一化 (w2 < 0 = 首批参考未就绪)
  auto &x_norm = ui.w2_norm;
  x_norm.resize(n_valid);
  float w2_max = 0.0f;
  bool has_dots = true;
  for (size_t i = 0; i < n_valid; ++i) {
    x_norm[i] = dist.lines[line_indices[i]].w2;
    if (x_norm[i] < 0.0f) {
      has_dots = false;
      break;
    }
    w2_max = std::max(w2_max, x_norm[i]);
  }
  if (has_dots) {
    const float inv = w2_max > 1e-9f ? 1.0f / w2_max : 1.0f;
    for (float &v : x_norm)
      v *= inv;
  } else {
    x_norm.clear();
  }

  if (ui.need_autofit) {
    ImPlot::SetNextAxesToFit();
  }

  int hovered_idx = -1; // index into line_indices
  double min_dist_sq = 1e9;
  bool plot_clicked = false;

  if (ImPlot::BeginPlot("##AssetsPDF", ImVec2(-1, -1))) {
    // x 轴显示刻度 (特征取值), y 轴隐藏 (密度无具体值意义)
    ImPlot::SetupAxes(nullptr, nullptr,
                      ImPlotAxisFlags_NoLabel,
                      ImPlotAxisFlags_NoLabel | ImPlotAxisFlags_NoTickLabels);

    ImPlotRect limits = ImPlot::GetPlotLimits();

    // ========================================================================
    // Phase 1: Hover detection (both PDF lines and W2 dots)
    // ========================================================================
    if (ImPlot::IsPlotHovered()) {
      ImPlotPoint mouse = ImPlot::GetPlotMousePos();

      // Get mouse position in pixels for dot detection
      ImVec2 mouse_pixels = ImGui::GetMousePos();

      // Get fixed plot pixel boundaries (scale invariant)
      ImVec2 plot_pos = ImPlot::GetPlotPos();
      ImVec2 plot_size = ImPlot::GetPlotSize();
      float dot_y_screen = plot_pos.y + 15.0f; // Fixed: 15px from plot top edge

      // Check W2 dots first (top band priority) - use pixel coordinates
      if (has_dots && std::abs(mouse_pixels.y - dot_y_screen) < 20.0f) {
        float best_dist_px = 15.0f; // 15 pixel threshold
        for (size_t i = 0; i < n_valid; ++i) {
          float dot_x_screen = plot_pos.x + x_norm[i] * plot_size.x;

          float dx_px = std::abs(mouse_pixels.x - dot_x_screen);
          if (dx_px < best_dist_px) {
            best_dist_px = dx_px;
            hovered_idx = static_cast<int>(i);
            min_dist_sq = 0.0;
          }
        }
      }

      // Check PDF lines (if not hovering dots; x 窗口裁剪, 只扫鼠标附近的段; 只有画出来的线可 hover)
      if (hovered_idx < 0) {
        for (size_t i = 0; i < n_valid; ++i) {
          const auto &ln = dist.lines[line_indices[i]];
          if (!ln.draw)
            continue;
          double d_sq = nearest_seg_dist_sq(ln.x.data(), ln.y.data(), ln.n_pts, mouse, limits);
          if (d_sq < min_dist_sq) {
            min_dist_sq = d_sq;
            hovered_idx = static_cast<int>(i);
          }
        }
      }
    }

    // ========================================================================
    // Phase 2: Draw PDF lines
    // hover 顶部点时: 先画其他全部资产线 (0.1 透明), 再画高亮曲线置顶 (聚焦)
    // hover 折线时: 只高亮该线, 其他保持 0.75 (现有行为)
    // ========================================================================
    const bool dot_hovered = (hovered_idx >= 0 && min_dist_sq == 0.0);
    if (dot_hovered) {
      // Pass 1: 其他全部资产线降到 0.1 透明
      ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 1.5f);
      for (size_t i = 0; i < n_valid; ++i) {
        if (static_cast<int>(i) == hovered_idx)
          continue;
        const auto &ln = dist.lines[line_indices[i]];
        ImVec4 color = AssetColor(ui, ln.asset);
        color.w = 0.1f;
        ImPlot::SetNextLineStyle(color, 1.0f);
        ImPlot::PlotLine("##pdf_dim", ln.x.data(), ln.y.data(), static_cast<int>(ln.n_pts));
      }
      ImPlot::PopStyleVar();
      // Pass 2: 高亮曲线置顶
      {
        const auto &ln = dist.lines[line_indices[hovered_idx]];
        ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 5.0f);
        ImPlot::SetNextLineStyle(ImVec4(1, 1, 1, 1), 1.0f);
        ImPlot::PlotLine("##pdf_outline", ln.x.data(), ln.y.data(), static_cast<int>(ln.n_pts));
        ImPlot::PopStyleVar();

        ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 3.0f);
        ImPlot::SetNextLineStyle(ImVec4(0, 1, 1, 1), 1.0f); // cyan
        ImPlot::PlotLine("##pdf_hl", ln.x.data(), ln.y.data(), static_cast<int>(ln.n_pts));
        ImPlot::PopStyleVar();
      }
    } else {
      for (size_t i = 0; i < n_valid; ++i) {
        const auto &ln = dist.lines[line_indices[i]];
        bool is_hovered = (static_cast<int>(i) == hovered_idx);
        if (!ln.draw && !is_hovered)
          continue; // 只画绘制子集 + hover 临时画

        ImVec4 color = AssetColor(ui, ln.asset);
        if (is_hovered) {
          // Highlighted: thick white outline + bright color
          ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 5.0f);
          ImPlot::SetNextLineStyle(ImVec4(1, 1, 1, 1), 1.0f);
          ImPlot::PlotLine("##pdf_outline", ln.x.data(), ln.y.data(), static_cast<int>(ln.n_pts));
          ImPlot::PopStyleVar();

          ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 3.0f);
          ImPlot::SetNextLineStyle(ImVec4(0, 1, 1, 1), 1.0f); // cyan
          ImPlot::PlotLine("##pdf_hl", ln.x.data(), ln.y.data(), static_cast<int>(ln.n_pts));
          ImPlot::PopStyleVar();
        } else {
          color.w = 0.75f;
          ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 1.5f);
          ImPlot::SetNextLineStyle(color, 1.0f);
          ImPlot::PlotLine("##pdf", ln.x.data(), ln.y.data(), static_cast<int>(ln.n_pts));
          ImPlot::PopStyleVar();
        }
      }
    }

    // ========================================================================
    // Phase 3: Draw W2 offset scatter (overlay on top, scale invariant)
    // 发布侧算好的 W2, 随全局分位逐批收敛; 颜色 = 左栏 [染色] 选项
    // ========================================================================
    if (has_dots) {
      ImDrawList *draw = ImPlot::GetPlotDrawList();

      // Get fixed plot pixel boundaries (scale invariant)
      ImVec2 plot_pos = ImPlot::GetPlotPos();
      ImVec2 plot_size = ImPlot::GetPlotSize();
      float y_screen = plot_pos.y + 15.0f; // Fixed: 15px from plot top edge

      // Draw asset dots
      for (size_t i = 0; i < n_valid; ++i) {
        float x_screen = plot_pos.x + x_norm[i] * plot_size.x;
        ImVec2 center(x_screen, y_screen);

        bool is_hovered = (static_cast<int>(i) == hovered_idx);

        if (is_hovered) {
          // Highlighted: large white outline + cyan fill
          draw->AddCircleFilled(center, 5.0f, IM_COL32(255, 255, 255, 255));
          draw->AddCircleFilled(center, 4.0f, IM_COL32(0, 255, 255, 255));
        } else {
          ImU32 color = ImGui::ColorConvertFloat4ToU32(AssetColor(ui, dist.lines[line_indices[i]].asset));
          draw->AddCircleFilled(center, 2.5f, color);
        }
      }
    }

    // Click detection
    if (ImPlot::IsPlotHovered() && ImGui::IsMouseClicked(0)) {
      plot_clicked = true;
    }

    ImPlot::EndPlot();
  }

  // Output: convert draw index to dist.lines index
  if (hovered_idx >= 0 && min_dist_sq < kHoverDistSq) {
    hovered_line_out = static_cast<int>(line_indices[hovered_idx]);
  } else {
    hovered_line_out = -1;
  }

  // Click detection
  if (plot_clicked) {
    clicked_dimension = 3; // ASSETS
  }

  // Highlight border if selected
  if (ui.selected_dimension == 3) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetItemRectMin();
    ImVec2 p_max = ImGui::GetItemRectMax();
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 3.0f);
  }
}

// ============================================================================
// Hovered Asset Info Panel (displayed in left column)
// ============================================================================

static void RenderHoveredAssetInfo(const Dist &dist, const Asset &asset,
                                   const AssetInfo &assetinfo, int hovered_line) {
  // Use remaining height in parent
  float remaining_height = ImGui::GetContentRegionAvail().y;
  ImGui::BeginChild("HoveredAssetPanel", ImVec2(350, remaining_height), true);

  ImGui::PushFont(ImGui::GetIO().Fonts->Fonts[0]);
  ImGui::TextUnformatted("[资产详情]");
  ImGui::PopFont();
  ImGui::Separator();

  // 重算后 lines 被整体换新, hover 残留下标可能指向未发布的线 → 一并挡掉
  if (hovered_line < 0 || static_cast<size_t>(hovered_line) >= dist.lines.size() ||
      dist.lines[hovered_line].n_pts == 0 ||
      static_cast<size_t>(dist.lines[hovered_line].asset) >= asset.items.size()) {
    ImGui::TextDisabled("(hover on PDF/dot)");
    ImGui::EndChild();
    return;
  }

  const auto &ln = dist.lines[hovered_line];
  const auto &asset_item = asset.items[ln.asset];

  // Get real-time info from AssetInfo
  std::string exchange_lower = asset_item.exchange;
  std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);
  std::string stock_key = exchange_lower + "." + asset_item.asset_code;
  const StockInfo *stock_info = assetinfo.find_stock_info(stock_key);

  // Asset name and code
  if (stock_info && !stock_info->name.empty()) {
    ImGui::Text("%s (%s.%s)", stock_info->name.c_str(),
                asset_item.asset_code.c_str(), asset_item.exchange.c_str());
  } else {
    ImGui::Text("%s.%s", asset_item.asset_code.c_str(), asset_item.exchange.c_str());
  }

  // Date range + market cap on same line
  auto format_date = [](const std::string &date) -> std::string {
    if (date.size() == 8)
      return date.substr(0, 4) + "/" + date.substr(4, 2) + "/" + date.substr(6, 2);
    return "--";
  };
  float market_cap = assetinfo.calculate_market_cap(stock_key);
  if (stock_info && !stock_info->ipoDate.empty()) {
    std::string end_str = stock_info->outDate.empty() ? "now" : format_date(stock_info->outDate);
    ImGui::Text("%s-%s  %.1f亿", format_date(stock_info->ipoDate).c_str(), end_str.c_str(),
                market_cap > 0 ? market_cap : 0.0f);
  } else {
    ImGui::Text("市值: %.1f亿", market_cap > 0 ? market_cap : 0.0f);
  }

  ImGui::Separator();

  // Two-column compact layout: Valuation | Statistics
  auto fmt_val = [](const std::string &s) -> std::string {
    if (s.empty())
      return "--";
    try {
      char buf[12];
      std::snprintf(buf, sizeof(buf), "%+.1f", std::stof(s));
      return buf;
    } catch (...) {
      return "--";
    }
  };

  if (ImGui::BeginTable("StatsTable", 2, ImGuiTableFlags_SizingFixedFit)) {
    ImGui::TableSetupColumn("Col1", ImGuiTableColumnFlags_WidthFixed, 160);
    ImGui::TableSetupColumn("Col2", ImGuiTableColumnFlags_WidthFixed, 160);

    // Row 1: n | PE
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("样本数 = %llu", static_cast<unsigned long long>(ln.n));
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PE = %s", stock_info ? fmt_val(stock_info->peTTM).c_str() : "--");

    // Row 2: Mean | PB
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("均值 = %.4f", ln.mean);
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PB = %s", stock_info ? fmt_val(stock_info->pbMRQ).c_str() : "--");

    // Row 3: Var | PS
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("方差 = %.4f", ln.var);
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PS = %s", stock_info ? fmt_val(stock_info->psTTM).c_str() : "--");

    // Row 4: Skew | PCF
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("偏度 = %.3f", ln.skew);
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PCF = %s", stock_info ? fmt_val(stock_info->pcfNcfTTM).c_str() : "--");

    // Row 5: Kurt | 行业
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("峰度 = %.3f", ln.kurt);
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("行业 = %s",
                stock_info && !stock_info->ind_name.empty() ? stock_info->ind_name.c_str() : "--");

    ImGui::EndTable();
  }

  ImGui::EndChild();
}

// ============================================================================
// Main Render
// ============================================================================

void RenderTabDist(DistService *service, SharedData &data, DistUIState &ui) {
  // Prefer box-zoom on LMB drag (consistent with other tabs, e.g. TabOrderFlow).
  // Default ImPlot mapping is pan=LMB drag, box-select=RMB drag.
  static bool input_map_configured = false;
  if (!input_map_configured) {
    ImPlot::MapInputReverse();
    input_map_configured = true;
  }

  // Auto-start worker thread
  if (!service->is_running()) {
    service->Start(data);
  }

  auto &dist = data.dist;

  // 流式维护 x/y range: 任何新发布 epoch (= 数据变了) 都 autofit 一次.
  // 覆盖: 构建期间每批, 末批到达 Done (含 tab 隐藏期间完成的构建), Cancelled 末态.
  // 稳态 (epoch 未变) 不 autofit, 把缩放还给用户.
  // 用 lines_epoch 做唯一判据, 不依赖 status 转移检测 —— 后者在 tab 隐藏期间会漏掉
  // 完成转移 (last_status 停在上一次 Done), 导致切回来 zoom 还停在上一个特征.
  const uint64_t cur_epoch = dist.lines_epoch.load(std::memory_order_acquire);
  const auto cur_status = dist.status.load(std::memory_order_acquire);
  if (cur_epoch != ui.last_lines_epoch &&
      (cur_status == Dist::Status::Building || cur_status == Dist::Status::Done ||
       cur_status == Dist::Status::Cancelled)) {
    ui.need_autofit = true;
  }
  ui.last_lines_epoch = cur_epoch;

  // 渲染帧内持锁: worker 块末/批末短锁发布, UI 读快照与聚合槽与其互斥
  std::lock_guard<std::mutex> dist_lock(dist.mutex);

  // Integrity (auto-fit height)
  float integrity_height = ImGui::GetTextLineHeightWithSpacing() + ImGui::GetStyle().WindowPadding.y * 1.5;
  ImGui::BeginChild("IntegrityBar", ImVec2(0, integrity_height), true);
  RenderIntegrity(dist.integrity);
  ImGui::EndChild();

  // Window control (auto-fit height: 2 rows + padding)
  float ctrl_height = ImGui::GetFrameHeightWithSpacing() * 2 + ImGui::GetStyle().WindowPadding.y * 1.5;
  ImGui::BeginChild("WindowCtrl", ImVec2(0, ctrl_height), true);
  RenderWindowControl(service, data, ui);
  ImGui::EndChild();

  // Main content: Left (Color Mode + Asset Info) + Right (PDFs)
  float content_height = ImGui::GetContentRegionAvail().y;
  ImGui::Columns(2, "MainCols", true);
  ImGui::SetColumnWidth(0, 350);

  // Left column: Color Mode Selector + Hovered Asset Info (剩余空间)
  ImGui::BeginChild("LeftSection", ImVec2(0, content_height), false);
  RenderColorModeSelector(ui, data.asset, data.assetinfo);
  RenderHoveredAssetInfo(dist, data.asset, data.assetinfo, ui.hovered_line);
  ImGui::EndChild();

  ImGui::NextColumn();

  // Right column: PDF panels
  ImGui::BeginChild("RightSection", ImVec2(0, content_height), false);

  // Top: Three PDF panels in a row (tight layout)
  float pdf_height = content_height * 0.5f;
  ImGui::BeginChild("PDFRow", ImVec2(0, pdf_height), false);

  ImGui::PushStyleVar(ImGuiStyleVar_ItemSpacing, ImVec2(0, 0));
  ImGui::Columns(3, "PDFCols", false);

  // Track clicked dimension (-1 = none clicked)
  int clicked_dimension = -1;

  // PDF by Month (focus month only)
  RenderPDFByMonth(dist, ui.focus_month_idx, ui.need_autofit, ui.selected_dimension, clicked_dimension);
  ImGui::NextColumn();

  // PDF by Weekday (global)
  RenderPDFByWeekday(dist, ui.need_autofit, ui.selected_dimension, clicked_dimension);
  ImGui::NextColumn();

  // PDF by Hour (global)
  RenderPDFByHour(dist, ui.need_autofit, ui.selected_dimension, clicked_dimension);

  ImGui::Columns(1);
  ImGui::PopStyleVar();
  ImGui::EndChild();

  // Bottom: Assets PDF (outputs ui.hovered_line)
  ImGui::BeginChild("AssetsPDFSection", ImVec2(0, 0), true);
  RenderAssetsPDF(dist, data.asset, data.assetinfo, ui, clicked_dimension, ui.hovered_line);
  ImGui::EndChild();

  // Update selected dimension if any plot was clicked
  if (clicked_dimension >= 0) {
    ui.selected_dimension = clicked_dimension;
  }

  ImGui::EndChild();

  ImGui::Columns(1);

  // Clear autofit flag after all plots rendered
  ui.need_autofit = false;
}

void StopTabDist(DistService *service, SharedData &data) {
  // 切走 tab: 只中断在跑构建, 内存与 worker 保留 (任务级回收在 OnCollapse 的 Shutdown);
  // 切回时 Idle/Cancelled 自动重算, Done 的结果直接复用
  service->RequestCancel();
  (void)data;
}

} // namespace GUI::Features
