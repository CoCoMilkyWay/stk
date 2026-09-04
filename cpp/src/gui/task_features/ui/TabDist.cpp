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
#include <array>
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
// W2 偏移 (每帧派生, 全程流式): 均值校准的 Wasserstein-L2 偏移距离,
// 相对"当前"全局分位 —— 随构建推进逐帧收敛, 无需收尾阶段.
// ============================================================================

constexpr int kW2Deciles = 19; // 5%, 10%, ..., 95%

// 分位查询: exportICDF 的 u 网格等距 → 直接定址 + 线性插值 (免二分)
static float QuantileAt(const KLLcache &kll, double q) {
  const auto icdf = kll.exportICDF();
  const double u0 = icdf.x[0], u1 = icdf.x[icdf.n - 1];
  if (q <= u0)
    return icdf.y[0];
  if (q >= u1)
    return icdf.y[icdf.n - 1];
  const double f = (q - u0) / (u1 - u0) * static_cast<double>(icdf.n - 1);
  const size_t lo = static_cast<size_t>(f);
  const double t = f - static_cast<double>(lo);
  return static_cast<float>(icdf.y[lo] + t * (icdf.y[lo + 1] - icdf.y[lo]));
}

struct W2GlobalRef {
  std::array<float, kW2Deciles> q;
  float mean = 0.0f;
  bool valid = false;
};

static W2GlobalRef ComputeW2GlobalRef(const Dist &dist) {
  W2GlobalRef ref;
  if (dist.total.totalCount() < kMinSamples)
    return ref;
  for (int d = 0; d < kW2Deciles; ++d)
    ref.q[d] = QuantileAt(dist.total, 0.05 * (d + 1));
  ref.mean = static_cast<float>(dist.total.mean());
  ref.valid = true;
  return ref;
}

static float ComputeW2(const KLLcache &kll, const W2GlobalRef &ref) {
  const float shift = static_cast<float>(kll.mean()) - ref.mean;
  float sum_sq = 0.0f;
  for (int d = 0; d < kW2Deciles; ++d) {
    const float qi = QuantileAt(kll, 0.05 * (d + 1));
    const float diff = (qi - shift) - ref.q[d];
    sum_sq += diff * diff;
  }
  return std::sqrt(sum_sq / kW2Deciles);
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
  // 进度: Phase IO (抽样天) + Phase 流 (资产)
  ImGui::Text(" (IO %zu/%zu | 资产 %zu/%zu)",
              dist.days_loaded.load(), dist.days_total.load(),
              dist.assets_done.load(), dist.assets.size());

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
// Moments Panel (Left Column)
// Color bands with boundary labels, current month value vs all months MAD
// ============================================================================

// Zone determination: 0=normal(green), 1=warn(yellow), 2=anomaly(red)
static int get_zone(float val, float normal_lo, float normal_hi, float warn_lo,
                    float warn_hi) {
  if (val >= normal_lo && val <= normal_hi)
    return 0;
  if (val >= warn_lo && val <= warn_hi)
    return 1;
  return 2;
}

static ImU32 zone_color(int zone) {
  if (zone == 0)
    return IM_COL32(60, 200, 60, 255); // green
  if (zone == 1)
    return IM_COL32(220, 200, 60, 255); // yellow
  return IM_COL32(220, 80, 80, 255);    // red
}

// Color band with boundary labels
// Format: Label text first, then bar below
static void RenderMomentBand(const char *label, float current_val,
                             float bound_lo, float bound_hi,
                             float range_lo, float range_hi, float normal_lo,
                             float normal_hi, float warn_lo, float warn_hi) {
  // Text: "Label: current (bound_lo/bound_hi)" with zone colors, first
  int zone_cur = get_zone(current_val, normal_lo, normal_hi, warn_lo, warn_hi);
  int zone_bound_lo = get_zone(bound_lo, normal_lo, normal_hi, warn_lo, warn_hi);
  int zone_bound_hi = get_zone(bound_hi, normal_lo, normal_hi, warn_lo, warn_hi);

  // Label with bold font and underline
  ImVec2 label_pos = ImGui::GetCursorScreenPos();

  // Use bold font if available (Fonts[0] is typically bold)
  ImFont *font = ImGui::GetIO().Fonts->Fonts.Size > 0 ? ImGui::GetIO().Fonts->Fonts[0] : ImGui::GetFont();
  ImGui::PushFont(font);

  char label_text[128];
  snprintf(label_text, sizeof(label_text), "%s: ", label);
  ImVec2 text_size = ImGui::CalcTextSize(label_text);

  ImGui::Text("%s", label_text);

  // Draw underline
  ImDrawList *draw_label = ImGui::GetWindowDrawList();
  draw_label->AddLine(ImVec2(label_pos.x, label_pos.y + text_size.y),
                      ImVec2(label_pos.x + text_size.x, label_pos.y + text_size.y),
                      ImGui::GetColorU32(ImGuiCol_Text), 1.0f);

  ImGui::PopFont();
  ImGui::SameLine(0, 0);
  ImGui::PushStyleColor(ImGuiCol_Text, zone_color(zone_cur));
  ImGui::Text("%.2f", current_val);
  ImGui::PopStyleColor();
  ImGui::SameLine(0, 0);
  ImGui::Text(" (");
  ImGui::SameLine(0, 0);
  ImGui::PushStyleColor(ImGuiCol_Text, zone_color(zone_bound_lo));
  ImGui::Text("%.2f", bound_lo);
  ImGui::PopStyleColor();
  ImGui::SameLine(0, 0);
  ImGui::Text("/");
  ImGui::SameLine(0, 0);
  ImGui::PushStyleColor(ImGuiCol_Text, zone_color(zone_bound_hi));
  ImGui::Text("%.2f", bound_hi);
  ImGui::PopStyleColor();
  ImGui::SameLine(0, 0);
  ImGui::Text(")");

  // Now draw the bar below
  ImDrawList *draw = ImGui::GetWindowDrawList();
  ImVec2 pos = ImGui::GetCursorScreenPos();
  float width = ImGui::GetContentRegionAvail().x;
  float bar_height = 12.0f; // Compact bar height

  float range = range_hi - range_lo;
  auto to_x = [&](float v) {
    return pos.x + std::clamp((v - range_lo) / range, 0.0f, 1.0f) * width;
  };

  // Bar vertical bounds (labels directly on bar)
  float y_top = pos.y;
  float y_bot = y_top + bar_height;

  // Draw color bands (all aligned y_top to y_bot)
  // Anomaly bands (red)
  draw->AddRectFilled(ImVec2(to_x(range_lo), y_top),
                      ImVec2(to_x(warn_lo), y_bot),
                      IM_COL32(180, 60, 60, 200));
  draw->AddRectFilled(ImVec2(to_x(warn_hi), y_top),
                      ImVec2(to_x(range_hi), y_bot),
                      IM_COL32(180, 60, 60, 200));

  // Warn bands (yellow)
  draw->AddRectFilled(ImVec2(to_x(warn_lo), y_top),
                      ImVec2(to_x(normal_lo), y_bot),
                      IM_COL32(200, 180, 60, 200));
  draw->AddRectFilled(ImVec2(to_x(normal_hi), y_top),
                      ImVec2(to_x(warn_hi), y_bot),
                      IM_COL32(200, 180, 60, 200));

  // Normal band (green)
  draw->AddRectFilled(ImVec2(to_x(normal_lo), y_top),
                      ImVec2(to_x(normal_hi), y_bot),
                      IM_COL32(60, 160, 60, 200));

  // Bound range markers (slightly inset) - either MAD or min/max
  draw->AddRect(ImVec2(to_x(bound_lo), y_top + 1),
                ImVec2(to_x(bound_hi), y_bot - 1),
                IM_COL32(255, 255, 255, 180), 0.0f, 0, 1.5f);

  // Current value marker (cyan diamond, centered)
  float cv_x = to_x(current_val);
  float cy = (y_top + y_bot) * 0.5f;
  draw->AddQuadFilled(ImVec2(cv_x, cy - 4), ImVec2(cv_x + 3, cy),
                      ImVec2(cv_x, cy + 4), ImVec2(cv_x - 3, cy),
                      IM_COL32(0, 255, 255, 255));

  // Boundary labels - small font, directly on bar
  // All labels drawn at same height, overlaid on the bar
  ImFont *small_font = ImGui::GetFont();
  float small_font_size = ImGui::GetFontSize() * 0.75f;          // Compact font size
  float label_y = y_top + (bar_height - small_font_size) * 0.5f; // Vertically centered
  char buf[16];

  // warn_lo (align right edge to boundary)
  snprintf(buf, sizeof(buf), "%.1f", warn_lo);
  float text_width_warn_lo = small_font->CalcTextSizeA(small_font_size, FLT_MAX, 0.0f, buf).x;
  draw->AddText(small_font, small_font_size, ImVec2(to_x(warn_lo) - text_width_warn_lo, label_y), IM_COL32(255, 255, 255, 255), buf);

  // normal_lo (align right edge to boundary)
  snprintf(buf, sizeof(buf), "%.1f", normal_lo);
  float text_width_normal_lo = small_font->CalcTextSizeA(small_font_size, FLT_MAX, 0.0f, buf).x;
  draw->AddText(small_font, small_font_size, ImVec2(to_x(normal_lo) - text_width_normal_lo, label_y), IM_COL32(255, 255, 255, 255), buf);

  // normal_hi (align left edge to boundary)
  snprintf(buf, sizeof(buf), "%.1f", normal_hi);
  draw->AddText(small_font, small_font_size, ImVec2(to_x(normal_hi) + 1, label_y), IM_COL32(255, 255, 255, 255), buf);

  // warn_hi (align left edge to boundary)
  snprintf(buf, sizeof(buf), "%.1f", warn_hi);
  draw->AddText(small_font, small_font_size, ImVec2(to_x(warn_hi) + 1, label_y), IM_COL32(255, 255, 255, 255), buf);

  ImGui::Dummy(ImVec2(width, bar_height));
}

static void RenderMomentsPanel(const Dist &dist, int selected_dimension, int focus_month_idx) {
  // Compact height: header + 4 moment bands (each = label line + 12px bar)
  float line_h = ImGui::GetTextLineHeightWithSpacing();
  float bar_px = 12.0f;
  float band_h = line_h + bar_px;
  float panel_h = line_h * 2 + band_h * 4 + ImGui::GetStyle().WindowPadding.y * 2;
  ImGui::BeginChild("MomentsPanel", ImVec2(350, panel_h), true);
  ImGui::PushFont(ImGui::GetIO().Fonts->Fonts[0]); // Use default font with bold style
  ImGui::TextUnformatted("[阶矩展开]");
  ImGui::PopFont();
  ImGui::SameLine();
  ImGui::TextDisabled("(?)");

  // Tooltip on help marker
  if (ImGui::IsItemHovered()) {
    ImGui::BeginTooltip();

    // Expansion objects table
    if (ImGui::BeginTable("ExpansionTable", 5, ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit)) {
      ImGui::TableSetupColumn("展开对象");
      ImGui::TableSetupColumn("展开变量");
      ImGui::TableSetupColumn("展开形式");
      ImGui::TableSetupColumn("名称 / 定理");
      ImGui::TableSetupColumn("为什么重要");
      ImGui::TableHeadersRow();

      // Row 1: 函数期望
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("函数期望 (E[f(X)])");
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("(X - E[X])");
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("幂级数");
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("Taylor + 矩展开");
      ImGui::TableSetColumnIndex(4);
      ImGui::Text("风险、PnL、凸性分析的基础");

      // Row 2: 特征函数
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("特征函数 (φ(t)=E[e^{itX}])");
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("(t)");
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("幂级数");
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("特征函数展开");
      ImGui::TableSetColumnIndex(4);
      ImGui::Text("所有矩的母体");

      // Row 3: 对数特征函数
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("对数特征函数 (log φ(t))");
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("(t)");
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("幂级数");
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("累积量展开");
      ImGui::TableSetColumnIndex(4);
      ImGui::Text("比矩稳定,叠加最干净");

      // Row 4: 概率密度函数 (Hermite)
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("概率密度函数 (p(x))");
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("Hermite 基");
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("正交级数");
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("Gram-Charlier A");
      ImGui::TableSetColumnIndex(4);
      ImGui::Text("最直接的\"分布级\"展开");

      // Row 5: 概率密度函数 (Edgeworth)
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("概率密度函数 (p(x))");
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("(n^{-1/2})");
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("渐近级数");
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("Edgeworth 展开");
      ImGui::TableSetColumnIndex(4);
      ImGui::Text("CLT 的高阶修正");

      // Row 6: 小噪声随机变量
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("小噪声随机变量 (X+ε)");
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("(ε)");
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("幂级数");
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("Delta Method");
      ImGui::TableSetColumnIndex(4);
      ImGui::Text("统计误差传播核心");

      ImGui::EndTable();
    }

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    // Moments definition table
    if (ImGui::BeginTable("MomentsDefTable", 4, ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit)) {
      ImGui::TableSetupColumn("阶数");
      ImGui::TableSetupColumn("普通矩 (m_n)");
      ImGui::TableSetupColumn("中心矩 (μ_n)");
      ImGui::TableSetupColumn("标准矩 (γ_n)");
      ImGui::TableHeadersRow();

      // Row: 定义
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("定义");
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("E[X^n]");
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("E[(X-E[X])^n]");
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("E[(X-E[X])^n]/(E[(X-E[X])^2])^(n/2)");

      // Row: 1阶
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("1");
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("均值");
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("0");
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("0");

      // Row: 2阶
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("2");
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("\\");
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("方差");
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("1");

      // Row: 3阶
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("3");
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("\\");
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("\\");
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("偏度");

      // Row: 4阶
      ImGui::TableNextRow();
      ImGui::TableSetColumnIndex(0);
      ImGui::Text("4");
      ImGui::TableSetColumnIndex(1);
      ImGui::Text("\\");
      ImGui::TableSetColumnIndex(2);
      ImGui::Text("\\");
      ImGui::TableSetColumnIndex(3);
      ImGui::Text("峰度");

      ImGui::EndTable();
    }
    ImGui::EndTooltip();
  }

  ImGui::Separator();

  // Collect moment values based on selected dimension (流式: 随资产完成度增长)
  std::vector<float> means, vars, skews, kurts;
  auto collect = [&](const KLLcache &kll) {
    if (kll.totalCount() < kMinSamples)
      return;
    means.push_back(static_cast<float>(kll.mean()));
    vars.push_back(static_cast<float>(kll.var()));
    skews.push_back(static_cast<float>(kll.skew()));
    kurts.push_back(static_cast<float>(kll.kurt()));
  };

  // Dimension: 0=MONTH, 1=WEEKDAY, 2=HOUR, 3=ASSETS (随机序流式发布, 槽发布即终态)
  switch (selected_dimension) {
  case 0:
    for (const auto &mc : dist.months)
      collect(mc.kll);
    break;
  case 1:
    for (const auto &kll : dist.by_weekday)
      collect(kll);
    break;
  case 2:
    for (const auto &kll : dist.by_hour)
      collect(kll);
    break;
  case 3:
    for (const auto &kll : dist.assets)
      collect(kll);
    break;
  }

  if (means.empty()) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  // Compute mean (average) for each moment type
  auto compute_mean = [](const std::vector<float> &vals) -> float {
    if (vals.empty())
      return 0.0f;
    float sum = 0.0f;
    for (float v : vals)
      sum += v;
    return sum / vals.size();
  };

  // Compute min/max
  auto compute_min = [](const std::vector<float> &vals) -> float {
    if (vals.empty())
      return 0.0f;
    return *std::min_element(vals.begin(), vals.end());
  };

  auto compute_max = [](const std::vector<float> &vals) -> float {
    if (vals.empty())
      return 0.0f;
    return *std::max_element(vals.begin(), vals.end());
  };

  // Compute MAD for each moment type
  auto compute_mad = [](const std::vector<float> &vals) -> float {
    if (vals.empty())
      return 0.0f;
    // Compute median
    std::vector<float> sorted = vals;
    std::sort(sorted.begin(), sorted.end());
    float median = sorted[sorted.size() / 2];
    // Compute MAD
    std::vector<float> abs_devs;
    abs_devs.reserve(vals.size());
    for (float v : vals) {
      abs_devs.push_back(std::abs(v - median));
    }
    std::sort(abs_devs.begin(), abs_devs.end());
    return abs_devs[abs_devs.size() / 2];
  };

  float mean_mean = compute_mean(means);
  float mean_var = compute_mean(vars);
  float mean_skew = compute_mean(skews);
  float mean_kurt = compute_mean(kurts);

  // For dimensions 0,1,2 (MONTH, WEEKDAY, HOUR): use min/max
  // For dimension 3 (ASSETS): use MAD
  float lower_mean, upper_mean, lower_var, upper_var;
  float lower_skew, upper_skew, lower_kurt, upper_kurt;

  if (selected_dimension == 3) {
    // ASSETS dimension: use MAD
    float mad_mean = compute_mad(means);
    float mad_var = compute_mad(vars);
    float mad_skew = compute_mad(skews);
    float mad_kurt = compute_mad(kurts);

    lower_mean = mean_mean - 2.5f * mad_mean;
    upper_mean = mean_mean + 2.5f * mad_mean;
    lower_var = mean_var - 2.5f * mad_var;
    upper_var = mean_var + 2.5f * mad_var;
    lower_skew = mean_skew - 2.5f * mad_skew;
    upper_skew = mean_skew + 2.5f * mad_skew;
    lower_kurt = mean_kurt - 2.5f * mad_kurt;
    upper_kurt = mean_kurt + 2.5f * mad_kurt;
  } else {
    // MONTH, WEEKDAY, HOUR dimensions: use min/max
    lower_mean = compute_min(means);
    upper_mean = compute_max(means);
    lower_var = compute_min(vars);
    upper_var = compute_max(vars);
    lower_skew = compute_min(skews);
    upper_skew = compute_max(skews);
    lower_kurt = compute_min(kurts);
    upper_kurt = compute_max(kurts);
  }

  // Get current value to display
  // For MONTH: use slider month value; for others: use mean
  float display_mean, display_var, display_skew, display_kurt;

  if (selected_dimension == 0 && focus_month_idx >= 0 &&
      focus_month_idx < static_cast<int>(dist.months.size()) &&
      dist.months[focus_month_idx].kll.totalCount() > 0) {
    // MONTH dimension: show slider month value
    const auto &kll = dist.months[focus_month_idx].kll;
    display_mean = static_cast<float>(kll.mean());
    display_var = static_cast<float>(kll.var());
    display_skew = static_cast<float>(kll.skew());
    display_kurt = static_cast<float>(kll.kurt());
  } else {
    // Other dimensions: show mean value
    display_mean = mean_mean;
    display_var = mean_var;
    display_skew = mean_skew;
    display_kurt = mean_kurt;
  }

  // Color bands with boundaries (compact layout, no extra spacing)
  RenderMomentBand("Mean/均值(1阶普通矩)", display_mean, lower_mean, upper_mean, -1.0f, 1.0f, -0.1f,
                   0.1f, -0.3f, 0.3f);
  RenderMomentBand("Var/方差(2阶中心矩)", display_var, lower_var, upper_var, 0.0f, 3.0f,
                   0.5f, 1.2f, 0.2f, 2.0f);
  RenderMomentBand("Skew/偏度(3阶标准矩)", display_skew, lower_skew, upper_skew, -4.0f, 4.0f,
                   -0.5f, 0.5f, -1.5f, 1.5f);
  RenderMomentBand("Kurt/峰度(4阶标准矩)", display_kurt, lower_kurt, upper_kurt, -3.0f, 15.0f,
                   0.0f, 3.0f, -1.0f, 6.0f);

  ImGui::EndChild();
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
                            DistUIState &ui, int &clicked_dimension, int &hovered_asset_out) {
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
        "相对当前全局分位每帧派生, 随构建流式收敛");
    ImGui::TextColored(ImVec4(0.7f, 0.9f, 1.0f, 1.0f),
                       "    W2(F_i, F_μ) = || (Q_i - E[X_i]) - (Q_μ - E[X_μ]) ||_2 = || ΔW2_i ||_2");
    ImGui::Text("\n颜色 = 行业 (一个行业一个颜色); 资产按随机序流式发布, 已画集合即全市场无偏抽样");
    ImGui::PopTextWrapPos();
    ImGui::EndTooltip();
  }
  ImGui::Separator();

  // 流式: 随机序发布 + 槽发布即终态 → 扫全部槽按 count 过滤即已发布集合 (无偏抽样)
  EnsureIndustryCache(ui, asset, assetinfo);

  std::vector<size_t> asset_indices;
  asset_indices.reserve(dist.assets.size());
  for (size_t a = 0; a < dist.assets.size(); ++a) {
    if (dist.assets[a].totalCount() >= kMinAssetSamples)
      asset_indices.push_back(a);
  }
  const size_t n_valid = asset_indices.size();

  if (n_valid == 0) {
    ImGui::Text("No data (need assets with n >= %zu)", kMinAssetSamples);
    return;
  }

  // W2 偏移散点: 每帧对当前全局分位派生 (流式, 无收尾阶段)
  const W2GlobalRef w2_ref = ComputeW2GlobalRef(dist);
  std::vector<float> x_norm;
  float w2_max = 0.0f;
  const bool has_dots = w2_ref.valid;
  if (has_dots) {
    x_norm.resize(n_valid);
    for (size_t i = 0; i < n_valid; ++i) {
      x_norm[i] = ComputeW2(dist.assets[asset_indices[i]], w2_ref);
      w2_max = std::max(w2_max, x_norm[i]);
    }
    const float inv = w2_max > 1e-9f ? 1.0f / w2_max : 1.0f;
    for (float &v : x_norm)
      v *= inv;
  }

  if (ui.need_autofit) {
    ImPlot::SetNextAxesToFit();
  }

  int hovered_idx = -1; // index into asset_indices
  double min_dist_sq = 1e9;
  bool plot_clicked = false;

  if (ImPlot::BeginPlot("##AssetsPDF", ImVec2(-1, -1))) {
    ImPlot::SetupAxes(nullptr, nullptr,
                      ImPlotAxisFlags_NoLabel | ImPlotAxisFlags_NoTickLabels,
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

      // Check PDF lines (if not hovering dots; x 窗口裁剪, 只扫鼠标附近的段)
      if (hovered_idx < 0) {
        for (size_t i = 0; i < n_valid; ++i) {
          const auto pdf = dist.assets[asset_indices[i]].exportPDF();
          double d_sq = nearest_seg_dist_sq(pdf.x, pdf.y, pdf.n, mouse, limits);
          if (d_sq < min_dist_sq) {
            min_dist_sq = d_sq;
            hovered_idx = static_cast<int>(i);
          }
        }
      }
    }

    // ========================================================================
    // Phase 2: Draw all PDFs (highlight hovered)
    // ========================================================================
    for (size_t i = 0; i < n_valid; ++i) {
      const auto pdf = dist.assets[asset_indices[i]].exportPDF();
      if (pdf.n == 0)
        continue;

      bool is_hovered = (static_cast<int>(i) == hovered_idx);
      ImVec4 color = IndustryColor(ui, asset_indices[i]);
      color.w = 0.75f; // 线多, 半透明降噪

      if (is_hovered) {
        // Highlighted: thick white outline + bright color
        ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 5.0f);
        ImPlot::SetNextLineStyle(ImVec4(1, 1, 1, 1), 1.0f);
        ImPlot::PlotLine("##pdf_outline", pdf.x, pdf.y, static_cast<int>(pdf.n));
        ImPlot::PopStyleVar();

        ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 3.0f);
        ImPlot::SetNextLineStyle(ImVec4(0, 1, 1, 1), 1.0f); // cyan
        ImPlot::PlotLine("##pdf_hl", pdf.x, pdf.y, static_cast<int>(pdf.n));
        ImPlot::PopStyleVar();
      } else {
        ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 1.5f);
        ImPlot::SetNextLineStyle(color, 1.0f);
        ImPlot::PlotLine("##pdf", pdf.x, pdf.y, static_cast<int>(pdf.n));
        ImPlot::PopStyleVar();
      }
    }

    // ========================================================================
    // Phase 3: Draw W2 offset scatter (overlay on top, scale invariant)
    // 流式: 已发布资产的散点每帧重派生, 随全局分位收敛; 颜色 = 行业
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
          ImU32 color = ImGui::ColorConvertFloat4ToU32(IndustryColor(ui, asset_indices[i]));
          draw->AddCircleFilled(center, 2.5f, color);
        }
      }

      // Draw reference labels: 0 (min) and max (流式最大值)
      char buf_max[16];
      std::snprintf(buf_max, sizeof(buf_max), "%.3f", w2_max);

      // Label positions (scale invariant)
      float x_screen_min = plot_pos.x;
      float x_screen_max = plot_pos.x + plot_size.x;

      // Use bold font if available
      ImFont *font = ImGui::GetIO().Fonts->Fonts.Size > 0 ? ImGui::GetIO().Fonts->Fonts[0] : ImGui::GetFont();
      float font_size = ImGui::GetFontSize();

      draw->AddText(font, font_size, ImVec2(x_screen_min + 5, y_screen + 8), IM_COL32(255, 255, 255, 255), "0");

      ImVec2 text_size = font->CalcTextSizeA(font_size, FLT_MAX, 0.0f, buf_max);
      draw->AddText(font, font_size, ImVec2(x_screen_max - text_size.x - 5, y_screen + 8), IM_COL32(255, 255, 255, 255), buf_max);
    }

    // Click detection
    if (ImPlot::IsPlotHovered() && ImGui::IsMouseClicked(0)) {
      plot_clicked = true;
    }

    ImPlot::EndPlot();
  }

  // Output: convert draw index to original asset index
  if (hovered_idx >= 0 && min_dist_sq < kHoverDistSq) {
    hovered_asset_out = static_cast<int>(asset_indices[hovered_idx]);
  } else {
    hovered_asset_out = -1;
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
                                   const AssetInfo &assetinfo, int hovered_asset) {
  // Use remaining height in parent
  float remaining_height = ImGui::GetContentRegionAvail().y;
  ImGui::BeginChild("HoveredAssetPanel", ImVec2(350, remaining_height), true);

  ImGui::PushFont(ImGui::GetIO().Fonts->Fonts[0]);
  ImGui::TextUnformatted("[资产详情]");
  ImGui::PopFont();
  ImGui::Separator();

  // 槽被重算清空后 hover 残留下标会指向空 sketch → 一并挡掉
  if (hovered_asset < 0 || static_cast<size_t>(hovered_asset) >= asset.items.size() ||
      static_cast<size_t>(hovered_asset) >= dist.assets.size() ||
      dist.assets[hovered_asset].totalCount() < kMinAssetSamples) {
    ImGui::TextDisabled("(hover on PDF/dot)");
    ImGui::EndChild();
    return;
  }

  const auto &asset_item = asset.items[hovered_asset];
  const auto &kll = dist.assets[hovered_asset];

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
    ImGui::Text("样本数 = %llu", static_cast<unsigned long long>(kll.totalCount()));
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PE = %s", stock_info ? fmt_val(stock_info->peTTM).c_str() : "--");

    // Row 2: Mean | PB
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("均值 = %.4f", kll.mean());
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PB = %s", stock_info ? fmt_val(stock_info->pbMRQ).c_str() : "--");

    // Row 3: Var | PS
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("方差 = %.4f", kll.var());
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PS = %s", stock_info ? fmt_val(stock_info->psTTM).c_str() : "--");

    // Row 4: Skew | PCF
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("偏度 = %.3f", kll.skew());
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PCF = %s", stock_info ? fmt_val(stock_info->pcfNcfTTM).c_str() : "--");

    // Row 5: Kurt | 行业
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("峰度 = %.3f", kll.kurt());
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("行业 = %s",
                stock_info && !stock_info->ind_name.empty() ? stock_info->ind_name.c_str() : "--");

    // Row 6: W2 偏移 (相对当前全局分位, 流式派生)
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    const W2GlobalRef w2_ref = ComputeW2GlobalRef(dist);
    if (w2_ref.valid) {
      ImGui::Text("W2 = %.4f", ComputeW2(kll, w2_ref));
    } else {
      ImGui::TextUnformatted("W2 = --");
    }

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

  // 流式维护 x/y range: 构建期间只要有新资产发布就 autofit (ImPlot 按当帧数据重算范围);
  // Done 后停止跟随, 把缩放还给用户 (完成瞬间再 fit 一次收尾)
  const size_t cur_assets_done = dist.assets_done.load(std::memory_order_acquire);
  const auto cur_status = dist.status.load(std::memory_order_acquire);
  if (cur_status == Dist::Status::Building && cur_assets_done != ui.last_assets_done) {
    ui.need_autofit = true;
  }
  if (ui.last_status != Dist::Status::Done && cur_status == Dist::Status::Done) {
    ui.need_autofit = true;
  }
  ui.last_assets_done = cur_assets_done;
  ui.last_status = cur_status;

  // 渲染帧内持锁: worker 逐资产短锁发布, UI 读 sketch (含懒缓存写) 与其互斥
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

  // Main content: Left (Moments + Asset Info) + Right (PDFs)
  float content_height = ImGui::GetContentRegionAvail().y;
  ImGui::Columns(2, "MainCols", true);
  ImGui::SetColumnWidth(0, 350);

  // Left column: Moments Panel (auto-height) + Hovered Asset Info (remaining)
  ImGui::BeginChild("LeftSection", ImVec2(0, content_height), false);
  RenderMomentsPanel(dist, ui.selected_dimension, ui.focus_month_idx);
  RenderHoveredAssetInfo(dist, data.asset, data.assetinfo, ui.hovered_asset);
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

  // Bottom: Assets PDF (outputs ui.hovered_asset)
  ImGui::BeginChild("AssetsPDFSection", ImVec2(0, 0), true);
  RenderAssetsPDF(dist, data.asset, data.assetinfo, ui, clicked_dimension, ui.hovered_asset);
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
  // 切走: 立刻中断在跑构建 (cancel + join), 释放全部 sketch/块内存 (切回自动重算)
  service->Stop();
  data.dist.clear();
}

} // namespace GUI::Features
