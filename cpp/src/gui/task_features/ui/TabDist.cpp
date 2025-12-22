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

namespace GUI::Features {

// ============================================================================
// Helpers
// ============================================================================

static const char *StatusText(Dist::Compute::Status s) {
  switch (s) {
  case Dist::Compute::Status::Idle:
    return "Idle";
  case Dist::Compute::Status::Building:
    return "Building...";
  case Dist::Compute::Status::Querying:
    return "Querying...";
  case Dist::Compute::Status::Done:
    return "Done";
  case Dist::Compute::Status::Error:
    return "Error";
  case Dist::Compute::Status::Cancelled:
    return "Cancelled";
  }
  return "?";
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

// Compute median and MAD from a vector of values
static std::pair<float, float> compute_median_mad(const std::vector<float> &vals) {
  if (vals.empty())
    return {0.0f, 0.0f};
  std::vector<float> sorted = vals;
  std::sort(sorted.begin(), sorted.end());
  float median = sorted[sorted.size() / 2];
  std::vector<float> abs_devs;
  abs_devs.reserve(vals.size());
  for (float v : vals) {
    abs_devs.push_back(std::abs(v - median));
  }
  std::sort(abs_devs.begin(), abs_devs.end());
  float mad = abs_devs[abs_devs.size() / 2];
  return {median, mad};
}

// ============================================================================
// Integrity Panel
// ============================================================================

static void RenderIntegrity(const Dist::Integrity &integrity) {
  ImGui::Text("Zero: %zu (%.1f%%)  NaN: %zu (%.1f%%)  +Inf: %zu (%.1f%%)  "
              "-Inf: %zu (%.1f%%)",
              integrity.n_zero, integrity.zero_pct(), integrity.n_nan,
              integrity.nan_pct(), integrity.n_pos_inf, integrity.inf_pct(),
              integrity.n_neg_inf, integrity.inf_pct());
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

// Get slider label: "YYYY/MM (n_samples)" or "YYYY/MM" if no cache
static std::string get_month_label(const Dist &dist, int idx,
                                   const std::vector<std::string> &months) {
  if (idx < 0 || idx >= static_cast<int>(months.size()))
    return "";
  std::string label = format_month(months[idx]);
  // Add n_samples if cache available
  if (idx < static_cast<int>(dist.cache.size()) && dist.cache[idx].valid) {
    label += " (" + std::to_string(dist.cache[idx].total.count()) + ")";
  }
  return label;
}

// Generate months list from config date range
static std::vector<std::string> generate_months(const std::string &start_date,
                                                const std::string &end_date) {
  std::vector<std::string> months;
  // Parse dates (YYYY-MM-DD or YYYYMMDD)
  auto parse_month = [](const std::string &date) -> std::string {
    std::string d;
    for (char c : date)
      if (c != '-')
        d += c;
    return d.size() >= 6 ? d.substr(0, 6) : "";
  };

  std::string start_month = parse_month(start_date);
  std::string end_month = parse_month(end_date);
  if (start_month.empty() || end_month.empty())
    return months;

  int y = std::stoi(start_month.substr(0, 4));
  int m = std::stoi(start_month.substr(4, 2));
  while (true) {
    char buf[8];
    snprintf(buf, sizeof(buf), "%04d%02d", y, m);
    std::string month_key = buf;
    if (month_key > end_month)
      break;
    months.push_back(month_key);
    if (++m > 12) {
      m = 1;
      ++y;
    }
  }
  return months;
}

static void RenderWindowControl(DistService *service, SharedData &data,
                                DistUIState &ui) {
  auto &dist = data.dist;

  // Row 1: Compute | Cancel | Status | By selector
  bool can_compute =
      !dist.compute.is_busy() && data.feature.selection.primary_feature_idx >= 0;
  ImGui::BeginDisabled(!can_compute);
  if (ImGui::Button("Compute")) {
    service->RequestCompute();
  }
  ImGui::EndDisabled();

  ImGui::SameLine();
  if (ImGui::Button("Cancel")) {
    dist.cancel();
  }

  ImGui::SameLine();
  size_t done = dist.compute.done.load();
  size_t total = dist.compute.total;
  ImGui::Text("Status: %s (%zu/%zu)", StatusText(dist.compute.status), done,
              total);

  // Row 2: Month slider (always visible from config date range)
  auto months = generate_months(data.config.start_date, data.config.end_date);
  if (!months.empty()) {
    int n_months = static_cast<int>(months.size());
    ui.focus_month_idx = std::clamp(ui.focus_month_idx, 0, n_months - 1);

    std::string label = get_month_label(dist, ui.focus_month_idx, months);
    ImGui::SetNextItemWidth(-1);
    if (ImGui::SliderInt("##FocusMonth", &ui.focus_month_idx, 0, n_months - 1, label.c_str())) {
      dist.input.focus_month_idx = ui.focus_month_idx;
    }
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
  float bar_height = 16.0f;

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
  draw->AddRect(ImVec2(to_x(bound_lo), y_top + 2),
                ImVec2(to_x(bound_hi), y_bot - 2),
                IM_COL32(255, 255, 255, 180), 0.0f, 0, 2.0f);

  // Current value marker (cyan diamond, centered)
  float cv_x = to_x(current_val);
  float cy = (y_top + y_bot) * 0.5f;
  draw->AddQuadFilled(ImVec2(cv_x, cy - 5), ImVec2(cv_x + 4, cy),
                      ImVec2(cv_x, cy + 5), ImVec2(cv_x - 4, cy),
                      IM_COL32(0, 255, 255, 255));

  // Boundary labels - small font, directly on bar
  // All labels drawn at same height, overlaid on the bar
  ImFont *small_font = ImGui::GetFont();
  float small_font_size = ImGui::GetFontSize() * 0.7f; // Small font size
  float label_y = y_top + 2;                           // Slightly below top of bar
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
  // Fixed height for 4 moment bands + header (~320px content)
  float line_h = ImGui::GetTextLineHeightWithSpacing();
  float band_h = line_h * 2.5f; // each band is about 2.5 lines
  float panel_h = line_h * 3 + band_h * 4 + ImGui::GetStyle().WindowPadding.y * 2;
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
      ImGui::Text("比矩稳定，叠加最干净");

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

  if (dist.compute.status != Dist::Compute::Status::Done) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  // Collect moment values based on selected dimension
  std::vector<float> means, vars, skews, kurts;

  // Dimension: 0=MONTH, 1=WEEKDAY, 2=HOUR, 3=ASSETS
  if (selected_dimension == 0) {
    // MONTH dimension
    for (const auto &mc : dist.cache) {
      if (mc.valid && mc.total.count() >= kMinSamples) {
        means.push_back(static_cast<float>(mc.total.mean()));
        vars.push_back(static_cast<float>(mc.total.var()));
        skews.push_back(static_cast<float>(mc.total.skew()));
        kurts.push_back(static_cast<float>(mc.total.kurt()));
      }
    }
  } else if (selected_dimension == 1) {
    // WEEKDAY dimension
    for (size_t wd = 0; wd < dist.global_by_weekday.size() && wd < 7; ++wd) {
      const auto &kll = dist.global_by_weekday[wd];
      if (kll.count() >= kMinSamples) {
        means.push_back(static_cast<float>(kll.mean()));
        vars.push_back(static_cast<float>(kll.var()));
        skews.push_back(static_cast<float>(kll.skew()));
        kurts.push_back(static_cast<float>(kll.kurt()));
      }
    }
  } else if (selected_dimension == 2) {
    // HOUR dimension
    for (size_t h = 0; h < dist.global_by_hour.size() && h < 24; ++h) {
      const auto &kll = dist.global_by_hour[h];
      if (kll.count() >= kMinSamples) {
        means.push_back(static_cast<float>(kll.mean()));
        vars.push_back(static_cast<float>(kll.var()));
        skews.push_back(static_cast<float>(kll.skew()));
        kurts.push_back(static_cast<float>(kll.kurt()));
      }
    }
  } else if (selected_dimension == 3) {
    // ASSETS dimension
    for (size_t a = 0; a < dist.global_by_asset.size(); ++a) {
      const auto &kll = dist.global_by_asset[a];
      if (kll.count() >= kMinSamples) {
        means.push_back(static_cast<float>(kll.mean()));
        vars.push_back(static_cast<float>(kll.var()));
        skews.push_back(static_cast<float>(kll.skew()));
        kurts.push_back(static_cast<float>(kll.kurt()));
      }
    }
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
      focus_month_idx < static_cast<int>(dist.cache.size()) &&
      dist.cache[focus_month_idx].valid) {
    // MONTH dimension: show slider month value
    const auto &mc = dist.cache[focus_month_idx];
    display_mean = static_cast<float>(mc.total.mean());
    display_var = static_cast<float>(mc.total.var());
    display_skew = static_cast<float>(mc.total.skew());
    display_kurt = static_cast<float>(mc.total.kurt());
  } else {
    // Other dimensions: show mean value
    display_mean = mean_mean;
    display_var = mean_var;
    display_skew = mean_skew;
    display_kurt = mean_kurt;
  }

  // Color bands with boundaries
  RenderMomentBand("Mean/均值(1阶普通矩)", display_mean, lower_mean, upper_mean, -1.0f, 1.0f, -0.1f,
                   0.1f, -0.3f, 0.3f);
  ImGui::Spacing();

  RenderMomentBand("Var/方差(2阶中心矩)", display_var, lower_var, upper_var, 0.0f, 3.0f,
                   0.5f, 1.2f, 0.2f, 2.0f);
  ImGui::Spacing();

  RenderMomentBand("Skew/偏度(3阶标准矩)", display_skew, lower_skew, upper_skew, -4.0f, 4.0f,
                   -0.5f, 0.5f, -1.5f, 1.5f);
  ImGui::Spacing();

  RenderMomentBand("Kurt/峰度(4阶标准矩)", display_kurt, lower_kurt, upper_kurt, -3.0f, 15.0f,
                   0.0f, 3.0f, -1.0f, 6.0f);

  ImGui::EndChild();
}

// ============================================================================
// PDF Panels - Multiple views with hover tooltips
// ============================================================================

// Common PDF data structure
struct PDFData {
  const float *x = nullptr;
  const float *y = nullptr;
  size_t n = 0;
  std::string label;
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
        if (pdfs[i].n == 0 || pdfs[i].month_idx < 0)
          continue;
        int dist = std::abs(pdfs[i].month_idx - focus_idx);
        max_dist = std::max(max_dist, dist);
      }
    }

    // Draw all lines with color based on distance to focus
    ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 2.0f);
    for (int i = 0; i < n_items; ++i) {
      if (pdfs[i].n == 0)
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
      ImPlot::PlotLine("##pdf", pdfs[i].x, pdfs[i].y, static_cast<int>(pdfs[i].n));
    }
    ImPlot::PopStyleVar();

    // Detect hover
    if (ImPlot::IsPlotHovered()) {
      ImPlotPoint mouse = ImPlot::GetPlotMousePos();
      ImPlotRect limits = ImPlot::GetPlotLimits();
      double x_range = limits.X.Max - limits.X.Min;
      double y_range = limits.Y.Max - limits.Y.Min;

      for (int i = 0; i < n_items; ++i) {
        if (pdfs[i].n < 2)
          continue;

        for (size_t j = 0; j + 1 < pdfs[i].n; ++j) {
          double nx1 = (pdfs[i].x[j] - limits.X.Min) / x_range;
          double ny1 = (pdfs[i].y[j] - limits.Y.Min) / y_range;
          double nx2 = (pdfs[i].x[j + 1] - limits.X.Min) / x_range;
          double ny2 = (pdfs[i].y[j + 1] - limits.Y.Min) / y_range;
          double nmx = (mouse.x - limits.X.Min) / x_range;
          double nmy = (mouse.y - limits.Y.Min) / y_range;

          double d_sq = point_to_segment_dist_sq(nmx, nmy, nx1, ny1, nx2, ny2);
          if (d_sq < min_dist_sq) {
            min_dist_sq = d_sq;
            hovered_idx = i;
          }
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
  if (hovered_idx >= 0 && min_dist_sq < 0.001) {
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

  // Unified check: wait for compute to finish
  if (dist.compute.status != Dist::Compute::Status::Done || dist.cache.empty()) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  int n_months = static_cast<int>(dist.cache.size());
  std::vector<PDFData> pdfs(n_months);

  for (int m = 0; m < n_months; ++m) {
    const auto &mc = dist.cache[m];
    if (mc.valid && mc.total.count() >= 10) {
      mc.total.exportPDF(pdfs[m].x, pdfs[m].y, pdfs[m].n);
      pdfs[m].label = mc.month;
      pdfs[m].month_idx = m; // Save original month index
    }
  }

  int hovered = RenderPDFPlot("##PDFMonth", pdfs, focus_month_idx, need_autofit, [&](int idx) {
    const auto &kll = dist.cache[idx].total;
    ImGui::Text("%s", pdfs[idx].label.c_str());
    ImGui::Text("n=%zu", kll.count());
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

  // Unified check: wait for compute to finish
  if (dist.compute.status != Dist::Compute::Status::Done || dist.global_by_weekday.size() != 7) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  // Use pre-computed global data (no aggregation)
  const auto &global_weekday = dist.global_by_weekday;

  const char *wd_names[] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};
  std::vector<PDFData> pdfs(7);

  for (size_t wd = 0; wd < 7; ++wd) {
    if (global_weekday[wd].count() >= 10) {
      global_weekday[wd].exportPDF(pdfs[wd].x, pdfs[wd].y, pdfs[wd].n);
      pdfs[wd].label = wd_names[wd];
    }
  }

  int hovered = RenderPDFPlot("##PDFWeekday", pdfs, -1, need_autofit, [&](int idx) {
    const auto &kll = global_weekday[idx];
    ImGui::Text("%s", wd_names[idx]);
    ImGui::Text("n=%zu", kll.count());
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

  // Unified check: wait for compute to finish
  if (dist.compute.status != Dist::Compute::Status::Done || dist.global_by_hour.size() != 24) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  // Use pre-computed global data (no aggregation)
  const auto &global_hour = dist.global_by_hour;

  std::vector<PDFData> pdfs(24);

  for (size_t h = 0; h < 24; ++h) {
    if (global_hour[h].count() >= 10) {
      global_hour[h].exportPDF(pdfs[h].x, pdfs[h].y, pdfs[h].n);
      pdfs[h].label = "Hour " + std::to_string(h);
    }
  }

  int hovered = RenderPDFPlot("##PDFHour", pdfs, -1, need_autofit, [&](int idx) {
    const auto &kll = global_hour[idx];
    ImGui::Text("Hour %d", idx);
    ImGui::Text("n=%zu", kll.count());
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

static void RenderAssetsPDF(const Dist &dist, bool need_autofit,
                            int selected_dimension, int &clicked_dimension,
                            int &hovered_asset_out) {
  ImGui::Text("PDF密度(资产截面)");
  ImGui::Separator();

  if (dist.compute.status != Dist::Compute::Status::Done || dist.global_by_asset.empty()) {
    ImGui::Text("No data");
    return;
  }

  if (need_autofit) {
    ImPlot::SetNextAxesToFit();
  }

  size_t n_assets = dist.global_by_asset.size();
  int hovered_asset = -1;
  double min_dist_sq = 1e9;
  bool plot_clicked = false;

  if (ImPlot::BeginPlot("##AssetsPDF", ImVec2(-1, -1))) {
    ImPlot::SetupAxes(nullptr, nullptr,
                      ImPlotAxisFlags_NoLabel | ImPlotAxisFlags_NoTickLabels,
                      ImPlotAxisFlags_NoLabel | ImPlotAxisFlags_NoTickLabels);

    ImPlotRect limits = ImPlot::GetPlotLimits();
    double x_range = limits.X.Max - limits.X.Min;
    double y_range = limits.Y.Max - limits.Y.Min;

    // ========================================================================
    // Phase 1: Hover detection (both PDF lines and stability dots)
    // ========================================================================
    if (ImPlot::IsPlotHovered()) {
      ImPlotPoint mouse = ImPlot::GetPlotMousePos();
      double nmx = (mouse.x - limits.X.Min) / x_range;
      double nmy = (mouse.y - limits.Y.Min) / y_range;

      // Check stability dots first (top band priority)
      if (dist.stability.valid && !dist.stability.x_norm.empty() && nmy > 0.93f) {
        float best_dist = 0.03f;
        for (size_t a = 0; a < n_assets; ++a) {
          float dx = static_cast<float>(std::abs(nmx - dist.stability.x_norm[a]));
          if (dx < best_dist) {
            best_dist = dx;
            hovered_asset = static_cast<int>(a);
            min_dist_sq = 0.0;
          }
        }
      }

      // Check PDF lines (if not hovering stability)
      if (hovered_asset < 0) {
        for (size_t a = 0; a < n_assets; ++a) {
          const auto &kll = dist.global_by_asset[a];
          if (kll.count() < 10) continue;
          const float *px, *py;
          size_t pn;
          kll.exportPDF(px, py, pn);
          if (pn == 0) continue;

          for (size_t j = 0; j + 1 < pn; ++j) {
            double nx1 = (px[j] - limits.X.Min) / x_range;
            double ny1 = (py[j] - limits.Y.Min) / y_range;
            double nx2 = (px[j + 1] - limits.X.Min) / x_range;
            double ny2 = (py[j + 1] - limits.Y.Min) / y_range;
            double d_sq = point_to_segment_dist_sq(nmx, nmy, nx1, ny1, nx2, ny2);
            if (d_sq < min_dist_sq) {
              min_dist_sq = d_sq;
              hovered_asset = static_cast<int>(a);
            }
          }
        }
      }
    }

    // ========================================================================
    // Phase 2: Draw all PDFs (highlight hovered)
    // ========================================================================
    for (size_t a = 0; a < n_assets; ++a) {
      const auto &kll = dist.global_by_asset[a];
      if (kll.count() < 10) continue;
      const float *x, *y;
      size_t n;
      kll.exportPDF(x, y, n);
      if (n == 0) continue;

      bool is_hovered = (static_cast<int>(a) == hovered_asset);
      float t = static_cast<float>(a) / static_cast<float>(n_assets);
      ImVec4 color = ImPlot::SampleColormap(t, ImPlotColormap_Hot);

      if (is_hovered) {
        // Highlighted: thick white outline + bright color
        ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 5.0f);
        ImPlot::SetNextLineStyle(ImVec4(1, 1, 1, 1), 1.0f);
        ImPlot::PlotLine("##pdf_outline", x, y, static_cast<int>(n));
        ImPlot::PopStyleVar();

        ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 3.0f);
        ImPlot::SetNextLineStyle(ImVec4(0, 1, 1, 1), 1.0f); // cyan
        ImPlot::PlotLine("##pdf_hl", x, y, static_cast<int>(n));
        ImPlot::PopStyleVar();
      } else {
        ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 1.5f);
        ImPlot::SetNextLineStyle(color, 1.0f);
        ImPlot::PlotLine("##pdf", x, y, static_cast<int>(n));
        ImPlot::PopStyleVar();
      }
    }

    // ========================================================================
    // Phase 3: Draw stability scatter (highlight hovered)
    // ========================================================================
    if (dist.stability.valid && !dist.stability.x_norm.empty()) {
      float y_top = static_cast<float>(limits.Y.Max - y_range * 0.02);
      float x_min = static_cast<float>(limits.X.Min);
      float x_max = static_cast<float>(limits.X.Max);

      for (size_t a = 0; a < n_assets; ++a) {
        float x_pos = x_min + dist.stability.x_norm[a] * (x_max - x_min);
        bool is_hovered = (static_cast<int>(a) == hovered_asset);

        if (is_hovered) {
          // Highlighted: large white outline + cyan fill
          ImPlot::SetNextMarkerStyle(ImPlotMarker_Circle, 10, ImVec4(1,1,1,1), 2, ImVec4(1,1,1,1));
          ImPlot::PlotScatter("##stab_outline", &x_pos, &y_top, 1);
          ImPlot::SetNextMarkerStyle(ImPlotMarker_Circle, 8, ImVec4(0,1,1,1), 0, ImVec4(0,1,1,1));
          ImPlot::PlotScatter("##stab_hl", &x_pos, &y_top, 1);
        } else {
          ImVec4 color = ImPlot::SampleColormap(dist.stability.color_t[a], ImPlotColormap_Viridis);
          ImPlot::SetNextMarkerStyle(ImPlotMarker_Circle, 3, color, 0, color);
          ImPlot::PlotScatter("##stability", &x_pos, &y_top, 1);
        }
      }
    }

    // Click detection
    if (ImPlot::IsPlotHovered() && ImGui::IsMouseClicked(0)) {
      plot_clicked = true;
    }

    ImPlot::EndPlot();
  }

  // Click detection
  if (plot_clicked) {
    clicked_dimension = 3; // ASSETS
  }

  // Highlight border if selected
  if (selected_dimension == 3) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetItemRectMin();
    ImVec2 p_max = ImGui::GetItemRectMax();
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 3.0f);
  }

  // Output hovered asset for external display
  hovered_asset_out = (min_dist_sq < 0.001) ? hovered_asset : -1;
}

// ============================================================================
// Hovered Asset Info Panel (displayed in left column)
// ============================================================================

static void RenderHoveredAssetInfo(const Dist &dist, const Asset &asset,
                                   const AssetInfo &asset_info, int hovered_asset) {
  // Use remaining height in parent
  float remaining_height = ImGui::GetContentRegionAvail().y;
  ImGui::BeginChild("HoveredAssetPanel", ImVec2(350, remaining_height), true);

  ImGui::PushFont(ImGui::GetIO().Fonts->Fonts[0]);
  ImGui::TextUnformatted("[资产详情]");
  ImGui::PopFont();
  ImGui::Separator();

  if (hovered_asset < 0 || static_cast<size_t>(hovered_asset) >= asset.items.size() ||
      static_cast<size_t>(hovered_asset) >= dist.global_by_asset.size()) {
    ImGui::TextDisabled("(hover on PDF/dot)");
    ImGui::EndChild();
    return;
  }

  const auto &asset_item = asset.items[hovered_asset];
  const auto &kll = dist.global_by_asset[hovered_asset];

  // Get real-time info from AssetInfo
  std::string exchange_lower = asset_item.exchange;
  std::transform(exchange_lower.begin(), exchange_lower.end(), exchange_lower.begin(), ::tolower);
  std::string stock_key = exchange_lower + "." + asset_item.asset_code;
  const StockInfo *stock_info = asset_info.find_stock_info(stock_key);

  // Asset name and code
  if (stock_info && !stock_info->name.empty()) {
    ImGui::Text("%s (%s.%s)", stock_info->name.c_str(),
                asset_item.asset_code.c_str(), asset_item.exchange.c_str());
  } else {
    ImGui::Text("%s.%s", asset_item.asset_code.c_str(), asset_item.exchange.c_str());
  }

  // Date range + market cap on same line
  auto format_date = [](const std::string &date) -> std::string {
    if (date.size() == 8) return date.substr(0, 4) + "/" + date.substr(4, 2) + "/" + date.substr(6, 2);
    return "--";
  };
  float market_cap = asset_info.calculate_market_cap(stock_key);
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
    if (s.empty()) return "--";
    try {
      char buf[12];
      std::snprintf(buf, sizeof(buf), "%+.1f", std::stof(s));
      return buf;
    } catch (...) { return "--"; }
  };

  if (ImGui::BeginTable("StatsTable", 2, ImGuiTableFlags_SizingFixedFit)) {
    ImGui::TableSetupColumn("Col1", ImGuiTableColumnFlags_WidthFixed, 160);
    ImGui::TableSetupColumn("Col2", ImGuiTableColumnFlags_WidthFixed, 160);

    // Row 1: n | PE
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("n = %zu", kll.count());
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PE = %s", stock_info ? fmt_val(stock_info->peTTM).c_str() : "--");

    // Row 2: Mean | PB
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("Mean = %.4f", kll.mean());
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PB = %s", stock_info ? fmt_val(stock_info->pbMRQ).c_str() : "--");

    // Row 3: Var | PS
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("Var = %.4f", kll.var());
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PS = %s", stock_info ? fmt_val(stock_info->psTTM).c_str() : "--");

    // Row 4: Skew | PCF
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("Skew = %.3f", kll.skew());
    ImGui::TableSetColumnIndex(1);
    ImGui::Text("PCF = %s", stock_info ? fmt_val(stock_info->pcfNcfTTM).c_str() : "--");

    // Row 5: Kurt
    ImGui::TableNextRow();
    ImGui::TableSetColumnIndex(0);
    ImGui::Text("Kurt = %.3f", kll.kurt());

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

  // Auto-start coroutine
  if (!service->is_running()) {
    service->StartCompute(data.gui.Coro(), data);
  }

  auto &dist = data.dist;

  // Trigger autofit when compute just finished
  static auto last_status = dist.compute.status;
  if (last_status != Dist::Compute::Status::Done &&
      dist.compute.status == Dist::Compute::Status::Done) {
    ui.need_autofit = true;
  }
  last_status = dist.compute.status;

  // Integrity (auto-fit height)
  float integrity_height = ImGui::GetTextLineHeightWithSpacing() + ImGui::GetStyle().WindowPadding.y * 1.5;
  ImGui::BeginChild("IntegrityBar", ImVec2(0, integrity_height), true);
  RenderIntegrity(dist.result.integrity);
  ImGui::EndChild();

  // Window control (auto-fit height: 2 rows + padding)
  float ctrl_height = ImGui::GetFrameHeightWithSpacing() * 2 + ImGui::GetStyle().WindowPadding.y * 1.5;
  ImGui::BeginChild("WindowCtrl", ImVec2(0, ctrl_height), true);
  RenderWindowControl(service, data, ui);
  ImGui::EndChild();

  // Track hovered asset across frames
  static int hovered_asset = -1;

  // Main content: Left (Moments + Asset Info) + Right (PDFs)
  float content_height = ImGui::GetContentRegionAvail().y;
  ImGui::Columns(2, "MainCols", true);
  ImGui::SetColumnWidth(0, 350);

  // Left column: Moments Panel (auto-height) + Hovered Asset Info (remaining)
  ImGui::BeginChild("LeftSection", ImVec2(0, content_height), false);
  RenderMomentsPanel(dist, ui.selected_dimension, ui.focus_month_idx);
  RenderHoveredAssetInfo(dist, data.asset, data.asset_info, hovered_asset);
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

  // Bottom: Assets PDF (outputs hovered_asset)
  ImGui::BeginChild("AssetsPDFSection", ImVec2(0, 0), true);
  RenderAssetsPDF(dist, ui.need_autofit, ui.selected_dimension, clicked_dimension, hovered_asset);
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
  service->StopCompute(data.gui.Coro(), data);
}

} // namespace GUI::Features
