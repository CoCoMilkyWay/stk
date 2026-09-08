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
// 维度 (四图对仗): 槽数 / 槽 sketch / 槽标签 —— 焦点滑条、PDF 面板、详情面板共用
//   0 月度漂移 (months)  1 周内偏移 (by_weekday)  2 日内偏移 (by_tod)  3 资产截面 (lines)
// ============================================================================

enum Dim : int { DIM_MONTH = 0,
                 DIM_WEEKDAY = 1,
                 DIM_TOD = 2,
                 DIM_ASSETS = 3 };

static const char *kDimTitles[DistUIState::kDims] = {"PDF密度(月度漂移)", "PDF密度(周内偏移)",
                                                     "PDF密度(日内偏移)", "资产截面(分布密度 + 分位数偏移)"};
static const char *kWeekdayNames[7] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};

// Format month key "YYYYMM" -> "YYYY/MM"
static std::string format_month(const std::string &m) {
  if (m.size() >= 6)
    return m.substr(0, 4) + "/" + m.substr(4, 2);
  return m;
}

// 槽数 (滑条范围). 月取 config 区间月份表 (构建前也有), 资产取资产轴
static int DimCount(const DistUIState &ui, const Asset &asset, int dim) {
  switch (dim) {
  case DIM_MONTH:
    return static_cast<int>(ui.months.size());
  case DIM_WEEKDAY:
    return 7;
  case DIM_TOD:
    return static_cast<int>(kTodBins);
  case DIM_ASSETS:
    return static_cast<int>(asset.items.size());
  }
  return 0;
}

// 聚合维度 (0-2) 的槽 sketch; 数据未就绪 (槽表还没建) 返回 nullptr. 资产维度走 lines 快照
static const KLLcache *DimSlot(const Dist &dist, int dim, int i) {
  switch (dim) {
  case DIM_MONTH:
    return i < static_cast<int>(dist.months.size()) ? &dist.months[i].kll : nullptr;
  case DIM_WEEKDAY:
    return i < static_cast<int>(dist.by_weekday.size()) ? &dist.by_weekday[i] : nullptr;
  case DIM_TOD:
    return i < static_cast<int>(dist.by_tod.size()) ? &dist.by_tod[i] : nullptr;
  }
  return nullptr;
}

// 槽名 (不含样本数): "2024/01" / "Mon" / "09:15" / "000001 平安银行"
static std::string DimName(const DistUIState &ui, const Asset &asset, int dim, int i) {
  switch (dim) {
  case DIM_MONTH:
    return format_month(ui.months[i]);
  case DIM_WEEKDAY:
    return kWeekdayNames[i];
  case DIM_TOD: {
    char buf[8];
    format_time_hm(buf, sizeof(buf), L1_to_Clock(kTodBinStart[i]));
    return buf;
  }
  case DIM_ASSETS:
    return asset.items[i].asset_code + " " + asset.items[i].asset_name;
  }
  return "";
}

// 槽样本数 (0 = 尚无数据)
static uint64_t DimSamples(const Dist &dist, int dim, int i) {
  if (dim == DIM_ASSETS)
    return i < static_cast<int>(dist.lines.size()) ? dist.lines[i].n : 0;
  const KLLcache *kll = DimSlot(dist, dim, i);
  return kll ? kll->totalCount() : 0;
}

// 滑条标签: "槽名 (n)" / "槽名"
static std::string DimLabel(const Dist &dist, const DistUIState &ui, const Asset &asset, int dim, int i) {
  std::string label = DimName(ui, asset, dim, i);
  const uint64_t n = DimSamples(dist, dim, i);
  if (n > 0)
    label += " (" + std::to_string(n) + ")";
  return label;
}

// ============================================================================
// Window Control Panel
// Row 1: Compute | Cancel | Status (n/m)
// Row 2: 通用焦点滑条: 作用于 selected_dimension, 各维度焦点独立记忆
// ============================================================================

static void RenderWindowControl(DistService *service, SharedData &data,
                                DistUIState &ui) {
  auto &dist = data.dist;
  const Dist::Status status = dist.status.load(std::memory_order_acquire);

  // Row 1: Compute | Cancel | Status
  const bool is_l1 = (data.feature.selection.selected_level == 1);
  bool can_compute = status != Dist::Status::Building &&
                     data.feature.selection.primary_feature_idx() >= 0 && is_l1;
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

  // Row 2: 焦点滑条 (月份表来自 config 区间, 缓存; 枚举逻辑与 DistService 共用)
  const std::string months_key = data.config.start_date + "|" + data.config.end_date;
  if (ui.months_key != months_key) {
    ui.months_key = months_key;
    ui.months = dist_enumerate_months(data.config.start_date, data.config.end_date);
  }
  const int dim = ui.selected_dimension;
  const int n = DimCount(ui, data.asset, dim);
  if (n > 0) {
    int &focus = ui.focus[dim];
    focus = std::clamp(focus, 0, n - 1);
    const std::string label = DimLabel(dist, ui, data.asset, dim, focus);
    ImGui::SetNextItemWidth(-1);
    ImGui::SliderInt("##Focus", &focus, 0, n - 1, label.c_str());
    ui.focus_active = ImGui::IsItemActive(); // 按住拖动中 → 该维度的图高亮焦点
  } else {
    ui.focus_active = false;
  }
}

// ============================================================================
// 统一高亮 (四图 + 图4 hover 共用): 目标线 白描边 + cyan 置顶; 高亮模式下其余线压到 kDimAlpha
// ============================================================================

constexpr float kDimAlpha = 0.1f;

static void PlotHighlightLine(const float *x, const float *y, size_t n) {
  ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 5.0f);
  ImPlot::SetNextLineStyle(ImVec4(1, 1, 1, 1), 1.0f);
  ImPlot::PlotLine("##hl_outline", x, y, static_cast<int>(n));
  ImPlot::PopStyleVar();
  ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 3.0f);
  ImPlot::SetNextLineStyle(ImVec4(0, 1, 1, 1), 1.0f);
  ImPlot::PlotLine("##hl", x, y, static_cast<int>(n));
  ImPlot::PopStyleVar();
}

// 该维度是否处于"滑条拖动高亮"模式
static bool FocusHighlighting(const DistUIState &ui, int dim) {
  return ui.focus_active && ui.selected_dimension == dim;
}

// ============================================================================
// Asset Color (资产截面图 顶部散点 + PDF 折线)
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

// 选中维度的边框高亮 (四图共用)
static void DrawSelectedBorder() {
  ImDrawList *draw = ImGui::GetWindowDrawList();
  draw->AddRect(ImGui::GetItemRectMin(), ImGui::GetItemRectMax(), IM_COL32(0, 255, 255, 255), 0.0f, 0, 3.0f);
}

// 聚合维度 (月/周/日内) PDF 面板: 每槽一条线, 离焦点越近越亮 (Hot colormap), 焦点线加粗置顶.
// PDF 零拷贝: 指向 KLL 内部重建缓存, 帧内有效. 点击图 → 选中该维度 (滑条切过去).
static void RenderPDFByDim(const Dist &dist, DistUIState &ui, const Asset &asset, int dim,
                           int &clicked_dimension) {
  char child_id[16];
  snprintf(child_id, sizeof(child_id), "PDFDim%d", dim);
  ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(2, 2));
  ImGui::BeginChild(child_id, ImVec2(0, 0), false);
  ImGui::PopStyleVar();
  ImGui::TextUnformatted(kDimTitles[dim]);
  ImGui::Separator();

  const int n_items = DimCount(ui, asset, dim);
  if (n_items == 0 || DimSlot(dist, dim, 0) == nullptr) {
    ImGui::Text("No data");
    ImGui::EndChild();
    return;
  }

  // 流式: 槽 sketch 随批次增长, count 够了就画
  std::vector<KLLcache::LinePtr> lines(n_items, KLLcache::LinePtr{nullptr, nullptr, 0});
  int max_dist = 0;
  bool any_line = false;
  const int focus = ui.focus[dim];
  for (int i = 0; i < n_items; ++i) {
    const KLLcache *kll = DimSlot(dist, dim, i);
    if (kll && kll->totalCount() >= 10) {
      lines[i] = kll->exportPDF();
      any_line |= lines[i].n > 0;
      max_dist = std::max(max_dist, std::abs(i - focus));
    }
  }

  int hovered_idx = -1;
  double min_dist_sq = 1e9;
  bool plot_clicked = false;

  // 有线才 fit, 并就此消费掉 pending
  const bool need_autofit = ui.fit[dim] && any_line;
  ui.fit[dim] &= !need_autofit;
  if (need_autofit)
    ImPlot::SetNextAxesToFit();

  char plot_id[16];
  snprintf(plot_id, sizeof(plot_id), "##PDF%d", dim);
  if (ImPlot::BeginPlot(plot_id, ImVec2(-1, -1))) {
    ImPlot::SetupAxes(nullptr, nullptr,
                      ImPlotAxisFlags_NoLabel | ImPlotAxisFlags_NoTickLabels,
                      ImPlotAxisFlags_NoLabel | ImPlotAxisFlags_NoTickLabels);

    // 亮度 = 与焦点的距离 (越近越亮); 滑条拖动中: 其余压暗, 焦点线置顶高亮 (松手即恢复)
    const bool dimmed = FocusHighlighting(ui, dim);
    ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 2.0f);
    for (int i = 0; i < n_items; ++i) {
      if (lines[i].n == 0 || (dimmed && i == focus))
        continue;
      const float t = max_dist > 0 ? 1.0f - static_cast<float>(std::abs(i - focus)) / static_cast<float>(max_dist) : 0.5f;
      ImVec4 color = ImPlot::SampleColormap(t, ImPlotColormap_Hot);
      if (dimmed)
        color.w = kDimAlpha;
      ImPlot::SetNextLineStyle(color, 1.0f);
      ImPlot::PlotLine("##pdf", lines[i].x, lines[i].y, static_cast<int>(lines[i].n));
    }
    ImPlot::PopStyleVar();

    if (dimmed && focus < n_items && lines[focus].n > 0)
      PlotHighlightLine(lines[focus].x, lines[focus].y, lines[focus].n);

    // Hover (x 窗口裁剪, 只扫鼠标附近的段)
    if (ImPlot::IsPlotHovered()) {
      const ImPlotPoint mouse = ImPlot::GetPlotMousePos();
      const ImPlotRect limits = ImPlot::GetPlotLimits();
      for (int i = 0; i < n_items; ++i) {
        const double d_sq = nearest_seg_dist_sq(lines[i].x, lines[i].y, lines[i].n, mouse, limits);
        if (d_sq < min_dist_sq) {
          min_dist_sq = d_sq;
          hovered_idx = i;
        }
      }
      plot_clicked = ImGui::IsMouseClicked(0);
    }

    ImPlot::EndPlot();
  }

  // Tooltip (无视觉高亮, 不与焦点线打架)
  if (hovered_idx >= 0 && min_dist_sq < kHoverDistSq) {
    const KLLcache &kll = *DimSlot(dist, dim, hovered_idx);
    ImGui::BeginTooltip();
    ImGui::TextUnformatted(DimName(ui, asset, dim, hovered_idx).c_str());
    ImGui::Text("n=%llu", static_cast<unsigned long long>(kll.totalCount()));
    ImGui::Text("mean=%.4f std=%.4f", kll.mean(), std::sqrt(kll.var()));
    ImGui::Text("skew=%.4f kurt=%.4f", kll.skew(), kll.kurt());
    ImGui::EndTooltip();
  }

  if (plot_clicked)
    clicked_dimension = dim;
  if (ui.selected_dimension == dim)
    DrawSelectedBorder();

  ImGui::EndChild();
}

// ============================================================================
// Assets PDF Plot
// ============================================================================

static void PlotHighlightLine(const Dist::AssetLine &ln) {
  PlotHighlightLine(ln.x.data(), ln.y.data(), ln.n_pts);
}

// 资产截面: 高亮模式 = hover 到线/点, 或滑条按住中 (焦点 = ui.focus[DIM_ASSETS]); 高亮线置顶,
// 其余线全资产压暗作背景; 松手/移开即恢复常态. 输出 hovered_line_out (无 hover → -1,
// 详情面板回落到焦点资产)
static void RenderAssetsPDF(const Dist &dist, const Asset &asset, const AssetInfo &assetinfo,
                            DistUIState &ui, int &clicked_dimension, int &hovered_line_out) {
  // Title with tooltip
  ImGui::TextUnformatted(kDimTitles[DIM_ASSETS]);
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
                "PDF 细线只画固定随机子集 (纯顶点预算, 即全市场无偏抽样)\n"
                "按住顶部滑条 / hover 时高亮该资产, 其余压暗; 松手即恢复");
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

  // W2 偏移散点: 发布侧算好原始值, 每帧只做 max 归一化. 不 gate: 有值的就画, 无值 (<0,
  // 首批参考未就绪) 的跳过, 随批次增量出现
  auto &x_norm = ui.w2_norm;
  x_norm.resize(n_valid);
  float w2_max = 0.0f;
  for (size_t i = 0; i < n_valid; ++i) {
    x_norm[i] = dist.lines[line_indices[i]].w2;
    w2_max = std::max(w2_max, x_norm[i]);
  }
  const float inv = w2_max > 1e-9f ? 1.0f / w2_max : 1.0f;
  for (float &v : x_norm)
    if (v >= 0.0f)
      v *= inv;

  // 到这里 n_valid > 0 (无线的 No data 路径已提前 return, pending 留到下批): fit 并消费
  if (ui.fit[DIM_ASSETS]) {
    ui.fit[DIM_ASSETS] = false;
    ImPlot::SetNextAxesToFit();
  }

  int hovered_idx = -1; // index into line_indices
  double min_dist_sq = 1e9;
  bool plot_clicked = false;

  // 焦点资产: 只在滑条按住时高亮 (松手即恢复常态, 与 hover 同待遇); 无高亮时视为无焦点
  const bool focus_hl = FocusHighlighting(ui, DIM_ASSETS);
  const int focus_asset = focus_hl ? ui.focus[DIM_ASSETS] : -1;
  const bool focus_drawable = focus_asset >= 0 && static_cast<size_t>(focus_asset) < dist.lines.size() &&
                              dist.lines[focus_asset].n_pts > 0;

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
      if (std::abs(mouse_pixels.y - dot_y_screen) < 20.0f) {
        float best_dist_px = 15.0f; // 15 pixel threshold
        for (size_t i = 0; i < n_valid; ++i) {
          if (x_norm[i] < 0.0f)
            continue;
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
          if (!ln.draw && static_cast<int>(line_indices[i]) != focus_asset)
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
    // 底层: 常态只画绘制子集 (0.75); 高亮模式 (hover 或滑条拖动) 全资产压到 kDimAlpha 作背景
    // 置顶: 焦点资产线 (常亮), 再 hover 线 (临时) —— 统一高亮画法, hover 盖在焦点上
    // ========================================================================
    if (hovered_idx >= 0 && min_dist_sq >= kHoverDistSq)
      hovered_idx = -1; // 最近线也够不着: 不算 hover
    const int hovered_line = hovered_idx >= 0 ? static_cast<int>(line_indices[hovered_idx]) : -1;
    const bool dimmed = hovered_line >= 0 || focus_hl;
    ImPlot::PushStyleVar(ImPlotStyleVar_LineWeight, 1.5f);
    for (size_t i = 0; i < n_valid; ++i) {
      const int line = static_cast<int>(line_indices[i]);
      if (line == hovered_line || line == focus_asset)
        continue; // 置顶层单独画
      const auto &ln = dist.lines[line];
      if (!ln.draw && !dimmed)
        continue;
      ImVec4 color = AssetColor(ui, ln.asset);
      color.w = dimmed ? kDimAlpha : 0.75f;
      ImPlot::SetNextLineStyle(color, 1.0f);
      ImPlot::PlotLine("##pdf", ln.x.data(), ln.y.data(), static_cast<int>(ln.n_pts));
    }
    ImPlot::PopStyleVar();
    if (focus_drawable && focus_asset != hovered_line)
      PlotHighlightLine(dist.lines[focus_asset]);
    if (hovered_line >= 0)
      PlotHighlightLine(dist.lines[hovered_line]);

    // ========================================================================
    // Phase 3: Draw W2 offset scatter (overlay on top, scale invariant)
    // 发布侧算好的 W2, 随全局分位逐批收敛; 颜色 = 左栏 [染色] 选项
    // ========================================================================
    {
      ImDrawList *draw = ImPlot::GetPlotDrawList();

      // Get fixed plot pixel boundaries (scale invariant)
      ImVec2 plot_pos = ImPlot::GetPlotPos();
      ImVec2 plot_size = ImPlot::GetPlotSize();
      float y_screen = plot_pos.y + 15.0f; // Fixed: 15px from plot top edge

      // Draw asset dots (无 W2 的跳过; 焦点 / hover 点放大置顶, 与折线同一高亮色)
      int top_a = -1, top_b = -1; // 置顶的 line_indices 下标: 焦点, hover
      for (size_t i = 0; i < n_valid; ++i) {
        if (x_norm[i] < 0.0f)
          continue;
        const int line = static_cast<int>(line_indices[i]);
        if (line == focus_asset)
          top_a = static_cast<int>(i);
        if (static_cast<int>(i) == hovered_idx)
          top_b = static_cast<int>(i);
        if (line == focus_asset || static_cast<int>(i) == hovered_idx)
          continue;
        ImVec2 center(plot_pos.x + x_norm[i] * plot_size.x, y_screen);
        ImU32 color = ImGui::ColorConvertFloat4ToU32(AssetColor(ui, dist.lines[line].asset));
        draw->AddCircleFilled(center, 2.5f, color);
      }
      for (int top : {top_a, top_b}) {
        if (top < 0)
          continue;
        ImVec2 center(plot_pos.x + x_norm[top] * plot_size.x, y_screen);
        draw->AddCircleFilled(center, 5.0f, IM_COL32(255, 255, 255, 255));
        draw->AddCircleFilled(center, 4.0f, IM_COL32(0, 255, 255, 255));
      }
    }

    // Click detection
    if (ImPlot::IsPlotHovered() && ImGui::IsMouseClicked(0)) {
      plot_clicked = true;
    }

    ImPlot::EndPlot();
  }

  // Output: convert draw index to dist.lines index (hovered_idx 已按阈值过滤)
  hovered_line_out = hovered_idx >= 0 ? static_cast<int>(line_indices[hovered_idx]) : -1;

  if (plot_clicked)
    clicked_dimension = DIM_ASSETS;
  if (ui.selected_dimension == DIM_ASSETS)
    DrawSelectedBorder();
}

// ============================================================================
// Asset Info Panel (left column): hover 优先, 否则焦点资产 (滑条)
// ============================================================================

static void RenderAssetInfo(const Dist &dist, const Asset &asset,
                            const AssetInfo &assetinfo, int hovered_line, int focus_asset) {
  // Use remaining height in parent
  float remaining_height = ImGui::GetContentRegionAvail().y;
  ImGui::BeginChild("AssetInfoPanel", ImVec2(350, remaining_height), true);

  ImGui::PushFont(ImGui::GetIO().Fonts->Fonts[0]);
  ImGui::TextUnformatted(hovered_line >= 0 ? "[资产详情: hover]" : "[资产详情: 焦点]");
  ImGui::PopFont();
  ImGui::Separator();

  // 重算后 lines 被整体换新, hover 残留下标可能指向未发布的线 → 一并挡掉
  auto showable = [&](int line) {
    return line >= 0 && static_cast<size_t>(line) < dist.lines.size() &&
           dist.lines[line].n_pts > 0 &&
           static_cast<size_t>(dist.lines[line].asset) < asset.items.size();
  };
  const int line = showable(hovered_line) ? hovered_line : (showable(focus_asset) ? focus_asset : -1);
  if (line < 0) {
    ImGui::TextDisabled("(hover on PDF/dot, or pick asset with the slider)");
    ImGui::EndChild();
    return;
  }

  const auto &ln = dist.lines[line];
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

  // 流式维护 x/y range: epoch 变了 (= 数据变了) 就 autofit 一次, 稳态把缩放还给用户.
  // epoch 跨构建单调 (reset/clear/每批发布都 +1), 换特征时 reset→publish 哪怕发生在两帧
  // 之间也不会被看成"没变" (归零版单批区间每次都停在 1, 会漏), 也不依赖 status 转移.
  // 置位后由各维图在画上数据那帧自行消费 (见 DistUIState::fit)
  const uint64_t cur_epoch = dist.lines_epoch.load(std::memory_order_acquire);
  if (cur_epoch != ui.last_lines_epoch)
    for (bool &f : ui.fit)
      f = true;
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

  // Left column: Color Mode Selector + Asset Info (hover 优先, 否则焦点资产; 剩余空间)
  ImGui::BeginChild("LeftSection", ImVec2(0, content_height), false);
  RenderColorModeSelector(ui, data.asset, data.assetinfo);
  RenderAssetInfo(dist, data.asset, data.assetinfo, ui.hovered_line, ui.focus[DIM_ASSETS]);
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

  // 三个聚合维度: 月度漂移 | 周内偏移 | 日内偏移 (同一渲染, 各自焦点)
  for (int dim = DIM_MONTH; dim <= DIM_TOD; ++dim) {
    if (dim != DIM_MONTH)
      ImGui::NextColumn();
    RenderPDFByDim(dist, ui, data.asset, dim, clicked_dimension);
  }

  ImGui::Columns(1);
  ImGui::PopStyleVar();
  ImGui::EndChild();

  // Bottom: Assets PDF (outputs ui.hovered_line)
  ImGui::BeginChild("AssetsPDFSection", ImVec2(0, 0), true);
  RenderAssetsPDF(dist, data.asset, data.assetinfo, ui, clicked_dimension, ui.hovered_line);
  ImGui::EndChild();

  // 点图 → 选中该维度 (顶部滑条切到它的焦点)
  if (clicked_dimension >= 0) {
    ui.selected_dimension = clicked_dimension;
  }

  ImGui::EndChild();

  ImGui::Columns(1);
}

void StopTabDist(DistService *service, SharedData &data) {
  // 切走 tab: 只中断在跑构建, 内存与 worker 保留 (任务级回收在 OnCollapse 的 Shutdown);
  // 切回时 Idle/Cancelled 自动重算, Done 的结果直接复用
  service->RequestCancel();
  (void)data;
}

} // namespace GUI::Features
