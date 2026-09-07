// TabTimeSeries - Time Series Analysis Tab Implementation
// ============================================================================
//
// 特征时序诊断 (不建模, 只看记忆结构):
//   Step 0: 平稳性检验 - ADF/KPSS (per-asset per-month)
//   Step 1: 频域分析   - 逐日 PSD, 检测周期性成分
//   Step 2: 自相关     - ACF/PACF 曲线 + 置信带
//
// ============================================================================

#include "gui/task_features/ui/TabTimeSeries.hpp"
#include "gui/task_features/services/TimeSeriesService.hpp"
#include "math/spectral/MultiResPSD.hpp"
#include "shared/Asset.hpp"
#include "shared/SharedData.hpp"
#include "shared/TimeSeries.hpp"

#include "imgui.h"
#include "implot.h"

#include <cstdio>

namespace GUI::Features {

// ============================================================================
// Helpers
// ============================================================================

static const char *StatusText(TimeSeries::Compute::Status s) {
  switch (s) {
  case TimeSeries::Compute::Status::Idle:
    return "Idle";
  case TimeSeries::Compute::Status::Loading:
    return "Loading...";
  case TimeSeries::Compute::Status::Building:
    return "Building...";
  case TimeSeries::Compute::Status::Done:
    return "Done";
  case TimeSeries::Compute::Status::Error:
    return "Error";
  case TimeSeries::Compute::Status::Cancelled:
    return "Cancelled";
  }
  return "?";
}

static ImVec4 StatusColor(TimeSeries::Compute::Status s) {
  switch (s) {
  case TimeSeries::Compute::Status::Idle:
  case TimeSeries::Compute::Status::Cancelled:
    return ImVec4(0.5f, 0.5f, 0.5f, 1.0f); // 灰色
  case TimeSeries::Compute::Status::Loading:
    return ImVec4(0.3f, 0.7f, 1.0f, 1.0f); // 蓝色
  case TimeSeries::Compute::Status::Building:
    return ImVec4(1.0f, 0.7f, 0.3f, 1.0f); // 橙黄色
  case TimeSeries::Compute::Status::Done:
    return ImVec4(0.4f, 0.9f, 0.5f, 1.0f); // 亮绿色
  case TimeSeries::Compute::Status::Error:
    return ImVec4(1.0f, 0.3f, 0.3f, 1.0f); // 红色
  }
  return ImVec4(1.0f, 1.0f, 1.0f, 1.0f);
}

// Pass/Fail/Warn indicator with color
static void RenderStatus(bool pass, bool warn = false) {
  if (pass) {
    ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "[PASS]");
  } else if (warn) {
    ImGui::TextColored(ImVec4(0.9f, 0.7f, 0.1f, 1.0f), "[WARN]");
  } else {
    ImGui::TextColored(ImVec4(0.8f, 0.2f, 0.2f, 1.0f), "[FAIL]");
  }
}

// ============================================================================
// Control Panel
// ============================================================================

static void RenderControlPanel([[maybe_unused]] TimeSeriesService *service, SharedData &data) {
  auto &ts = data.timeseries;

  // Help button with tooltip
  ImGui::TextDisabled("(?)");
  if (ImGui::IsItemHovered()) {
    ImGui::BeginTooltip();
    ImGui::PushTextWrapPos(450.0f);
    ImGui::TextUnformatted("特征时序诊断: 平稳性 (能不能直接用) / 频谱 (有没有确定性周期要剥) / 自相关 (记忆多长, 要不要平滑). 特征是因子原料, 这里不建模.");
    ImGui::PopTextWrapPos();
    ImGui::EndTooltip();
  }

  ImGui::SameLine();

  // Status display (auto-compute on step change)
  size_t done = ts.compute.done.load();
  size_t total = ts.compute.total.load();
  ImGui::Text("Status: ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(StatusColor(ts.compute.status), "%s", StatusText(ts.compute.status));
  ImGui::SameLine(0, 0);
  ImGui::Text(" (%zu/%zu)", done, total);
}

// ============================================================================
// Step 0: 平稳性检验 - Tooltip
// ============================================================================

static void RenderStep0Tooltip() {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(450.0f);

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "[平稳性检验]");
  ImGui::Separator();

  ImGui::TextUnformatted("核心问题: 序列的分布:");
  ImGui::TextUnformatted("  1. 是否随时间变化(强平稳)?");
  ImGui::TextUnformatted("  2. 是否有不变的一二阶矩, 和只依赖滞后的协方差(弱平稳: 随机变量线性时不变)?");
  ImGui::Spacing();

  ImGui::TextUnformatted("检验方法:");
  ImGui::TextUnformatted("├─ ADF (Augmented Dickey-Fuller) 单位根检验");
  ImGui::TextUnformatted("│   H0: 存在单位根 (非平稳)");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "│   判断: p < 0.05 → 拒绝H0 → 序列可以弱平稳 ✓");
  ImGui::TextUnformatted("│   注意: ADF对趋势敏感,需先去趋势(detrend)");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("└─ KPSS (Kwiatkowski-Phillips-Schmidt-Shin) 平稳性检验");
  ImGui::TextUnformatted("    H0: 序列平稳");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "    判断: p > 0.05 → 接受H0 → 序列可以弱平稳 ✓");
  ImGui::TextUnformatted("    注意: KPSS对季节性敏感,需先去季节(deseason)");
  ImGui::Spacing();

  ImGui::TextColored(ImVec4(0.7f, 0.9f, 0.7f, 1.0f), "输入平稳性=>梯度稳定性");
  ImGui::TextUnformatted("对于梯度算法: 若输入过程是平稳(时间不变)且遍历(样本量够), 且损失函数相对于参数可微(损失函数选择合理, 梯度局部线性近似)且可积(输入不重尾, 梯度不爆炸), 则对任意参数, 梯度过程是平稳且遍历的随机过程 (Birkhoff ergodic theorem) (梯度噪声可能会延长时间平均梯度收敛到期望的时间)");
  ImGui::Spacing();
  ImGui::TextUnformatted("对于非梯度算法(树模型, 聚类, 非深度概率模型(先验或后验))和数据挖掘(启发,演化算法), 输入'可被平稳化'(模型内部能够平稳化输入)仍然是模型参数收敛的必要条件 (可被平稳化难以被证明, 但是: 输入已经平稳 => 可被平稳化) ");
  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

// ============================================================================
// Step 1: 频域分析 - Tooltip
// ============================================================================

static void RenderStep1Tooltip() {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(450.0f);

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "[频域分析]");
  ImGui::Separator();

  ImGui::TextUnformatted("核心问题: 信号的频率结构是否影响模型表现, 是否能进行后面的分析?");
  ImGui::Spacing();

  ImGui::TextUnformatted("检验指标:");
  ImGui::TextUnformatted("├─ 主频频带(DC<->Nyquist采样频率)检查");
  ImGui::TextUnformatted("│   问题: 信号主频是否在预期频带,不过低?");
  ImGui::TextColored(ImVec4(1.0f, 0.6f, 0.6f, 1.0f),
                     "│   风险: 低频成分 → 可能是趋势误判, 影响模型表现");
  ImGui::TextUnformatted("│   处理: 考虑重采样或更长周期聚合");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("├─ 频谱宽度/连续性");
  ImGui::TextUnformatted("│   目标: 频谱较宽、相对连续、无明显尖峰");
  ImGui::TextUnformatted("│   原理: 满足最大熵原则 → 信息分布均匀 → 可预测性低(信息含量高)");
  ImGui::TextColored(ImVec4(1.0f, 0.6f, 0.6f, 1.0f),
                     "│   风险: 明显波峰 → 存在周期性成分 → 可能是季节误判, 影响模型表现");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("└─ Q因子 (Quality Factor)");
  ImGui::TextUnformatted("    公式: Q = f₀ / Δf  (峰值频率 / 峰宽)");
  ImGui::TextUnformatted("    判断:");
  ImGui::TextColored(ImVec4(0.5f, 1.0f, 0.5f, 1.0f),
                     "      Q < 3  → 宽频谱, 无需剥离周期成分 ✓");
  ImGui::Spacing();

  ImGui::TextColored(ImVec4(0.7f, 0.9f, 0.7f, 1.0f), "频谱形态解读:");
  ImGui::TextUnformatted("  - 平坦频谱 → 白噪声特征,低可预测性");
  ImGui::TextUnformatted("  - 1/f 衰减 → 粉噪声特征, 长记忆过程,考虑ARFIMA");
  ImGui::TextUnformatted("  - 尖峰 → 周期性成分,需识别并剥离");

  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

// ============================================================================
// Step 2: 自相关 - Tooltip
// ============================================================================

static void RenderStep2Tooltip() {
  ImGui::BeginTooltip();
  ImGui::PushTextWrapPos(450.0f);

  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "[自相关]");
  ImGui::Separator();

  ImGui::TextUnformatted("核心问题: 特征的记忆有多长? 组合前要不要平滑 / 差分?");
  ImGui::Spacing();

  ImGui::TextUnformatted("├─ ACF (自相关函数): 与滞后 k 的相关");
  ImGui::TextUnformatted("│   快速衰减 → 短记忆, 可直接用; 缓慢衰减 (幂律) → 长记忆, 考虑分数阶差分");
  ImGui::TextUnformatted("│   全在置信带内 → 近似白噪声, 无可用时序结构");
  ImGui::TextUnformatted("│");
  ImGui::TextUnformatted("└─ PACF (偏自相关): 控制中间滞后后的直接相关");
  ImGui::TextUnformatted("    显著的 lag 数 ≈ 有效回看窗口");
  ImGui::Spacing();

  ImGui::TextUnformatted("曲线 = 全资产平均; 置信带 = max(1.96/√n, 0.05)");

  ImGui::PopTextWrapPos();
  ImGui::EndTooltip();
}

// ============================================================================
// Left Panel: Step List with Results
// ============================================================================

static void RenderStepPanel(SharedData &data, TimeSeriesUIState &ui) {
  auto &ts = data.timeseries;
  bool can_switch = !ts.compute.is_busy();

  // Step 0: 平稳性检验
  ImGui::BeginChild("Step0Child", ImVec2(0, 80), true);
  {
    ImGui::Text("Step 0: 平稳性检验");
    ImGui::SameLine();
    ImGui::TextDisabled("(?)");
    if (ImGui::IsItemHovered())
      RenderStep0Tooltip();

    if (ImGui::IsWindowHovered() && ImGui::IsMouseClicked(0) && can_switch) {
      ui.selected_step = 0;
    }

    // 计算pass rate
    const auto &cache = ts.stationarity_cache;
    if (!cache.empty()) {
      size_t adf_pass = 0, kpss_pass = 0, valid = 0;
      for (const auto &mc : cache) {
        for (const auto &cell : mc.by_asset) {
          if (cell.valid) {
            ++valid;
            if (cell.adf_pass)
              ++adf_pass;
            if (cell.kpss_pass)
              ++kpss_pass;
          }
        }
      }
      float adf_rate = valid > 0 ? 100.0f * adf_pass / valid : 0.0f;
      float kpss_rate = valid > 0 ? 100.0f * kpss_pass / valid : 0.0f;

      ImGui::Text("  ADF:  %.1f%%", adf_rate);
      ImGui::SameLine();
      RenderStatus(adf_rate >= 95.0f);

      ImGui::Text("  KPSS: %.1f%%", kpss_rate);
      ImGui::SameLine();
      RenderStatus(kpss_rate >= 95.0f);
    } else {
      ImGui::TextDisabled("  (no data)");
    }
  }
  if (ui.selected_step == 0) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetWindowPos();
    ImVec2 p_max = ImVec2(p_min.x + ImGui::GetWindowWidth(),
                          p_min.y + ImGui::GetWindowHeight());
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 2.0f);
  }
  ImGui::EndChild();

  // Step 1: 频域分析
  ImGui::BeginChild("Step1Child", ImVec2(0, 120), true);
  {
    ImGui::Text("Step 1: 频域分析");
    ImGui::SameLine();
    ImGui::TextDisabled("(?)");
    if (ImGui::IsItemHovered())
      RenderStep1Tooltip();

    if (ImGui::IsWindowHovered() && ImGui::IsMouseClicked(0) && can_switch) {
      ui.selected_step = 1;
    }

    if (ts.step1_frequency.valid) {
      ImGui::Text("  秒级: %.1f%%", ts.step1_frequency.sec_power_ratio * 100);
      ImGui::Text("  分钟级: %.1f%%", ts.step1_frequency.min_power_ratio * 100);
      ImGui::Text("  小时级: %.1f%%", ts.step1_frequency.hour_power_ratio * 100);
      ImGui::Text("  DC: %.1f%%", ts.step1_frequency.dc_power_ratio * 100);
    } else {
      ImGui::TextDisabled("  (no data)");
    }
  }
  if (ui.selected_step == 1) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetWindowPos();
    ImVec2 p_max = ImVec2(p_min.x + ImGui::GetWindowWidth(),
                          p_min.y + ImGui::GetWindowHeight());
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 2.0f);
  }
  ImGui::EndChild();

  // Step 2: 自相关
  ImGui::BeginChild("Step2Child", ImVec2(0, 80), true);
  {
    ImGui::Text("Step 2: 自相关");
    ImGui::SameLine();
    ImGui::TextDisabled("(?)");
    if (ImGui::IsItemHovered())
      RenderStep2Tooltip();

    if (ImGui::IsWindowHovered() && ImGui::IsMouseClicked(0) && can_switch) {
      ui.selected_step = 2;
    }

    const auto &ac = ts.step2_acf;
    if (ac.valid && ac.acf_values.size() > 1) {
      ImGui::Text("  ACF(1):  %.3f", ac.acf_values[1]);
      ImGui::Text("  PACF(1): %.3f", ac.pacf_values[1]);
    } else {
      ImGui::TextDisabled("  (no data)");
    }
  }
  if (ui.selected_step == 2) {
    ImDrawList *draw = ImGui::GetWindowDrawList();
    ImVec2 p_min = ImGui::GetWindowPos();
    ImVec2 p_max = ImVec2(p_min.x + ImGui::GetWindowWidth(),
                          p_min.y + ImGui::GetWindowHeight());
    draw->AddRect(p_min, p_max, IM_COL32(0, 255, 255, 255), 0.0f, 0, 2.0f);
  }
  ImGui::EndChild();
}

// ============================================================================
// Right Panel: Visualization (Step-dependent)
// ============================================================================

// ADF: want p < 0.05, deviation = max(0, p - 0.05)
static ImU32 GetADFColor(const TimeSeries::StationarityCell &cell) {
  if (!cell.valid)
    return IM_COL32(60, 60, 60, 255);

  float dev = std::max(0.0f, cell.adf_pvalue - 0.05f);
  float t = std::min(1.0f, dev / 0.05f);

  uint8_t r = static_cast<uint8_t>(40 + t * 160);
  uint8_t g = static_cast<uint8_t>(180 - t * 120);
  return IM_COL32(r, g, 40, 255);
}

// KPSS: want p > 0.05, deviation = max(0, 0.05 - p)
static ImU32 GetKPSSColor(const TimeSeries::StationarityCell &cell) {
  if (!cell.valid)
    return IM_COL32(60, 60, 60, 255);

  float dev = std::max(0.0f, 0.05f - cell.kpss_pvalue);
  float t = std::min(1.0f, dev / 0.05f);

  uint8_t r = static_cast<uint8_t>(40 + t * 160);
  uint8_t g = static_cast<uint8_t>(180 - t * 120);
  return IM_COL32(r, g, 40, 255);
}

static void RenderStep0Plot(const TimeSeries &ts, const Asset &asset,
                            bool /*need_autofit*/) {
  const auto &cache = ts.stationarity_cache;
  if (cache.empty()) {
    ImGui::TextDisabled("No data - run compute first");
    return;
  }

  const size_t n_months = cache.size();
  const size_t n_assets = cache[0].n_assets;

  if (n_assets == 0 || n_months == 0) {
    ImGui::TextDisabled("No data available");
    return;
  }

  // 颜色说明
  ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "绿");
  ImGui::SameLine(0, 0);
  ImGui::Text("=pass  ");
  ImGui::SameLine();
  ImGui::TextColored(ImVec4(0.8f, 0.2f, 0.2f, 1.0f), "红");
  ImGui::SameLine(0, 0);
  ImGui::Text("=fail (ADF: p<0.05, KPSS: p>0.05)");
  ImGui::Separator();

  ImVec2 avail = ImGui::GetContentRegionAvail();
  float half_width = (avail.x - 20.0f) * 0.5f; // 中间留点间隔
  float heatmap_width = half_width - 60.0f;    // 留给label的空间

  float cell_w = std::max(4.0f, heatmap_width / static_cast<float>(n_months));
  float cell_h = std::max(2.0f, (avail.y - 50.0f) / static_cast<float>(n_assets));
  cell_w = std::min(cell_w, 20.0f);
  cell_h = std::min(cell_h, 8.0f);

  ImDrawList *draw_list = ImGui::GetWindowDrawList();
  ImVec2 base_pos = ImGui::GetCursorScreenPos();

  int hovered_month = -1;
  int hovered_asset = -1;
  int hovered_type = -1; // 0=ADF, 1=KPSS

  // Lambda: 绘制单个热力图
  auto DrawHeatmap = [&](float offset_x, const char *title,
                         ImU32 (*color_func)(const TimeSeries::StationarityCell &),
                         int type_id) {
    float title_x = base_pos.x + offset_x + 50.0f;
    draw_list->AddText(ImVec2(title_x, base_pos.y), IM_COL32(200, 200, 200, 255), title);

    // Month labels
    for (size_t m = 0; m < n_months; ++m) {
      if (m % 6 == 0) {
        const std::string &month = cache[m].month;
        std::string label = month.substr(2, 2) + "/" + month.substr(4, 2);
        float x = base_pos.x + offset_x + 50.0f + m * cell_w;
        draw_list->AddText(ImVec2(x, base_pos.y + 15.0f), IM_COL32(150, 150, 150, 255),
                           label.c_str());
      }
    }

    float start_y = base_pos.y + 30.0f;
    for (size_t a = 0; a < n_assets; ++a) {
      float y = start_y + a * cell_h;
      for (size_t m = 0; m < n_months; ++m) {
        if (a >= cache[m].by_asset.size())
          continue;
        float x = base_pos.x + offset_x + 50.0f + m * cell_w;
        const auto &cell = cache[m].by_asset[a];
        ImU32 color = color_func(cell);

        ImVec2 p_min(x, y);
        ImVec2 p_max(x + cell_w - 1.0f, y + cell_h - 1.0f);
        draw_list->AddRectFilled(p_min, p_max, color);

        ImVec2 mouse = ImGui::GetMousePos();
        if (mouse.x >= p_min.x && mouse.x < p_max.x &&
            mouse.y >= p_min.y && mouse.y < p_max.y) {
          hovered_month = static_cast<int>(m);
          hovered_asset = static_cast<int>(a);
          hovered_type = type_id;
          draw_list->AddRect(p_min, p_max, IM_COL32(255, 255, 255, 255), 0.0f, 0, 2.0f);
        }
      }
    }
  };

  // 左侧: ADF热力图
  DrawHeatmap(0.0f, "ADF (H0: non-stationary, p<0.05 pass)", GetADFColor, 0);
  // 右侧: KPSS热力图
  DrawHeatmap(half_width + 10.0f, "KPSS (H0: stationary, p>0.05 pass)", GetKPSSColor, 1);

  ImVec2 canvas_size(avail.x, cell_h * n_assets + 40.0f);
  ImGui::Dummy(canvas_size);

  // Tooltip
  if (hovered_month >= 0 && hovered_asset >= 0 &&
      static_cast<size_t>(hovered_month) < cache.size() &&
      static_cast<size_t>(hovered_asset) < cache[hovered_month].by_asset.size()) {
    const auto &cell = cache[hovered_month].by_asset[hovered_asset];
    const std::string &month = cache[hovered_month].month;

    ImGui::BeginTooltip();

    if (static_cast<size_t>(hovered_asset) < asset.items.size()) {
      const auto &item = asset.items[hovered_asset];
      ImGui::Text("%s %s", item.asset_code.c_str(), item.asset_name.c_str());
    } else {
      ImGui::Text("Asset #%d", hovered_asset);
    }
    ImGui::Text("%s/%s", month.substr(0, 4).c_str(), month.substr(4, 2).c_str());
    ImGui::Separator();

    if (cell.valid) {
      if (hovered_type == 0) {
        // ADF tooltip
        if (cell.adf_pass) {
          ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "ADF p=%.3f [PASS]", cell.adf_pvalue);
        } else {
          ImGui::TextColored(ImVec4(0.8f, 0.2f, 0.2f, 1.0f), "ADF p=%.3f [FAIL]", cell.adf_pvalue);
        }
      } else {
        // KPSS tooltip
        if (cell.kpss_pass) {
          ImGui::TextColored(ImVec4(0.2f, 0.8f, 0.2f, 1.0f), "KPSS p=%.3f [PASS]", cell.kpss_pvalue);
        } else {
          ImGui::TextColored(ImVec4(0.8f, 0.2f, 0.2f, 1.0f), "KPSS p=%.3f [FAIL]", cell.kpss_pvalue);
        }
      }
      ImGui::Text("Samples: %zu", cell.n_samples);
    } else {
      ImGui::TextDisabled("Invalid (insufficient samples)");
    }

    ImGui::EndTooltip();
  }
}

static void RenderStep1Plot(TimeSeries &ts, const Asset &asset, bool need_autofit) {
  // 框选缩放模式 (左键框选, 右键拖拽)
  static bool input_configured = false;
  if (!input_configured) {
    ImPlot::MapInputReverse();
    input_configured = true;
  }

  auto &psd = ts.psd_cache;
  if (!psd.valid || psd.render_data.empty()) {
    ImGui::TextDisabled("No data - run compute first");
    return;
  }

  const size_t valid_days = psd.valid_days();
  if (valid_days == 0) {
    ImGui::TextDisabled("No valid days");
    return;
  }

  constexpr size_t N_BINS = TimeSeries::PSDHeatmap::N_SCALE_BINS;

  // 默认选中第一天
  if (psd.selected_day < 0) {
    psd.selected_day = 0;
  }

  // 刻度标签指针 (低频在上，高频在下)
  static std::vector<const char *> tick_ptrs;
  static std::vector<double> heatmap_tick_pos;
  tick_ptrs.resize(psd.tick_labels.size());
  heatmap_tick_pos.resize(psd.tick_positions.size());
  for (size_t i = 0; i < psd.tick_labels.size(); ++i) {
    tick_ptrs[i] = psd.tick_labels[i].c_str();
    heatmap_tick_pos[i] = psd.tick_positions[i] + 0.5; // 格子中心
  }

  // Compact 布局: 固定两个图
  float avail_height = ImGui::GetContentRegionAvail().y;
  float heatmap_height = avail_height * 0.55f;
  float lineplot_height = avail_height - heatmap_height - 4.0f;

  // 可拖拽竖线位置
  static double drag_x = 0.5;
  if (psd.selected_day >= 0 && static_cast<size_t>(psd.selected_day) < valid_days) {
    drag_x = psd.selected_day + 0.5;
  }

  // ========== 图1: 热力图 ==========
  // 低频(DC)在上，高频(2s)在下
  // 默认范围: X从first_valid_day开始，Y从default_y_start到N_BINS
  static bool need_fit = true;
  if (need_autofit)
    need_fit = true;

  ImPlot::PushColormap(ImPlotColormap_Viridis);

  if (ImPlot::BeginPlot("##PSDHeatmap", ImVec2(-1, heatmap_height), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes("Date", "Period");

    if (need_fit) {
      // 默认X从first_valid_day开始
      ImPlot::SetupAxisLimits(ImAxis_X1,
                              static_cast<double>(psd.first_valid_day),
                              static_cast<double>(valid_days), ImGuiCond_Always);
      // 默认Y从default_y_start到N_BINS (低频在上)
      ImPlot::SetupAxisLimits(ImAxis_Y1,
                              static_cast<double>(psd.default_y_start),
                              static_cast<double>(N_BINS), ImGuiCond_Always);
      need_fit = false;
    }

    if (!heatmap_tick_pos.empty()) {
      ImPlot::SetupAxisTicks(ImAxis_Y1, heatmap_tick_pos.data(),
                             static_cast<int>(heatmap_tick_pos.size()),
                             tick_ptrs.data());
    }

    // 热力图 (不反转bounds，低频在上)
    ImPlot::PlotHeatmap("##psd",
                        psd.render_data.data(),
                        static_cast<int>(N_BINS),
                        static_cast<int>(valid_days),
                        psd.scale_min, psd.scale_max,
                        nullptr,
                        ImPlotPoint(0, 0),
                        ImPlotPoint(static_cast<double>(valid_days), static_cast<double>(N_BINS)));

    // 可拖拽竖线选择日期
    if (ImPlot::DragLineX(0, &drag_x, ImVec4(1, 0.5f, 0, 1), 2.0f)) {
      int new_day = static_cast<int>(drag_x);
      new_day = std::clamp(new_day, 0, static_cast<int>(valid_days) - 1);
      psd.selected_day = new_day;
      drag_x = new_day + 0.5; // snap to center
    }

    // Tooltip
    // PlotHeatmap: row 0 在顶部(Y=N_BINS), row N-1 在底部(Y=0)
    // render_data: row r 存储 bin (N_BINS - 1 - r)
    // 所以 Y 位置对应 bin = floor(Y), row = N_BINS - 1 - bin
    if (ImPlot::IsPlotHovered()) {
      ImPlotPoint mouse = ImPlot::GetPlotMousePos();
      int day_i = static_cast<int>(mouse.x);
      int scale_bin = static_cast<int>(mouse.y);

      if (day_i >= 0 && static_cast<size_t>(day_i) < valid_days &&
          scale_bin >= 0 && scale_bin < static_cast<int>(N_BINS)) {

        size_t scale_idx = static_cast<size_t>(scale_bin);
        size_t day_idx = psd.valid_indices[day_i];
        const std::string &date = psd.dates[day_idx];
        // row = N_BINS - 1 - bin
        size_t row = N_BINS - 1 - scale_idx;
        float power_log = psd.render_data[row * valid_days + day_i];

        ImGui::BeginTooltip();
        ImGui::Text("%s/%s/%s", date.substr(0, 4).c_str(),
                    date.substr(4, 2).c_str(), date.substr(6, 2).c_str());
        ImGui::Text("Scale: %s", math::spectral::get_scale_label(scale_idx));
        ImGui::Text("Power: %.2f", power_log);
        ImGui::EndTooltip();
      }

      // 双击时恢复默认范围
      if (ImGui::IsMouseDoubleClicked(0)) {
        need_fit = true;
      }
    }

    ImPlot::EndPlot();
  }
  ImPlot::PopColormap();

  // ========== 图2: Per-asset 功率谱线图 ==========
  size_t day_idx = psd.valid_indices[psd.selected_day];
  const std::string &date = psd.dates[day_idx];

  // 图2固定范围: min=-1, max=3
  static std::vector<float> psd_log;
  static std::vector<float> day_avg;
  psd_log.resize(N_BINS);
  day_avg.resize(N_BINS);

  // 计算跨资产平均
  std::fill(day_avg.begin(), day_avg.end(), 0.0f);
  size_t valid_asset_count = 0;
  for (size_t a = 0; a < psd.n_assets; ++a) {
    const float *src = psd.asset_day_psd(day_idx, a);
    bool has_data = false;
    for (size_t k = 0; k < N_BINS; ++k) {
      if (src[k] > 0) {
        has_data = true;
        break;
      }
    }
    if (has_data) {
      ++valid_asset_count;
      for (size_t k = 0; k < N_BINS; ++k) {
        day_avg[k] += src[k];
      }
    }
  }
  if (valid_asset_count > 0) {
    float inv = 1.0f / valid_asset_count;
    for (size_t k = 0; k < N_BINS; ++k) {
      day_avg[k] *= inv;
    }
  }
  const float *total = day_avg.data();

  char title[64];
  snprintf(title, sizeof(title), "%s/%s/%s##Line",
           date.substr(0, 4).c_str(), date.substr(4, 2).c_str(), date.substr(6, 2).c_str());

  if (ImPlot::BeginPlot(title, ImVec2(-1, lineplot_height), ImPlotFlags_NoLegend)) {
    ImPlot::SetupAxes("Period", "Power");
    ImPlot::SetupAxisLimits(ImAxis_X1, 0, static_cast<double>(N_BINS), ImGuiCond_Once);
    ImPlot::SetupAxisLimits(ImAxis_Y1, -1.0f, 3.0f, ImGuiCond_Always);

    if (!psd.tick_positions.empty()) {
      ImPlot::SetupAxisTicks(ImAxis_X1, psd.tick_positions.data(),
                             static_cast<int>(psd.tick_positions.size()),
                             tick_ptrs.data());
    }

    // 每个 asset (浅色细线) - 使用 psd.plot_x
    for (size_t a = 0; a < psd.n_assets; ++a) {
      const float *src = psd.asset_day_psd(day_idx, a);
      for (size_t k = 0; k < N_BINS; ++k) {
        float v = src[k];
        psd_log[k] = (v > 1e-20f) ? std::log10(v) : -20.0f;
      }
      ImPlot::SetNextLineStyle(ImVec4(0.5f, 0.5f, 0.8f, 0.15f), 1.0f);
      char lbl[16];
      snprintf(lbl, sizeof(lbl), "##a%zu", a);
      ImPlot::PlotLine(lbl, psd.plot_x.data(), psd_log.data(), static_cast<int>(N_BINS));
    }

    // 总功率谱 (粗线)
    for (size_t k = 0; k < N_BINS; ++k) {
      float v = total[k];
      psd_log[k] = (v > 1e-20f) ? std::log10(v) : -20.0f;
    }
    ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.8f, 0.2f, 1.0f), 2.0f);
    ImPlot::PlotLine("Total", psd.plot_x.data(), psd_log.data(), static_cast<int>(N_BINS));

    // Tooltip
    if (ImPlot::IsPlotHovered()) {
      ImPlotPoint mouse = ImPlot::GetPlotMousePos();
      int display_x = static_cast<int>(mouse.x + 0.5);
      display_x = std::clamp(display_x, 0, static_cast<int>(N_BINS) - 1);
      size_t scale_idx = static_cast<size_t>(display_x);

      float total_log = (total[scale_idx] > 1e-20f) ? std::log10(total[scale_idx]) : -20.0f;

      ImGui::BeginTooltip();
      ImGui::Text("Scale: %s", math::spectral::get_scale_label(scale_idx));
      ImGui::Text("Total: %.2f", total_log);

      // Top 3 assets
      std::vector<std::pair<float, size_t>> ranked;
      ranked.reserve(psd.n_assets);
      for (size_t a = 0; a < psd.n_assets; ++a) {
        ranked.emplace_back(psd.asset_day_psd(day_idx, a)[scale_idx], a);
      }
      std::partial_sort(ranked.begin(),
                        ranked.begin() + std::min<size_t>(3, ranked.size()),
                        ranked.end(),
                        [](auto &a, auto &b) { return a.first > b.first; });

      ImGui::Separator();
      for (size_t i = 0; i < std::min<size_t>(3, ranked.size()); ++i) {
        size_t a = ranked[i].second;
        float p = ranked[i].first;
        float pl = (p > 1e-20f) ? std::log10(p) : -20.0f;
        const char *code = (a < asset.items.size()) ? asset.items[a].asset_code.c_str() : "?";
        ImGui::Text("%s: %.2f", code, pl);
      }
      ImGui::EndTooltip();
    }

    ImPlot::EndPlot();
  }
}

static void RenderStep2Plot(const TimeSeries &ts, bool need_autofit) {
  ImGui::Text("自相关 (ACF / PACF, 全资产平均, 置信带 ±%.3f)", ts.step2_acf.confidence_bound);
  ImGui::Separator();

  if (!ts.step2_acf.valid || ts.step2_acf.acf_values.empty()) {
    ImGui::TextDisabled("No data - run compute first");
    return;
  }

  float height = (ImGui::GetContentRegionAvail().y - ImGui::GetTextLineHeightWithSpacing()) * 0.5f;

  if (need_autofit) {
    ImPlot::SetNextAxesToFit();
  }
  if (ImPlot::BeginPlot("##ACFPlot", ImVec2(-1, height))) {
    ImPlot::SetupAxes("Lag", "ACF");

    const auto &acf = ts.step2_acf.acf_values;
    int n = static_cast<int>(acf.size());

    float cb = ts.step2_acf.confidence_bound;
    float cb_neg = -cb;
    ImPlot::PlotInfLines("##cb_pos", &cb, 1, ImPlotInfLinesFlags_Horizontal);
    ImPlot::PlotInfLines("##cb_neg", &cb_neg, 1, ImPlotInfLinesFlags_Horizontal);

    std::vector<float> lags(n);
    for (int i = 0; i < n; ++i)
      lags[i] = static_cast<float>(i);
    ImPlot::PlotBars("ACF", lags.data(), acf.data(), n, 0.4);

    ImPlot::EndPlot();
  }

  if (need_autofit) {
    ImPlot::SetNextAxesToFit();
  }
  if (ImPlot::BeginPlot("##PACFPlot", ImVec2(-1, height))) {
    ImPlot::SetupAxes("Lag", "PACF");

    const auto &pacf = ts.step2_acf.pacf_values;
    int n = static_cast<int>(pacf.size());

    float cb = ts.step2_acf.confidence_bound;
    float cb_neg = -cb;
    ImPlot::PlotInfLines("##cb_pos", &cb, 1, ImPlotInfLinesFlags_Horizontal);
    ImPlot::PlotInfLines("##cb_neg", &cb_neg, 1, ImPlotInfLinesFlags_Horizontal);

    std::vector<float> lags(n);
    for (int i = 0; i < n; ++i)
      lags[i] = static_cast<float>(i);
    ImPlot::PlotBars("PACF", lags.data(), pacf.data(), n, 0.4);

    ImPlot::EndPlot();
  }
}

static void RenderVisualizationPanel(SharedData &data, TimeSeriesUIState &ui) {
  auto &ts = data.timeseries;

  switch (ui.selected_step) {
  case 0:
    RenderStep0Plot(ts, data.asset, ui.need_autofit);
    break;
  case 1:
    RenderStep1Plot(ts, data.asset, ui.need_autofit);
    break;
  case 2:
    RenderStep2Plot(ts, ui.need_autofit);
    break;
  default:
    ImGui::TextDisabled("Invalid step");
    break;
  }
}

// ============================================================================
// Main Render
// ============================================================================

void RenderTabTimeSeries(TimeSeriesService *service, SharedData &data,
                         TimeSeriesUIState &ui) {
  // Auto-start coroutine
  if (!service->is_running()) {
    service->StartCompute(data.coromgr, data);
  }

  auto &ts = data.timeseries;

  // Trigger autofit when compute just finished
  static auto last_status = ts.compute.status;
  if (last_status != TimeSeries::Compute::Status::Done &&
      ts.compute.status == TimeSeries::Compute::Status::Done) {
    ui.need_autofit = true;
  }
  last_status = ts.compute.status;

  // Control panel
  float ctrl_height =
      ImGui::GetFrameHeightWithSpacing() + ImGui::GetStyle().WindowPadding.y * 2;
  ImGui::BeginChild("ControlBar", ImVec2(0, ctrl_height), true);
  RenderControlPanel(service, data);
  ImGui::EndChild();

  // Main content: Left (Steps) + Right (Visualization)
  float content_height = ImGui::GetContentRegionAvail().y;
  ImGui::Columns(2, "MainCols", true);
  ImGui::SetColumnWidth(0, 350);

  // Left: Step list
  ImGui::BeginChild("StepPanel", ImVec2(0, content_height), false);
  RenderStepPanel(data, ui);
  ImGui::EndChild();

  ImGui::NextColumn();

  // Right: Visualization
  ImGui::BeginChild("VizPanel", ImVec2(0, content_height), true);
  RenderVisualizationPanel(data, ui);
  ImGui::EndChild();

  ImGui::Columns(1);

  // Clear autofit flag
  ui.need_autofit = false;
}

void StopTabTimeSeries(TimeSeriesService *service, SharedData &data) {
  // 只在计算进行中时才取消，避免覆盖 Done 状态
  if (data.timeseries.compute.is_busy()) {
    data.timeseries.cancel();
  }
  if (service && service->is_running()) {
    service->StopCompute(data.coromgr, data);
  }
}

} // namespace GUI::Features
