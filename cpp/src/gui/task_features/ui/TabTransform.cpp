#include "gui/task_features/ui/TabTransform.hpp"
#include "gui/task_features/services/TransformService.hpp"
#include "gui/task_features/ui/Common.hpp"
#include "misc/profiler.hpp"
#include "shared/Asset.hpp"
#include "shared/Feature.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "implot.h"

#include <algorithm>
#include <array>
#include <cfloat>
#include <cmath>
#include <cstdio>
#include <vector>

namespace GUI::Features {

using analysis::DayPSD;
using analysis::kVR;

// ============================================================================
// Helpers
// ============================================================================

// 通过率 → 颜色: 红 (0) → 黄 (0.5) → 绿 (1); < 0 = 无数据 (灰)
static ImU32 RateColor(float r) {
  if (r < 0.0f)
    return IM_COL32(70, 70, 70, 255);
  const float g = std::min(1.0f, 2.0f * r);
  const float rd = std::min(1.0f, 2.0f * (1.0f - r));
  return IM_COL32(static_cast<int>(200 * rd + 40), static_cast<int>(200 * g + 40), 60, 255);
}

static ImVec4 AssetColor(size_t idx, size_t total) {
  const float t = total > 1 ? static_cast<float>(idx) / static_cast<float>(total - 1) : 0.0f;
  return ImPlot::SampleColormap(t, ImPlotColormap_Spectral);
}

// 通用参数 slider (math::Operator 元数据驱动)
static bool RenderOperatorParams(math::Operator &op, const char *suffix) {
  bool changed = false;
  for (size_t i = 0; i < op.param_count; ++i) {
    const auto &m = op.meta[i];
    ImGui::SameLine();
    ImGui::SetNextItemWidth(90);
    char label[48];
    snprintf(label, sizeof(label), "%s##%s%zu", m.name, suffix, i);
    changed |= ImGui::SliderFloat(label, &op[i], m.min_val, m.max_val, "%.2f");
  }
  return changed;
}

// ============================================================================
// Row 1: Compute | Cancel | Status | 特征 | 完整性
// ============================================================================

static void RenderControl(TransformService *service, SharedData &data, TransformUIState &ui) {
  const auto &sel = data.feature.selection;
  const int action = RenderStreamControl(data.transform, data, "天");
  if (action > 0)
    service->RequestCompute(data, ui.params, ui.focus);
  else if (action < 0)
    service->RequestCancel();

  ImGui::SameLine(0, 20);
  if (sel.primary_feature_idx() >= 0 && sel.selected_level >= 0 && sel.selected_level < static_cast<int>(LEVEL_COUNT)) {
    const auto &meta = data.feature.metadata.features[sel.selected_level];
    if (sel.primary_feature_idx() < static_cast<int>(meta.size()))
      ImGui::Text("L%d %s", sel.selected_level, meta[sel.primary_feature_idx()].code);
  } else {
    ImGui::TextDisabled("无特征");
  }
}

// 链路账目 (输入 → 链末输出), 与 Common 的值账目 RenderIntegrity 语义不同
static void RenderChainIntegrity(const Transform::Integrity &it) {
  if (it.n_total == 0) {
    ImGui::TextDisabled("完整性: --");
    return;
  }
  const float in_pct = 100.0f * static_cast<float>(it.n_in_valid) / static_cast<float>(it.n_total);
  const float out_pct = 100.0f * static_cast<float>(it.n_out_valid) / static_cast<float>(it.n_total);
  const float nan_pct = 100.0f * static_cast<float>(it.n_in_nan) / static_cast<float>(it.n_total);
  ImGui::Text("完整性: 输入有效 %.1f%%  输出有效 %.1f%%  ", in_pct, out_pct);
  ImGui::SameLine(0, 0);
  ImGui::TextColored(nan_pct > 0.0f ? ImVec4(1.0f, 0.9f, 0.3f, 1.0f) : ImVec4(0.2f, 0.8f, 0.4f, 1.0f),
                     "NaN %.2f%%", nan_pct);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("输出有效 < 输入有效 的差 = 差分预热 / TOD 轮廓未成形 / 常数槽");
}

// ============================================================================
// Row 2: 变换链参数 (顺序 == 计算顺序). 返回 true = 有改动
// ============================================================================

template <class E, size_t N>
static bool EnumCombo(const char *id, E &v, const char *const (&names)[N], float width) {
  bool changed = false;
  ImGui::SetNextItemWidth(width);
  if (ImGui::BeginCombo(id, names[static_cast<size_t>(v)])) {
    for (size_t i = 0; i < N; ++i) {
      const E e = static_cast<E>(i);
      if (ImGui::Selectable(names[i], v == e) && v != e) {
        v = e;
        changed = true;
      }
    }
    ImGui::EndCombo();
  }
  return changed;
}

static bool RenderPipeline(Transform::Params &p) {
  TraceN("UI:Pipeline");
  bool changed = false;

  // 1. 季节
  ImGui::Text("季节");
  ImGui::SameLine();
  changed |= EnumCombo("##season", p.season, math::stationary::TodProfile::MODE_NAMES, 110);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("日内轮廓去季节: 每资产每分钟槽的 expanding 均值/标准差 (截至昨日, 因果).\n"
                      "日内 U 形是确定性周期, 不是单位根 —— 差分/MA 不对症, 先减轮廓.");

  // 2. 平稳化
  ImGui::SameLine(0, 16);
  ImGui::Text("平稳化");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(110);
  if (ImGui::BeginCombo("##st", Transform::stationary_def(p.stationary).name)) {
    for (size_t i = 0; i < Transform::g_stationary_count; ++i) {
      const auto &e = Transform::g_stationary[i];
      if (ImGui::Selectable(e.def->name, p.stationary == e.method) && p.stationary != e.method) {
        p.stationary = e.method;
        p.reset_stationary();
        changed = true;
      }
    }
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("按天分段, 天首从零起 (隔夜跳空不进差分/滤波); 差分预热段输出 NaN");
  changed |= RenderOperatorParams(p.st, "st");

  // 3. 带通
  ImGui::SameLine(0, 16);
  changed |= ImGui::Checkbox("带通", &p.bandpass);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("IIR 因果前向滤波 (天首复位), 通带 = PSD 图上两根光标之间的周期 [%.1f, %.1f] 分钟",
                      p.bp_lo_period, p.bp_hi_period);
  if (p.bandpass) {
    ImGui::SameLine();
    changed |= EnumCombo("##iir", p.iir_type, Transform::IIR_TYPE_NAMES, 110);
    ImGui::SameLine();
    ImGui::SetNextItemWidth(70);
    changed |= ImGui::SliderInt("阶##iir", &p.iir_order, 1, 8);
    ImGui::SameLine();
    ImGui::TextDisabled("[%.0f-%.0f min]", p.bp_lo_period, p.bp_hi_period);
  }

  // 4. TS 归一
  ImGui::SameLine(0, 16);
  ImGui::Text("TS归一");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(90);
  if (ImGui::BeginCombo("##ts", math::normalize::GetMethod(p.ts_norm).name)) {
    for (size_t i = 0; i < math::normalize::g_method_count; ++i) {
      const auto &d = math::normalize::g_methods[i];
      if (ImGui::Selectable(d.name, p.ts_norm == d.method) && p.ts_norm != d.method) {
        p.ts_norm = d.method;
        p.reset_ts();
        changed = true;
      }
    }
    ImGui::EndCombo();
  }
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("天内 expanding 统计 (只用历史), 每天从零起");
  changed |= RenderOperatorParams(p.ts, "ts");

  // 5. 截面 (生产同一套 cs::{Tf, Method})
  ImGui::SameLine(0, 16);
  ImGui::Text("截面");
  ImGui::SameLine();
  changed |= EnumCombo("##cstf", p.cs_tf, cs::TF_NAMES, 70);
  ImGui::SameLine();
  changed |= EnumCombo("##csm", p.cs_method, cs::METHOD_NAMES, 110);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("每 (天, 分钟) 一个截面: Tf 元素预变换 → Method. 与回测/实盘 features/Method/CS.hpp 同一份代码.\n"
                      "NeutralRank 需 mcap + ind_l1 上下文列 (行业 + log 市值中性化)");

  // 6. 构建天数档位: 只算区间头 N 天 (快速迭代), 不往后算
  ImGui::SameLine(0, 16);
  ImGui::Text("天数");
  ImGui::SameLine();
  {
    static constexpr size_t kDays[] = {1, 5, 30, 0};
    static constexpr const char *kDayNames[] = {"1", "5", "30", "全部"};
    size_t cur = 3;
    for (size_t i = 0; i < 4; ++i)
      if (p.max_days == kDays[i])
        cur = i;
    ImGui::SetNextItemWidth(70);
    if (ImGui::BeginCombo("##days", kDayNames[cur])) {
      for (size_t i = 0; i < 4; ++i)
        if (ImGui::Selectable(kDayNames[i], cur == i) && cur != i) {
          p.max_days = kDays[i];
          changed = true;
        }
      ImGui::EndCombo();
    }
    if (ImGui::IsItemHovered())
      ImGui::SetTooltip("重算只算区间头 N 天, 不往后算 (统计/分布/序列都只覆盖头 N 天); 全部 = 全区间");
  }

  return changed;
}

// ============================================================================
// Row 3: 统计子集焦点滑条 + ADF/KPSS 通过率热力条
// ============================================================================

static void RenderFocusAndHeatmap(const Transform &tf, const Asset &asset, TransformUIState &ui) {
  TraceN("UI:Heatmap");
  const size_t n = tf.stat_lines.size(); // 全资产 (槽位 == 子轴下标)
  if (n == 0) {
    ImGui::TextDisabled("资产: --");
    ImGui::Dummy(ImVec2(0, 34));
    return;
  }
  ui.focus = std::clamp(ui.focus, 0, static_cast<int>(n) - 1);

  // 滑条: 标签 = 全局资产下标.交易所.名字 (n_days 检验天数)
  {
    const auto &st = tf.stat_lines[ui.focus];
    const uint32_t a = st.asset; // 全局轴下标 (items 查询)
    char label[128];
    if (a < asset.items.size()) {
      const auto &it = asset.items[a];
      snprintf(label, sizeof(label), "%u.%s.%s (%u 天)", a, it.exchange.c_str(), it.asset_name.c_str(), st.n_days);
    } else {
      snprintf(label, sizeof(label), "%u (%u 天)", a, st.n_days);
    }
    ImGui::Text("资产 (%zu)", n);
    ImGui::SameLine();
    ImGui::SetNextItemWidth(-1);
    ImGui::SliderInt("##focus", &ui.focus, 0, static_cast<int>(n) - 1, label);
  }

  // 热力条: 两行 (ADF 通过率 / KPSS 通过率), 每列一个资产 (全市场时 1px 起)
  const float label_w = 44.0f, cell_h = 12.0f;
  const ImVec2 avail = ImGui::GetContentRegionAvail();
  const float cell_w = std::max(1.0f, (avail.x - label_w) / static_cast<float>(n));
  ImDrawList *draw = ImGui::GetWindowDrawList();
  const ImVec2 pos = ImGui::GetCursorScreenPos();
  const float x0 = pos.x + label_w;
  draw->AddText(ImVec2(pos.x, pos.y), IM_COL32(180, 180, 180, 255), "ADF");
  draw->AddText(ImVec2(pos.x, pos.y + cell_h + 2), IM_COL32(180, 180, 180, 255), "KPSS");
  for (size_t s = 0; s < n; ++s) {
    const auto &st = tf.stat_lines[s];
    const float x = x0 + s * cell_w;
    draw->AddRectFilled(ImVec2(x, pos.y), ImVec2(x + cell_w - 1, pos.y + cell_h), RateColor(st.adf_rate()));
    draw->AddRectFilled(ImVec2(x, pos.y + cell_h + 2), ImVec2(x + cell_w - 1, pos.y + 2 * cell_h + 2), RateColor(st.kpss_rate()));
  }
  // 焦点列描边
  {
    const float x = x0 + ui.focus * cell_w;
    draw->AddRect(ImVec2(x - 1, pos.y - 1), ImVec2(x + cell_w, pos.y + 2 * cell_h + 3), IM_COL32(0, 255, 255, 255));
  }
  ImGui::Dummy(ImVec2(avail.x, 2 * cell_h + 4));

  // 悬停 / 点选
  ui.hovered_stat = -1;
  if (ImGui::IsItemHovered()) {
    const ImVec2 m = ImGui::GetMousePos();
    if (m.x >= x0) {
      const int s = static_cast<int>((m.x - x0) / cell_w);
      if (s >= 0 && s < static_cast<int>(n)) {
        ui.hovered_stat = s;
        const auto &st = tf.stat_lines[s];
        const uint32_t a = st.asset; // 全局轴下标 (items 查询)
        ImGui::BeginTooltip();
        if (a < asset.items.size())
          ImGui::Text("%u %s.%s", a, asset.items[a].exchange.c_str(), asset.items[a].asset_name.c_str());
        if (st.n_days > 0) {
          ImGui::Text("检验天数 %u", st.n_days);
          ImGui::Text("ADF  通过 %.0f%%  均值统计量 %.2f", 100.0f * st.adf_rate(), st.adf_sum / st.n_days);
          ImGui::Text("KPSS 通过 %.0f%%  均值统计量 %.3f", 100.0f * st.kpss_rate(), st.kpss_sum / st.n_days);
        } else {
          ImGui::TextDisabled("无有效天");
        }
        ImGui::EndTooltip();
        if (ImGui::IsMouseClicked(0))
          ui.focus = s;
      }
    }
  }
}

// ============================================================================
// 序列视图: 焦点资产全程 (逐批增长) 原始 (+TOD 轮廓) | 链末输出
// 快照只存焦点资产 (内存与 universe 大小无关); 换焦点 = 重跑构建 (与改参数同流程)
// 两图轴永远互锁 (SetupAxisLinks): 任一图缩放/平移另一图跟随;
// autofit 不能各图各 fit —— 手动求 原始/TOD/输出 数据并集写进链接值, 两图同视野
// ============================================================================

static void RenderSeries(const Transform &tf, const Asset &asset, TransformUIState &ui, float height) {
  TraceN("UI:Series");
  const auto &sn = tf.series;
  const bool has = sn.n_days > 0;
  const size_t n_pts = sn.n_days * kVR;
  static std::vector<float> tod_tile;

  // 有数据才 fit. pending 只在"并集范围真写进了链接值 且 至少一图真画了"那帧消费:
  // 焦点资产首批可能全 NaN (lo > hi, 无可 fit), 或子窗被裁剪 BeginPlot 失败 —— 都留到下帧
  bool fitted = false;
  if (ui.fit_series && has) {
    float lo = FLT_MAX, hi = -FLT_MAX;
    auto scan = [&](const float *v, size_t n) {
      for (size_t i = 0; i < n; ++i)
        if (v[i] == v[i]) {
          lo = std::min(lo, v[i]);
          hi = std::max(hi, v[i]);
        }
    };
    scan(sn.raw.data(), n_pts);
    scan(sn.out.data(), n_pts);
    if (tf.params.season != Transform::Season::None)
      scan(sn.tod_mean.data(), kVR);
    if (lo <= hi) {
      const double margin = hi > lo ? 0.05 * (hi - lo) : 1.0;
      ui.series_y_min = lo - margin;
      ui.series_y_max = hi + margin;
      ui.series_x_min = 0.0;
      ui.series_x_max = static_cast<double>(n_pts);
      fitted = true;
    }
  }
  bool drawn = false;   // 本帧至少一图 BeginPlot 成功
  bool dbl_fit = false; // 双击任一图/轴: ImPlot 自带 fit 只看本图数据 → 改走两图并集 (下帧 autofit)
  auto poll_dbl = [&] { // BeginPlot 内调用
    if (ImGui::IsMouseDoubleClicked(ImGuiMouseButton_Left) &&
        (ImPlot::IsPlotHovered() || ImPlot::IsAxisHovered(ImAxis_X1) || ImPlot::IsAxisHovered(ImAxis_Y1)))
      dbl_fit = true;
  };

  ImGui::BeginChild("RawPlot", ImVec2(ImGui::GetContentRegionAvail().x * 0.5f, height), true);
  if (has) {
    const char *name = sn.asset < asset.items.size() ? asset.items[sn.asset].asset_name.c_str() : "?";
    ImGui::Text("原始 %u.%s  %s ~ %s (%zu 天)", sn.asset, name, sn.date_begin.c_str(), sn.date_end.c_str(), sn.n_days);
  } else {
    ImGui::TextDisabled("原始");
  }
  if (ImPlot::BeginPlot("##Raw", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    drawn = true;
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel, ImPlotAxisFlags_NoLabel);
    ImPlot::SetupAxisLinks(ImAxis_X1, &ui.series_x_min, &ui.series_x_max);
    ImPlot::SetupAxisLinks(ImAxis_Y1, &ui.series_y_min, &ui.series_y_max);
    poll_dbl();
    if (has) {
      ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.85f, 0.3f, 1.0f), 1.0f);
      ImPlot::PlotLine("##raw", sn.raw.data(), static_cast<int>(n_pts), 1.0, 0.0, ImPlotLineFlags_SkipNaN);
      if (tf.params.season != Transform::Season::None) {
        // TOD 均值轮廓按天平铺叠在原始上 (截至末批的轮廓)
        tod_tile.resize(n_pts);
        const float *tm = sn.tod_mean.data();
        for (size_t d = 0; d < sn.n_days; ++d)
          std::copy_n(tm, kVR, tod_tile.data() + d * kVR);
        ImPlot::SetNextLineStyle(ImVec4(0.3f, 0.9f, 1.0f, 0.9f), 1.5f);
        ImPlot::PlotLine("##tod", tod_tile.data(), static_cast<int>(n_pts), 1.0, 0.0, ImPlotLineFlags_SkipNaN);
      }
    }
    ImPlot::EndPlot();
  }
  ImGui::EndChild();

  ImGui::SameLine();
  ImGui::BeginChild("OutPlot", ImVec2(0, height), true);
  ImGui::Text("链末输出");
  if (ImPlot::BeginPlot("##Out", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    drawn = true;
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel, ImPlotAxisFlags_NoLabel);
    ImPlot::SetupAxisLinks(ImAxis_X1, &ui.series_x_min, &ui.series_x_max);
    ImPlot::SetupAxisLinks(ImAxis_Y1, &ui.series_y_min, &ui.series_y_max);
    poll_dbl();
    if (has) {
      ImPlot::SetNextLineStyle(ImVec4(0.3f, 0.9f, 0.5f, 1.0f), 1.0f);
      ImPlot::PlotLine("##out", sn.out.data(), static_cast<int>(n_pts), 1.0, 0.0, ImPlotLineFlags_SkipNaN);
    }
    ImPlot::EndPlot();
  }
  ImGui::EndChild();

  if (fitted && drawn)
    ui.fit_series = false;
  if (dbl_fit) // 本帧 ImPlot 已按单图数据 fit 并推进链接值; 下帧用两图并集覆盖 (与 autofit 同一路径)
    ui.fit_series = true;
}

// ============================================================================
// 输出分布 (全局 + 绘制子集 + 焦点) | 单日 PSD 均值 (周期轴 + 带通光标) | 单日 ACF/PACF 均值
// ============================================================================

static void RenderDistAndPSD(const Transform &tf, TransformUIState &ui, float height, bool &bp_changed) {
  TraceN("UI:DistPSD");
  const float col_w = ImGui::GetContentRegionAvail().x / 3.0f;
  const size_t A = tf.lines.size();
  // 焦点槽位 == 子轴下标 (统计无子集, 与 lines 同轴)
  const uint32_t focus_asset = ui.focus >= 0 && ui.focus < static_cast<int>(A) ? static_cast<uint32_t>(ui.focus) : UINT32_MAX;

  // 有数据才 fit (PDF 画的是总体线 + 绘制子集线, 二者任一就绪即算有). pending 只在 BeginPlot
  // 成功那帧消费: SetNextAxesToFit 在 BeginPlot 失败 (子窗 SkipItems / 被裁剪) 时会被 ImPlot 静默丢弃
  bool pdf_has = tf.global.n_pts > 0;
  if (ui.fit_pdf && !pdf_has)
    for (const auto &ln : tf.lines)
      if (ln.draw && ln.n_pts > 0) {
        pdf_has = true;
        break;
      }
  const bool fit_pdf = ui.fit_pdf && pdf_has;
  const bool fit_acf = ui.fit_acf && tf.acf_n > 0;

  // 左: 分布 (全局 + 绘制子集 + 焦点; 全部是 PdfSnap 成品, 零计算只画)
  ImGui::BeginChild("PDFPlot", ImVec2(col_w, height), true);
  ImGui::Text("链末输出分布  n=%llu", static_cast<unsigned long long>(tf.global.n));
  if (tf.global.n_pts > 0) {
    ImGui::SameLine();
    ImGui::TextDisabled("mean %.3g  sd %.3g  skew %.2f  kurt %.2f", tf.global.mean, tf.global.sd(),
                        tf.global.skew, tf.global.kurt);
  }
  if (fit_pdf)
    ImPlot::SetNextAxesToFit();
  if (ImPlot::BeginPlot("##PDF", ImVec2(-1, -1), ImPlotFlags_NoLegend)) {
    ui.fit_pdf &= !fit_pdf;
    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoLabel, ImPlotAxisFlags_NoLabel);
    for (size_t a = 0; a < A; ++a) {
      const auto &ln = tf.lines[a];
      if (!ln.draw || ln.n_pts == 0 || a == focus_asset)
        continue;
      ImVec4 c = AssetColor(a, A);
      c.w = 0.35f;
      ImPlot::SetNextLineStyle(c, 0.8f);
      ImPlot::PlotLine("##a", ln.x.data(), ln.y.data(), static_cast<int>(ln.n_pts));
    }
    if (tf.global.n_pts > 0) {
      ImPlot::SetNextLineStyle(ImVec4(1, 1, 1, 1), 2.5f);
      ImPlot::PlotLine("##global", tf.global.x.data(), tf.global.y.data(), static_cast<int>(tf.global.n_pts));
    }
    if (focus_asset < A && tf.lines[focus_asset].n_pts > 0)
      PlotHighlightLine(tf.lines[focus_asset]);
    ImPlot::EndPlot();
  }
  ImGui::EndChild();

  ImGui::SameLine();

  // 中: PSD (x = 周期 分钟, log; y = log10 功率)
  ImGui::BeginChild("PSDPlot", ImVec2(col_w, height), true);
  ImGui::Text("单日 PSD 均值  (%llu 资产·天)", static_cast<unsigned long long>(tf.psd_n));
  constexpr size_t NF = DayPSD::N_FREQS;
  static std::vector<float> px, py;
  px.resize(NF - 1);
  py.resize(NF - 1);
  for (size_t k = 1; k < NF; ++k) { // 跳 DC
    px[k - 1] = DayPSD::period_of(k);
    const float v = tf.psd_mean[k];
    py[k - 1] = v > 1e-20f ? std::log10(v) : -20.0f;
  }
  // PSD 只 autofit y: x 是光标所在轴, 带通光标拖动 → 重算 → epoch 变 → x refit 会和光标共振
  // (轴跳光标跟着跳), 所以 x 固定默认视野 [2, N] 只由用户缩放; y 与其他图同规则 (BeginPlot 成功才消费).
  // 拖动中 (任一控件 active, 与下方请求节流的 released 同判据) 也不 fit y: y 每批 refit → y 刻度
  // 文本宽度变 → PlotRect 左边界横移 → x 刻度/光标像素位置抖 (x 范围其实没变). 松手后再消费 pending.
  const bool fit_psd = ui.fit_psd && tf.psd_n > 0 && !ImGui::IsAnyItemActive();
  if (fit_psd)
    ImPlot::SetNextAxisToFit(ImAxis_Y1);
  // NoBoxSelect: 左键 (MapInputReverse 后 = box-select) 与带通光标 DragLineX 共用,
  // 释放时会把 X 范围 zoom 到拖动跨度 (与光标共振 → flicker). PSD 是诊断图, 缩放靠光标/滚轮即可.
  if (ImPlot::BeginPlot("##PSD", ImVec2(-1, -1), ImPlotFlags_NoLegend | ImPlotFlags_NoBoxSelect)) {
    ui.fit_psd &= !fit_psd;
    ImPlot::SetupAxes("周期 (min)", "log10 P");
    ImPlot::SetupAxisScale(ImAxis_X1, ImPlotScale_Log10);
    ImPlot::SetupAxisLimits(ImAxis_X1, 2.0, static_cast<double>(DayPSD::N), ImGuiCond_Once);
    ImPlot::SetupAxisLimits(ImAxis_Y1, -10.0, 0.0, ImGuiCond_Once);

    // 通带阴影
    const ImPlotRect lim = ImPlot::GetPlotLimits();
    ImPlot::PushPlotClipRect();
    {
      ImVec2 p0 = ImPlot::PlotToPixels(ui.params.bp_lo_period, lim.Y.Max);
      ImVec2 p1 = ImPlot::PlotToPixels(ui.params.bp_hi_period, lim.Y.Min);
      ImPlot::GetPlotDrawList()->AddRectFilled(p0, p1, ui.params.bandpass ? IM_COL32(100, 200, 100, 60) : IM_COL32(150, 150, 150, 40));
    }
    ImPlot::PopPlotClipRect();

    if (tf.psd_n > 0) {
      ImPlot::SetNextLineStyle(ImVec4(1.0f, 0.8f, 0.2f, 1.0f), 2.0f);
      ImPlot::PlotLine("##psd", px.data(), py.data(), static_cast<int>(NF - 1));
    }

    // 带通光标 (周期): lo < hi. DragLineX 按住不动也返回 true (IsMouseDragging 粘滞), 值真变了才算改动
    double lo = ui.params.bp_lo_period, hi = ui.params.bp_hi_period;
    constexpr double kMinRatio = 1.2;
    bool moved = false;
    moved |= ImPlot::DragLineX(100, &lo, ImVec4(0.3f, 0.9f, 0.3f, 1.0f), 2.0f);
    if (ImGui::IsItemActive() || ImGui::IsItemHovered())
      ImPlot::Annotation(lo, lim.Y.Max, ImVec4(0.3f, 0.9f, 0.3f, 1.0f), ImVec2(5, -10), false, "%.1f min", lo);
    moved |= ImPlot::DragLineX(101, &hi, ImVec4(0.9f, 0.3f, 0.3f, 1.0f), 2.0f);
    if (ImGui::IsItemActive() || ImGui::IsItemHovered())
      ImPlot::Annotation(hi, lim.Y.Max, ImVec4(0.9f, 0.3f, 0.3f, 1.0f), ImVec2(5, -10), false, "%.1f min", hi);
    if (moved) {
      lo = std::clamp(lo, static_cast<double>(Transform::Params::kMinPeriod), static_cast<double>(Transform::Params::kMaxPeriod) / kMinRatio);
      hi = std::clamp(hi, lo * kMinRatio, static_cast<double>(Transform::Params::kMaxPeriod));
      const float new_lo = static_cast<float>(lo), new_hi = static_cast<float>(hi);
      if (new_lo != ui.params.bp_lo_period || new_hi != ui.params.bp_hi_period) {
        ui.params.bp_lo_period = new_lo;
        ui.params.bp_hi_period = new_hi;
        bp_changed = ui.params.bandpass; // 带通关着时光标只是预览, 不触发重算
      }
    }
    ImPlot::EndPlot();
  }
  ImGui::EndChild();

  ImGui::SameLine();

  // 右: ACF / PACF (上下两图, 跳 lag 0; 参考带 = 单天 Bartlett ±1.96/√VR)
  ImGui::BeginChild("ACFPlot", ImVec2(0, height), true);
  {
    constexpr int NL = static_cast<int>(kTfMaxLag);
    const float cb = 1.96f / std::sqrt(static_cast<float>(kVR));
    const float cb_neg = -cb;
    ImGui::Text("单日 ACF / PACF 均值  (%llu 资产·天)  参考带 ±%.3f", static_cast<unsigned long long>(tf.acf_n), cb);
    static std::array<float, kTfMaxLag> lags = [] {
      std::array<float, kTfMaxLag> l{};
      for (size_t i = 0; i < kTfMaxLag; ++i)
        l[i] = static_cast<float>(i + 1);
      return l;
    }();
    const float sub_h = (ImGui::GetContentRegionAvail().y - ImGui::GetStyle().ItemSpacing.y) * 0.5f;
    int acf_drawn = 0; // 上下两图都画上了才消费 pending
    auto plot = [&](const char *id, const char *ylabel, const std::array<float, kTfMaxLag + 1> &v, ImVec4 color) {
      if (fit_acf)
        ImPlot::SetNextAxisToFit(ImAxis_Y1);
      if (ImPlot::BeginPlot(id, ImVec2(-1, sub_h), ImPlotFlags_NoLegend)) {
        ++acf_drawn;
        ImPlot::SetupAxes("lag (min)", ylabel);
        ImPlot::SetupAxisLimits(ImAxis_X1, 0.0, static_cast<double>(NL) + 1.0, ImGuiCond_Once);
        ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
        ImPlot::PlotInfLines("##cbp", &cb, 1, ImPlotInfLinesFlags_Horizontal);
        ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
        ImPlot::PlotInfLines("##cbn", &cb_neg, 1, ImPlotInfLinesFlags_Horizontal);
        if (tf.acf_n > 0) {
          ImPlot::SetNextFillStyle(color);
          ImPlot::PlotBars("##v", lags.data(), v.data() + 1, NL, 0.6);
        }
        ImPlot::EndPlot();
      }
    };
    plot("##ACF", "ACF", tf.acf_mean, ImVec4(0.3f, 0.7f, 1.0f, 0.9f));
    plot("##PACF", "PACF", tf.pacf_mean, ImVec4(1.0f, 0.6f, 0.3f, 0.9f));
    if (fit_acf && acf_drawn == 2)
      ui.fit_acf = false;
  }
  ImGui::EndChild();
}

// ============================================================================
// Main
// ============================================================================

void RenderTabTransform(TransformService *service, SharedData &data, TransformUIState &ui) {
  TraceN("UI:RenderTabTransform");
  static bool input_configured = false;
  if (!input_configured) {
    ImPlot::MapInputReverse();
    input_configured = true;
  }
  if (!service->is_running())
    service->Start(data);

  auto &tf = data.transform;

  // 控件 (不持锁)
  RenderControl(service, data, ui);
  bool changed = RenderPipeline(ui.params);

  // 流式维护 x/y range: epoch 变了 (= 数据变了) 就 autofit 一次, 稳态把缩放还给用户.
  // epoch 跨构建单调 (reset/clear/每批发布都 +1), 换特征时 reset→publish 哪怕发生在两帧
  // 之间也不会被看成"没变", 也不依赖 status 转移 (Building 中换特征 cancel→restart
  // 全程停在 Building 会漏). 置位后由各图在画上数据那帧自行消费 (见 TransformUIState).
  const uint64_t cur_epoch = tf.epoch.load(std::memory_order_acquire);
  // focus 快照必须在渲染前比 (滑条/热力条点选在下面的渲染里改 ui.focus):
  // 若帧末才记, 改动帧就被覆盖, 焦点变化永远检测不到 → 不 autofit 也不触发重算
  const bool focus_changed = ui.focus != ui.last_focus;
  if (cur_epoch != ui.last_epoch || focus_changed)
    ui.fit_series = ui.fit_pdf = ui.fit_psd = ui.fit_acf = true;
  ui.last_epoch = cur_epoch;
  ui.last_focus = ui.focus;

  const float avail_h = ImGui::GetContentRegionAvail().y - 3 * ImGui::GetTextLineHeightWithSpacing() - 40.0f;
  const float plot_h = std::max(100.0f, avail_h * 0.5f);

  bool bp_changed = false;
  {
    std::lock_guard<std::mutex> lock(tf.mutex);
    RenderChainIntegrity(tf.integrity);
    RenderFocusAndHeatmap(tf, data.asset, ui);
    RenderSeries(tf, data.asset, ui, plot_h);
    RenderDistAndPSD(tf, ui, plot_h, bp_changed);
  }

  // 参数/焦点改动: 记 dirty, 拖动中按时间 gate 节流发请求, 松手必发 —— 边拖边算
  // (序列快照只存焦点资产全程, 换焦点必须重跑构建, 与改参数同流程)
  ui.dirty |= changed || bp_changed || focus_changed;
  if (ui.dirty) {
    const float now = static_cast<float>(ImGui::GetTime());
    constexpr float kDragGate = 0.15f; // 拖动中至少隔 150ms 才发新请求 (取消在跑, 从头重来)
    const bool released = !ImGui::IsAnyItemActive();
    const bool due = ui.last_req_time < 0.0f || (now - ui.last_req_time) >= kDragGate;
    if (released || due) {
      ui.dirty = false;
      ui.last_req_time = now;
      service->RequestCompute(data, ui.params, ui.focus);
    }
  }
}

void StopTabTransform(TransformService *service, SharedData &data) {
  // 切走 tab: 只中断在跑构建 (内存与 worker 保留); 切出 Features 任务由 Shutdown 整体释放
  service->RequestCancel();
  (void)data;
}

} // namespace GUI::Features
