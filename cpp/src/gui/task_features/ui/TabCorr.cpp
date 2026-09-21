// Tab Corr Implementation — 特征相关矩阵下三角热图 + 悬停对 lead-lag + 全矩阵 lag 峭点图
#include "gui/task_features/ui/TabCorr.hpp"
#include "gui/task_features/services/CorrService.hpp"
#include "gui/task_features/ui/Common.hpp" // 状态文本/颜色
#include "gui/task_features/ui/TabFeature.hpp"
#include "shared/SharedData.hpp"

#include "imgui.h"
#include "implot.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <mutex>
#include <vector>

namespace GUI::Features {

// 带符号模式的色标: ImPlot 内建 RdBu 低端是红 (ρ = -1 会画成红), 与"红 = 正相关"的
// 惯例相反 → 注册一份倒序的 (蓝 → 白 → 红), 直接配 [-1, 1] 用, 不玩反向量程.
// 按名字查而非缓存下标: ImPlot 上下文重建 (Reinit) 后自动重新注册
static ImPlotColormap signed_colormap() {
  static const char *kName = "CorrBuRd";
  const ImPlotColormap found = ImPlot::GetColormapIndex(kName);
  if (found >= 0)
    return found;
  static const ImU32 cols[11] = {// Color Brewer RdBu 倒序
                                 IM_COL32(5, 48, 97, 255), IM_COL32(33, 102, 172, 255), IM_COL32(67, 147, 195, 255),
                                 IM_COL32(146, 197, 222, 255), IM_COL32(209, 229, 240, 255), IM_COL32(247, 247, 247, 255),
                                 IM_COL32(253, 219, 199, 255), IM_COL32(244, 165, 130, 255), IM_COL32(214, 96, 77, 255),
                                 IM_COL32(178, 24, 43, 255), IM_COL32(103, 0, 31, 255)};
  return ImPlot::AddColormap(kName, cols, 11, /*qual=*/false);
}

// ============================================================================
// 聚类重排: 平均链接凝聚聚类, 距离 d = 1 - |ρ| (无样本的对 d = 1 = 最远)
// ============================================================================
// 目的只有一个: 把相关的列挪到一起, 热图出块对角, 冗余族一眼可见.
// 用 |ρ| 而非 ρ: 一个特征和它的相反数是同一份信息, 该抱团.
// O(n³), n 可到几百 → 选聚类序时自动算, 但构建中不跟着每天的发布重算 (Done 后补一次).
static std::vector<int> cluster_order(const std::vector<float> &rho, size_t n) {
  assert(n >= 2 && rho.size() == n * n);
  const size_t total = 2 * n - 1;              // 簇 id: 0..n-1 叶, n..2n-2 内部节点
  std::vector<double> sum(total * total, 0.0); // 簇间相似度和 (|ρ|)
  for (size_t i = 0; i < n; ++i)
    for (size_t j = 0; j < n; ++j) {
      const float r = rho[i * n + j];
      sum[i * total + j] = (i == j) ? 0.0 : (r == r ? std::fabs(r) : 0.0);
    }

  std::vector<size_t> sz(total, 0);
  std::vector<std::array<size_t, 2>> kids(total, {0, 0});
  std::vector<char> active(total, 0), leaf(total, 0);
  for (size_t i = 0; i < n; ++i) {
    sz[i] = 1;
    active[i] = 1;
    leaf[i] = 1;
  }
  for (size_t nid = n; nid < total; ++nid) {
    size_t ba = 0, bb = 0;
    double best = -1.0;
    for (size_t a = 0; a < nid; ++a) {
      if (!active[a])
        continue;
      for (size_t b = a + 1; b < nid; ++b) {
        if (!active[b])
          continue;
        const double avg = sum[a * total + b] / static_cast<double>(sz[a] * sz[b]);
        if (avg > best) { // 平局取先遇到的对 (下标序) → 确定性
          best = avg;
          ba = a;
          bb = b;
        }
      }
    }
    kids[nid] = {ba, bb};
    sz[nid] = sz[ba] + sz[bb];
    for (size_t c = 0; c < nid; ++c) {
      if (!active[c])
        continue;
      const double v = sum[ba * total + c] + sum[bb * total + c];
      sum[nid * total + c] = sum[c * total + nid] = v;
    }
    active[ba] = active[bb] = 0;
    active[nid] = 1;
  }

  // 树线性化 (显式栈, n 可到几百, 不递归)
  std::vector<int> out;
  out.reserve(n);
  std::vector<size_t> stack{total - 1};
  while (!stack.empty()) {
    const size_t c = stack.back();
    stack.pop_back();
    if (leaf[c]) {
      out.push_back(static_cast<int>(c));
      continue;
    }
    stack.push_back(kids[c][1]); // 后入先出 → 左子树先出
    stack.push_back(kids[c][0]);
  }
  assert(out.size() == n);
  return out;
}

// ============================================================================
// 画图缓冲: 按显示序重排 + 阈值/绝对值处理 (缓存键不变就不重建)
// ============================================================================
// NaN (样本不足) 与 |ρ| < 阈值 一并按 0 画 —— 两者在图上都读作"这里没东西", 区别由
// 悬停文字给出 (n = 0 会写明样本不足). PlotHeatmap 无逐格透明度, 这是最不误导的画法.
// 只填下三角 (r >= c): 矩阵对称, 上三角是镜像, 画出来只占地方; 绘制侧按行画 r+1 格.
// lag 图 (lag != nullptr): 值 = |ρ| 最大的 lag (0 中性色); |ρ_best| < 阈值 的对按 0 画 ——
//   弱相关对的 argmax 是噪声, 不压掉整张图就是雪花.
static void rebuild_disp(const Correlation &corr, const CorrLag *lag, CorrUIState &ui) {
  const size_t n = corr.n();
  ui.disp.assign(n * n, 0.0f);
  ui.n_blank = 0;

  ui.disp_order.resize(n);
  const bool use_cluster =
      ui.order_mode == 1 && ui.cluster_n == n && ui.cluster_order.size() == n;
  for (size_t k = 0; k < n; ++k)
    ui.disp_order[k] = use_cluster ? ui.cluster_order[k] : static_cast<int>(k);

  if (ui.lag_mode && !lag)
    return; // lag 图开着但快照未对齐: 整张画中性色, 不拿 ρ 冒充 lag

  for (size_t r = 0; r < n; ++r) {
    const size_t i = static_cast<size_t>(ui.disp_order[r]);
    for (size_t c = 0; c <= r; ++c) {
      const size_t j = static_cast<size_t>(ui.disp_order[c]);
      const float v = lag ? lag->rho_best[i * n + j] : corr.rho[i * n + j];
      if (v != v) {
        ++ui.n_blank;
        continue; // 已是 0
      }
      const float a = std::fabs(v);
      if (a < ui.threshold)
        continue;
      ui.disp[r * n + c] = lag ? static_cast<float>(lag->lag_best[i * n + j])
                               : (ui.abs_mode ? a : v);
    }
  }
}

// lag 快照能否与矩阵按同一下标读: 同层同列集 (列集变了未算完之前不匹配, UI 画空)
static bool lag_matches(const CorrLag &lag, const Correlation &corr) {
  return lag.level == corr.level && lag.cols == corr.cols && lag.rho_best.size() == corr.rho.size();
}

static uint64_t disp_key(const Correlation &corr, const CorrLag *lag, const CorrUIState &ui) {
  uint64_t k = corr.epoch.load(std::memory_order_acquire);
  k = k * 1099511628211ull + corr.n();
  k = k * 1099511628211ull + static_cast<uint64_t>(ui.order_mode);
  k = k * 1099511628211ull + ui.cluster_epoch;
  k = k * 1099511628211ull + static_cast<uint64_t>(ui.abs_mode);
  k = k * 1099511628211ull + static_cast<uint64_t>(ui.threshold * 1000.0f);
  k = k * 1099511628211ull + static_cast<uint64_t>(ui.lag_mode);
  k = k * 1099511628211ull + (lag ? lag->epoch.load(std::memory_order_acquire) : 0);
  return k;
}

// ============================================================================
// lead-lag 面板 (选中对): CorrPair 快照, UI 零计算只画
// ============================================================================

static void render_pair_panel(SharedData &data, const CorrLag *lag, CorrUIState &ui) {
  const Correlation &corr = data.corr;
  const size_t n = corr.n();
  if (ui.sel_i < 0 || ui.sel_j < 0 || (size_t)ui.sel_i >= n || (size_t)ui.sel_j >= n) {
    ImGui::TextDisabled("悬停热图任一格子: 自动算该对的 lead-lag 曲线");
    return;
  }
  const auto &meta_list = data.feature.metadata.features[corr.level];
  const uint32_t ca = corr.cols[ui.sel_i], cb = corr.cols[ui.sel_j];
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "%s", meta_list[ca].code);
  ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.3f, 1.0f), "%s", meta_list[cb].code);
  const size_t o = (size_t)ui.sel_i * n + ui.sel_j;
  const float r0 = corr.rho[o];
  if (r0 == r0)
    ImGui::Text("同期 ρ = %.3f  (n = %u)", r0, corr.pair_n[o]);
  else
    ImGui::TextDisabled("同期 ρ: 样本不足");
  if (lag) {
    if (lag->rho_best[o] == lag->rho_best[o])
      ImGui::Text("lag 图: 峭点 lag = %+d, ρ = %.3f", (int)lag->lag_best[o], lag->rho_best[o]);
    else
      ImGui::TextDisabled("lag 图: 样本不足");
  }

  if (ui.sel_i == ui.sel_j) {
    ImGui::TextDisabled("(自己与自己)");
    return;
  }

  ImGui::Separator();
  const CorrPair &pair = data.corr_pair;
  std::lock_guard<std::mutex> lock(pair.mutex);
  // 请求在 RenderTabCorr 悬停处已提交; 这里只画. 未匹配 = worker 还没接到新对 (一帧内)
  const bool matched = (pair.col_a == ca && pair.col_b == cb && pair.level == corr.level);
  const auto st = pair.status.load(std::memory_order_acquire);
  ImGui::Text("Lead-Lag: ");
  ImGui::SameLine(0, 0);
  if (matched) {
    ImGui::TextColored(StatusColor(st), "%s", StatusText(st));
    ImGui::SameLine();
    ImGui::TextDisabled("(天 %zu/%zu)", pair.done.load(std::memory_order_relaxed),
                        pair.total.load(std::memory_order_relaxed));
  } else {
    ImGui::TextDisabled("排队中");
  }

  // 图框常驻; 曲线只在该对算完 (Done) 后一次画出 —— 不做逐天增量渲染, 悬停换对时也不闪旧线
  const bool ready = matched && st == analysis::Status::Done;
  static std::vector<float> xs, ys; // GUI 单线程, 帧内复用
  xs.clear();
  ys.clear();
  if (ready)
    for (size_t k = 0; k < kCorrLags; ++k)
      if (pair.rho[k] == pair.rho[k]) {
        xs.push_back(static_cast<float>(CorrPair::lag_of(k)));
        ys.push_back(pair.rho[k]);
      }
  if (ImPlot::BeginPlot("##leadlag", ImVec2(-1, 200), ImPlotFlags_NoLegend)) {
    // lag > 0 = 前者领先后者. 峰值不在 0 → 后者是前者的延迟/平滑副本;
    // 慢变列曲线天然平坦, 对它们这张图没有判别力 (见 Correlation.hpp)
    ImPlot::SetupAxes("lag (行)", "ρ", 0, ImPlotAxisFlags_AutoFit);
    ImPlot::SetupAxisLimits(ImAxis_X1, -(double)kCorrMaxLag, (double)kCorrMaxLag,
                            ImPlotCond_Always);
    const float zero = 0.0f;
    ImPlot::SetNextLineStyle(ImVec4(0.6f, 0.6f, 0.6f, 0.8f), 1.0f);
    ImPlot::PlotInfLines("##zero", &zero, 1, 0);
    if (xs.size() >= 2) {
      ImPlot::SetNextLineStyle(ImVec4(0.2f, 0.9f, 0.9f, 1.0f), 2.0f);
      ImPlot::PlotLine("##rho", xs.data(), ys.data(), (int)xs.size());
    }
    ImPlot::EndPlot();
  }
  if (ready && xs.size() < 2)
    ImGui::TextDisabled("曲线: 样本不足");
}

// ============================================================================
// Main Render
// ============================================================================

void RenderTabCorr(CorrService *service, CorrPairService *pair_service,
                   CorrLagService *lag_service, SharedData &data, CorrUIState &ui) {
  assert(service && pair_service && lag_service);
  Correlation &corr = data.corr;

  // ---- 行集跟随 Feature 表的过滤 (同一口径): 变了就重算 ----
  static std::vector<int> rows; // GUI 单线程, 帧内复用
  FilteredFeatureIndices(data, rows);
  const int level = data.feature.selection.selected_level;
  if (rows != ui.req_rows || level != ui.req_level) {
    ui.req_rows = rows;
    ui.req_level = level;
    ui.sel_i = ui.sel_j = -1;
    ui.cluster_n = 0; // 列集变了, 旧聚类序作废
    service->RequestCompute(data, rows);
  }
  // lag 图跟同一行集, 但只在勾选时算 (重); 勾着且请求快照过期 → 提交
  if (ui.lag_mode && (rows != ui.lag_req_rows || level != ui.lag_req_level)) {
    ui.lag_req_rows = rows;
    ui.lag_req_level = level;
    lag_service->RequestCompute(data, rows);
  }

  // ---- 1. 状态行 ----
  const auto status = corr.status.load(std::memory_order_acquire);
  ImGui::BeginDisabled(status == analysis::Status::Building);
  if (ImGui::Button("Compute"))
    service->RequestCompute(data, rows);
  ImGui::EndDisabled();
  ImGui::SameLine();
  if (ImGui::Button("Cancel"))
    service->RequestCancel();
  ImGui::SameLine();
  ImGui::Text("Status: ");
  ImGui::SameLine(0, 0);
  ImGui::TextColored(StatusColor(status), "%s", StatusText(status));
  ImGui::SameLine(0, 0);
  ImGui::Text(" (天 %zu/%zu)", corr.done.load(std::memory_order_relaxed),
              corr.total.load(std::memory_order_relaxed));

  std::lock_guard<std::mutex> lock(corr.mutex);
  const size_t n = corr.n();
  if (n < 2) {
    ImGui::Separator();
    if (status == analysis::Status::Building)
      ImGui::TextDisabled("构建中: 第一天算完即出图...");
    else
      ImGui::TextDisabled("该层可算的特征不足 2 列 (L0 目前只有 _meta 一列); 或过滤后行集过窄.");
    return;
  }
  assert(corr.rho.size() == n * n);

  ImGui::SameLine();
  ImGui::TextDisabled("| %zu x %zu 特征", n, n);

  // ---- 2. 选项行 ----
  ImGui::Checkbox("|rho|", &ui.abs_mode);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("只看强度 (单极色标); 关掉则带符号 (红正蓝负)");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(160);
  ImGui::SliderFloat("阈值", &ui.threshold, 0.0f, 0.95f, "%.2f");
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("|rho| 低于此值的格子按 0 画 (压掉噪声, 露出块结构)");
  ImGui::SameLine();
  ImGui::RadioButton("表格序", &ui.order_mode, 0);
  ImGui::SameLine();
  ImGui::RadioButton("聚类序", &ui.order_mode, 1);
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("按 1-|rho| 做平均链接聚类重排, 热图出块对角");
  if (ui.order_mode == 1) {
    // 自动聚类: 边长变了立刻算; 构建中基于早期快照先画 (不跟着每天发布重算 O(n³)),
    // Done 后按终版补算一次
    const uint64_t ep = corr.epoch.load(std::memory_order_acquire);
    const bool stale = ui.cluster_n != n || ui.cluster_order.size() != n;
    if (stale || (status != analysis::Status::Building && ui.cluster_epoch != ep)) {
      ui.cluster_order = cluster_order(corr.rho, n);
      ui.cluster_n = n;
      ui.cluster_epoch = ep;
    } else if (ui.cluster_epoch != ep) {
      ImGui::SameLine();
      ImGui::TextDisabled("(聚类序基于早期快照, 构建完重排)");
    }
  }

  ImGui::SameLine();
  if (ImGui::Checkbox("Lag 图", &ui.lag_mode)) {
    if (!ui.lag_mode) {
      lag_service->RequestCancel(); // 关掉即止损; 快照失效, 再勾时重新提交
      ui.lag_req_rows.clear();
      ui.lag_req_level = -1;
    } else if (lag_matches(data.corr_lag, corr) &&
               data.corr_lag.status.load(std::memory_order_acquire) == analysis::Status::Done) {
      ui.lag_req_rows = rows; // 上次算完的还能用 (行集没变), 不重算
      ui.lag_req_level = level;
    }
  }
  if (ImGui::IsItemHovered())
    ImGui::SetTooltip("每对在 ±%zu 行内 |rho| 最大的 lag 着色: 0 = 中性, 正 = 行特征领先列特征.\n"
                      "非 0 的格子 = 平移关系, 值得去右侧曲线细看. 成本 ≈ 矩阵 2~3 倍, 勾上才算",
                      kCorrMaxLag);
  const CorrLag *lag = nullptr;
  std::unique_lock<std::mutex> lag_lock(data.corr_lag.mutex, std::defer_lock);
  if (ui.lag_mode) {
    lag_lock.lock();
    const auto lst = data.corr_lag.status.load(std::memory_order_acquire);
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(lst), "%s", StatusText(lst));
    ImGui::SameLine(0, 0);
    ImGui::TextDisabled(" (天 %zu/%zu)", data.corr_lag.done.load(std::memory_order_relaxed),
                        data.corr_lag.total.load(std::memory_order_relaxed));
    if (lag_matches(data.corr_lag, corr))
      lag = &data.corr_lag;
    else {
      ImGui::SameLine();
      ImGui::TextDisabled("(列集未对齐, 等首日发布)");
    }
  }

  // ---- 画图缓冲 ----
  const uint64_t key = disp_key(corr, lag, ui);
  if (key != ui.disp_key || ui.disp.size() != n * n) {
    rebuild_disp(corr, lag, ui);
    ui.disp_key = key;
  }
  if (ui.n_blank > 0) {
    ImGui::SameLine();
    ImGui::TextDisabled("| 空格 %u (样本 < %zu)", ui.n_blank, kCorrMinPairN);
  }

  ImGui::Separator();

  // ---- 3. 热图 + 右侧选中对面板 ----
  const auto &meta_list = data.feature.metadata.features[corr.level];
  const float panel_w = 380.0f;
  const float avail_w = ImGui::GetContentRegionAvail().x;
  const float map_w = std::max(200.0f, avail_w - panel_w - ImGui::GetStyle().ItemSpacing.x);

  ImGui::BeginChild("##corr_map", ImVec2(map_w, 0), false);
  {
    // 轴刻度只在列少时标 (几百列的标签会糊成一片; 具体是谁靠悬停)
    constexpr size_t kMaxTicks = 40;
    const bool show_ticks = n <= kMaxTicks;
    if (show_ticks) {
      ui.tick_pos.resize(n);
      ui.tick_labels.resize(n);
      for (size_t k = 0; k < n; ++k) {
        ui.tick_pos[k] = static_cast<double>(k) + 0.5;
        ui.tick_labels[k] = meta_list[corr.cols[ui.disp_order[k]]].code;
      }
    }

    // lag 图用 PiYG (紫 ← 白 → 绿), 与 ρ 图的蓝白红一眼分得开; 量程 ±kCorrMaxLag
    ImPlot::PushColormap(ui.lag_mode ? ImPlotColormap_PiYG
                                     : (ui.abs_mode ? ImPlotColormap_Viridis : signed_colormap()));
    const double scale_lo = ui.lag_mode ? -(double)kCorrMaxLag : (ui.abs_mode ? 0.0 : -1.0);
    const double scale_hi = ui.lag_mode ? (double)kCorrMaxLag : 1.0;
    constexpr float kScaleW = 70.0f; // 右侧色标条
    const ImVec2 avail = ImGui::GetContentRegionAvail();
    const ImVec2 map_size(avail.x - kScaleW - ImGui::GetStyle().ItemSpacing.x, avail.y);
    if (ImPlot::BeginPlot("##corr", map_size,
                          ImPlotFlags_NoLegend | ImPlotFlags_NoMenus | ImPlotFlags_Equal)) {
      // PlotHeatmap 自己把第 0 行画在 bounds 上沿 (reverse_y), 故 Y 轴不再 Invert:
      // 屏幕第 r 行 ↔ y ∈ [n-r-1, n-r)
      ImPlot::SetupAxes(nullptr, nullptr,
                        ImPlotAxisFlags_Lock | ImPlotAxisFlags_NoGridLines,
                        ImPlotAxisFlags_Lock | ImPlotAxisFlags_NoGridLines);
      ImPlot::SetupAxisLimits(ImAxis_X1, 0, (double)n, ImPlotCond_Always);
      ImPlot::SetupAxisLimits(ImAxis_Y1, 0, (double)n, ImPlotCond_Always);
      if (show_ticks) {
        // Y 轴刻度位置要随行序翻转 (第 r 行在 y = n - r - 0.5)
        ui.tick_pos_y.resize(n);
        for (size_t k = 0; k < n; ++k)
          ui.tick_pos_y[k] = (double)n - (double)k - 0.5;
        ImPlot::SetupAxisTicks(ImAxis_X1, ui.tick_pos.data(), (int)n, ui.tick_labels.data());
        ImPlot::SetupAxisTicks(ImAxis_Y1, ui.tick_pos_y.data(), (int)n, ui.tick_labels.data());
      }
      // 下三角逐行画 (第 r 行 r+1 格): PlotHeatmap 无逐格透明度, 整块画会把上三角镜像也涂上
      char lbl[16];
      for (size_t r = 0; r < n; ++r) {
        std::snprintf(lbl, sizeof lbl, "##r%zu", r);
        ImPlot::PlotHeatmap(lbl, ui.disp.data() + r * n, 1, (int)(r + 1), scale_lo, scale_hi,
                            nullptr, ImPlotPoint(0, (double)(n - r - 1)),
                            ImPlotPoint((double)(r + 1), (double)(n - r)));
      }

      // 悬停反算格子: 列 c = floor(x), 行 r = n - 1 - floor(y); 只认下三角 (c <= r).
      // 悬停即选中并触发该对 lead-lag (换对即换请求, 在跑的被取消)
      if (ImPlot::IsPlotHovered()) {
        const ImPlotPoint mp = ImPlot::GetPlotMousePos();
        const long long cx = (long long)std::floor(mp.x);
        const long long cy = (long long)n - 1 - (long long)std::floor(mp.y);
        if (cx >= 0 && cy >= 0 && cy < (long long)n && cx <= cy) {
          const size_t i = (size_t)ui.disp_order[(size_t)cy];
          const size_t j = (size_t)ui.disp_order[(size_t)cx];
          const float r = corr.rho[i * n + j];
          const uint32_t pn = corr.pair_n[i * n + j];
          ImGui::BeginTooltip();
          ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "%s",
                             meta_list[corr.cols[i]].code);
          ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.3f, 1.0f), "%s",
                             meta_list[corr.cols[j]].code);
          if (r == r)
            ImGui::Text("rho = %+.3f   n = %u", r, pn);
          else
            ImGui::TextDisabled("样本不足 (n = %u)", pn);
          if (lag) {
            const float rb = lag->rho_best[i * n + j];
            if (rb == rb)
              ImGui::Text("lag* = %+d   rho(lag*) = %+.3f", (int)lag->lag_best[i * n + j], rb);
            else
              ImGui::TextDisabled("lag 图: 样本不足");
          }
          ImGui::EndTooltip();
          if ((int)i != ui.sel_i || (int)j != ui.sel_j) {
            ui.sel_i = (int)i;
            ui.sel_j = (int)j;
            if (i != j)
              pair_service->RequestCompute(data, corr.cols[i], corr.cols[j]);
          }
        }
      }
      ImPlot::EndPlot();
    }
    ImGui::SameLine();
    ImPlot::ColormapScale(ui.lag_mode ? "lag" : (ui.abs_mode ? "|rho|" : "rho"), scale_lo,
                          scale_hi, ImVec2(kScaleW, avail.y), ui.lag_mode ? "%.0f" : "%.1f");
    ImPlot::PopColormap();
  }
  ImGui::EndChild();

  ImGui::SameLine();
  ImGui::BeginChild("##corr_pair", ImVec2(0, 0), true);
  render_pair_panel(data, lag, ui);
  ImGui::EndChild();
}

void StopTabCorr(CorrService *service, CorrPairService *pair_service, CorrLagService *lag_service,
                 SharedData & /*data*/) {
  if (service)
    service->RequestCancel();
  if (pair_service)
    pair_service->RequestCancel();
  if (lag_service)
    lag_service->RequestCancel();
}

void InvalidateCorrRequests(CorrUIState &ui) {
  ui.req_rows.clear();
  ui.req_level = -1;
  ui.lag_req_rows.clear();
  ui.lag_req_level = -1;
}

} // namespace GUI::Features
