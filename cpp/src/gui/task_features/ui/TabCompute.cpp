// Tab Compute Implementation
#include "gui/task_features/ui/TabCompute.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_features/services/ComputeService.hpp"
#include "misc/affinity.hpp"
#include "shared/Config.hpp"
#include "shared/FeatureLib.hpp"

#include "imgui.h"

#include <algorithm>

#include <string>

namespace GUI::Features {

// 库清单 + 逐日 stat 已落盘 (以档内末层日文件为 commit 标记, 见 FeatureRead::has_date)
static LibStatus lib_status(const Config &config, const StockDaysVec &stock_days, std::string_view name) {
  const FeatureLib lib = load_lib(config, name);
  LibStatus s{lib.name, lib.tier};
  if (!lib.ready())
    return s;
  s.ready = true;
  s.first = lib.dates.front();
  s.last = lib.dates.back();
  s.lag = static_cast<size_t>(std::count_if(stock_days.begin(), stock_days.end(),
                                            [&](const CalendarDay &d) { return d.trading && d.date > s.last; }));
  s.assets = lib.axis.size();
  s.days = lib.dates.size();
  s.done = static_cast<size_t>(std::count_if(lib.dates.begin(), lib.dates.end(),
                                             [&](misc::Ymd d) { return FeatureRead::has_date(lib.tier, lib.dir, d); }));
  return s;
}

void RenderTabCompute(ComputeService *service, ComputeState &state, const StockDaysVec &stock_days, Config &config) {
  const bool is_running = service->busy();
  const bool is_cancelling = service->cancelling();

  // ========================================================================
  // State Machine Logic
  // ========================================================================
  switch (state.ui_state) {
  case ComputeUIState::Idle:
    // Do nothing, waiting for user action
    break;

  case ComputeUIState::ShowingPopup:
    // Wait for popup to render (at least 1 frame)
    state.popup_frame_count++;
    if (state.popup_frame_count >= 2) {
      // Popup has been rendered, trigger start immediately
      state.trigger_start = true;
      state.ui_state = ComputeUIState::WaitingStart;
    }
    break;

  case ComputeUIState::WaitingStart:
    // Wait for computation to actually start
    if (is_running) {
      state.ui_state = ComputeUIState::Computing;
    }
    break;

  case ComputeUIState::Computing:
    // Wait for computation to finish
    if (!is_running) {
      // Reset to idle
      state.ui_state = ComputeUIState::Idle;
      state.popup_frame_count = 0;
      state.trigger_start = false;
    }
    break;
  }

  ImGui::TextWrapped("Feature Computation - Multi-threaded feature extraction from binary database");
  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // ========================================================================
  // Section 1: Status
  // ========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Status");
  ImGui::Spacing();

  if (is_cancelling)
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "cancelling");
  else
    TextPhase(service->phase.load(std::memory_order_acquire));

  if (is_running) {
    ImGui::SameLine();
    ImGui::Text("%zu / %zu days", service->done.load(std::memory_order_relaxed),
                service->total.load(std::memory_order_relaxed));
    ImGui::SameLine();
    ImGui::TextDisabled("(GUI throttled, compute owns cores)");
  }

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // ========================================================================
  // Section 2: Configuration
  // ========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Configuration");
  ImGui::Spacing();

  // 核布局: 机器物理核拓扑 + 按档预取 / 截面 / 落盘核数推导, TS worker 数不可配 (见 StageLayout)
  const StageLayout::Topology &topo = misc::Affinity::physical_cores();
  const int phys = static_cast<int>(topo.size());

  for (const TierId tier : TIER_IDS) {
    const StageLayout layout = StageLayout::make(topo, state.config.pf_cores[tier], state.config.cs_cores[tier], state.config.io_cores[tier]);

    ImGui::Text("Stages %s (%d 物理核 / %u 逻辑 CPU):", tier_name(tier), layout.total_phys(), misc::Affinity::core_count());
    ImGui::Spacing();

    if (!ImGui::BeginTable(("StageLayout##" + std::string(tier_name(tier))).c_str(), 5,
                           ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit |
                               ImGuiTableFlags_Resizable | ImGuiTableFlags_NoSavedSettings))
      continue;
    ImGui::TableSetupColumn("Stage");
    ImGui::TableSetupColumn("线程");
    ImGui::TableSetupColumn("专核 (物理)");
    ImGui::TableSetupColumn("物理核号");
    ImGui::TableSetupColumn("说明", ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableHeadersRow();

    auto stage_row = [](const StageLayout::Stage &stage) {
      ImGui::TableNextRow();
      ImGui::TableNextColumn();
      ImGui::Text("%s", stage.name);
      ImGui::TableNextColumn();
      ImGui::Text("%d", stage.threads);
      ImGui::TableNextColumn();
      ImGui::Text("%d", stage.cores);
      ImGui::TableNextColumn();
      const std::string cores = StageLayout::core_text(stage);
      ImGui::Text("%s", cores.c_str());
      ImGui::TableNextColumn();
      ImGui::TextWrapped("%s", stage.desc);
    };

    for (const StageLayout::Stage &stage : layout.stages())
      stage_row(stage);

    ImGui::EndTable();

    const std::string id = tier_name(tier);
    ImGui::Text("Pool Slots: %d", state.config.ring_slots(tier));
    ImGui::SameLine();
    ImGui::TextDisabled("(自动 = %d (T+N 悬挂 + 时序下一日 + 截面跨日) + 截面核 + 落盘核 + %d)",
                        ComputeConfig::kMinRingSlots, ComputeConfig::kRingJitter);

    // 预取 / 截面 / 落盘: 按日轮转, 每核独占一个物理核, 从时序让出; 三者之和至多 物理核 − 1 (至少留 1 个 TS 物理核)
    int *const stage_cores[] = {&state.config.pf_cores[tier], &state.config.cs_cores[tier], &state.config.io_cores[tier]};
    const char *const stage_labels[] = {"PF Cores:", "CS Cores:", "IO Cores:"};
    const char *const stage_ids[] = {"##pf_cores_", "##cs_cores_", "##io_cores_"};
    for (size_t k = 0; k < 3; ++k) {
      const int others = *stage_cores[(k + 1) % 3] + *stage_cores[(k + 2) % 3];
      ImGui::Text("%s", stage_labels[k]);
      ImGui::SameLine();
      ImGui::SetNextItemWidth(150);
      if (ImGui::InputInt((stage_ids[k] + id).c_str(), stage_cores[k], 1, 1))
        *stage_cores[k] = std::clamp(*stage_cores[k], 1, phys - 1 - others);
      if (k + 1 < 3)
        ImGui::SameLine();
    }
    ImGui::SameLine();
    ImGui::TextDisabled("(预取 / 截面 / 落盘按日轮转, 每核独占一个物理核, 从时序让出; 看仪表盘哪段忙满就给哪段加核)");
    ImGui::Spacing();
  }

  ImGui::Spacing();
  ImGui::Text("Adopt %%:");
  ImGui::SameLine();
  ImGui::SetNextItemWidth(150);
  if (ImGui::InputInt("##adopt_pct", &state.config.adopt_pct, 1, 5)) {
    state.config.adopt_pct = std::clamp(state.config.adopt_pct, 0, 100);
  }
  ImGui::SameLine();
  ImGui::TextDisabled("(时序核负载再平衡阈值: lagger 每天最多让出持仓 N%%, leader 每天最多领养均值 N%%; 0 = 关闭)");

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // ========================================================================
  // Section 3: Architecture
  // ========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Architecture");
  ImGui::Spacing();

  ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(0.6f, 0.6f, 0.6f, 1.0f)); // Gray text
  ImGui::PushFont(ImGui::GetIO().Fonts->Fonts[0]);                      // Use smaller font if available

  ImGui::TextWrapped("按照日期, 切分多频率Tensor: {[T, F, A]_L_sec(tick), [T, F, A]_L_1min(minute)}_dayN");
  ImGui::Text("T (Time):    max = 100,000  (100K time indices per day (ms/s))");
  ImGui::Text("A (Asset):   max = 1,000    (1K assets per pool)");
  ImGui::Text("F (Feature): max = 1,000    (all feature types combined)");
  ImGui::Text("特征子类型示例: F_TS: (600 时序特征) F_CS (250 截面特征) F_LB (50 标签) F_SH (50 共享中间值) F_META (50 元数据)");
  ImGui::Text("访问模式分析 (优化目标: 最小化总内存访问时间)");

  // Access pattern table
  if (ImGui::BeginTable("AccessPattern", 7,
                        ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit |
                            ImGuiTableFlags_Resizable | ImGuiTableFlags_NoSavedSettings)) {
    ImGui::TableSetupColumn("操作");
    ImGui::TableSetupColumn("循环结构");
    ImGui::TableSetupColumn("单次vector访问");
    ImGui::TableSetupColumn("总内存访问量");
    ImGui::TableSetupColumn("权重");
    ImGui::TableSetupColumn("并行度");
    ImGui::TableSetupColumn("描述", ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableHeadersRow();

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("TS_write_TS");
    ImGui::TableNextColumn();
    ImGui::Text("for a: for t: write[F_TS]");
    ImGui::TableNextColumn();
    ImGui::Text("连续写600个");
    ImGui::TableNextColumn();
    ImGui::Text("TxAxF_TS = 240GB");
    ImGui::TableNextColumn();
    ImGui::Text("39%%");
    ImGui::TableNextColumn();
    ImGui::Text("10 cores");
    ImGui::TableNextColumn();
    ImGui::TextWrapped("每个core处理~100个assets, 对每个asset遍历时间, 在(a,t)处连续写F_TS个features");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("CS_read_TS");
    ImGui::TableNextColumn();
    ImGui::Text("for t: for f: read[A]");
    ImGui::TableNextColumn();
    ImGui::Text("连续读1000个");
    ImGui::TableNextColumn();
    ImGui::Text("Tx(F_TS+F_OT)xA = 280GB");
    ImGui::TableNextColumn();
    ImGui::Text("45%%");
    ImGui::TableNextColumn();
    ImGui::Text("1 core");
    ImGui::TableNextColumn();
    ImGui::TextWrapped("在每个时刻t, 对每个feature f, 连续读取所有A个assets的值(截面计算)");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("CS_write_CS");
    ImGui::TableNextColumn();
    ImGui::Text("for t: for f: write[A]");
    ImGui::TableNextColumn();
    ImGui::Text("连续写1000个");
    ImGui::TableNextColumn();
    ImGui::Text("TxF_CSxA = 100GB");
    ImGui::TableNextColumn();
    ImGui::Text("16%%");
    ImGui::TableNextColumn();
    ImGui::Text("1 core");
    ImGui::TableNextColumn();
    ImGui::TextWrapped("在每个时刻t, 对每个feature f, 连续写入所有A个assets的值(截面结果)");

    ImGui::EndTable();
  }

  // Memory layout table
  if (ImGui::BeginTable("MemoryLayout", 6,
                        ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit |
                            ImGuiTableFlags_Resizable | ImGuiTableFlags_NoSavedSettings)) {
    ImGui::TableSetupColumn("布局");
    ImGui::TableSetupColumn("地址公式");
    ImGui::TableSetupColumn("TS_write(39%%)");
    ImGui::TableSetupColumn("CS_read(45%%)");
    ImGui::TableSetupColumn("CS_write(16%%)");
    ImGui::TableSetupColumn("推荐度");
    ImGui::TableHeadersRow();

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[T][A][F]");
    ImGui::TableNextColumn();
    ImGui::Text("(t*A+a)*F+f");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("跳4KB");
    ImGui::TableNextColumn();
    ImGui::Text("跳4KB");
    ImGui::TableNextColumn();
    ImGui::Text("良好");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[T][F][A]");
    ImGui::TableNextColumn();
    ImGui::Text("(t*F+f)*A+a");
    ImGui::TableNextColumn();
    ImGui::Text("跳4KB");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("最优 √√√");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[A][T][F]");
    ImGui::TableNextColumn();
    ImGui::Text("(a*T+t)*F+f");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("较差");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[A][F][T]");
    ImGui::TableNextColumn();
    ImGui::Text("(a*F+f)*T+t");
    ImGui::TableNextColumn();
    ImGui::Text("跳400KB");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("极差");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[F][T][A]");
    ImGui::TableNextColumn();
    ImGui::Text("(f*T+t)*A+a");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("连续");
    ImGui::TableNextColumn();
    ImGui::Text("不推荐");

    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::Text("[F][A][T]");
    ImGui::TableNextColumn();
    ImGui::Text("(f*A+a)*T+t");
    ImGui::TableNextColumn();
    ImGui::Text("跳400MB");
    ImGui::TableNextColumn();
    ImGui::Text("跳400KB");
    ImGui::TableNextColumn();
    ImGui::Text("跳400KB");
    ImGui::TableNextColumn();
    ImGui::Text("极差");

    ImGui::EndTable();
  }

  ImGui::PopFont();
  ImGui::PopStyleColor();

  ImGui::Spacing();
  ImGui::Separator();
  ImGui::Spacing();

  // ========================================================================
  // Section 4: Pools (每池一行: 清单 / 落盘进度 / 判废, 各自 Compute)
  // ========================================================================
  ImGui::TextColored(ImVec4(0.4f, 0.8f, 1.0f, 1.0f), "Pools");
  ImGui::SameLine();
  if (ImGui::SmallButton("Refresh"))
    state.libs_dirty = true;
  ImGui::SameLine();
  ImGui::TextDisabled("(all: bar1m + 基本面, 全市场, %s 起至末个 bar1m 日; 池: 订单簿还原, %s 起至末个已编码日 (不晚于 all), "
                      "成员 = Bar 池 D−1 收盘 pool_* 列 → 先算 all; T-n = 末日之后的交易日数)",
                      misc::dash(::config::PIPELINE_START_DATE).c_str(), misc::dash(config.start_date).c_str());
  ImGui::Spacing();

  if (state.libs_dirty && !is_running) {
    state.libs_dirty = false;
    state.libs.clear();
    for (const PoolDef &d : feature_pools())
      state.libs.push_back(lib_status(config, stock_days, d.name));
  }
  // 旁读档的库落盘齐全才放行本档 (池的成员来自 Bar 池)
  PerTier<bool> tier_complete;
  for (const TierId t : TIER_IDS)
    tier_complete[t] = std::ranges::all_of(state.libs, [&](const LibStatus &s) { return s.tier != t || (s.ready && s.done == s.days); });
  const auto sides_complete = [&](TierId t) {
    return std::ranges::all_of(TIER_IDS, [&](TierId s) { return !tier_side(t, s) || tier_complete[s]; });
  };

  if (ImGui::BeginTable("Pools", 8,
                        ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit |
                            ImGuiTableFlags_Resizable | ImGuiTableFlags_NoSavedSettings)) {
    ImGui::TableSetupColumn("池");
    ImGui::TableSetupColumn("中文名");
    ImGui::TableSetupColumn("档");
    ImGui::TableSetupColumn("区间");
    ImGui::TableSetupColumn("资产");
    ImGui::TableSetupColumn("已落盘");
    ImGui::TableSetupColumn("状态");
    ImGui::TableSetupColumn("操作", ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableHeadersRow();

    for (const LibStatus &s : state.libs) {
      ImGui::TableNextRow();
      ImGui::TableNextColumn();
      ImGui::Text("%s", s.name.c_str());
      ImGui::TableNextColumn();
      ImGui::Text("%s", pool_def(s.name).name_cn);
      ImGui::TableNextColumn();
      ImGui::Text("%s", tier_name(s.tier));
      ImGui::TableNextColumn();
      if (s.ready)
        ImGui::Text("%s ~ %s (T-%zu)", misc::dash(s.first).c_str(), misc::dash(s.last).c_str(), s.lag);
      ImGui::TableNextColumn();
      if (s.ready)
        ImGui::Text("%zu", s.assets);
      ImGui::TableNextColumn();
      if (s.ready)
        ImGui::Text("%zu / %zu", s.done, s.days);
      ImGui::TableNextColumn();
      const bool computing = is_running && state.config.pool == s.name;
      if (computing)
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "computing");
      else if (FeatureRead::dir_stale(config.FeatureLibDir(s.name)))
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "stale");
      else if (!s.ready)
        ImGui::TextDisabled("未建");
      else if (s.done == s.days)
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "done");
      else
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "partial");
      ImGui::TableNextColumn();
      const bool blocked = state.ui_state != ComputeUIState::Idle || !sides_complete(s.tier);
      ImGui::BeginDisabled(blocked);
      if (ImGui::SmallButton(("Compute##" + s.name).c_str())) {
        state.config.pool = s.name;
        state.ui_state = ComputeUIState::ShowingPopup;
        state.popup_frame_count = 0;
      }
      ImGui::EndDisabled();
    }
    ImGui::EndTable();
  }

  if (state.ui_state != ComputeUIState::Idle) {
    if (is_cancelling) {
      ImGui::TextWrapped("Cancelling at date boundary...");
    } else if (ImGui::Button("Cancel Compute", ImVec2(200, 30))) {
      service->RequestCancel();
    }
  }

  // ========================================================================
  // Popup Display (based on state machine)
  // ========================================================================
  const bool should_show_popup = (state.ui_state == ComputeUIState::ShowingPopup ||
                                  state.ui_state == ComputeUIState::WaitingStart ||
                                  state.ui_state == ComputeUIState::Computing);

  if (should_show_popup) {
    // Set popup position to center on first frame
    ImVec2 center = ImGui::GetMainViewport()->GetCenter();
    ImGui::SetNextWindowPos(center, ImGuiCond_Appearing, ImVec2(0.5f, 0.5f));
    ImGui::OpenPopup("Feature Computation##Compute");
  }

  if (ImGui::BeginPopupModal("Feature Computation##Compute", nullptr,
                             ImGuiWindowFlags_AlwaysAutoResize | ImGuiWindowFlags_NoResize)) {
    // Check if we should close popup (state returned to Idle)
    if (state.ui_state == ComputeUIState::Idle) {
      ImGui::CloseCurrentPopup();
      ImGui::EndPopup();
      return; // Exit early to avoid rendering popup content
    }

    // Fixed width to prevent resize when text changes
    ImGui::PushItemWidth(500.0f);

    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.0f, 0.8f, 0.0f, 1.0f));
    ImGui::Text("Feature Computation Starting");
    ImGui::PopStyleColor();

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    const TierId run_tier = pool_def(state.config.pool).tier;
    const bool bar_tier = run_tier == TierId::Bar;
    ImGui::Text("Pool %s, Period:", state.config.pool.c_str());
    ImGui::Indent();
    ImGui::BulletText("Start: %s", misc::dash(bar_tier ? ::config::PIPELINE_START_DATE : config.start_date).c_str());
    ImGui::BulletText("End: %s", bar_tier ? "末个 bar1m 日 (须有次日交易日)" : "min(末个已编码日, all 末日)");
    ImGui::Unindent();

    ImGui::Spacing();
    ImGui::Text("System Status:");
    ImGui::Indent();
    ImGui::BulletText("GUI entering low-refresh mode");
    ImGui::BulletText("%s", StageLayout::make(topo, state.config.pf_cores[run_tier], state.config.cs_cores[run_tier], state.config.io_cores[run_tier]).summary().c_str());
    ImGui::BulletText("Allocating %d ring slots", state.config.ring_slots(run_tier));
    ImGui::BulletText("Processing backtest period only");
    ImGui::Unindent();

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    // Fixed-width text area to prevent popup resize
    ImGui::BeginChild("StatusText", ImVec2(450.0f, 30.0f), false, ImGuiWindowFlags_NoScrollbar);

    // Display status based on state machine
    switch (state.ui_state) {
    case ComputeUIState::ShowingPopup:
      ImGui::Text("Starting computation...");
      break;
    case ComputeUIState::WaitingStart:
    case ComputeUIState::Computing:
      ImGui::Text("%s", is_cancelling ? "Cancelling at date boundary..." : "Computing... GUI is throttled.");
      break;
    default:
      break;
    }

    ImGui::EndChild();

    if (state.ui_state == ComputeUIState::WaitingStart || state.ui_state == ComputeUIState::Computing) {
      if (is_cancelling) {
        ImGui::BeginDisabled();
        ImGui::Button("Cancelling...", ImVec2(160, 28));
        ImGui::EndDisabled();
      } else if (ImGui::Button("Cancel Compute", ImVec2(160, 28))) {
        service->RequestCancel();
      }
    }

    ImGui::PopItemWidth();

    ImGui::EndPopup();
  }
}

} // namespace GUI::Features
