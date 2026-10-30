// Tab Compute Implementation
#include "gui/task_features/ui/TabCompute.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_features/services/ComputeService.hpp"
#include "misc/affinity.hpp"
#include "shared/Config.hpp"
#include "shared/FeatureLib.hpp"
#include "shared/SharedData.hpp"

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
  s.outside = lib.outside.size();
  s.resumable = resumable(lib);
  s.dates = lib.dates;
  return s;
}

// 池行操作按钮悬浮: 禁用 (灰) 时给原因, 可用时给将要算的日期段
static void action_tooltip(const LibStatus &s, const char *busy, bool resume) {
  if (!ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    return;
  const bool prefix = s.plan.size() >= s.dates.size() && std::equal(s.dates.begin(), s.dates.end(), s.plan.begin());
  if (busy)
    ImGui::SetTooltip("%s", busy);
  else if (s.plan.empty() && !s.side_partial.empty())
    ImGui::SetTooltip("旁读池 %s 未落盘齐, 本池日期轴截不到它的末日: 先把它算完", s.side_partial.c_str());
  else if (s.plan.empty())
    ImGui::SetTooltip("源日历无可算日");
  else if (!resume)
    ImGui::SetTooltip("全量: %s ~ %s (%zu 日), 按规划重建资产轴从头算",
                      misc::dash(s.plan.front()).c_str(), misc::dash(s.plan.back()).c_str(), s.plan.size());
  else if (!s.ready)
    ImGui::SetTooltip("库未建: 先 Compute 全量");
  else if (!s.complete())
    ImGui::SetTooltip("库未落盘齐 (%zu / %zu): 只能 Compute 全量", s.done, s.days);
  else if (!s.resumable)
    ImGui::SetTooltip("末日续算点与库清单对不上: 只能 Compute 全量");
  else if (!prefix)
    ImGui::SetTooltip("库日期轴不是当前规划的前缀 (源日历 / 旁读池末日变了): 只能 Compute 全量");
  else if (s.plan.size() == s.dates.size())
    ImGui::SetTooltip("无新日: 库已到数据末日 %s", misc::dash(s.last).c_str());
  else
    ImGui::SetTooltip("续算: %s ~ %s (%zu 新日), 沿用库资产轴 (%zu 资产), 轴外 %zu 个不纳入",
                      misc::dash(s.plan[s.dates.size()]).c_str(), misc::dash(s.plan.back()).c_str(),
                      s.plan.size() - s.dates.size(), s.assets, s.outside);
}

// 起一轮 (弹窗状态机从 ShowingPopup 走)
static void begin_compute(ComputeState &state, const LibStatus &s, bool resume) {
  state.config.pool = s.name;
  state.config.dates = s.plan;
  state.config.resume = resume;
  state.ui_state = ComputeUIState::ShowingPopup;
  state.popup_frame_count = 0;
}

void TickCompute(ComputeService &service, ComputeState &state, SharedData &data, bool inputs_ready) {
  const Config &config = data.config;
  const bool is_running = service.busy();

  // ---- 弹窗状态机 ----
  switch (state.ui_state) {
  case ComputeUIState::Idle:
    break;
  case ComputeUIState::ShowingPopup:
    // 弹窗至少画过一帧再开跑 (开跑即低刷新)
    if (++state.popup_frame_count >= 2) {
      service.Request(state.config);
      state.ui_state = ComputeUIState::WaitingStart;
    }
    break;
  case ComputeUIState::WaitingStart:
    if (is_running)
      state.ui_state = ComputeUIState::Computing;
    break;
  case ComputeUIState::Computing:
    if (!is_running) {
      state.ui_state = ComputeUIState::Idle;
      state.popup_frame_count = 0;
      state.libs_dirty = true;
      if (service.phase.load(std::memory_order_acquire) == Phase::Cancelled)
        state.auto_append = false;
    }
    break;
  }
  if (!inputs_ready)
    return;

  // ---- 池表: 数据换进 (同步 / 编码收尾都以扫描换进结束) 即重扫, 数据末日 / 未计算随之前移; 并起一轮自动续算 ----
  if (state.generation != data.asset.generation) {
    state.generation = data.asset.generation;
    state.libs_dirty = true;
    state.auto_append = true;
    state.auto_done.clear();
  }
  if (state.libs_dirty && !is_running) {
    state.libs_dirty = false;
    state.libs.clear();
    for (const PoolDef &d : feature_pools())
      state.libs.push_back(lib_status(config, data.assetinfo.get_stock_days(), d.name));
    // 规划: 本档源日历截到各旁读档当前池的库末日; 旁读库未落盘齐 → 不可算
    PerTier<std::vector<misc::Ymd>> source;
    for (const TierId t : TIER_IDS)
      source[t] = source_dates(data, t);
    for (LibStatus &s : state.libs) {
      s.plan = source[s.tier];
      for (const TierId t : TIER_IDS) {
        if (!tier_side(s.tier, t))
          continue;
        const LibStatus &side = *std::ranges::find(state.libs, data.feature.selection.pool_of(t), &LibStatus::name);
        if (side.complete()) {
          std::erase_if(s.plan, [&](misc::Ymd d) { return d > side.last; });
        } else {
          s.plan.clear();
          s.side_partial = side.name;
        }
      }
    }
  }

  // ---- 自动续算: 空闲 (无在算 / 无全量确认 / 无编码在飞) 时依次 Append 可续算且未判废的池, 每池一轮至多一次;
  //      池表序 all 在前, 它算完重扫, 旁读它的池规划随之前移再轮到; 无可续算 / 取消即止 ----
  if (!state.auto_append || state.ui_state != ComputeUIState::Idle || is_running || state.libs_dirty || !state.confirm_pool.empty() ||
      data.taskstate.database.encode_inflight)
    return;
  const auto it = std::ranges::find_if(state.libs, [&](const LibStatus &s) {
    return s.appendable() && !FeatureRead::dir_stale(config.FeatureLibDir(s.name)) && std::ranges::find(state.auto_done, s.name) == state.auto_done.end();
  });
  if (it == state.libs.end()) {
    state.auto_append = false;
    return;
  }
  state.auto_done.push_back(it->name);
  begin_compute(state, *it, true);
}

void RenderTabCompute(ComputeService *service, ComputeState &state, SharedData &data) {
  const Config &config = data.config;
  const bool is_running = service->busy();
  const bool is_cancelling = service->cancelling();

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
  ImGui::TextDisabled("(all: bar1m + 基本面, 全市场, %s 起至末个 bar1m 日; 池: 订单簿还原, %s 起至末个已编码日 (不晚于 all 末日), "
                      "成员 = Bar 池 D−1 收盘 pool_* 列 → 先把 all 算齐; 未计算 = 库末日之后的交易日数; 数据末日 = 现在重算能到的末日; "
                      "Append = 从库末日续算新日 (沿用库的资产轴, 与同轴全量逐位一致; 同步 / 编码换进数据后, 可续算且未判废的池自动依次 Append); 轴外 = 沿用的轴之外新上市 / 新进池的资产, 要纳入就 Compute 全量)",
                      misc::dash(::config::PIPELINE_START_DATE).c_str(), misc::dash(config.start_date).c_str());
  ImGui::Spacing();

  // 编码在飞不开算: Lob 按帧目录快照读日文件, 编码会 rename 换掉它们
  const char *const busy = state.ui_state != ComputeUIState::Idle    ? "特征计算进行中"
                           : data.taskstate.database.encode_inflight ? "编码进行中: 编码会 rename 换掉日文件, 收尾后再算"
                                                                     : nullptr;

  if (ImGui::BeginTable("Pools", 12,
                        ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit |
                            ImGuiTableFlags_Resizable | ImGuiTableFlags_NoSavedSettings)) {
    ImGui::TableSetupColumn("池");
    ImGui::TableSetupColumn("中文名");
    ImGui::TableSetupColumn("档");
    ImGui::TableSetupColumn("区间");
    ImGui::TableSetupColumn("数据末日");
    ImGui::TableSetupColumn("资产");
    ImGui::TableSetupColumn("天数");
    ImGui::TableSetupColumn("未计算");
    ImGui::TableSetupColumn("轴外");
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
        ImGui::Text("%s ~ %s", misc::dash(s.first).c_str(), misc::dash(s.last).c_str());
      ImGui::TableNextColumn();
      if (!s.plan.empty())
        ImGui::Text("%s", misc::dash(s.plan.back()).c_str());
      else
        ImGui::TextDisabled("-");
      ImGui::TableNextColumn();
      if (s.ready)
        ImGui::Text("%zu", s.assets);
      ImGui::TableNextColumn();
      if (s.ready)
        ImGui::Text("%zu", s.days);
      ImGui::TableNextColumn();
      if (s.ready && s.lag > 0)
        ImGui::Text("%zu", s.lag);
      else if (s.ready)
        ImGui::TextDisabled("0");
      ImGui::TableNextColumn();
      if (s.ready && s.outside > 0)
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%zu", s.outside);
      else if (s.ready)
        ImGui::TextDisabled("0");
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
      else if (!s.complete())
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "partial");
      else if (!s.plan.empty() && s.plan.back() > s.last)
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "behind");
      else
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "done");
      ImGui::TableNextColumn();
      const bool blocked = busy || s.plan.empty();
      ImGui::BeginDisabled(blocked);
      if (ImGui::SmallButton(("Compute##" + s.name).c_str()))
        state.confirm_pool = s.name;
      ImGui::EndDisabled();
      action_tooltip(s, busy, false);
      ImGui::SameLine();
      ImGui::BeginDisabled(blocked || !s.appendable());
      if (ImGui::SmallButton(("Append##" + s.name).c_str()))
        begin_compute(state, s, true);
      ImGui::EndDisabled();
      action_tooltip(s, busy, true);
    }
    ImGui::EndTable();
  }

  // 全量确认: 弹窗期间池表可能重扫 / 编码开跑, 确认时按当前行重判
  if (!state.confirm_pool.empty())
    ImGui::OpenPopup("确认全量计算##Compute");
  if (ImGui::BeginPopupModal("确认全量计算##Compute", nullptr, ImGuiWindowFlags_AlwaysAutoResize)) {
    const auto it = std::ranges::find(state.libs, state.confirm_pool, &LibStatus::name);
    const bool ok = it != state.libs.end() && !busy && !it->plan.empty();
    if (it != state.libs.end()) {
      const LibStatus &s = *it;
      ImGui::Text("池 %s (%s): 全量 Compute", s.name.c_str(), pool_def(s.name).name_cn);
      if (!s.plan.empty())
        ImGui::BulletText("区间 %s ~ %s (%zu 天), 按规划重建资产轴从头算", misc::dash(s.plan.front()).c_str(),
                          misc::dash(s.plan.back()).c_str(), s.plan.size());
      if (s.ready)
        ImGui::BulletText("现有库 %s ~ %s (%zu 天, %zu 资产, 已落盘 %zu) 开跑即清空", misc::dash(s.first).c_str(),
                          misc::dash(s.last).c_str(), s.days, s.assets, s.done);
      if (s.appendable())
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "可 Append 续算 %zu 个新日, 不必全量", s.plan.size() - s.days);
    }
    if (!ok)
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s", busy ? busy : "当前不可算");
    ImGui::Spacing();
    ImGui::BeginDisabled(!ok);
    if (ImGui::Button("Compute", ImVec2(120, 0))) {
      begin_compute(state, *it, false);
      state.confirm_pool.clear();
      ImGui::CloseCurrentPopup();
    }
    ImGui::EndDisabled();
    ImGui::SameLine();
    if (ImGui::Button("Cancel", ImVec2(120, 0))) {
      state.confirm_pool.clear();
      ImGui::CloseCurrentPopup();
    }
    ImGui::EndPopup();
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
    ImGui::Text("Pool %s (%s), Period:", state.config.pool.c_str(), state.config.resume ? "Append" : "Full");
    ImGui::Indent();
    ImGui::BulletText("Start: %s", misc::dash(state.config.dates.front()).c_str());
    ImGui::BulletText("End: %s (%zu days)", misc::dash(state.config.dates.back()).c_str(), state.config.dates.size());
    ImGui::Unindent();

    ImGui::Spacing();
    ImGui::Text("System Status:");
    ImGui::Indent();
    ImGui::BulletText("GUI entering low-refresh mode");
    ImGui::BulletText("%s", StageLayout::make(topo, state.config.pf_cores[run_tier], state.config.cs_cores[run_tier], state.config.io_cores[run_tier]).summary().c_str());
    ImGui::BulletText("Allocating %d ring slots", state.config.ring_slots(run_tier));
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
