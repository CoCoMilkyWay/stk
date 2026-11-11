// Tab Compute Implementation
#include "gui/compute/ui/TabCompute.hpp"
#include "features/Storage/FeatureLib.hpp"
#include "features/Storage/FeatureRead.hpp"
#include "fundamental/Spec.hpp"
#include "gui/Tasks.hpp"
#include "gui/compute/services/ComputeService.hpp"
#include "gui/util/PoolTip.hpp"
#include "misc/affinity.hpp"
#include "shared/SharedData.hpp"
#include "worker/compute/Layout.hpp"  // StageLayout
#include "worker/compute/TsTuner.hpp" // RingBudget

#include "imgui.h"

#include <algorithm>
#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <iterator>
#include <string>
#include <vector>

namespace GUI::Compute {

constexpr ImVec4 kSectionColor{0.4f, 0.8f, 1.0f, 1.0f};
// 档组 (库表行内各组列 / 核布局各栏按档的依赖序 TIER_ORDER): 组头字色 / 格底色 (同色相, [TIER_ORDER 下标])
constexpr ImVec4 kTierText[] = {{1.0f, 0.75f, 0.3f, 1.0f}, {0.35f, 0.7f, 1.0f, 1.0f}};
constexpr ImVec4 kTierTint[] = {{1.0f, 0.75f, 0.3f, 0.06f}, {0.35f, 0.7f, 1.0f, 0.06f}};
static_assert(std::size(kTierText) == TIER_COUNT && std::size(kTierTint) == TIER_COUNT, "档组配色每档一色");
constexpr ImVec4 kBusyTint{0.7f, 0.4f, 1.0f, 0.18f};
// 每档同构子列: 状态 | 规划日期轴 (数据初始 / 数据末日 / 天数) | 落盘 (已落盘 / 未落盘) | 资产轴 (资产 / 轴外) | 操作
constexpr const char *kLibCols[] = {"状态", "数据初始", "数据末日", "天数", "已落盘", "未落盘", "资产", "轴外", "操作"};
constexpr int kLibColN = static_cast<int>(std::size(kLibCols));

// 上一个控件的悬浮说明 (禁用的也给)
__attribute__((format(printf, 1, 2))) static void hover_tip(const char *fmt, ...) {
  if (!ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    return;
  va_list ap;
  va_start(ap, fmt);
  ImGui::SetTooltipV(fmt, ap);
  va_end(ap);
}

// 规划日期轴为空的原因 (库表按钮 / 规划日期轴格同一份)
static std::string empty_plan_reason(const LibPlan &s) {
  return s.blocked ? "上游库 " + lib_name(*s.blocked) + " 未落盘齐: 先把它算完" : "源日历在上游库日期轴上无可算日 (子池另须晚于 all.bar 首日)";
}

// 库行操作按钮悬浮: 禁用 (灰) 时给原因, 可用时给将要算的日期段
static void action_tooltip(const LibPlan &s, const char *busy, bool resume) {
  if (!ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    return;
  const std::vector<misc::Ymd> &dates = s.lib.dates;
  const bool prefix = s.plan.size() >= dates.size() && std::equal(dates.begin(), dates.end(), s.plan.begin());
  if (busy)
    ImGui::SetTooltip("%s", busy);
  else if (s.plan.empty())
    ImGui::SetTooltip("%s", empty_plan_reason(s).c_str());
  else if (!resume)
    ImGui::SetTooltip("全量: %s ~ %s (%zu 日), 按规划重建资产轴从头算",
                      misc::dash(s.plan.front()).c_str(), misc::dash(s.plan.back()).c_str(), s.plan.size());
  else if (!s.lib.ready())
    ImGui::SetTooltip("库未建: 先 Rebuild");
  else if (!s.complete())
    ImGui::SetTooltip("库未落盘齐 (%zu / %zu): 只能 Rebuild", s.done, dates.size());
  else if (!s.resumable)
    ImGui::SetTooltip("末日续算点与库清单对不上 (lob 另须资产轴在同池 bar 轴内): 只能 Rebuild");
  else if (!prefix)
    ImGui::SetTooltip("库日期轴不是当前规划的前缀 (源日历 / 上游库日期轴变了): 只能 Rebuild");
  else if (s.plan.size() == dates.size())
    ImGui::SetTooltip("无新日: 库已到数据末日 %s", misc::dash(dates.back()).c_str());
  else
    ImGui::SetTooltip("续算: %s ~ %s (%zu 新日), 沿用库资产轴 (%zu 资产), 轴外 %zu 个不纳入",
                      misc::dash(s.plan[dates.size()]).c_str(), misc::dash(s.plan.back()).c_str(),
                      s.plan.size() - dates.size(), s.lib.axis.size(), s.lib.outside.size());
}

// 状态列: 文字 / 色 / 悬浮说明同一条判定链
static void state_cell(const LibPlan &s, bool computing, bool stale, bool resume) {
  const auto put = [](TaskStatus::Kind k, const char *text) { ImGui::TextColored(StatusColor(k), "%s", text); };
  if (computing) {
    put(TaskStatus::Kind::Busy, "computing");
    hover_tip("正在%s", resume ? "续算 (Append)" : "全量重建 (Rebuild)");
  } else if (stale) {
    put(TaskStatus::Kind::Warn, "stale");
    hover_tip("读端发现库头与当前字段表 / 池轴不符, 已删库: Rebuild 重建");
  } else if (!s.lib.ready()) {
    put(TaskStatus::Kind::Muted, "未建");
    hover_tip("无库清单: Rebuild 建库");
  } else if (!s.complete()) {
    put(TaskStatus::Kind::Warn, "partial");
    hover_tip("已落盘 %zu / %zu 日 (上一轮中断): 只能 Rebuild", s.done, s.lib.dates.size());
  } else if (!s.plan.empty() && s.plan.back() > s.lib.dates.back()) {
    put(TaskStatus::Kind::Warn, "behind");
    hover_tip("库末日 %s, 数据末日 %s: %s", misc::dash(s.lib.dates.back()).c_str(), misc::dash(s.plan.back()).c_str(),
              s.appendable() ? "可 Append 续算 (数据换进后自动续)" : "不可续算 (原因见 Append 悬浮)");
  } else {
    put(TaskStatus::Kind::Ready, "done");
    hover_tip("已到数据末日 %s", misc::dash(s.lib.dates.back()).c_str());
  }
}

// 一库的同构子列 (kLibCols 序), 格底色 tint
static void lib_cells(ComputeState &state, const LibPlan &s, bool computing, bool stale, const char *busy, ImU32 tint) {
  const auto cell = [tint] {
    ImGui::TableNextColumn();
    ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, tint);
  };
  const auto none = [] { ImGui::TextDisabled("-"); };
  const auto count = [](size_t n) { n > 0 ? ImGui::Text("%zu", n) : ImGui::TextDisabled("0"); };
  const FeatureLib &lib = s.lib;
  const bool ready = lib.ready();

  cell();
  state_cell(s, computing, stale, state.config.resume);
  const auto plan_date = [&](misc::Ymd d) {
    if (!s.plan.empty())
      ImGui::Text("%s", misc::dash(d).c_str());
    else {
      none();
      hover_tip("%s", empty_plan_reason(s).c_str());
    }
  };
  // ---- 规划日期轴 ----
  cell();
  plan_date(s.plan.empty() ? 0 : s.plan.front());
  cell();
  plan_date(s.plan.empty() ? 0 : s.plan.back());
  cell();
  !s.plan.empty() ? ImGui::Text("%zu", s.plan.size()) : none();
  // ---- 落盘 ----
  cell();
  if (ready && s.done < lib.dates.size()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%zu", s.done);
    hover_tip("已落盘 %zu / %zu 日 (上一轮中断): 只能 Rebuild", s.done, lib.dates.size());
  } else
    count(s.done);
  cell();
  if (s.plan.empty())
    none();
  else {
    count(s.pending);
    if (ready)
      hover_tip("库 %s ~ %s", misc::dash(lib.dates.front()).c_str(), misc::dash(lib.dates.back()).c_str());
  }
  // ---- 资产轴 ----
  cell();
  ready ? ImGui::Text("%zu", lib.axis.size()) : none();
  cell();
  if (!ready)
    none();
  else if (!lib.outside.empty()) {
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%zu", lib.outside.size());
    hover_tip("沿用的资产轴之外新上市 / 新进池的资产 %zu 个: 要纳入就 Rebuild", lib.outside.size());
  } else
    ImGui::TextDisabled("0");
  // ---- 操作 ----
  cell();
  const std::string id = lib_name(lib.id);
  const bool blocked = busy || s.plan.empty();
  ImGui::BeginDisabled(blocked);
  if (ImGui::SmallButton(("Rebuild##" + id).c_str()))
    state.confirm = lib.id;
  ImGui::EndDisabled();
  action_tooltip(s, busy, false);
  ImGui::SameLine();
  ImGui::BeginDisabled(blocked || !s.appendable());
  if (ImGui::SmallButton(("Append##" + id).c_str()))
    state.begin(s, true);
  ImGui::EndDisabled();
  action_tooltip(s, busy, true);
  ImGui::SameLine();
  ImGui::BeginDisabled(busy || !lib.ready());
  if (ImGui::SmallButton(("Remove##" + id).c_str()))
    state.confirm_remove = lib.id;
  ImGui::EndDisabled();
  if (busy)
    hover_tip("%s", busy);
  else if (!lib.ready())
    hover_tip("库未建: 无可删");
  else
    hover_tip("删库: %s ~ %s (%zu 天, %zu 资产) 的全部特征结果连同续算点; 先弹窗确认", misc::dash(lib.dates.front()).c_str(),
              misc::dash(lib.dates.back()).c_str(), lib.dates.size(), lib.axis.size());
}

// 不可开算的原因 (库表按钮 / 全量确认同一份; nullptr = 可算). 编码在飞不开算: Lob 按帧目录快照读日文件, 编码会 rename 换掉它们
static const char *busy_reason(const ComputeState &state, const SharedData &data) {
  return state.ui_state != ComputeUIState::Idle     ? "特征计算进行中"
         : data.task_state.database.encode_inflight ? "编码进行中: 编码会 rename 换掉日文件, 收尾后再算"
                                                    : nullptr;
}

// 进度条 (库 / 模式 / 日数) + 取消 (同行)
static void progress_line(ComputeService &service, const ComputeState &state, bool is_cancelling) {
  const size_t done = service.done.load(std::memory_order_relaxed), total = service.total.load(std::memory_order_relaxed);
  char overlay[96];
  std::snprintf(overlay, sizeof(overlay), "%s  %s  %zu / %zu 日", lib_name(state.config.lib).c_str(), state.config.resume ? "Append" : "Rebuild",
                done, total);
  ImGui::ProgressBar(total > 0 ? static_cast<float>(done) / static_cast<float>(total) : 0.0f, ImVec2(420.0f, 0.0f), overlay);
  ImGui::SameLine();
  ImGui::BeginDisabled(is_cancelling);
  if (ImGui::Button(is_cancelling ? "Cancelling..." : "Cancel"))
    service.RequestCancel();
  ImGui::EndDisabled();
  hover_tip("%s", is_cancelling ? "在日边界停下" : "在日边界停下; 已落盘日保留, 之后可续算");
}

// 状态行: 在算 = 进度条, 否则阶段名; 后接自动续算状态
static void render_status(ComputeService &service, const ComputeState &state, bool is_running, bool is_cancelling) {
  if (is_running)
    progress_line(service, state, is_cancelling);
  else
    TextPhase(service.phase.load(std::memory_order_acquire));
  ImGui::SameLine();
  if (state.auto_append)
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Busy), "自动续算中");
  else
    ImGui::TextDisabled("自动续算: 待数据换进");
  hover_tip("同步 / 编码换进数据后, 空闲时按规划序 (上游在前: all 先于子池, 每池 bar 先于 lob) 依次 Append 可续算且未判废的库;\n"
            "每轮收尾即进程内重启 (各页按新库重建) 再找下一个; 无可续算 / 取消即止");
}

// 档在 TIER_ORDER 中的位次 (档组配色下标)
static size_t order_of(TierId t) { return static_cast<size_t>(std::ranges::find(TIER_ORDER, t) - TIER_ORDER.begin()); }

// 本页的档组 (TIER_ORDER 序) / 池行: 含本页库的才列
static std::vector<TierId> side_tiers(LibSide side) {
  std::vector<TierId> v;
  for (const TierId t : TIER_ORDER)
    if (std::ranges::any_of(pool_ids(), [&](PoolId p) { return on_side(side, lib_id(p, t)); }))
      v.push_back(t);
  return v;
}
static bool side_pool(LibSide side, PoolId p) {
  return std::ranges::any_of(TIER_IDS, [&](TierId t) { return on_side(side, lib_id(p, t)); });
}

// 库表: 一行一池, 本页各档一组同构子列 (他页的库格留空)
static void render_libraries(ComputeState &state, const SharedData &data, bool is_running, LibSide side) {
  const Config &config = data.config;
  const char *const busy = busy_reason(state, data);
  const std::vector<TierId> tiers = side_tiers(side);

  if (side == LibSide::Alpha) {
    ImGui::SetNextItemWidth(200);
    ImGui::InputTextWithHint("##pool_filter", "过滤池名 / 中文名", state.pool_filter, sizeof(state.pool_filter));
    ImGui::SameLine();
  }
  if (ImGui::SmallButton("Refresh"))
    state.libs_dirty = true;
  hover_tip("重扫库清单与落盘日 (进页 / 一轮算完 / 数据换进时自动重扫)");
  ImGui::SameLine();
  ImGui::TextDisabled("(?)");
  hover_tip("库 = 池 × 档, 每池两档各一库:\n"
            "• bar = bar1m + 基本面, %s 起至分钟线与基本面都齐的末日 (基本面: 行 D 所需的 D 盘后 / D+1 盘前表已入盘)\n"
            "• lob = 订单簿还原, %s 起的已编码日, 止于首个待编码日之前 (取同池 bar 日期轴上的日)\n"
            "• 资产轴: all.bar = 全部标的; 其余 = 本库日期段内进过池的标的 (lob 只看自己的回测段, ⊆ 同池 bar 轴)\n"
            "• 成员: all = 当日活跃 (全市场); 子池 bar = all.bar D−1 收盘 pool_* 列; lob = 同池 bar 当日 cs_valid\n"
            "• 上游: lob ← 同池 bar (成员 + 截面旁读); 子池 bar ← all.bar. 上游库未落盘齐则本库不可算\n"
            "\n"
            "• 数据初始 / 数据末日 / 天数 = 规划日期轴 (现有数据能算到的全段, 不必先算)\n"
            "• 已落盘 = 库已落盘日数; 未落盘 = 规划日期轴上尚未落盘的日数\n"
            "• 资产 = 库资产轴 (建库即冻结); 轴外 = 冻结轴之外新上市 / 新进池的资产, 要纳入就 Rebuild\n"
            "\n"
            "• Rebuild = 全量重建 (清库, 按规划重建资产轴从头算; 先弹窗确认)\n"
            "• Append = 从库末日续算新日 (沿用库资产轴, 与同轴全量逐位一致)\n"
            "• Remove = 删库 (清单 / 日文件 / 续算点; 先弹窗确认); 下游库保留, 本库重新落盘齐前不可算\n"
            "\n"
            "• Beta 页 = all.bar (全市场根库: Style 调优与 Market 的源); Alpha 页 = all.lob + 子池各档 (因子的张量库)",
            misc::dash(fundamental::kSyncStart).c_str(), misc::dash(config.start_date).c_str());

  const auto shown = [&](PoolId p) {
    const PoolDef &d = pool_def(p);
    return side_pool(side, p) && (!state.pool_filter[0] || side == LibSide::Beta || std::strstr(d.name.c_str(), state.pool_filter) || std::strstr(d.name_cn.c_str(), state.pool_filter));
  };
  size_t n_shown = 0, n_side = 0;
  for (const PoolId p : pool_ids()) {
    n_shown += shown(p);
    n_side += side_pool(side, p);
  }
  if (side == LibSide::Alpha) {
    ImGui::SameLine();
    ImGui::TextDisabled("%zu / %zu 池", n_shown, n_side);
  }

  constexpr int kPoolColN = 2;
  const int n_cols = kPoolColN + kLibColN * static_cast<int>(tiers.size());
  const float height = ImGui::GetFrameHeightWithSpacing() * (static_cast<float>(std::min<size_t>(n_shown, 24)) + 2.5f);
  if (!ImGui::BeginTable("Libraries", n_cols,
                         ImGuiTableFlags_BordersOuter | ImGuiTableFlags_BordersInnerV | ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_ScrollX |
                             ImGuiTableFlags_ScrollY | ImGuiTableFlags_NoSavedSettings,
                         ImVec2(0.0f, height)))
    return;
  ImGui::TableSetupScrollFreeze(kPoolColN, 2);
  ImGui::TableSetupColumn("池");
  ImGui::TableSetupColumn("中文名");
  for (const TierId t : tiers)
    for (const char *c : kLibCols)
      ImGui::TableSetupColumn((std::string(c) + "##" + tier_name(t)).c_str());
  // 组头行: 每档组首列标档名
  ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
  for (int c = 0; c < n_cols; ++c) {
    ImGui::TableSetColumnIndex(c);
    ImGui::PushID(c);
    if (c >= kPoolColN && (c - kPoolColN) % kLibColN == 0) {
      const TierId t = tiers[static_cast<size_t>((c - kPoolColN) / kLibColN)];
      ImGui::PushStyleColor(ImGuiCol_Text, kTierText[order_of(t)]);
      ImGui::TableHeader(tier_name(t));
      ImGui::PopStyleColor();
    } else
      ImGui::TableHeader("");
    ImGui::PopID();
  }
  ImGui::TableHeadersRow();

  for (const PoolId p : pool_ids()) {
    if (!shown(p))
      continue;
    const PoolDef &pd = pool_def(p);
    ImGui::TableNextRow();
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(pd.name.c_str());
    if (ImGui::IsItemHovered())
      GUI::pool_tip(p);
    ImGui::TableNextColumn();
    ImGui::TextUnformatted(pd.name_cn.c_str());
    if (ImGui::IsItemHovered())
      GUI::pool_tip(p);
    for (const TierId t : tiers) {
      const LibId l = lib_id(p, t);
      const bool computing = is_running && state.config.lib == l;
      const ImU32 tint = ImGui::GetColorU32(computing ? kBusyTint : kTierTint[order_of(t)]);
      if (state.libs && on_side(side, l)) {
        lib_cells(state, (*state.libs)[l], computing, FeatureRead::lib_stale(l), busy, tint);
        continue;
      }
      for (int c = 0; c < kLibColN; ++c) { // 他页的库 / 库表未扫 (重扫在下一帧)
        ImGui::TableNextColumn();
        ImGui::TableSetBgColor(ImGuiTableBgTarget_CellBg, tint);
        if (c == 0 && !on_side(side, l)) {
          ImGui::TextDisabled("-");
          hover_tip("%s 在 %s → Compute", lib_name(l).c_str(), side == LibSide::Alpha ? "Beta" : "Alpha");
        }
      }
    }
  }
  ImGui::EndTable();
}

// 一档的核布局: 物理核拓扑 + 按档预取 / 截面 / 落盘核数推导, TS worker 数不可配 (见 StageLayout)
static void stage_column(ComputeState &state, TierId tier, const StageLayout::Topology &topo, int phys) {
  const StageLayout layout = StageLayout::make(topo, state.config.pf_cores[tier], state.config.cs_cores[tier], state.config.io_cores[tier]);
  ImGui::Text("%d 物理核 / %u 逻辑 CPU, 日环自动", layout.total_phys(), misc::Affinity::core_count());
  hover_tip("日环初值 = 下游段 (T+N 悬挂 + 时序下一日 + 截面跨日 + 截面核 + 落盘核 + 1) + 时序余量 (按初始分配的逐日负载模拟);\n"
            "运行中领跑核撞日环边且之后自己成了最慢者 (损失落在关键路径) 才扩格, 至多用到 %.0f%% 内存; 时序核持续快慢由领养按实测忙碌占比修正",
            (1.0 - RingBudget::kMemMargin) * 100.0);
  if (ImGui::BeginTable("StageLayout", 5, ImGuiTableFlags_Borders | ImGuiTableFlags_SizingFixedFit | ImGuiTableFlags_NoSavedSettings)) {
    ImGui::TableSetupColumn("Stage");
    ImGui::TableSetupColumn("线程");
    ImGui::TableSetupColumn("专核");
    ImGui::TableSetupColumn("物理核号");
    ImGui::TableSetupColumn("说明", ImGuiTableColumnFlags_WidthStretch);
    ImGui::TableHeadersRow();
    for (const StageLayout::Stage &stage : layout.stages()) {
      ImGui::TableNextRow();
      ImGui::TableNextColumn();
      ImGui::Text("%s", stage.name);
      ImGui::TableNextColumn();
      ImGui::Text("%d", stage.threads);
      ImGui::TableNextColumn();
      ImGui::Text("%d", stage.cores);
      ImGui::TableNextColumn();
      ImGui::Text("%s", StageLayout::core_text(stage).c_str());
      ImGui::TableNextColumn();
      ImGui::TextWrapped("%s", stage.desc);
    }
    ImGui::EndTable();
  }
  // 预取 / 截面 / 落盘: 三者之和至多 物理核 − 1 (至少留 1 个 TS 物理核)
  int *const stage_cores[] = {&state.config.pf_cores[tier], &state.config.cs_cores[tier], &state.config.io_cores[tier]};
  const char *const stage_labels[] = {"PF", "CS", "IO"};
  for (size_t k = 0; k < 3; ++k) {
    if (k > 0)
      ImGui::SameLine();
    const int others = *stage_cores[(k + 1) % 3] + *stage_cores[(k + 2) % 3];
    ImGui::Text("%s", stage_labels[k]);
    ImGui::SameLine();
    ImGui::SetNextItemWidth(100);
    if (ImGui::InputInt((std::string("##") + stage_labels[k]).c_str(), stage_cores[k], 1, 1))
      *stage_cores[k] = std::clamp(*stage_cores[k], 1, phys - 1 - others);
  }
}

// 核布局: 本页各档一栏 (对称)
static void render_stages(ComputeState &state, LibSide side) {
  const StageLayout::Topology &topo = misc::Affinity::physical_cores();
  const int phys = static_cast<int>(topo.size());
  const std::vector<TierId> tiers = side_tiers(side);
  if (ImGui::BeginTable("Stages", static_cast<int>(tiers.size()),
                        ImGuiTableFlags_BordersInnerV | ImGuiTableFlags_SizingStretchSame | ImGuiTableFlags_NoSavedSettings)) {
    for (const TierId t : tiers)
      ImGui::TableSetupColumn(tier_name(t));
    ImGui::TableNextRow(ImGuiTableRowFlags_Headers);
    for (size_t g = 0; g < tiers.size(); ++g) {
      ImGui::TableSetColumnIndex(static_cast<int>(g));
      ImGui::PushStyleColor(ImGuiCol_Text, kTierText[order_of(tiers[g])]);
      ImGui::TableHeader(tier_name(tiers[g]));
      ImGui::PopStyleColor();
    }
    ImGui::TableNextRow();
    for (size_t g = 0; g < tiers.size(); ++g) {
      ImGui::TableSetColumnIndex(static_cast<int>(g));
      ImGui::PushID(static_cast<int>(g));
      stage_column(state, tiers[g], topo, phys);
      ImGui::PopID();
    }
    ImGui::EndTable();
  }
  ImGui::TextDisabled("PF / CS / IO = 预取 / 截面 / 落盘核数: 按日轮转, 每核独占一个物理核, 从时序让出; 看仪表盘哪段忙满就给哪段加核");
}

void RenderTabCompute(ComputeHub &hub, SharedData &data, LibSide side) {
  assert(hub.service && "输入就绪才可进 Compute 页");
  ComputeService *const service = hub.service.get();
  ComputeState &state = hub.state;
  const bool is_running = service->busy();
  const bool is_cancelling = service->cancelling();
  const StageLayout::Topology &topo = misc::Affinity::physical_cores();

  render_status(*service, state, is_running, is_cancelling);
  ImGui::Spacing();

  // 先配核布局 (按机器定) 再算库
  if (ImGui::CollapsingHeader("Stages", ImGuiTreeNodeFlags_DefaultOpen))
    render_stages(state, side);
  if (ImGui::CollapsingHeader("Libraries", ImGuiTreeNodeFlags_DefaultOpen))
    render_libraries(state, data, is_running, side);

  // ========================================================================
  // Architecture (说明, 默认折叠)
  // ========================================================================
  if (ImGui::CollapsingHeader("Architecture")) {
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
  }

  // 全量确认: 弹窗期间库表可能重扫 / 编码开跑, 确认时按当前行重判
  const char *const busy = busy_reason(state, data);
  if (state.confirm)
    ImGui::OpenPopup("确认全量重建##Compute");
  if (ImGui::BeginPopupModal("确认全量重建##Compute", nullptr, ImGuiWindowFlags_AlwaysAutoResize)) {
    const LibPlan *const s = state.confirm && state.libs ? &(*state.libs)[*state.confirm] : nullptr;
    const bool ok = s && !busy && !s->plan.empty();
    if (s) {
      const FeatureLib &lib = s->lib;
      ImGui::Text("库 %s (%s): 全量 Rebuild", lib_name(lib.id).c_str(), pool_def(lib_pool(lib.id)).name_cn.c_str());
      if (!s->plan.empty())
        ImGui::BulletText("区间 %s ~ %s (%zu 天), 按规划重建资产轴从头算", misc::dash(s->plan.front()).c_str(),
                          misc::dash(s->plan.back()).c_str(), s->plan.size());
      if (lib.ready())
        ImGui::BulletText("现有库 %s ~ %s (%zu 天, %zu 资产, 已落盘 %zu) 开跑即清空", misc::dash(lib.dates.front()).c_str(),
                          misc::dash(lib.dates.back()).c_str(), lib.dates.size(), lib.axis.size(), s->done);
      if (s->appendable())
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "可 Append 续算 %zu 个新日, 不必全量", s->plan.size() - lib.dates.size());
    }
    if (!ok)
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s", busy ? busy : "当前不可算");
    ImGui::Spacing();
    ImGui::BeginDisabled(!ok);
    if (ImGui::Button("Rebuild", ImVec2(120, 0))) {
      state.begin(*s, false);
      state.confirm.reset();
      ImGui::CloseCurrentPopup();
    }
    ImGui::EndDisabled();
    ImGui::SameLine();
    if (ImGui::Button("Cancel", ImVec2(120, 0))) {
      state.confirm.reset();
      ImGui::CloseCurrentPopup();
    }
    ImGui::EndPopup();
  }

  // 删库确认: 同上, 确认时按当前行重判
  if (state.confirm_remove)
    ImGui::OpenPopup("确认删库##Compute");
  if (ImGui::BeginPopupModal("确认删库##Compute", nullptr, ImGuiWindowFlags_AlwaysAutoResize)) {
    const LibPlan *const s = state.confirm_remove && state.libs ? &(*state.libs)[*state.confirm_remove] : nullptr;
    const bool ok = s && !busy && s->lib.ready();
    if (s) {
      const FeatureLib &lib = s->lib;
      ImGui::Text("库 %s (%s): 删除全部特征结果", lib_name(lib.id).c_str(), pool_def(lib_pool(lib.id)).name_cn.c_str());
      ImGui::BulletText("目录 %s", lib.dir.c_str());
      if (lib.ready())
        ImGui::BulletText("%s ~ %s (%zu 天, %zu 资产, 已落盘 %zu) 连同续算点, 删后只能 Rebuild 重建", misc::dash(lib.dates.front()).c_str(),
                          misc::dash(lib.dates.back()).c_str(), lib.dates.size(), lib.axis.size(), s->done);
      const auto below = [&](LibId l) { // l 在本库之下 (沿 lib_parent 上溯经过本库)
        for (auto up = lib_parent(l); up; up = lib_parent(up->lib))
          if (up->lib == lib.id)
            return true;
        return false;
      };
      const auto n_below = std::ranges::count_if(lib_ids(), [&](LibId l) { return below(l) && (*state.libs)[l].lib.ready(); });
      if (n_below > 0)
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "下游已建库 %d 个保留, 本库重新落盘齐前它们不可算 / 续算", static_cast<int>(n_below));
    }
    if (!ok)
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "%s", busy ? busy : "库未建: 无可删");
    ImGui::Spacing();
    ImGui::BeginDisabled(!ok);
    if (ImGui::Button("Remove", ImVec2(120, 0))) {
      remove_lib(*state.confirm_remove);
      state.confirm_remove.reset();
      data.reinit = {.requested = true, .online = false, .append = false}; // 库变了: 进程内重启, 不顺手起续算
      ImGui::CloseCurrentPopup();
    }
    ImGui::EndDisabled();
    ImGui::SameLine();
    if (ImGui::Button("Cancel", ImVec2(120, 0))) {
      state.confirm_remove.reset();
      ImGui::CloseCurrentPopup();
    }
    ImGui::EndPopup();
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

    const TierId run_tier = lib_tier(state.config.lib);
    ImGui::Text("Lib %s (%s), Period:", lib_name(state.config.lib).c_str(), state.config.resume ? "Append" : "Full");
    ImGui::Indent();
    ImGui::BulletText("Start: %s", misc::dash(state.config.dates.front()).c_str());
    ImGui::BulletText("End: %s (%zu days)", misc::dash(state.config.dates.back()).c_str(), state.config.dates.size());
    ImGui::Unindent();

    ImGui::Spacing();
    ImGui::Text("System Status:");
    ImGui::Indent();
    ImGui::BulletText("GUI entering low-refresh mode");
    ImGui::BulletText("%s", StageLayout::make(topo, state.config.pf_cores[run_tier], state.config.cs_cores[run_tier], state.config.io_cores[run_tier]).summary().c_str());
    ImGui::BulletText("Ring slots: auto (simulated, grows on demand)");
    ImGui::Unindent();

    ImGui::Spacing();
    ImGui::Separator();
    ImGui::Spacing();

    if (state.ui_state == ComputeUIState::ShowingPopup)
      ImGui::Text("Starting computation...");
    else
      progress_line(*service, state, is_cancelling);

    ImGui::PopItemWidth();

    ImGui::EndPopup();
  }
}

} // namespace GUI::Compute