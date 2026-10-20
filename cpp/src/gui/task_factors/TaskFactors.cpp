// Task Factors - 因子算子库 (factor/) 的 GUI 入口.
//   Operators: OpTable 全部算子对拍 + 计时. 不依赖特征库 / 数据库扫描 (合成张量), 故任何时候可进.
//     首次进页默认不算: 先载入本地 operators.json 显示 (校验不过则删文件); 只有手动 Run —— 或本地无有效
//     快照时的那一次自动补算 —— 才真跑 worker, 一轮结束落盘覆盖.
//   Factors: <factor_dir>/<池>/<层>/ 一因子一文件的整体面板 (池 + 层 = 页顶全局选择, Factors / Inspect 共用, 与特征面板双向同步; 换层 = 服务整体重建). 进页只扫描解析 (任何时候可进); Run 才读特征库评估
//     (要资产轴就绪), 结果回写各因子文件.
//   Inspect: Factors 页点行高光的单因子展示 (分层累计 / 期限结构 / IC 分布 / 资产 K 线). 行信息来自 FactorsService; 一级 Row 由自己的
//     InspectService 对该因子单独读库算出 (高光因子变了自动起算, Compute = 弃缓存重读); 选中资产的分钟 K 线由 KlineService 逐日流式读.
#include "gui/task_factors/TaskFactors.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_factors/services/FactorsService.hpp"
#include "gui/task_factors/services/InspectService.hpp"
#include "gui/task_factors/services/KlineService.hpp"
#include "gui/task_factors/services/OperatorsService.hpp"
#include "gui/task_factors/services/SearchService.hpp"
#include "gui/task_factors/ui/TabFactors.hpp"
#include "gui/task_factors/ui/TabInspect.hpp"
#include "gui/task_factors/ui/TabOperators.hpp"
#include "shared/FeatureLib.hpp" // feature_pools (页顶池选择)
#include "shared/SharedData.hpp" // config.factor_dir / 池 / 日期区间, feature.metadata, asset.items

#include "imgui.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>

namespace GUI::Tasks {

enum TabIdx {
  TAB_OPERATORS = 0,
  TAB_FACTORS,
  TAB_INSPECT,
  TAB_COUNT
};

struct TaskFactorsState {
  std::unique_ptr<Factors::OperatorsService> operators_service;
  Factors::OperatorsUIState operators_ui;
  bool operators_started = false; // 首次 Draw 载入本地 json, 没有才 Request 一次
  // 一轮结束检测 (Running → Done / Cancelled 那一帧把算子表落地 operators.json)
  Phase prev_operators_phase = Phase::Idle;

  // 本页已生效的 (池, 因子层): 跟 data.feature.selection (与特征面板同一份) 走, 所有服务空闲时才追上; seg = 0 = 选择不是因子层 (空页, 无服务)
  int seg = 0;
  std::string pool;
  // 页顶层按钮后的因子文件数 (所选池各因子层目录): 池变 / 因子服务进 Done (扫描 / Run / Add / 删除后的 Rescan) 时重数
  std::string counted_key; // config.factor_dir | 所选池; 空 = 没数过
  size_t level_files[std::size(Factors::kFactorLevels)] = {};
  Phase prev_factors_phase = Phase::Idle;
  std::shared_ptr<const Factors::FeatureTable> feature_table; // 本层字段表视图 (按层建一次, Factors / Search / Inspect 共享)
  std::unique_ptr<Factors::FactorsService> factors_service;
  Factors::FactorsUIState factors_ui;
  std::string factors_scanned_dir;                        // 上次扫描的目录 (池切换 → 自动重扫)
  std::unique_ptr<Factors::SearchService> search_service; // Factors 页 Search (与 factors_service 同建); 写了 weights 的那一轮结束 → 自动 Run
  Phase prev_search_phase = Phase::Idle;

  std::unique_ptr<Factors::InspectService> inspect_service; // 首次进 Factors / Inspect 页建 (构造即起 worker); 缓存 (标签 + 特征平面) 随它活到任务 Destroy
  std::unique_ptr<Factors::KlineService> kline_service;     // Inspect 右下选中资产的分钟 K 线流 (与 inspect_service 同生灭)
  Factors::InspectUIState inspect_ui;
};

// 特征面板选择 (池档, 层) 对应的因子层段长; 不是该池的因子层 (L_sec / Bar 池 L_1min) = 0
static int selected_factor_seg(const Feature::Selection &sel) {
  for (const Factors::FactorLevel &l : Factors::kFactorLevels)
    if (Factors::level_for_pool(l, sel.tier) && static_cast<int>(l.lvl) == sel.selected_level)
      return l.seg;
  return 0;
}

// 换层: 四个服务整体重建 (析构 Stop() join worker; 下次 Draw 按新层建)
static void reset_level_services(TaskFactorsState &s) {
  s.search_service.reset();
  s.prev_search_phase = Phase::Idle;
  s.factors_service.reset();
  s.factors_scanned_dir.clear();
  s.kline_service.reset();
  s.inspect_service.reset();
  s.inspect_ui = Factors::InspectUIState{};
  s.feature_table.reset();
}

// Search all 出队一个发 Search; 队空 = 整目录 Run 一次并收尾. 请求建不成 (资产轴 / 区间) = 整轮放弃
static void search_all_next(const SharedData &data, TaskFactorsState &s) {
  Factors::FactorsUIState &ui = s.factors_ui;
  assert(ui.search_all_n > 0 && s.factors_service && s.search_service);
  if (ui.search_queue.empty()) {
    ui.search_all_n = 0;
    Factors::FactorsRequest req;
    if (Factors::MakeFactorsRequest(data, s.pool, s.seg, /*evaluate=*/true, ui.backend == 1, req))
      s.factors_service->Request(req);
    return;
  }
  const Factors::FactorRow row = std::move(ui.search_queue.front());
  ui.search_queue.erase(ui.search_queue.begin());
  Factors::SearchRequest req;
  if (Factors::MakeSearchRequest(data, s.pool, s.seg, row, ui.backend == 1, req)) {
    s.search_service->Request(req);
  } else {
    ui.search_queue.clear();
    ui.search_all_n = 0;
  }
}

// Factors 子页的作用域 (每帧从 config 取; 池 / 层取本页已生效的)
static Factors::FactorsUIContext factors_context(const SharedData &data, const std::string &pool, int seg) {
  Factors::FactorsUIContext c;
  c.seg = seg;
  c.pool = pool;
  c.factor_dir = Factors::factor_dir_of(data, pool, seg);
  c.sell_impact = data.config.sell_impact;
  c.axis_ready = !data.asset.items.empty();
  c.assets = &data.asset.items;
  return c;
}

TaskHandle CreateFactorsTask() {
  auto state = std::make_shared<TaskFactorsState>();

  TaskHandle handle;
  handle.name = "Factors";
  handle.storage = state;
  handle.tabs = {"Operators", "Factors", "Inspect"};

  // 不设 OnCollapse: 切走任务 worker 继续跑完 (单线程 + ~90 行小结构, 左栏状态标签照常更新, 切回直接看表)

  // 每帧 (无论是否选中): 一轮跑完 / 取消那一帧把算子表落地 <factor_dir>/operators.json
  // (与池无关, 全局一份; 取消也落, 已跑完的行不白算). 切走任务 worker 仍在跑, 故放 Update 不放 Draw
  handle.Update = [state](SharedData &data) {
    // Search all: 一个文件搜完 (忙 → Done / Cancelled, 含 Loading 期就放弃的) → 出队下一个, 队空整目录 Run; 被取消 → 整轮放弃
    // 单个 Search 写了 weights 的那一轮 (Running → Done) 结束: 重扫 + 只 Run 该因子文件 (Search 跑时 Run 被禁, 不会撞)
    if (state->search_service && state->factors_service) {
      const Phase sst = state->search_service->phase.load(std::memory_order_acquire);
      const Phase prev = state->prev_search_phase;
      const bool finished = (sst == Phase::Done || sst == Phase::Cancelled) && (prev == Phase::Scanning || prev == Phase::Loading || prev == Phase::Running);
      if (finished && state->factors_ui.search_all_n > 0) {
        if (sst == Phase::Done) {
          search_all_next(data, *state);
        } else {
          state->factors_ui.search_queue.clear();
          state->factors_ui.search_all_n = 0;
        }
      } else if (sst == Phase::Done && prev == Phase::Running) {
        bool wrote = false;
        std::string file;
        {
          std::lock_guard<std::mutex> lock(state->search_service->mutex);
          wrote = state->search_service->result.wrote;
          file = state->search_service->result.file;
        }
        if (wrote) {
          Factors::FactorsRequest req;
          if (Factors::MakeFactorsRequest(data, state->pool, state->seg, /*evaluate=*/true, state->factors_ui.backend == 1, req)) {
            req.spec.only_file = file;
            state->factors_service->Request(req);
          }
        }
      }
      state->prev_search_phase = sst;
    }
    if (!state->operators_service)
      return;
    auto &svc = *state->operators_service;
    const Phase st = svc.phase.load(std::memory_order_acquire);
    const bool finished = st == Phase::Done || st == Phase::Cancelled;
    if (finished && state->prev_operators_phase == Phase::Running)
      Factors::SaveOperatorTableJson(data.config.factor_dir, svc);
    state->prev_operators_phase = st;
  };

  handle.Status = [state](const SharedData & /*data*/, int idx) -> TaskStatus {
    assert(idx == -1 || idx < TAB_COUNT);
    if (idx == TAB_FACTORS) {
      if (state->search_service && state->search_service->busy()) { // Search 在跑时 Factors 行显示它的进度 (Run 此时被禁)
        const auto &ss = *state->search_service;
        const size_t total = ss.total.load(std::memory_order_relaxed);
        return {TaskStatus::Kind::Busy, "optimizing " + std::to_string(total > 0 ? 100 * ss.done.load(std::memory_order_relaxed) / total : 0) + "%"};
      }
      if (!state->factors_service)
        return {};
      const auto &fs = *state->factors_service;
      if (fs.phase.load(std::memory_order_acquire) == Phase::Done && fs.broken() > 0)
        return {TaskStatus::Kind::Warn, std::to_string(fs.broken()) + " BROKEN"};
      return ProgressStatus(fs);
    }
    if (idx == TAB_INSPECT)
      return state->inspect_service ? ProgressStatus(*state->inspect_service) : TaskStatus{};
    // 任务行与 Operators 行同一状态
    if (!state->operators_service)
      return {};
    const auto &svc = *state->operators_service;
    if (svc.phase.load(std::memory_order_acquire) == Phase::Done && svc.failed() > 0)
      return {TaskStatus::Kind::Error, std::to_string(svc.failed()) + " FAIL"};
    return ProgressStatus(svc);
  };

  handle.Draw = [state](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    if (!state->operators_service)
      state->operators_service = std::make_unique<Factors::OperatorsService>();
    auto &svc = *state->operators_service;

    // 默认不自动跑: 先吃本地 operators.json (校验不过它自己会删掉文件), 没有有效快照才起算一轮.
    // 载入的一轮状态直接是 Done 且不经 Running, Update 的落盘判据 (prev == Running) 因此不会触发
    if (!state->operators_started) {
      state->operators_started = true;
      if (!Factors::LoadOperatorTableJson(data.config.factor_dir, svc, state->operators_ui))
        svc.Request(state->operators_ui.req);
    }

    ImGui::BeginChild("FactorsTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    switch (idx) {
    case TAB_OPERATORS: {
      const int action = Factors::RenderTabOperators(svc, state->operators_ui);
      if (action == 1)
        svc.Request(state->operators_ui.req);
      else if (action == -1)
        svc.RequestCancel();
      break;
    }
    case TAB_FACTORS:
    case TAB_INSPECT: {
      // 池 + 层 = data.feature.selection (与特征面板双向同步: 页顶控件直接改它, 特征面板改了这里跟着变).
      // 本页 (pool, seg) 在所有服务空闲时追上选择: 换层 = 四个服务整体重建 (字段表 / 目录 / Inspect 缓存都随层);
      // 同层换池只换目录 (下方自动重扫); 有服务在跑 = 旧作用域跑完再切
      Feature::Selection &sel = data.feature.selection;
      {
        ImGui::BeginGroup();
        ImGui::TextUnformatted("Pool:");
        ImGui::SameLine();
        const std::string cur_pool = sel.pool();
        ImGui::SetNextItemWidth(140);
        if (ImGui::BeginCombo("##factor_pool", cur_pool.c_str())) {
          float cn_x = 0.0f; // 中文名列起点 (对齐最长池名)
          for (const PoolDef &d : feature_pools())
            cn_x = std::max(cn_x, ImGui::CalcTextSize(d.name).x);
          cn_x += ImGui::GetCursorPosX() + 2.0f * ImGui::GetStyle().ItemSpacing.x;
          for (const PoolDef &d : feature_pools()) {
            const bool selected = cur_pool == d.name;
            const bool clicked = ImGui::Selectable(d.name, selected);
            ImGui::SameLine(cn_x);
            ImGui::TextDisabled("%s", d.name_cn);
            if (clicked && !selected) {
              if (d.tier != sel.tier) {
                sel.clear_filters(); // 同特征面板: 换档 = 换字段表, 过滤作废
                sel.tier = d.tier;
                if (selected_factor_seg(sel) == 0) { // 原层在新池不是因子层 → 该池首个因子层
                  const auto first =
                      std::ranges::find_if(Factors::kFactorLevels, [&](const Factors::FactorLevel &l) { return Factors::level_for_pool(l, d.tier); });
                  assert(first != std::end(Factors::kFactorLevels) && "该池没有因子层");
                  sel.selected_level = static_cast<int>(first->lvl);
                }
              }
              sel.pools[d.tier] = d.name;
            }
            if (selected)
              ImGui::SetItemDefaultFocus();
          }
          ImGui::EndCombo();
        }
        ImGui::SameLine();
        ImGui::TextUnformatted("Level:");
        {
          const std::string key = data.config.factor_dir + "|" + sel.pool();
          const Phase fst = state->factors_service ? state->factors_service->phase.load(std::memory_order_acquire) : Phase::Idle;
          if (key != state->counted_key || (fst == Phase::Done && state->prev_factors_phase != Phase::Done)) {
            state->counted_key = key;
            for (size_t i = 0; i < std::size(Factors::kFactorLevels); ++i) {
              const Factors::FactorLevel &l = Factors::kFactorLevels[i];
              state->level_files[i] =
                  Factors::level_for_pool(l, sel.tier) ? Factors::list_factor_files(Factors::factor_dir_of(data, sel.pool(), l.seg)).size() : 0;
            }
          }
          state->prev_factors_phase = fst;
        }
        for (size_t i = 0; i < std::size(Factors::kFactorLevels); ++i) {
          const Factors::FactorLevel &l = Factors::kFactorLevels[i];
          const bool usable = Factors::level_for_pool(l, sel.tier);
          char label[48];
          if (usable)
            std::snprintf(label, sizeof(label), "%s (%zu)###flvl%zu", l.name, state->level_files[i], i);
          else
            std::snprintf(label, sizeof(label), "%s###flvl%zu", l.name, i);
          ImGui::SameLine();
          ImGui::BeginDisabled(!usable);
          if (ImGui::RadioButton(label, usable && static_cast<int>(l.lvl) == sel.selected_level))
            sel.selected_level = static_cast<int>(l.lvl);
          ImGui::EndDisabled();
        }
        ImGui::EndGroup();
        if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
          Factors::tip("池 + 因子层 (Factors / Inspect 共用; 与特征面板的池 / 层双向同步)\n"
                       "• 因子层 = 所用特征的最细层; 叶写全名 档.层.列 (lob.1m.x / bar.5m.x …)\n"
                       "• min: 只 Lob 池 (盘口标签 + 冲击成本, 可混 bar.* 叶)\n"
                       "• 5min .. day: 任何池 (all 池标签, 无冲击成本); Lob 池 = 当日池成员截面\n"
                       "• 评估期 = 所用特征库落盘日交集 (不看 config 区间)\n"
                       "• 窗长 d 等参数按本层行计\n"
                       "• 因子目录 <factor_dir>/<池>/<层>/; 有服务在跑时, 跑完才切过去");
      }
      {
        const bool busy = (state->factors_service && state->factors_service->busy()) || (state->search_service && state->search_service->busy()) ||
                          (state->inspect_service && state->inspect_service->busy()) || state->factors_ui.search_all_n > 0;
        const int want_seg = selected_factor_seg(sel);
        const std::string want_pool = want_seg != 0 ? sel.pool() : std::string{};
        if (!busy) {
          if (want_seg != state->seg) {
            reset_level_services(*state);
            state->seg = want_seg;
          }
          if (want_pool != state->pool) {
            state->prev_search_phase = Phase::Idle; // 旧池那轮 Search 的结束不再触发 Update 的自动 Run
            state->pool = want_pool;
          }
        } else if (want_seg != state->seg || want_pool != state->pool) {
          ImGui::SameLine();
          ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.3f, 1.0f), "(服务跑完后切到 %s / %s)", sel.pool().c_str(), level_name(static_cast<size_t>(sel.selected_level)));
        }
      }
      if (state->seg == 0) {
        ImGui::TextDisabled("%s / %s 不是因子层 (因子层: Lob 池 min; 任何池 5min .. day)", sel.pool().c_str(), level_name(static_cast<size_t>(sel.selected_level)));
        break;
      }
      // 首次进页 / 换层: 字段表视图按层建一次 (编译期字段表, 不随运行变), 只扫描不算; 池切换 → 目录变 → 自动重扫
      if (!state->feature_table)
        state->feature_table = std::make_shared<Factors::FeatureTable>(Factors::BuildFeatureTable(data.feature, state->seg));
      if (!state->factors_service) {
        state->factors_service = std::make_unique<Factors::FactorsService>();
        state->factors_service->SetFeatureTable(state->feature_table);
        state->search_service = std::make_unique<Factors::SearchService>();
        state->search_service->SetFeatureTable(state->feature_table);
      }
      auto &fs = *state->factors_service;
      auto &ss = *state->search_service;
      const Factors::FactorsUIContext ctx = factors_context(data, state->pool, state->seg);
      if (state->factors_scanned_dir != ctx.factor_dir) {
        state->factors_scanned_dir = ctx.factor_dir;
        Factors::FactorsRequest req;
        Factors::MakeFactorsRequest(data, state->pool, state->seg, /*evaluate=*/false, false, req);
        fs.Request(req);
      }
      if (!state->inspect_service) {
        state->inspect_service = std::make_unique<Factors::InspectService>();
        state->inspect_service->SetFeatureTable(state->feature_table);
        state->kline_service = std::make_unique<Factors::KlineService>();
      }
      auto &is = *state->inspect_service;
      int iaction = 0; // Inspect 请求: 1 起算 / -1 取消
      if (idx == TAB_INSPECT) {
        iaction = Factors::RenderTabInspect(fs, is, *state->kline_service, state->factors_ui, state->inspect_ui, ctx);
      } else {
        const int action = Factors::RenderTabFactors(fs, ss, state->factors_ui, ctx);
        if (action == 1 || action == 2) {
          Factors::FactorsRequest req;
          if (Factors::MakeFactorsRequest(data, state->pool, state->seg, /*evaluate=*/action == 1, state->factors_ui.backend == 1, req))
            fs.Request(req);
        } else if (action == -1) {
          fs.RequestCancel();
        } else if (action == 3) {
          Factors::SearchRequest req;
          if (Factors::MakeSearchRequest(data, state->pool, state->seg, state->factors_ui.search_row, state->factors_ui.backend == 1, req))
            ss.Request(req);
        } else if (action == 4) {
          search_all_next(data, *state);
        } else if (action == -3) {
          ss.RequestCancel();
        }
      }
      // 自动起算两页都看: Factors 页点行高光即起算, 不等切到 Inspect (左栏 Inspect 状态同步走)
      if (iaction == 0 && Factors::InspectAutoRequest(fs, is, state->factors_ui, state->inspect_ui, ctx))
        iaction = 1;
      if (iaction == 1) {
        Factors::InspectRequest req;
        if (Factors::MakeInspectRequest(data, state->pool, state->seg, state->inspect_ui.req_row, state->inspect_ui.req_reload, req))
          is.Request(req);
      } else if (iaction == -1) {
        is.RequestCancel();
      }
      break;
    }
    default:
      break;
    }
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    state->operators_service.reset(); // 析构 Stop() join worker
    state->operators_started = false;
    state->search_service.reset(); // 析构 Stop() join worker
    state->prev_search_phase = Phase::Idle;
    state->factors_ui.search_queue.clear(); // Search all 随服务一起作废
    state->factors_ui.search_all_n = 0;
    state->factors_service.reset();
    state->factors_scanned_dir.clear();
    state->kline_service.reset();   // 析构 Stop() join worker
    state->inspect_service.reset(); // 析构 Stop() join worker, 缓存随之释放
    state->inspect_ui = Factors::InspectUIState{};
    state->feature_table.reset();
  };

  return handle;
}

} // namespace GUI::Tasks
