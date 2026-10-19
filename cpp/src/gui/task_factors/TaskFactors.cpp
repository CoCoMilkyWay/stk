// Task Factors - 因子算子库 (factor/) 的 GUI 入口.
//   Operators: OpTable 全部算子对拍 + 计时. 不依赖特征库 / 数据库扫描 (合成张量), 故任何时候可进.
//     首次进页默认不算: 先载入本地 operators.json 显示 (校验不过则删文件); 只有手动 Run —— 或本地无有效
//     快照时的那一次自动补算 —— 才真跑 worker, 一轮结束落盘覆盖.
//   Factors: <factor_dir>/<库>/<层>/ 一因子一文件的整体面板 (层 = 页顶全局下拉, Factors / Inspect 共用; 换层 = 服务整体重建). 进页只扫描解析 (任何时候可进); Run 才读特征库评估
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
#include "shared/SharedData.hpp" // config.factor_dir / 票池 / 日期区间, feature.metadata, asset.items

#include "imgui.h"

#include <cassert>
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

  int seg = factor::kSegMax; // 因子层 (Factors::kFactorLevels), 默认 L_min
  std::unique_ptr<Factors::FactorsService> factors_service;
  Factors::FactorsUIState factors_ui;
  std::string factors_scanned_dir;                        // 上次扫描的目录 (票池切换 → 自动重扫)
  std::unique_ptr<Factors::SearchService> search_service; // Factors 页 Search (与 factors_service 同建); 写了 weights 的那一轮结束 → 自动 Run
  Phase prev_search_phase = Phase::Idle;

  std::unique_ptr<Factors::InspectService> inspect_service; // 首次进 Factors / Inspect 页建 (构造即起 worker); 缓存 (标签 + 特征平面) 随它活到任务 Destroy
  std::unique_ptr<Factors::KlineService> kline_service;     // Inspect 右下选中资产的分钟 K 线流 (与 inspect_service 同生灭)
  Factors::InspectUIState inspect_ui;
};

// Factors 子页的作用域 (每帧从 config 取)
static Factors::FactorsUIContext factors_context(const SharedData &data, int seg) {
  Factors::FactorsUIContext c;
  c.seg = seg;
  c.pool = data.feature.selection.lib_of(Factors::factor_level(seg).tier);
  c.factor_dir = Factors::factor_dir_of(data, seg);
  c.start_date = data.config.start_date;
  c.end_date = data.config.end_date;
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
  // (与票池无关, 全局一份; 取消也落, 已跑完的行不白算). 切走任务 worker 仍在跑, 故放 Update 不放 Draw
  handle.Update = [state](SharedData &data) {
    // Search 写了 weights 的那一轮 (Running → Done) 结束: 重扫 + 只 Run 该因子文件 (Search 跑时 Run 被禁, 不会撞)
    if (state->search_service && state->factors_service) {
      const Phase sst = state->search_service->phase.load(std::memory_order_acquire);
      if (sst == Phase::Done && state->prev_search_phase == Phase::Running) {
        bool wrote = false;
        std::string file;
        {
          std::lock_guard<std::mutex> lock(state->search_service->mutex);
          wrote = state->search_service->result.wrote;
          file = state->search_service->result.file;
        }
        if (wrote) {
          Factors::FactorsRequest req;
          if (Factors::MakeFactorsRequest(data, state->seg, /*evaluate=*/true, state->factors_ui.backend == 1, req)) {
            req.only_file = file;
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
      // 因子层 (全局): 换层 = 四个服务整体重建 (字段表 / 目录 / Inspect 缓存都随层); 任一在跑时禁用
      {
        const bool busy = (state->factors_service && state->factors_service->busy()) || (state->search_service && state->search_service->busy()) ||
                          (state->inspect_service && state->inspect_service->busy());
        ImGui::BeginDisabled(busy);
        ImGui::SetNextItemWidth(90);
        if (ImGui::BeginCombo("Level", Factors::factor_level(state->seg).name)) {
          for (const Factors::FactorLevel &l : Factors::kFactorLevels)
            if (ImGui::Selectable(l.name, l.seg == state->seg) && l.seg != state->seg) {
              state->seg = l.seg;
              state->search_service.reset(); // 析构 Stop() join worker
              state->prev_search_phase = Phase::Idle;
              state->factors_service.reset();
              state->factors_scanned_dir.clear();
              state->kline_service.reset();
              state->inspect_service.reset();
              state->inspect_ui = Factors::InspectUIState{};
            }
          ImGui::EndCombo();
        }
        ImGui::EndDisabled();
        if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
          Factors::tip("因子层 (Factors / Inspect 共用)\n"
                       "• min = Lob 票池库 (盘口特征 + 冲击成本); 5min .. day = Bar 库 (全市场, 无冲击成本)\n"
                       "• 窗长 d 等参数按本层行计; 标签取 L_min 列在行末分钟采样\n"
                       "• 因子目录 <factor_dir>/<库>/<层>/; 有服务在跑时不能换");
      }
      // 首次进页 / 换层: 字段表视图按层建一次 (编译期字段表, 不随运行变), 只扫描不算; 票池切换 → 目录变 → 自动重扫
      const Feature::Metadata &meta = data.feature.meta(Factors::factor_level(state->seg).tier);
      if (!state->factors_service) {
        state->factors_service = std::make_unique<Factors::FactorsService>();
        state->factors_service->SetFeatureTable(Factors::BuildFeatureTable(meta, state->seg));
        state->search_service = std::make_unique<Factors::SearchService>();
        state->search_service->SetFeatureTable(Factors::BuildFeatureTable(meta, state->seg));
      }
      auto &fs = *state->factors_service;
      auto &ss = *state->search_service;
      const Factors::FactorsUIContext ctx = factors_context(data, state->seg);
      if (state->factors_scanned_dir != ctx.factor_dir) {
        state->factors_scanned_dir = ctx.factor_dir;
        Factors::FactorsRequest req;
        Factors::MakeFactorsRequest(data, state->seg, /*evaluate=*/false, false, req);
        fs.Request(req);
      }
      if (!state->inspect_service) {
        state->inspect_service = std::make_unique<Factors::InspectService>();
        state->inspect_service->SetFeatureTable(Factors::BuildFeatureTable(meta, state->seg));
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
          if (Factors::MakeFactorsRequest(data, state->seg, /*evaluate=*/action == 1, state->factors_ui.backend == 1, req))
            fs.Request(req);
        } else if (action == -1) {
          fs.RequestCancel();
        } else if (action == 3) {
          Factors::SearchRequest req;
          if (Factors::MakeSearchRequest(data, state->seg, state->factors_ui.search_row, state->factors_ui.backend == 1, req))
            ss.Request(req);
        } else if (action == -3) {
          ss.RequestCancel();
        }
      }
      // 自动起算两页都看: Factors 页点行高光即起算, 不等切到 Inspect (左栏 Inspect 状态同步走)
      if (iaction == 0 && Factors::InspectAutoRequest(fs, is, state->factors_ui, state->inspect_ui, ctx))
        iaction = 1;
      if (iaction == 1) {
        Factors::InspectRequest req;
        if (Factors::MakeInspectRequest(data, state->seg, state->inspect_ui.req_row, state->inspect_ui.req_reload, req))
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
    state->factors_service.reset();
    state->factors_scanned_dir.clear();
    state->kline_service.reset();   // 析构 Stop() join worker
    state->inspect_service.reset(); // 析构 Stop() join worker, 缓存随之释放
    state->inspect_ui = Factors::InspectUIState{};
  };

  return handle;
}

} // namespace GUI::Tasks
