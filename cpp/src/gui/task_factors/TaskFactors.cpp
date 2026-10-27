// Task Factors - 因子算子库 (factor/) 的 GUI 入口.
//   Operators: OpTable 全部算子对拍 + 计时. 不依赖特征库 / 数据库扫描 (合成张量), 故任何时候可进.
//     首次进页默认不算: 先载入本地 operators.json 显示 (校验不过则删文件); 只有手动 Run —— 或本地无有效
//     快照时的那一次自动补算 —— 才真跑 worker, 一轮结束落盘覆盖.
//   Factors: <factor_dir>/<池>/<层>/ 一因子一文件的整体面板 (池 + 层 = 页顶全局选择, Factors / Inspect 共用, 与特征面板双向同步; 换层 = 服务整体重建,
//     字段表按层建一次全页共享). 进页只扫描解析 (任何时候可进); Run 才读特征库评估 (要资产轴就绪), 结果回写各因子文件.
//   Inspect: Factors 页点行高光的单因子展示 (分层累计 / 期限结构 / IC 分布 / 资产 K 线). 行信息来自 FactorsService; 一级 Row 由自己的
//     InspectService 对该因子单独读库算出 (高光因子变了自动起算, Compute = 弃缓存重读); 选中资产的分钟 K 线由 KlineService 逐日流式读.
//   Portfolio: 因子组合 (<portfolio_dir>/<name>.json; 在 Factors 页 1. 区右侧两小框选 / 编辑) 的等权组合曲线 + 池指数. 跨池跨层,
//     与页顶池 / 层无关 (换层不重建); 组合内容变 / 因子重扫后重解析, 有效条目变才发请求 (PortfolioService 只算新增的因子).
#include "gui/task_factors/TaskFactors.hpp"
#include "gui/Tasks.hpp"
#include "gui/task_factors/services/FactorsService.hpp"
#include "gui/task_factors/services/InspectService.hpp"
#include "gui/task_factors/services/KlineService.hpp"
#include "gui/task_factors/services/OperatorsService.hpp"
#include "gui/task_factors/services/PortfolioService.hpp"
#include "gui/task_factors/services/SearchService.hpp"
#include "gui/task_factors/ui/TabFactors.hpp"
#include "gui/task_factors/ui/TabInspect.hpp"
#include "gui/task_factors/ui/TabOperators.hpp"
#include "gui/task_factors/ui/TabPortfolio.hpp"
#include "shared/FeatureLib.hpp" // feature_pools (页顶池选择)
#include "shared/SharedData.hpp" // config.factor_dir / 池 / 日期区间, feature.metadata, asset.items

#include "imgui.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <vector>

namespace GUI::Tasks {

enum TabIdx {
  TAB_OPERATORS = 0,
  TAB_FACTORS,
  TAB_INSPECT,
  TAB_PORTFOLIO,
  TAB_COUNT
};

struct TaskFactorsState {
  std::unique_ptr<Factors::OperatorsService> operators_service;
  Factors::OperatorsUIState operators_ui;
  bool operators_started = false; // 首次 Draw 载入本地 json, 没有才 Request 一次
  // 一轮结束检测 (Running → Done / Cancelled 那一帧把算子表落地 operators.json)
  Phase prev_operators_phase = Phase::Idle;

  // 本页已生效的 (池, 因子层): 跟 data.feature.selection (与特征面板同一份) 走, 所有服务空闲时才追上; lv 空 = 选择不是因子层 (空页, 无服务)
  std::optional<Factors::LevelId> lv;
  std::string pool;
  // 页顶层按钮后的因子文件数 (所选池各因子层目录): 池变 / 因子服务进 Done (扫描 / Run / Add / 删除后的 Rescan) 时重数
  std::string counted_key; // config.factor_dir | 所选池; 空 = 没数过
  Factors::PerLevel<size_t> level_files;
  bool level_defaulted = false;   // 首次进页已按因子文件数定过默认层 (只一次, 之后随用户 / 特征面板)
  std::vector<size_t> pool_files; // [PoolId] 页顶池下拉里各池因子文件数 (该池各因子层之和): 每次打开下拉重数
  Phase prev_factors_phase = Phase::Idle;
  Factors::FeatureTables tables; // 各层字段表视图 (按层建一次, Factors / Search / Inspect / Portfolio 共享, 换层不重建)
  std::unique_ptr<Factors::FactorsService> factors_service;
  Factors::FactorsUIState factors_ui;
  std::string factors_scanned_dir;                        // 上次扫描的目录 (池切换 → 自动重扫)
  std::unique_ptr<Factors::SearchService> search_service; // Factors 页 Search (与 factors_service 同建); 写了 weights 的那一轮结束 → 自动 Run
  Phase prev_search_phase = Phase::Idle;

  std::unique_ptr<Factors::InspectService> inspect_service; // 首次进 Factors / Inspect 页建 (构造即起 worker); 缓存 (标签 + 特征平面) 随它活到任务 Destroy
  std::unique_ptr<Factors::KlineService> kline_service;     // Inspect 右下选中资产的分钟 K 线流 (与 inspect_service 同生灭)
  Factors::InspectUIState inspect_ui;

  // 组合 (与层无关, 换层不重建; 首次进 Factors / Inspect / Portfolio 页建)
  std::unique_ptr<Factors::PortfolioService> portfolio_service;
  Factors::PortfolioUIState portfolio_ui;
  uint64_t portfolio_gen = ~0ull;        // 已解析的 portfolio_ui.content_gen
  bool portfolio_dirty = false;          // 解析过, 请求还没发 (资产轴未就绪时挂着)
  std::string portfolio_sig;             // 上次请求的内容签名
  Phase portfolio_prev_fs = Phase::Idle; // 因子服务进 Done (因子文件可能改了) → 重解析
};

// 特征面板选择 (池档, 层) 对应的因子层; 不是该池的因子层 (L_sec / Bar 池 L_1min) = 空
static std::optional<Factors::LevelId> selected_factor_level(const Feature::Selection &sel) {
  for (const Factors::FactorLevel &l : Factors::kFactorLevels)
    if (Factors::level_for_pool(l, sel.tier) && static_cast<int>(l.lvl) == sel.selected_level)
      return l.id;
  return std::nullopt;
}

// 本页已生效层的字段表
static std::shared_ptr<const Factors::FeatureTable> level_table(const SharedData &data, TaskFactorsState &s) {
  assert(s.lv);
  return Factors::table_of(s.tables, data.feature, *s.lv);
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
}

// Search all 出队一个发 Search; 队空 = 整目录 Run 一次并收尾. 请求建不成 (资产轴 / 区间) = 整轮放弃
static void search_all_next(const SharedData &data, TaskFactorsState &s) {
  Factors::FactorsUIState &ui = s.factors_ui;
  assert(ui.search_all_n > 0 && s.factors_service && s.search_service);
  if (ui.search_queue.empty()) {
    ui.search_all_n = 0;
    Factors::FactorsRequest req;
    if (Factors::MakeFactorsRequest(data, level_table(data, s), s.pool, /*evaluate=*/true, ui.backend == 1, req))
      s.factors_service->Request(req);
    return;
  }
  const Factors::FactorRow row = std::move(ui.search_queue.front());
  ui.search_queue.erase(ui.search_queue.begin());
  Factors::SearchRequest req;
  if (Factors::MakeSearchRequest(data, level_table(data, s), s.pool, row, ui.search_hold, ui.backend == 1, req)) {
    s.search_service->Request(req);
  } else {
    ui.search_queue.clear();
    ui.search_all_n = 0;
  }
}

// 组合: 内容变 / 因子服务进 Done → 重解析; 有效条目 / 池集合变 (或 reload) 才发请求, 不打断在跑的
static void portfolio_sync(const SharedData &data, TaskFactorsState &s, bool reload) {
  Factors::PortfolioUIState &pui = s.portfolio_ui;
  const Phase fst = s.factors_service ? s.factors_service->phase.load(std::memory_order_acquire) : Phase::Idle;
  const bool fs_done = fst == Phase::Done && s.portfolio_prev_fs != Phase::Done;
  s.portfolio_prev_fs = fst;
  if (pui.content_gen != s.portfolio_gen || fs_done) {
    s.portfolio_gen = pui.content_gen;
    Factors::ResolvePortfolio(data, s.tables, pui.entries, pui.items);
    ++pui.items_gen;
    s.portfolio_dirty = true;
  }
  if ((s.portfolio_dirty || reload) && !data.asset.items.empty()) {
    s.portfolio_dirty = false;
    Factors::PortfolioRequest req;
    Factors::MakePortfolioRequest(data, s.tables, pui.items, reload, s.factors_ui.backend == 1, req);
    ++pui.items_gen; // 作用域建不成的条目在这里标失效
    std::string sig = req.sig();
    if (reload || sig != s.portfolio_sig) {
      s.portfolio_sig = std::move(sig);
      s.portfolio_service->Request(std::move(req));
    }
  }
}

// Factors 子页的作用域 (每帧从 config 取; 池 / 层取本页已生效的)
static Factors::FactorsUIContext factors_context(const SharedData &data, std::shared_ptr<const Factors::FeatureTable> ft, const std::string &pool) {
  Factors::FactorsUIContext c;
  c.lv = ft->lv;
  c.pool = pool;
  c.factor_dir = Factors::factor_dir_of(data, pool, c.lv);
  c.ft = std::move(ft);
  c.fixed_impact = data.config.fixed_impact;
  c.axis_ready = !data.asset.items.empty();
  c.assets = &data.asset.items;
  return c;
}

TaskHandle CreateFactorsTask() {
  auto state = std::make_shared<TaskFactorsState>();

  TaskHandle handle;
  handle.name = "Factors";
  handle.storage = state;
  handle.tabs = {"Operators", "Factors", "Inspect", "Portfolio"};

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
          if (Factors::MakeFactorsRequest(data, level_table(data, *state), state->pool, /*evaluate=*/true, state->factors_ui.backend == 1, req)) {
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
    if (idx == TAB_PORTFOLIO)
      return state->portfolio_service ? ProgressStatus(*state->portfolio_service) : TaskStatus{};
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

    if (idx != TAB_OPERATORS) {
      if (!state->portfolio_service)
        state->portfolio_service = std::make_unique<Factors::PortfolioService>();
      Factors::SyncPortfolioDir(state->portfolio_ui, data.config.portfolio_dir);
    }
    bool portfolio_reload = false;

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
      // 本页 (pool, lv) 在所有服务空闲时追上选择: 换层 = 四个服务整体重建 (目录 / Inspect 缓存都随层, 字段表按层常驻);
      // 同层换池只换目录 (下方自动重扫); 有服务在跑 = 旧作用域跑完再切
      Feature::Selection &sel = data.feature.selection;
      {
        ImGui::BeginGroup();
        ImGui::TextUnformatted("Pool:");
        ImGui::SameLine();
        const std::string cur_pool = sel.pool();
        ImGui::SetNextItemWidth(140);
        if (ImGui::BeginCombo("##factor_pool", cur_pool.c_str())) {
          if (ImGui::IsWindowAppearing()) {
            state->pool_files.clear();
            for (const PoolDef &d : feature_pools()) {
              size_t n = 0;
              for (const Factors::FactorLevel &l : Factors::kFactorLevels)
                if (Factors::level_for_pool(l, d.tier))
                  n += Factors::list_factor_files(Factors::factor_dir_of(data, d.name, l.id)).size();
              state->pool_files.push_back(n);
            }
          }
          assert(state->pool_files.size() == feature_pools().size());
          float cn_x = 0.0f; // 中文名列起点 (对齐最长池名)
          for (const PoolDef &d : feature_pools())
            cn_x = std::max(cn_x, ImGui::CalcTextSize(d.name).x);
          cn_x += ImGui::GetCursorPosX() + 2.0f * ImGui::GetStyle().ItemSpacing.x;
          for (size_t pi = 0; pi < feature_pools().size(); ++pi) {
            const PoolDef &d = feature_pools()[pi];
            const bool selected = cur_pool == d.name;
            const bool clicked = ImGui::Selectable(d.name, selected);
            ImGui::SameLine(cn_x);
            ImGui::TextDisabled("%s (%zu)", d.name_cn, state->pool_files[pi]);
            if (clicked && !selected) {
              if (d.tier != sel.tier) {
                sel.clear_filters(); // 同特征面板: 换档 = 换字段表, 过滤作废
                sel.tier = d.tier;
                if (!selected_factor_level(sel)) { // 原层在新池不是因子层 → 该池首个因子层
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
            for (const Factors::FactorLevel &l : Factors::kFactorLevels)
              state->level_files[l.id] =
                  Factors::level_for_pool(l, sel.tier) ? Factors::list_factor_files(Factors::factor_dir_of(data, sel.pool(), l.id)).size() : 0;
          }
          state->prev_factors_phase = fst;
          if (!state->level_defaulted) { // 首次进页: 切到该池最低频的有因子文件的层 (都没有 → 不动)
            state->level_defaulted = true;
            for (const Factors::FactorLevel &l : Factors::kFactorLevels | std::views::reverse)
              if (state->level_files[l.id] > 0) {
                sel.selected_level = static_cast<int>(l.lvl);
                break;
              }
          }
        }
        for (const Factors::FactorLevel &l : Factors::kFactorLevels) {
          const bool usable = Factors::level_for_pool(l, sel.tier);
          char label[48];
          if (usable)
            std::snprintf(label, sizeof(label), "%s (%zu)###flvl%d", l.name, state->level_files[l.id], l.id);
          else
            std::snprintf(label, sizeof(label), "%s###flvl%d", l.name, l.id);
          ImGui::SameLine();
          ImGui::BeginDisabled(!usable);
          if (ImGui::RadioButton(label, usable && static_cast<int>(l.lvl) == sel.selected_level))
            sel.selected_level = static_cast<int>(l.lvl);
          ImGui::EndDisabled();
        }
        ImGui::EndGroup();
        // 因子层 = 所用特征的最细层 (叶写全名 档.层.列); min 层盘口标签 + 冲击成本, 5min .. day 用 all 池标签, Lob 池 = 当日池成员截面;
        // 因子目录 <factor_dir>/<池>/<层>/
        if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
          Factors::tip("池 + 因子层 (与特征面板同步; 括号里是因子数)\n"
                       "• min 层只有 Lob 池能用\n"
                       "• 窗长 d 等参数按本层行数计\n"
                       "• 有计算在跑时, 跑完才切过去");
      }
      {
        const bool busy = (state->factors_service && state->factors_service->busy()) || (state->search_service && state->search_service->busy()) ||
                          (state->inspect_service && state->inspect_service->busy()) || state->factors_ui.search_all_n > 0;
        const std::optional<Factors::LevelId> want_lv = selected_factor_level(sel);
        const std::string want_pool = want_lv ? sel.pool() : std::string{};
        if (!busy) {
          if (want_lv != state->lv || want_pool != state->pool) { // 换作用域: 高光作废, 新表扫出后重新默认高光 (rIC 最高)
            state->factors_ui.view_file.clear();
            state->factors_ui.view_defaulted = false;
          }
          if (want_lv != state->lv) {
            reset_level_services(*state);
            state->lv = want_lv;
          }
          if (want_pool != state->pool) {
            state->prev_search_phase = Phase::Idle; // 旧池那轮 Search 的结束不再触发 Update 的自动 Run
            state->pool = want_pool;
          }
        } else if (want_lv != state->lv || want_pool != state->pool) {
          ImGui::SameLine();
          ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.3f, 1.0f), "(服务跑完后切到 %s / %s)", sel.pool().c_str(), level_name(static_cast<size_t>(sel.selected_level)));
        }
      }
      if (!state->lv) {
        ImGui::TextDisabled("%s / %s 不是因子层 (因子层: Lob 池 min; 任何池 5min .. day)", sel.pool().c_str(), level_name(static_cast<size_t>(sel.selected_level)));
        break;
      }
      // 首次进页 / 换层: 只扫描不算 (字段表视图按层建一次, 编译期字段表, 不随运行变); 池切换 → 目录变 → 自动重扫
      const std::shared_ptr<const Factors::FeatureTable> ft = level_table(data, *state);
      if (!state->factors_service) {
        state->factors_service = std::make_unique<Factors::FactorsService>();
        state->search_service = std::make_unique<Factors::SearchService>();
      }
      auto &fs = *state->factors_service;
      auto &ss = *state->search_service;
      const Factors::FactorsUIContext ctx = factors_context(data, ft, state->pool);
      if (state->factors_scanned_dir != ctx.factor_dir) {
        state->factors_scanned_dir = ctx.factor_dir;
        Factors::FactorsRequest req;
        Factors::MakeFactorsRequest(data, ft, state->pool, /*evaluate=*/false, false, req);
        fs.Request(req);
      }
      if (!state->inspect_service) {
        state->inspect_service = std::make_unique<Factors::InspectService>();
        state->kline_service = std::make_unique<Factors::KlineService>();
      }
      auto &is = *state->inspect_service;
      int iaction = 0; // Inspect 请求: 1 起算 / -1 取消
      if (idx == TAB_INSPECT) {
        iaction = Factors::RenderTabInspect(fs, is, *state->kline_service, state->factors_ui, state->inspect_ui, ctx);
      } else {
        const int action = Factors::RenderTabFactors(fs, ss, state->factors_ui, state->portfolio_ui, ctx);
        if (action == 1 || action == 5 || action == 2) {
          Factors::FactorsRequest req;
          if (Factors::MakeFactorsRequest(data, ft, state->pool, /*evaluate=*/action != 2, state->factors_ui.backend == 1, req)) {
            if (action == 1)
              req.spec.only_file = state->factors_ui.run_file;
            fs.Request(req);
          }
        } else if (action == -1) {
          fs.RequestCancel();
        } else if (action == 3) {
          Factors::SearchRequest req;
          if (Factors::MakeSearchRequest(data, ft, state->pool, state->factors_ui.search_row, state->factors_ui.search_hold,
                                         state->factors_ui.backend == 1, req))
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
        if (Factors::MakeInspectRequest(data, ft, state->pool, state->inspect_ui.req_row, state->inspect_ui.req_reload,
                                        state->factors_ui.backend == 1, req))
          is.Request(req);
      } else if (iaction == -1) {
        is.RequestCancel();
      }
      break;
    }
    case TAB_PORTFOLIO: {
      const int action = Factors::RenderTabPortfolio(*state->portfolio_service, state->portfolio_ui, data.config.fixed_impact);
      if (action == -1)
        state->portfolio_service->RequestCancel();
      portfolio_reload = action == 1;
      break;
    }
    default:
      break;
    }
    if (idx != TAB_OPERATORS)
      portfolio_sync(data, *state, portfolio_reload);
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
    state->tables = {};
    state->level_defaulted = false;
    state->portfolio_service.reset(); // 析构 Stop() join worker, 缓存随之释放
    state->portfolio_ui = Factors::PortfolioUIState{};
    state->portfolio_gen = ~0ull;
    state->portfolio_dirty = false;
    state->portfolio_sig.clear();
    state->portfolio_prev_fs = Phase::Idle;
  };

  return handle;
}

} // namespace GUI::Tasks
