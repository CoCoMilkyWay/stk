// Task Alpha - 因子挖掘: 子池张量 → 算子 → 因子 → 单因子展示. 共用的服务与状态见 gui/task_alpha/AlphaHub.hpp.
//   Compute: 库表的 Alpha 段 (all.lob + 子池各档, 因子的张量库), 与 Beta → Compute 同一个计算服务 (见 TabCompute.hpp).
//   Operators: OpTable 全部算子对拍 + 计时. 不依赖特征库 / 数据库扫描 (合成张量), 故任何时候可进.
//     首次进页默认不算: 先载入本地 operators.json 显示 (校验不过则删文件); 只有手动 Run —— 或本地无有效
//     快照时的那一次自动补算 —— 才真跑 worker, 一轮结束落盘覆盖.
//   Factors: <factor_dir>/<池>/<层>/ 一因子一文件的整体面板 (池 + 层 = 页顶条, Factors / Inspect 共用; 换层 = 服务整体重建,
//     字段表按层建一次全页共享). 进页只扫描解析 (任何时候可进); Run 才读特征库评估 (要资产轴就绪), 结果回写各因子文件.
//     1. 区右侧两小框编辑因子组合 (<portfolio_dir>/<name>.json), Market → NAV 画它的曲线.
//   Inspect: Factors 页点行高光的单因子展示 (分层累计 / 期限结构 / IC 分布 / 资产 K 线). 行信息来自 RunService; 一级 Row 由自己的
//     InspectService 对该因子单独读库算出 (高光因子变了自动起算, Recompute = 弃缓存重读); 选中资产的分钟 K 线由 KlineService 逐日流式读.
#include "gui/task_alpha/TaskAlpha.hpp"
#include "gui/Tasks.hpp"
#include "gui/compute/ui/TabCompute.hpp"
#include "gui/task_alpha/AlphaHub.hpp"
#include "shared/FeatureLib.hpp" // feature_pools (页顶池选择)
#include "shared/SharedData.hpp" // config.factor_dir / 池 / 日期区间, feature.metadata, asset.items

#include "imgui.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Alpha {

void EnsurePortfolio(const SharedData &data, AlphaHub &s) {
  if (!s.portfolio_service)
    s.portfolio_service = std::make_unique<Market::PortfolioService>();
  Market::SyncPortfolioDir(s.portfolio_ui, data.config.portfolio_dir);
  Market::SyncPortfolioWatch(s.portfolio_ui, data.config.config_dir);
}

void SyncPortfolio(const SharedData &data, AlphaHub &s, bool reload) {
  Market::PortfolioUIState &pui = s.portfolio_ui;
  const Phase fst = s.run_service ? s.run_service->phase.load(std::memory_order_acquire) : Phase::Idle;
  const bool fs_done = fst == Phase::Done && s.portfolio_prev_fs != Phase::Done;
  s.portfolio_prev_fs = fst;
  if (pui.content_gen != s.portfolio_gen || fs_done) {
    s.portfolio_gen = pui.content_gen;
    Market::ResolvePortfolio(data, s.tables, pui.entries, pui.items);
    ++pui.items_gen;
    s.portfolio_dirty = true;
  }
  if ((s.portfolio_dirty || reload) && !data.asset.items.empty()) {
    s.portfolio_dirty = false;
    Market::PortfolioRequest req;
    Market::MakePortfolioRequest(data, s.tables, pui.items, reload, s.factors_ui.backend == 1, req);
    ++pui.items_gen; // 作用域建不成的条目在这里标失效
    std::string sig = req.sig();
    if (reload || sig != s.portfolio_sig) {
      s.portfolio_sig = std::move(sig);
      s.portfolio_service->Request(std::move(req));
    }
  }
}

} // namespace GUI::Alpha
namespace GUI::Tasks {

enum TabIdx {
  TAB_COMPUTE = 0,
  TAB_OPERATORS,
  TAB_FACTORS,
  TAB_INSPECT,
  TAB_COUNT
};

// 本页已生效层的字段表
static std::shared_ptr<const Alpha::FeatureTable> level_table(const SharedData &data, Alpha::AlphaHub &s) {
  assert(s.lv);
  return Alpha::table_of(s.tables, data.feature, *s.lv);
}

// 换层: 四个服务整体重建 (析构 Stop() join worker; 下次 Draw 按新层建)
static void reset_level_services(Alpha::AlphaHub &s) {
  s.search_service.reset();
  s.prev_search_phase = Phase::Idle;
  s.run_service.reset();
  s.factors_scanned_dir.clear();
  s.kline_service.reset();
  s.inspect_service.reset();
  s.inspect_ui = Alpha::InspectUIState{};
}

// Search all 出队一个发 Search; 队空 = 整目录 Run 一次并收尾. 请求建不成 (资产轴 / 区间) = 整轮放弃.
// wrote = 上一项落盘了; 没落盘的 fresh 项把 fresh 顺延给同文件下一档 (该文件仍须整份覆盖)
static void search_all_next(const SharedData &data, Alpha::AlphaHub &s, bool wrote) {
  Alpha::FactorsUIState &ui = s.factors_ui;
  assert(ui.search_all_n > 0 && s.run_service && s.search_service);
  if (!wrote && ui.search_cur.fresh && !ui.search_queue.empty() && ui.search_queue.front().row.file == ui.search_cur.row.file)
    ui.search_queue.front().fresh = true;
  if (ui.search_queue.empty()) {
    ui.search_all_n = 0;
    Alpha::RunRequest req;
    if (Alpha::MakeRunRequest(data, level_table(data, s), s.pool, /*evaluate=*/true, ui.backend == 1, req))
      s.run_service->Request(req);
    return;
  }
  ui.search_cur = std::move(ui.search_queue.front());
  ui.search_queue.erase(ui.search_queue.begin());
  const Alpha::SearchJob &job = ui.search_cur;
  Alpha::SearchRequest req;
  if (Alpha::MakeSearchRequest(data, level_table(data, s), s.pool, job.row, job.hold, job.fresh, ui.backend == 1, req)) {
    s.search_service->Request(req);
  } else {
    ui.search_queue.clear();
    ui.search_all_n = 0;
  }
}

// Factors 子页的作用域 (每帧从 config 取; 池 / 层取本页已生效的)
static Alpha::FactorsUIContext factors_context(const SharedData &data, std::shared_ptr<const Alpha::FeatureTable> ft, const std::string &pool) {
  Alpha::FactorsUIContext c;
  c.lv = ft->lv;
  c.pool = pool;
  c.factor_dir = Alpha::factor_dir_of(data, pool, c.lv);
  c.ft = std::move(ft);
  c.fixed_impact = data.config.fixed_impact;
  c.axis_ready = !data.asset.items.empty();
  c.assets = &data.asset.items;
  return c;
}

std::shared_ptr<Alpha::AlphaHub> CreateAlphaHub() { return std::make_shared<Alpha::AlphaHub>(); }

TaskHandle CreateAlphaTask(std::shared_ptr<Compute::ComputeHub> compute, std::shared_ptr<Alpha::AlphaHub> state) {
  TaskHandle handle;
  handle.name = "Alpha";
  handle.storage = state;
  handle.tabs = {"Compute", "Operators", "Factors", "Inspect"};

  handle.Enabled = [compute](const SharedData & /*data*/, int idx) { return idx != TAB_COMPUTE || compute->service != nullptr; };

  // 不设 OnCollapse: 切走任务 worker 继续跑完 (单线程 + ~90 行小结构, 左栏状态标签照常更新, 切回直接看表)

  // 每帧 (无论是否选中): 一轮跑完 / 取消那一帧把算子表落地 <factor_dir>/operators.json
  // (与池无关, 全局一份; 取消也落, 已跑完的行不白算). 切走任务 worker 仍在跑, 故放 Update 不放 Draw
  handle.Update = [state](SharedData &data) {
    // Search all: 一个文件搜完 (忙 → Done / Cancelled, 含 Loading 期就放弃的) → 出队下一个, 队空整目录 Run; 被取消 → 整轮放弃
    // 单个 Search 写了 weights 的那一轮 (Running → Done) 结束: 重扫 + 只 Run 该因子文件 (Search 跑时 Run 被禁, 不会撞)
    if (state->search_service && state->run_service) {
      const Phase sst = state->search_service->phase.load(std::memory_order_acquire);
      const Phase prev = state->prev_search_phase;
      const bool finished = (sst == Phase::Done || sst == Phase::Cancelled) && (prev == Phase::Scanning || prev == Phase::Loading || prev == Phase::Running);
      if (finished && state->factors_ui.search_all_n > 0) {
        if (sst == Phase::Done) {
          bool wrote = false;
          {
            std::lock_guard<std::mutex> lock(state->search_service->mutex);
            wrote = state->search_service->result.wrote;
          }
          search_all_next(data, *state, wrote);
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
          Alpha::RunRequest req;
          if (Alpha::MakeRunRequest(data, level_table(data, *state), state->pool, /*evaluate=*/true, state->factors_ui.backend == 1, req)) {
            req.spec.only_file = file;
            state->run_service->Request(req);
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
      Alpha::SaveOperatorTableJson(data.config.factor_dir, svc);
    state->prev_operators_phase = st;
  };

  handle.Status = [state, compute](const SharedData &data, int idx) -> TaskStatus {
    assert(idx >= 0 && idx < TAB_COUNT);
    if (idx == TAB_COMPUTE)
      return Compute::ComputeStatus(*compute, data, Compute::LibSide::Alpha);
    if (idx == TAB_FACTORS) {
      if (state->search_service && state->search_service->busy()) { // Search 在跑时 Factors 行显示它的进度 (Run 此时被禁)
        const auto &ss = *state->search_service;
        const size_t total = ss.total.load(std::memory_order_relaxed);
        return {TaskStatus::Kind::Busy, "optimizing " + std::to_string(total > 0 ? 100 * ss.done.load(std::memory_order_relaxed) / total : 0) + "%"};
      }
      if (!state->run_service)
        return {};
      const auto &fs = *state->run_service;
      if (fs.phase.load(std::memory_order_acquire) == Phase::Done && fs.broken() > 0)
        return {TaskStatus::Kind::Warn, std::to_string(fs.broken()) + " broken"};
      return ProgressStatus(fs);
    }
    if (idx == TAB_INSPECT)
      return state->inspect_service ? ProgressStatus(*state->inspect_service) : TaskStatus{};
    // Operators
    if (!state->operators_service)
      return {};
    const auto &svc = *state->operators_service;
    if (svc.phase.load(std::memory_order_acquire) == Phase::Done && svc.failed() > 0)
      return {TaskStatus::Kind::Error, std::to_string(svc.failed()) + " failed"};
    return ProgressStatus(svc);
  };

  handle.Draw = [state, compute](SharedData &data, int idx) {
    assert(idx >= 0 && idx < TAB_COUNT);
    if (idx == TAB_COMPUTE) {
      ImGui::BeginChild("AlphaTab", ImVec2(0, 0), false);
      ImGui::Spacing();
      Compute::RenderTabCompute(*compute, data, Compute::LibSide::Alpha);
      ImGui::EndChild();
      return;
    }
    if (!state->operators_service)
      state->operators_service = std::make_unique<Alpha::OperatorsService>();
    auto &svc = *state->operators_service;

    // 默认不自动跑: 先吃本地 operators.json (校验不过它自己会删掉文件), 没有有效快照才起算一轮.
    // 载入的一轮状态直接是 Done 且不经 Running, Update 的落盘判据 (prev == Running) 因此不会触发
    if (!state->operators_started) {
      state->operators_started = true;
      if (!Alpha::LoadOperatorTableJson(data.config.factor_dir, svc, state->operators_ui))
        svc.Request(state->operators_ui.req);
    }

    if (idx != TAB_OPERATORS)
      Alpha::EnsurePortfolio(data, *state);

    ImGui::BeginChild("AlphaTab", ImVec2(0, 0), false);
    ImGui::Spacing();
    switch (idx) {
    case TAB_OPERATORS: {
      const int action = Alpha::RenderTabOperators(svc, state->operators_ui);
      if (action == 1)
        svc.Request(state->operators_ui.req);
      else if (action == -1)
        svc.RequestCancel();
      break;
    }
    case TAB_FACTORS:
    case TAB_INSPECT: {
      // 页顶条选 (want_pool, want_lv); 已生效的 (pool, lv) 在所有服务空闲时追上: 换层 = 四个服务整体重建
      // (目录 / Inspect 缓存都随层, 字段表按层常驻); 同层换池只换目录 (下方自动重扫); 有服务在跑 = 取消旧作用域, 停下再切
      {
        ImGui::BeginGroup();
        ImGui::TextUnformatted("Pool:");
        ImGui::SameLine();
        ImGui::SetNextItemWidth(140);
        if (ImGui::BeginCombo("##factor_pool", pool_def(state->want_pool).name)) {
          if (ImGui::IsWindowAppearing()) {
            state->pool_files.clear();
            for (const PoolDef &d : feature_pools()) {
              size_t n = 0;
              for (const Alpha::FactorLevel &l : Alpha::kFactorLevels)
                n += Alpha::list_factor_files(Alpha::factor_dir_of(data, d.name, l.id)).size();
              state->pool_files.push_back(n);
            }
          }
          assert(state->pool_files.size() == feature_pools().size());
          float cn_x = 0.0f; // 中文名列起点 (对齐最长池名)
          for (const PoolDef &d : feature_pools())
            cn_x = std::max(cn_x, ImGui::CalcTextSize(d.name).x);
          cn_x += ImGui::GetCursorPosX() + 2.0f * ImGui::GetStyle().ItemSpacing.x;
          for (const PoolId p : pool_ids()) {
            const PoolDef &d = pool_def(p);
            const bool selected = state->want_pool == p;
            if (ImGui::Selectable(d.name, selected))
              state->want_pool = p;
            ImGui::SameLine(cn_x);
            ImGui::TextDisabled("%s (%zu)", d.name_cn, state->pool_files[p]);
            if (selected)
              ImGui::SetItemDefaultFocus();
          }
          ImGui::EndCombo();
        }
        ImGui::SameLine();
        ImGui::TextUnformatted("Level:");
        {
          const std::string key = data.config.factor_dir + "|" + pool_def(state->want_pool).name;
          const Phase fst = state->run_service ? state->run_service->phase.load(std::memory_order_acquire) : Phase::Idle;
          const bool repool = key != state->counted_key;
          if (repool || (fst == Phase::Done && state->prev_factors_phase != Phase::Done)) {
            state->counted_key = key;
            for (const Alpha::FactorLevel &l : Alpha::kFactorLevels)
              state->level_files[l.id] = Alpha::list_factor_files(Alpha::factor_dir_of(data, pool_def(state->want_pool).name, l.id)).size();
          }
          state->prev_factors_phase = fst;
          if (repool) // 进页 / 换池: 切到该池最低频的有因子文件的层 (都没有 → 不动)
            for (const Alpha::FactorLevel &l : Alpha::kFactorLevels | std::views::reverse)
              if (state->level_files[l.id] > 0) {
                state->want_lv = l.id;
                break;
              }
        }
        for (const Alpha::FactorLevel &l : Alpha::kFactorLevels) {
          char label[48];
          std::snprintf(label, sizeof(label), "%s (%zu)###flvl%d", l.name, state->level_files[l.id], l.id);
          ImGui::SameLine();
          if (ImGui::RadioButton(label, l.id == state->want_lv))
            state->want_lv = l.id;
        }
        ImGui::EndGroup();
        // 因子层 = 所用特征的最细层 (叶写全名 档.层.列); 门控 / 标签在因子池的 Bar 库 (截面 = 该库 cs_valid);
        // 用了 lob.* 叶 (只 min 层有) 评估期缩到同池 Lob 库落盘日, 另有真实冲击段; 因子目录 <factor_dir>/<池>/<层>/
        if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
          tip("池 + 因子层 (括号里是因子数)\n"
              "• 窗长 d 等参数按本层行数计\n"
              "• 有计算在跑时, 先取消, 停下后切过去");
      }
      {
        const bool busy = (state->run_service && state->run_service->busy()) || (state->search_service && state->search_service->busy()) ||
                          (state->inspect_service && state->inspect_service->busy()) || state->factors_ui.search_all_n > 0;
        const Alpha::LevelId want_lv = state->want_lv;
        const std::string want_pool = pool_def(state->want_pool).name;
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
        } else if (want_lv != state->lv || want_pool != state->pool) { // 换作用域 = 取消旧作用域在跑的 (Search all 整轮放弃), 停下那帧再切
          if (state->run_service)
            state->run_service->RequestCancel();
          if (state->search_service)
            state->search_service->RequestCancel();
          if (state->inspect_service)
            state->inspect_service->RequestCancel();
          state->factors_ui.search_queue.clear();
          state->factors_ui.search_all_n = 0;
          ImGui::SameLine();
          ImGui::TextColored(ImVec4(1.0f, 0.8f, 0.3f, 1.0f), "(取消中, 停下后切到 %s / %s)", want_pool.c_str(), Alpha::factor_level(want_lv).name);
        }
      }
      assert(state->lv); // 有服务在跑 ⇒ 之前已生效过
      // 首次进页 / 换层: 只扫描不算 (字段表视图按层建一次, 编译期字段表, 不随运行变); 池切换 → 目录变 → 自动重扫
      const std::shared_ptr<const Alpha::FeatureTable> ft = level_table(data, *state);
      if (!state->run_service) {
        state->run_service = std::make_unique<Alpha::RunService>();
        state->search_service = std::make_unique<Alpha::SearchService>();
      }
      auto &fs = *state->run_service;
      auto &ss = *state->search_service;
      const Alpha::FactorsUIContext ctx = factors_context(data, ft, state->pool);
      if (state->factors_scanned_dir != ctx.factor_dir) {
        state->factors_scanned_dir = ctx.factor_dir;
        Alpha::RunRequest req;
        Alpha::MakeRunRequest(data, ft, state->pool, /*evaluate=*/false, false, req);
        fs.Request(req);
      }
      if (!state->inspect_service) {
        state->inspect_service = std::make_unique<Alpha::InspectService>();
        state->kline_service = std::make_unique<Alpha::KlineService>();
      }
      auto &is = *state->inspect_service;
      int iaction = 0; // Inspect 请求: 1 起算 / -1 取消
      if (idx == TAB_INSPECT) {
        iaction = Alpha::RenderTabInspect(fs, is, *state->kline_service, state->factors_ui, state->inspect_ui, ctx);
      } else {
        const int action = Alpha::RenderTabFactors(fs, ss, state->factors_ui, state->portfolio_ui, ctx);
        if (action == 1 || action == 5 || action == 2) {
          Alpha::RunRequest req;
          if (Alpha::MakeRunRequest(data, ft, state->pool, /*evaluate=*/action != 2, state->factors_ui.backend == 1, req)) {
            if (action == 1)
              req.spec.only_file = state->factors_ui.run_file;
            fs.Request(req);
          }
        } else if (action == -1) {
          fs.RequestCancel();
        } else if (action == 3) {
          Alpha::SearchRequest req;
          if (Alpha::MakeSearchRequest(data, ft, state->pool, state->factors_ui.search_one.row, state->factors_ui.search_one.hold,
                                       state->factors_ui.search_one.fresh, state->factors_ui.backend == 1, req))
            ss.Request(req);
        } else if (action == 4) {
          state->factors_ui.search_cur = Alpha::SearchJob{};
          search_all_next(data, *state, /*wrote=*/true);
        } else if (action == -3) {
          ss.RequestCancel();
        }
      }
      // 自动起算两页都看: Factors 页点行高光即起算, 不等切到 Inspect (左栏 Inspect 状态同步走)
      if (iaction == 0 && Alpha::InspectAutoRequest(fs, is, state->factors_ui, state->inspect_ui, ctx))
        iaction = 1;
      if (iaction == 1) {
        Alpha::InspectRequest req;
        if (Alpha::MakeInspectRequest(data, ft, state->pool, state->inspect_ui.req_row, state->inspect_ui.req_reload,
                                      state->factors_ui.backend == 1, state->inspect_ui.req_export, state->inspect_ui.top_n, req))
          is.Request(req);
      } else if (iaction == -1) {
        is.RequestCancel();
      }
      break;
    }
    default:
      break;
    }
    if (idx != TAB_OPERATORS)
      Alpha::SyncPortfolio(data, *state, false);
    ImGui::EndChild();
  };

  handle.Destroy = [state]() {
    state->operators_service.reset(); // 析构 Stop() join worker
    state->operators_started = false;
    state->search_service.reset(); // 析构 Stop() join worker
    state->prev_search_phase = Phase::Idle;
    state->factors_ui.search_queue.clear(); // Search all 随服务一起作废
    state->factors_ui.search_all_n = 0;
    state->run_service.reset();
    state->factors_scanned_dir.clear();
    state->kline_service.reset();   // 析构 Stop() join worker
    state->inspect_service.reset(); // 析构 Stop() join worker
    state->inspect_ui = Alpha::InspectUIState{};
    state->tables = {};
    state->counted_key.clear();
    state->portfolio_service.reset(); // 析构 Stop() join worker
    state->portfolio_ui = Market::PortfolioUIState{};
    state->portfolio_gen = ~0ull;
    state->portfolio_dirty = false;
    state->portfolio_sig.clear();
    state->portfolio_prev_fs = Phase::Idle;
  };

  return handle;
}

} // namespace GUI::Tasks
