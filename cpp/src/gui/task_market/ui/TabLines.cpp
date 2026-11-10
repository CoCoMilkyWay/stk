// Market 行情页 Lines: 头 / 布局 / 三级缓存 — 见头文件; 主图 / 区间条 = LinesPlot.cpp, 右图例 = LinesSheet.cpp
#include "gui/task_market/ui/TabLines.hpp"
#include "disk/Disk.hpp"

#include "gui/Tasks.hpp" // StatusColor
#include "gui/task_market/ui/LinesCommon.hpp"
#include "gui/util/Tip.hpp"
#include "imgui.h"
#include "nlohmann/json.hpp"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

namespace GUI::Market {

using json = nlohmann::json;

namespace {

// ---- 头: [NAV: 组合 / 成本] [NAV / VAL: log] Recompute / Cancel / 状态 / Clear (选择与图回默认) / 页说明 + [NAV: 失效 / 失败条目原因].
//      返回 1 = Recompute, -1 = Cancel, 0 = 无 ----
int head(int page, PortfolioService &svc, const PortfolioUIState &pf, LinesUIState &ui, const LinesDerived &d, const PortfolioResult &res, const std::string &message,
         double fixed_impact) {
  int action = 0;
  if (page == kPageNav) {
    ImGui::TextColored(kHead, "Portfolio:");
    ImGui::SameLine();
    if (pf.name.empty()) {
      ImGui::TextDisabled("无");
      if (ImGui::IsItemHovered())
        tip("在 Alpha → Factors 页 1. 区右侧新建 / 选组合; 无组合时只少因子 / 策略两组");
    } else {
      size_t n_bad = 0;
      for (const PortfolioItem &it : pf.items)
        n_bad += !it.missing.empty();
      ImGui::Text("%s  %zu 因子 (算成 %d", pf.name.c_str(), pf.entries.size(), d.n_ready);
      ImGui::SameLine(0, 0);
      if (d.n_error > 0) {
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), ", 失败 %d", d.n_error);
        ImGui::SameLine(0, 0);
      }
      if (n_bad > 0) {
        ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), ", 失效 %zu", n_bad);
        ImGui::SameLine(0, 0);
      }
      ImGui::Text(")");
    }
    ImGui::SameLine();
    ImGui::TextDisabled("|");
    ImGui::SameLine();
    ImGui::TextDisabled("有冲击");
    if (ImGui::IsItemHovered())
      tip("换手成本 = 各因子自己的最小冲击段, 不可选\n"
          "• 每行开 1/h̄ 净值一批 (h̄ = 平均持有行数, 全日平均满仓), 换手按 Δw: 上一批平仓与本批建仓同一价格点时只交接差额 (留仓不成交);\n"
          "  新进成交不了不建仓、续持者封板照持也加不了仓, 平仓成交不了的顺延\n"
          "• 用了 lob.* 叶: 增仓 × 最小金额段冲击 (封顶 max_impact) + 平仓量 × 固定冲击 %.1f bp\n"
          "• 否则固定段: 增仓量 / 平仓量都按固定冲击 %.1f bp\n"
          "• 组合只算 G20 多头; 无冲击线不扣",
          fixed_impact * 1e4, fixed_impact * 1e4);
    ImGui::SameLine();
  }
  if (page == kPageNav || page == kPageVal) {
    ImGui::Checkbox("log", &ui.log_y);
    if (ImGui::IsItemHovered())
      tip("y 轴 (净值) 切对数刻度");
    ImGui::SameLine();
  }
  const bool busy = svc.busy();
  ImGui::BeginDisabled(busy);
  if (ImGui::Button("Recompute", ImVec2(90, 0)))
    action = 1;
  ImGui::EndDisabled();
  if (ImGui::IsItemHovered(ImGuiHoveredFlags_AllowWhenDisabled))
    tip("弃缓存重算全部因子 + 池指数 (特征库重算过之后用)\n"
        "• 平时不用: 组合加 / 删因子自动只算新的那个");
  ImGui::SameLine();
  ImGui::BeginDisabled(!busy);
  if (ImGui::Button("Cancel", ImVec2(60, 0)))
    action = -1;
  ImGui::EndDisabled();
  ImGui::SameLine();
  {
    const Phase ph = svc.phase.load(std::memory_order_acquire);
    const int jd = svc.jobs_done.load(std::memory_order_relaxed), jt = svc.jobs_total.load(std::memory_order_relaxed);
    const int done = static_cast<int>(svc.done.load(std::memory_order_relaxed)), total = static_cast<int>(svc.total.load(std::memory_order_relaxed));
    const ImVec4 bc = StatusColor(TaskStatus::Kind::Busy);
    switch (ph) {
    case Phase::Idle:
    case Phase::Scanning:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Muted), "idle");
      break;
    case Phase::Loading:
      ImGui::TextColored(bc, "[%d/%d] loading %d/%d days", jd + 1, jt, done, total);
      break;
    case Phase::Running:
      ImGui::TextColored(bc, "[%d/%d] running %d/%d", jd + 1, jt, done, total);
      break;
    case Phase::Done:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Ready), "done");
      break;
    case Phase::Cancelled:
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "cancelled %d/%d", jd, jt);
      break;
    }
  }
  if (!message.empty()) {
    ImGui::SameLine();
    ImGui::TextColored(StatusColor(TaskStatus::Kind::Warn), "| %s", message.c_str());
  }
  ImGui::SameLine();
  if (ImGui::Button("Clear", ImVec2(60, 0))) {
    ui.pick = LinesPick{.of = pf.name};
    ui.range = LinesRange{.d0 = 0, .d1 = static_cast<int>(d.dates.size())};
    ui.log_y = false;
  }
  if (ImGui::IsItemHovered())
    tip("清掉全部选择, 图回默认 (空): NAV / FLOW 选中行 / VAL / CTA 标的 / 展开的行 / 叠加的成分股 / 区间 (回全段) / log 关");
  ImGui::SameLine();
  ImGui::TextDisabled("(?)");
  if (ImGui::IsItemHovered())
    tip("%s", kPageDefs[page].tip);
  if (page != kPageNav)
    return action;
  for (const PortfolioItem &it : pf.items) {
    const auto f = res.factors.find(it.e.key());
    const std::string &why = !it.missing.empty() ? it.missing : f != res.factors.end() ? f->second->error
                                                                                       : it.missing;
    if (!why.empty())
      ImGui::TextColored(StatusColor(TaskStatus::Kind::Error), "  %s  %s", EntryLabel(it.e).c_str(), why.c_str());
  }
  return action;
}

} // namespace

std::vector<std::string> LoadWatch() {
  if (!disk::exists(disk::WatchFile))
    return {};
  const json j = json::parse(disk::read(disk::WatchFile), nullptr, /*allow_exceptions=*/false);
  assert(!j.is_discarded() && j.is_object() && j.contains("watch") && j["watch"].is_array() && "自选文件: 顶层须是 {watch: [...]}");
  std::vector<std::string> w;
  for (const json &o : j["watch"]) {
    assert(o.is_string() && "自选文件: 条目须是字符串");
    w.push_back(o.get<std::string>());
  }
  return w;
}

// 一条目一行 (人能直接读 / diff)
void SaveWatch(const std::vector<std::string> &watch) {
  std::ostringstream f;
  {
    f << "{\n  \"watch\": [";
    for (size_t i = 0; i < watch.size(); ++i)
      f << (i ? ",\n    " : "\n    ") << json(watch[i]).dump();
    f << (watch.empty() ? "]\n}\n" : "\n  ]\n}\n");
  }
  disk::write(disk::WatchFile, {}, f.str());
}

void SyncLinesWatch(LinesUIState &ui) {
  if (ui.watch_loaded)
    return;
  ui.watch_loaded = true;
  ui.watch = LoadWatch();
}

int RenderTabLines(int page, PortfolioService &svc, CtaService &cta, const PortfolioUIState &pf, LinesUIState &ui, double fixed_impact,
                   const Quote::QuoteHub *mk, const StockInfos &infos) {
  assert(page >= 0 && page < kPages);
  PortfolioResult res;
  uint64_t gen = 0;
  std::string message;
  {
    std::lock_guard<std::mutex> lock(svc.mutex);
    res = svc.result; // shared_ptr 拷贝, 不拷序列
    gen = svc.result_gen;
    message = svc.message;
  }
  LinesDerived &d = ui.der.val;

  // ---- 三级缓存, 各级的键 = 它读的东西: 派生 (键含成分组: 展开行 / 选中的个股) → 滚动分位 (日轴代 + 标的估值腿) / 末点分位 (估值代)
  //      → 视图 (派生 gen + 二级两份 gen + 标的): 上级重算 → 下级自动重算 ----
  if (ui.pick.of != pf.name) // 换组合 → 选中清掉
    ui.pick = LinesPick{.of = pf.name};
  ui.der.update(LinesDerKey{gen, pf.items_gen, ui.basket.feed.shown, fixed_impact, infos.size(), ui.pick.open, ui.pick.stocks(kPicks), ui.watch},
                [&](LinesDerived &dv) {
                  const std::vector<misc::Ymd> old = dv.dates;
                  MakeLinesDerived(pf.items, pf.name, res, ui.basket.feed.data, fixed_impact, ui.pick.open, ui.pick.stocks(kPicks), ui.watch, infos,
                                   dv);
                  if (dv.dates != old) // 日轴变 → 区间回全段
                    ui.range = LinesRange{.d0 = 0, .d1 = static_cast<int>(dv.dates.size())};
                });
  if (mk)
    SyncLinesCons(ui, *mk->quote);
  const std::map<std::string, LinesQuote> &one = ui.pick.rows[kPickOne];
  const LinesAt pin = one.empty() ? LinesAt{} : d.at(one.begin()->first); // VAL 标的
  const LinesLeg lg = pin.g >= 0 ? d.row(pin).val : LinesLeg{};           // 其估值腿
  ui.roll.update(LinesRollKey{d.axis_gen, lg, ui.band_years}, [&](LinesRolls &r) { MakeLinesRoll(d, lg, ui.band_years, r); });
  ui.pct.update(LinesPctKey{d.val_gen, ui.band_years, ui.range.d1}, [&](LinesPct &p) { MakeLinesPct(d, ui.range.d1, ui.band_years, p); });
  ui.view.update(LinesViewKey{ui.der.gen, ui.roll.gen, ui.pct.gen, pin, ui.range.d0, ui.range.d1},
                 [&](LinesView &vw) { MakeLinesView(d, ui.range.d0, ui.range.d1, pin, ui.roll.val, ui.pct.val, vw); });

  const int action = head(page, svc, pf, ui, d, res, message, fixed_impact);
  const bool nav = page == kPageNav;

  // ---- 布局: 左 = 日内 (NAV 开日内, 切 1/3 高) / 主图 (NAV ↔ VAL) / 区间条 自上而下, FLOW = 只一张资金流图, CTA = 只两图;
  //      最右 = 右图例 (各页同宽 sheet_width; CTA = 中栏与右表各半) ----
  constexpr float kBarH = 90.0f;
  const ImGuiStyle &st = ImGui::GetStyle();
  const ImVec2 av = ImGui::GetContentRegionAvail();
  const float lw = sheet_width(av.x);
  const float left = av.x - lw - st.ItemSpacing.x;
  const float ph = av.y - kBarH - st.ItemSpacing.y;
  const float ih = nav && ui.intraday ? std::floor((ph - st.ItemSpacing.y) / 3.0f) : 0.0f;
  // 选中的有行情键的行 (篮子 / 成分) 的行情叠加图 (日内栏 / FLOW 同一份选中, 线色 = 主图的), 贴边占满 size
  const auto basket_child = [&](const char *id, const ImVec2 &size, bool flow) {
    ImGui::PushStyleVar(ImGuiStyleVar_WindowPadding, ImVec2(0, 0));
    ImGui::BeginChild(id, size);
    ImGui::PopStyleVar();
    if (mk) {
      std::vector<Market::OverlayPick> picks;
      for_each_layer([&](int g) {
        for (size_t r = 0; r < d.rows[g].size(); ++r)
          if (const LinesRow &h = d.rows[g][r]; h.q.ok() && picked(ui, page, h))
            picks.push_back({h.q.side, h.q.key, ImGui::ColorConvertFloat4ToU32(row_color(d, g, r, kTrade)), h.tag});
      });
      if (flow)
        Market::RenderBasketFlow(*mk, ui.basket, picks);
      else
        Market::RenderBasketIntraday(*mk, ui.basket, picks);
    } else {
      ImGui::TextDisabled("行情等基本面就绪…");
    }
    ImGui::EndChild();
  };
  if (page == kPageFlow) {
    basket_child("##pf_flow", ImVec2(left, av.y), true);
    ImGui::SameLine();
    ImGui::BeginChild("##pf_flegend", ImVec2(lw, av.y), ImGuiChildFlags_Borders);
    RenderLinesSheet(ui, page, mk ? Market::FlowDays(*mk, ui.basket) : std::vector<misc::Ymd>{}, infos);
    ImGui::EndChild();
    return action;
  }
  if (page == kPageCta) {
    const float pw = std::floor((lw - st.ItemSpacing.x) / 2.0f);
    RenderLinesCta(cta, ui, pin.g >= 0 ? d.row(pin).cn.c_str() : nullptr, ImVec2(left, av.y), pw);
    ImGui::SameLine();
    ImGui::BeginChild("##pf_flegend", ImVec2(lw - pw - st.ItemSpacing.x, av.y), ImGuiChildFlags_Borders);
    RenderLinesSheet(ui, page, {}, infos);
    ImGui::EndChild();
    return action;
  }
  ImGui::BeginGroup();
  if (ih > 0.0f)
    basket_child("##pf_intra", ImVec2(left, ih), false);
  RenderLinesPlot(ui, pin, nav, ImVec2(left, ih > 0.0f ? ph - ih - st.ItemSpacing.y : ph)); // fit 只作用于紧接的 BeginPlot: 须在日内栏之后
  RenderLinesRange(ui, pin, nav, ImVec2(left, kBarH));
  ImGui::EndGroup();
  ImGui::SameLine();
  ImGui::BeginChild("##pf_flegend", ImVec2(lw, av.y), ImGuiChildFlags_Borders);
  RenderLinesSheet(ui, page, {}, infos);
  ImGui::EndChild();
  return action;
}

} // namespace GUI::Market