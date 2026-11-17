// Views — 见头文件. 因子 CPU 后端与统计契约的消费者: CMake MATH_PRECISE 源.
#include "gui/task_agent/Views.hpp"
#include "gui/task_agent/tools/look/Kernel.hpp"

#include <cassert>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Agent {

ViewRunner::ViewRunner() = default;
ViewRunner::~ViewRunner() = default;

ToolOut ViewRunner::run(const Call &c, const ViewEnv &env, Engine::FactorFlow &F) {
  using Fn = ToolOut (*)(const Call &, const ViewEnv &, Engine::FactorFlow &);
  // 下标 = Tool (ViewFeature .. ViewStockDays 一一对应; ViewBook 另带常驻重放资源)
  static constexpr Fn kViews[] = {ViewFeature, ViewLayers, ViewTrades, ViewHolders, ViewResponse, ViewExplain, ViewSnapshot, ViewStockDay, ViewStockDays};
  static_assert(static_cast<size_t>(Tool::ViewBook) == std::size(kViews));
  if (c.tool == Tool::ViewBook) {
    if (!book_)
      book_ = std::make_unique<Replay>();
    return ViewBook(c, env, F, *book_);
  }
  const size_t idx = static_cast<size_t>(c.tool);
  assert(idx < std::size(kViews) && "不是观察工具");
  return kViews[idx](c, env, F);
}

std::string ViewRunner::rep_layers(const std::string &expr, int sign, const ViewEnv &env, Engine::FactorFlow &F) {
  using namespace look;
  const Engine::FeatureTable &ft = *env.fsc.ft;
  FactorSpec fac{.e = ft.reparse(expr), .dir = st::dir_of(sign), .label = expr};
  std::string dummy;
  [[maybe_unused]] const bool ok_rf = fx::root_frame(fac.e, ft.seg, fac.frame, dummy);
  assert(ok_rf);
  const int seg = ft.seg;
  const std::vector<Slot> ps = parts_of(seg, env.open, env.clock);
  const std::vector<int> pr = part_of_row(ps, seg);
  Layer all;
  std::vector<Layer> bs(ps.size());
  std::vector<misc::Ymd> dates;
  Engine::Job job;
  std::string err;
  StreamCfg cfg{.fac = &fac};
  const Engine::Rc rc = run_stream(env, F, cfg, dates, job, err, whole, [&](const ChunkView &cv) {
    for (int r = 0; r < cv.t_hi - cv.t_lo; ++r) {
      all.add(cv.rows[static_cast<size_t>(r)]);
      if (seg > 1)
        bs[static_cast<size_t>(pr[static_cast<size_t>((cv.u0 + r) % seg)])].add(cv.rows[static_cast<size_t>(r)]);
    }
  });
  if (rc != Engine::Rc::Ok)
    return err.empty() ? std::string() : "- 代表点" + err + "\n";
  std::string s = "- 代表点的 20 组每期超额 G1 → G20: " + spark(all.grps(), bp) + " bp · mono " + r3(all.mono()) + " · 多空 " + bp(all.ls()) + "bp\n";
  if (seg > 1) {
    s += "- 代表点分时段 mono:";
    for (size_t j = 0; j < ps.size(); ++j)
      s += (j ? " · " : " ") + ps[j].name + " " + r3(bs[j].mono());
    s += "\n";
  }
  return s + "- 观察工具的 factor 写本候选名即看代表点\n";
}

} // namespace GUI::Agent
