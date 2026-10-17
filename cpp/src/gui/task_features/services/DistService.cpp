#include "gui/task_features/services/DistService.hpp"
#include "shared/SharedData.hpp"

namespace GUI::Features {

Dist &DistService::target(SharedData &data) { return data.dist; }

void DistService::RequestCompute(SharedData &data) {
  const auto &sel = data.feature.selection;
  const ColRef *feat = sel.primary();
  if (!feat)
    return;

  Req req;
  req.scope = analysis::read_scope(data.config, sel.lib_of(feat->tier));
  if (req.scope.months.empty())
    return;
  // 网格 = 特征自己的层; 门控按特征元数据的 valid_type 决定是否带 ts_valid (L_min 全列 ALL, L_sec 按 DATA / DEPTH)
  req.spec.plan = analysis::read_plan(data.feature, feat->tier, feat->level, {*feat});
  Request(std::move(req));
}

} // namespace GUI::Features
