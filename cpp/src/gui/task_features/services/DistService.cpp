#include "gui/task_features/services/DistService.hpp"
#include "shared/SharedData.hpp"

namespace GUI::Features {

Dist &DistService::target(SharedData &data) { return data.dist; }

void DistService::RequestCompute(SharedData &data) {
  const auto &sel = data.feature.selection;
  if (sel.primary_feature_idx() < 0 || sel.selected_level != static_cast<int>(analysis::kLevel))
    return; // 分析只在 L1 上跑 (L0 数据量下全量分布分析无意义)
  const size_t feat = static_cast<size_t>(sel.primary_feature_idx());

  Req req;
  req.scope = analysis::read_scope(data.config, sel.lib());
  if (req.scope.months.empty())
    return;
  // 门控: 按特征元数据的 valid_type 决定是否带 ts_valid (编码见 Meta.hpp; L1 全列 ALL, L0 按 DATA / DEPTH)
  req.spec.cols = analysis::column_set(data.feature, analysis::kLevel, {feat});
  Request(std::move(req));
}

} // namespace GUI::Features
