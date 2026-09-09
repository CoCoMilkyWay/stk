#include "gui/task_features/services/DistService.hpp"
#include "shared/SharedData.hpp"

namespace GUI::Features {

Dist &DistService::target(SharedData &data) { return data.dist; }

void DistService::RequestCompute(SharedData &data) {
  const auto &sel = data.feature.selection;
  if (sel.primary_feature_idx() < 0 || sel.selected_level != static_cast<int>(analysis::kLevel))
    return; // 分析只在 L1 上跑 (L0 数据量下全量分布分析无意义)
  const size_t feat = static_cast<size_t>(sel.primary_feature_idx());
  const auto &meta = data.feature.metadata;
  const auto &meta_list = meta.features[analysis::kLevel];
  assert(feat < meta_list.size());

  DistRequest req;
  req.scope = analysis::read_scope(data.config, data.asset.items.size());
  if (req.scope.months.empty())
    return;
  req.columns = {feat};
  // valid 列: 按特征元数据的 valid_type 决定是否带 _meta 门控列 (编码见 Meta.hpp; L1 只有 DATA 门控)
  if (meta_list[feat].valid_type != L2::ValidType::ALL)
    req.columns.push_back(meta.col_of(analysis::kLevel, "_meta"));
  submit(std::move(req));
}

void DistService::reset(Dist &dist, DistRequest &req) {
  dist.reset_for_build(std::move(req.columns), std::move(req.scope.months), std::move(req.scope.uni.ids));
}

} // namespace GUI::Features
