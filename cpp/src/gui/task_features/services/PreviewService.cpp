#include "gui/task_features/services/PreviewService.hpp"
#include "shared/SharedData.hpp"

namespace GUI::Features {

FeaturePreview &PreviewService::target(SharedData &data) { return data.preview; }

void PreviewService::RequestCompute(SharedData &data) {
  // 预览只在 L1 上跑; 覆盖该层全部非 META 特征, 不看选中
  const auto &meta = data.feature.metadata;
  const auto &meta_list = meta.features[analysis::kLevel];
  assert(!meta_list.empty());

  PreviewRequest req;
  req.scope = analysis::read_scope(data.config, data.asset.items.size());
  if (req.scope.months.empty())
    return;
  req.n_features = meta_list.size();
  req.meta_col = meta.col_of(analysis::kLevel, "_meta");
  for (size_t i = 0; i < meta_list.size(); ++i) {
    if (meta_list[i].data_type == FeatureDataType::META)
      continue; // 元数据列不预览 (含 _meta 自身)
    req.feat_cols.push_back(i);
    req.valid_types.push_back(meta_list[i].valid_type);
  }
  assert(!req.feat_cols.empty());
  submit(std::move(req));
}

void PreviewService::reset(FeaturePreview &pv, PreviewRequest &req) {
  pv.reset_for_build(std::move(req.feat_cols), std::move(req.valid_types), req.meta_col,
                     std::move(req.scope.months), req.n_features, req.scope.uni.size());
}

} // namespace GUI::Features
