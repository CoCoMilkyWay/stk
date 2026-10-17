#include "gui/task_features/services/PreviewService.hpp"
#include "shared/SharedData.hpp"

namespace GUI::Features {

FeaturePreview &PreviewService::target(SharedData &data) { return data.preview; }

void PreviewService::RequestCompute(SharedData &data) {
  // 预览只在 L1 上跑; 覆盖该层全部非 META 特征, 不看选中
  const auto &meta = data.feature.metadata;
  const auto &meta_list = meta.features[analysis::kLevel];
  assert(!meta_list.empty());

  Req req;
  req.scope = analysis::read_scope(data.config, data.asset.items.size());
  if (req.scope.months.empty())
    return;
  req.spec.n_features = meta_list.size();
  req.spec.lim_dn = meta.col_of(analysis::kLevel, "lim_dn");
  req.spec.lim_up = meta.col_of(analysis::kLevel, "lim_up");
  std::vector<size_t> cols;
  for (size_t i = 0; i < meta_list.size(); ++i) {
    if (meta_list[i].data_type == FeatureDataType::META)
      continue; // 元数据列不预览 (含 ts_valid 自身)
    cols.push_back(i);
  }
  assert(!cols.empty());
  req.spec.cols = analysis::column_set(data.feature, analysis::kLevel, std::move(cols));
  Request(std::move(req));
}

} // namespace GUI::Features
