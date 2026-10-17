#include "gui/task_features/services/PreviewService.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cstring>

namespace GUI::Features {

FeaturePreview &PreviewService::target(SharedData &data) { return data.preview; }

void PreviewService::RequestCompute(SharedData &data) {
  // 预览只在 L1 上跑; 覆盖该层全部非 META 特征, 不看选中
  const auto &meta = data.feature.meta();
  const auto &meta_list = meta.features[analysis::kLevel];
  assert(!meta_list.empty());

  Req req;
  req.scope = analysis::read_scope(data.config, data.asset.items.size(), data.feature.selection.tier);
  if (req.scope.months.empty())
    return;
  req.spec.n_features = meta_list.size();
  // price 笼: 涨跌停两列只在 All 档 (Fund); Pool 档无笼, 不判 price
  const bool caged = std::any_of(meta_list.begin(), meta_list.end(), [](const FeatureMetadata &f) { return std::strcmp(f.code, "lim_dn") == 0; });
  req.spec.lim_dn = caged ? meta.col_of(analysis::kLevel, "lim_dn") : FeaturePreview::Spec::kNoCage;
  req.spec.lim_up = caged ? meta.col_of(analysis::kLevel, "lim_up") : FeaturePreview::Spec::kNoCage;
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
