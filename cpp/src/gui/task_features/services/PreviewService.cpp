#include "gui/task_features/services/PreviewService.hpp"
#include "shared/SharedData.hpp"

namespace GUI::Features {

Preview &PreviewService::target(SharedData &data) { return data.preview; }

void PreviewService::RequestCompute(SharedData &data) {
  // 预览覆盖当前档每层全部非 META 特征 (各层网格 = 该层), 不看选中
  const TierId tier = data.feature.selection.tier;
  const LibId lib = data.feature.selection.cur_lib();
  const auto &meta = data.feature.meta(tier);

  Req req;
  // price 笼: 涨跌停两列只在 all 库 (票池库经旁读)
  const std::vector<ColRef> cage = {data.feature.ref(kAllLib, L_day, "lim_dn"), data.feature.ref(kAllLib, L_day, "lim_up")};
  req.spec.cage = {cage[0].col, cage[1].col};
  req.scope = analysis::read_scope(data.config, lib, cage);
  if (req.scope.months.empty())
    return;
  req.spec.tier = tier;
  for (size_t l = 0; l < LEVEL_COUNT; ++l) {
    const auto &meta_list = meta.features[l];
    req.spec.n_features[l] = meta_list.size();
    std::vector<ColRef> cols;
    for (size_t i = 0; i < meta_list.size(); ++i) {
      if (meta_list[i].data_type == FeatureDataType::META)
        continue; // 元数据列不预览 (含 ts_valid 自身)
      cols.push_back({lib, static_cast<uint8_t>(l), static_cast<uint16_t>(i)});
    }
    if (!cols.empty())
      req.spec.plans[l] = analysis::read_plan(data.feature, l, analysis::Rows::AsOf, std::move(cols));
  }
  Request(std::move(req));
}

} // namespace GUI::Features
