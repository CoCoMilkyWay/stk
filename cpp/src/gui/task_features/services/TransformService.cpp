#include "gui/task_features/services/TransformService.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>

#define TF_STR_(x) #x
#define TF_STR(x) TF_STR_(x)

namespace GUI::Features {

Transform &TransformService::target(SharedData &data) { return data.transform; }

void TransformService::RequestCompute(SharedData &data, const Transform::Params &params, int focus) {
  const auto &sel = data.feature.selection;
  if (sel.primary_feature_idx() < 0 || sel.selected_level != static_cast<int>(analysis::kLevel))
    return;
  const size_t feat = static_cast<size_t>(sel.primary_feature_idx());
  const auto &meta = data.feature.metadata;

  Req req;
  req.scope = analysis::read_scope(data.config, data.asset.items.size());
  if (req.scope.months.empty())
    return;
  req.spec.params = params;
  std::vector<size_t> cols{feat};
  if (params.cs_neutral()) {
    cols.push_back(meta.col_of(analysis::kLevel, TF_STR(NEUTRAL_RANK_MCAP)));
    cols.push_back(meta.col_of(analysis::kLevel, TF_STR(NEUTRAL_RANK_INDUSTRY)));
  }
  // 门控: 各列按自己的 valid_type, 任一需门控才带 ts_valid (L1 全列 ALL, L0 按 DATA / DEPTH)
  req.spec.cols = analysis::column_set(data.feature, analysis::kLevel, std::move(cols));
  // 焦点按新子轴 clamp: UI 槽位可能来自上一个 universe (切 universe 后子轴大小已变)
  req.spec.focus = static_cast<uint32_t>(std::clamp<int>(focus, 0, static_cast<int>(req.scope.uni.size()) - 1));
  Request(std::move(req));
}

} // namespace GUI::Features
