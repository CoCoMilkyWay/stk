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
  const auto &meta_list = meta.features[analysis::kLevel];
  assert(feat < meta_list.size());

  TransformRequest req;
  req.scope = analysis::read_scope(data.config, data.asset.items.size());
  if (req.scope.months.empty())
    return;
  req.params = params;
  req.columns = {feat};
  if (params.cs_neutral()) {
    req.columns.push_back(meta.col_of(analysis::kLevel, TF_STR(NEUTRAL_RANK_MCAP)));
    req.columns.push_back(meta.col_of(analysis::kLevel, TF_STR(NEUTRAL_RANK_INDUSTRY)));
  }
  // valid 列: 按特征元数据的 valid_type 决定是否带 _meta 门控列 (恒为末列; L1 只有 DATA 门控)
  if (meta_list[feat].valid_type != L2::ValidType::ALL) {
    req.columns.push_back(meta.col_of(analysis::kLevel, "_meta"));
    req.has_valid = true;
  }
  // 焦点按新子轴 clamp: UI 槽位可能来自上一个 universe (切 universe 后子轴大小已变)
  req.focus = static_cast<uint32_t>(std::clamp<int>(focus, 0, static_cast<int>(req.scope.uni.size()) - 1));
  submit(std::move(req));
}

void TransformService::reset(Transform &tf, TransformRequest &req) {
  tf.reset_for_build(req.params, std::move(req.columns), req.has_valid, std::move(req.scope.months),
                     std::move(req.scope.uni.ids), req.focus);
}

} // namespace GUI::Features
