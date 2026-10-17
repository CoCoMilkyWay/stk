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

  Req req;
  req.scope = analysis::read_scope(data.config, sel.lib());
  if (req.scope.months.empty())
    return;
  req.spec.params = params;
  // 门控: 各列按自己的 valid_type, 任一需门控才带 ts_valid (L_min 全列 ALL, L_sec 按 DATA / DEPTH)
  req.spec.cols = analysis::column_set(data.feature, analysis::kLevel, {feat});
  // NeutralRank 上下文 (市值 / 行业) 只在全局库: 两档一律 Bar 档旁读
  if (params.cs_neutral()) {
    const auto &all_meta = data.feature.meta(TierId::Bar);
    req.spec.side = analysis::column_set(data.feature, TierId::Bar, analysis::kLevel,
                                         {all_meta.col_of(analysis::kLevel, TF_STR(NEUTRAL_RANK_MCAP)),
                                          all_meta.col_of(analysis::kLevel, TF_STR(NEUTRAL_RANK_INDUSTRY))});
  }
  // 焦点按新库轴 clamp: UI 槽位可能来自上一个库 (切库后库轴大小已变)
  req.spec.focus = static_cast<uint32_t>(std::clamp<int>(focus, 0, static_cast<int>(req.scope.lib.axis.size()) - 1));
  Request(std::move(req));
}

} // namespace GUI::Features
