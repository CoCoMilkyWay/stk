#include "gui/task_features/services/TransformService.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>

#define TF_STR_(x) #x
#define TF_STR(x) TF_STR_(x)

namespace GUI::Features {

Transform &TransformService::target(SharedData &data) { return data.transform; }

void TransformService::RequestCompute(SharedData &data, const Transform::Params &params, int focus) {
  const auto &sel = data.feature.selection;
  const ColRef *feat = sel.primary();
  if (!feat)
    return;

  Req req;
  req.spec.params = params;
  if (feat->level == L_day)
    req.spec.params.season = Transform::Season::None; // L_day 无日内轮廓
  // 网格 = 特征自己的层; 门控: 各列按自己的 valid_type, 任一需门控才带 ts_valid (L_min 全列 ALL, L_sec 按 DATA / DEPTH)
  std::vector<ColRef> cols{*feat};
  // NeutralRank 上下文 (市值 / 行业) 只在 all 库 L_min: 一律旁读 all, as-of 投影到特征层
  if (params.cs_neutral())
    for (const char *code : {TF_STR(NEUTRAL_RANK_MCAP), TF_STR(NEUTRAL_RANK_INDUSTRY)})
      cols.push_back(data.feature.ref(kAllLib, L_min, code));
  if (!analysis::read_request(data.config, data.feature, {.level = feat->level, .cols = std::move(cols)}, req.scope, req.spec.plan))
    return;
  // 焦点按新 A 轴 clamp: UI 槽位可能来自上一个库 (切库后库轴大小已变)
  req.spec.focus = static_cast<uint32_t>(std::clamp<int>(focus, 0, static_cast<int>(req.scope.axis.size()) - 1));
  Request(std::move(req));
}

} // namespace GUI::Features
