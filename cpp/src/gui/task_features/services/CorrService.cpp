#include "gui/task_features/services/CorrService.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>

namespace GUI::Features {

// ============================================================================
// CorrService (矩阵)
// ============================================================================

bool MakeCorrRequest(SharedData &data, const std::vector<ColRef> &in, CorrRequest &req) {
  req = CorrRequest{};
  std::vector<ColRef> cols;
  for (const ColRef &c : in) {
    if (data.feature.meta(c).data_type == FeatureDataType::META)
      continue; // ts_valid 自身不进矩阵 (它是门控不是特征)
    cols.push_back(c);
  }
  std::sort(cols.begin(), cols.end());
  cols.erase(std::unique(cols.begin(), cols.end()), cols.end());
  if (cols.size() < 2)
    return false; // 可算的列不足 (L_sec 现状: 只有 ts_valid / cs_valid)

  size_t lvl = L_day; // 网格 = 最细层
  for (const ColRef &c : cols)
    lvl = std::min<size_t>(lvl, c.level);
  const TierId main = Feature::main_tier(cols);
  req.scope = analysis::read_scope(data.config, data.feature.selection.lib_of(main));
  req.spec.plan = analysis::read_plan(data.feature, main, lvl, std::move(cols));
  return !req.scope.months.empty();
}

Corr &CorrService::target(SharedData &data) { return data.corr; }

void CorrService::RequestCompute(SharedData &data, const std::vector<ColRef> &cols) {
  CorrRequest req;
  if (MakeCorrRequest(data, cols, req))
    Request(std::move(req));
}

// ============================================================================
// CorrLagService (全矩阵 lead-lag 峭点)
// ============================================================================

CorrLag &CorrLagService::target(SharedData &data) { return data.corr_lag; }

void CorrLagService::RequestCompute(SharedData &data, const std::vector<ColRef> &cols) {
  CorrRequest req;
  if (MakeCorrRequest(data, cols, req))
    Request(std::move(req));
}

// ============================================================================
// CorrPairService (单对 lead-lag)
// ============================================================================

CorrPair &CorrPairService::target(SharedData &data) { return data.corr_pair; }

void CorrPairService::RequestCompute(SharedData &data, size_t level, const ColRef &a, const ColRef &b) {
  assert(a != b && level <= std::min(a.level, b.level));
  CorrRequest req;
  const TierId main = Feature::main_tier({a, b});
  req.scope = analysis::read_scope(data.config, data.feature.selection.lib_of(main));
  if (req.scope.months.empty())
    return;
  req.spec.plan = analysis::read_plan(data.feature, main, level, {a, b}); // 按序: a 在前 (lag > 0 = a 领先)
  Request(std::move(req));
}

} // namespace GUI::Features
