#include "gui/task_features/services/CorrService.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>

namespace GUI::Features {

// ============================================================================
// CorrService (矩阵)
// ============================================================================

bool MakeCorrRequest(SharedData &data, const std::vector<int> &rows, CorrRequest &req) {
  const auto &sel = data.feature.selection;
  const size_t lvl = static_cast<size_t>(sel.selected_level);
  if (lvl == L_day)
    return false; // 日内 (lead-lag) 相关按日切片, L_day 一日一行无日内轴
  const auto &meta_list = data.feature.meta().features[lvl];

  req = CorrRequest{};
  std::vector<size_t> cols;
  for (int r : rows) {
    assert(r >= 0 && r < (int)meta_list.size());
    if (meta_list[r].data_type == FeatureDataType::META)
      continue; // ts_valid 自身不进矩阵 (它是门控不是特征)
    cols.push_back(static_cast<size_t>(r));
  }
  std::sort(cols.begin(), cols.end());
  cols.erase(std::unique(cols.begin(), cols.end()), cols.end());
  if (cols.size() < 2)
    return false; // 该层可算的列不足 (L_sec 现状: 只有 ts_valid / cs_valid)

  req.spec.level = lvl;
  req.spec.cols = analysis::column_set(data.feature, lvl, std::move(cols));
  req.scope = analysis::read_scope(data.config, sel.lib());
  return !req.scope.months.empty();
}

Corr &CorrService::target(SharedData &data) { return data.corr; }

void CorrService::RequestCompute(SharedData &data, const std::vector<int> &rows) {
  CorrRequest req;
  if (MakeCorrRequest(data, rows, req))
    Request(std::move(req));
}

// ============================================================================
// CorrLagService (全矩阵 lead-lag 峭点)
// ============================================================================

CorrLag &CorrLagService::target(SharedData &data) { return data.corr_lag; }

void CorrLagService::RequestCompute(SharedData &data, const std::vector<int> &rows) {
  CorrRequest req;
  if (MakeCorrRequest(data, rows, req))
    Request(std::move(req));
}

// ============================================================================
// CorrPairService (单对 lead-lag)
// ============================================================================

CorrPair &CorrPairService::target(SharedData &data) { return data.corr_pair; }

void CorrPairService::RequestCompute(SharedData &data, uint32_t col_a, uint32_t col_b) {
  assert(col_a != col_b);
  const auto &sel = data.feature.selection;
  const size_t lvl = static_cast<size_t>(sel.selected_level);
  if (lvl == L_day)
    return; // 同 MakeCorrRequest: L_day 无日内轴
  assert(col_a < data.feature.meta().features[lvl].size() && col_b < data.feature.meta().features[lvl].size());

  CorrRequest req;
  req.spec.level = lvl;
  req.spec.cols = analysis::column_set(data.feature, lvl, {col_a, col_b}); // 按序: a 在前 (lag > 0 = a 领先)
  req.scope = analysis::read_scope(data.config, sel.lib());
  if (req.scope.months.empty())
    return;
  Request(std::move(req));
}

} // namespace GUI::Features
