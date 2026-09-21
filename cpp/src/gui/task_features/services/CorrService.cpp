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
  const auto &meta = data.feature.metadata;
  const auto &meta_list = meta.features[lvl];

  req = CorrRequest{};
  req.level = lvl;
  req.meta_col = meta.col_of(lvl, "_meta");
  for (int r : rows) {
    assert(r >= 0 && r < (int)meta_list.size());
    if (meta_list[r].data_type == FeatureDataType::META)
      continue; // _meta 自身不进矩阵 (它是门控不是特征)
    req.cols.push_back(static_cast<uint32_t>(r));
  }
  std::sort(req.cols.begin(), req.cols.end());
  req.cols.erase(std::unique(req.cols.begin(), req.cols.end()), req.cols.end());
  if (req.cols.size() < 2)
    return false; // 该层可算的列不足 (L0 现状: 只有 _meta)
  for (uint32_t c : req.cols)
    req.valid_types.push_back(meta_list[c].valid_type);

  req.scope = analysis::read_scope(data.config, data.asset.items.size());
  return !req.scope.months.empty();
}

Correlation &CorrService::target(SharedData &data) { return data.corr; }

void CorrService::RequestCompute(SharedData &data, const std::vector<int> &rows) {
  CorrRequest req;
  if (MakeCorrRequest(data, rows, req))
    submit(std::move(req));
}

void CorrService::reset(Correlation &corr, CorrRequest &req) {
  corr.reset_for_build(req.level, std::move(req.cols), std::move(req.valid_types), req.meta_col,
                       std::move(req.scope.months), req.scope.uni.size());
}

// ============================================================================
// CorrLagService (全矩阵 lead-lag 峭点)
// ============================================================================

CorrLag &CorrLagService::target(SharedData &data) { return data.corr_lag; }

void CorrLagService::RequestCompute(SharedData &data, const std::vector<int> &rows) {
  CorrRequest req;
  if (MakeCorrRequest(data, rows, req))
    submit(std::move(req));
}

void CorrLagService::reset(CorrLag &lag, CorrRequest &req) {
  lag.reset_for_build(req.level, std::move(req.cols), std::move(req.valid_types), req.meta_col,
                      std::move(req.scope.months), req.scope.uni.size());
}

// ============================================================================
// CorrPairService (单对 lead-lag)
// ============================================================================

CorrPair &CorrPairService::target(SharedData &data) { return data.corr_pair; }

void CorrPairService::RequestCompute(SharedData &data, uint32_t col_a, uint32_t col_b) {
  assert(col_a != col_b);
  const auto &sel = data.feature.selection;
  const size_t lvl = static_cast<size_t>(sel.selected_level);
  const auto &meta = data.feature.metadata;
  const auto &meta_list = meta.features[lvl];
  assert(col_a < meta_list.size() && col_b < meta_list.size());

  CorrPairRequest req;
  req.level = lvl;
  req.col_a = col_a;
  req.col_b = col_b;
  req.vt_a = meta_list[col_a].valid_type;
  req.vt_b = meta_list[col_b].valid_type;
  req.meta_col = meta.col_of(lvl, "_meta");
  req.scope = analysis::read_scope(data.config, data.asset.items.size());
  if (req.scope.months.empty())
    return;
  submit(std::move(req));
}

void CorrPairService::reset(CorrPair &pair, CorrPairRequest &req) {
  pair.reset_for_build(req.level, req.col_a, req.col_b, req.vt_a, req.vt_b, req.meta_col,
                       std::move(req.scope.months), req.scope.uni.size());
}

} // namespace GUI::Features
