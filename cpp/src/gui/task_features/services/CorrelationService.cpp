#include "gui/task_features/services/CorrelationService.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>

namespace GUI::Features {

// ============================================================================
// CorrelationService (矩阵)
// ============================================================================

bool MakeCorrelationRequest(SharedData &data, const std::vector<ColRef> &in, CorrelationRequest &req) {
  req = CorrelationRequest{};
  std::vector<ColRef> cols;
  for (const ColRef &c : in) {
    if (data.feature.meta(c).data_type == FeatureDataType::META)
      continue; // ts_valid 自身不进矩阵 (它是门控不是特征)
    cols.push_back(c);
  }
  std::sort(cols.begin(), cols.end());
  cols.erase(std::unique(cols.begin(), cols.end()), cols.end());
  if (cols.size() < 2)
    return false; // 可算的列不足 (如 L_sec 只落 ts_valid / cs_valid)

  size_t lvl = L_1day; // 网格 = 最细层
  for (const ColRef &c : cols)
    lvl = std::min<size_t>(lvl, c.level);
  return reader::read_request(data.feature, {.level = lvl, .cols = std::move(cols)}, req.scope, req.spec.plan);
}

void CorrelationService::RequestCompute(SharedData &data, const std::vector<ColRef> &cols) {
  CorrelationRequest req;
  if (MakeCorrelationRequest(data, cols, req))
    Request(std::move(req));
}

// ============================================================================
// CorrelationLagService (全矩阵 lead-lag 峭点)
// ============================================================================

void CorrelationLagService::RequestCompute(SharedData &data, const std::vector<ColRef> &cols) {
  CorrelationRequest req;
  if (MakeCorrelationRequest(data, cols, req))
    Request(std::move(req));
}

// ============================================================================
// CorrelationPairService (单对 lead-lag)
// ============================================================================

void CorrelationPairService::RequestCompute(SharedData &data, LibId main, size_t level, const ColRef &a, const ColRef &b) {
  assert(a != b && level <= std::min(a.level, b.level));
  CorrelationRequest req;
  if (reader::read_request(data.feature, {.main = main, .level = level, .cols = {a, b}}, req.scope, req.spec.plan)) // 按序: a 在前 (lag > 0 = a 领先)
    Request(std::move(req));
}

} // namespace GUI::Features
