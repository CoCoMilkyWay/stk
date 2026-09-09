// DistService — Dist 的单 worker 线程编排 (骨架见 StreamService.hpp)
//
// Request: 选中特征列 (+ _meta 门控列, 按特征 valid_type) + ReadScope; 非 L1 / 无选择 静默忽略
#pragma once

#include "gui/task_features/services/StreamService.hpp"
#include "shared/Dist.hpp"

#include <vector>

namespace GUI::Features {

struct DistRequest {
  analysis::ReadScope scope;
  std::vector<size_t> columns; // [值列 (+ valid 列)]
};

class DistService : public StreamService<DistService, DistRequest> {
public:
  // GUI 线程: 参数快照 + 取消在跑
  void RequestCompute(SharedData &data);

  static constexpr const char *kWorkerName = "DistWorker";
  static Dist &target(SharedData &data);
  void reset(Dist &dist, DistRequest &req);
};

} // namespace GUI::Features
