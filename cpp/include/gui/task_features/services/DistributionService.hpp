// DistributionService — Distribution 的单 worker 线程编排 (骨架见 StreamService.hpp)
//
// Request: 选中特征列 (+ ts_valid 门控列, 按特征 valid_type) + ReadScope; 非 L_1min / 无选择 静默忽略
#pragma once

#include "gui/task_features/services/Distribution.hpp"
#include "gui/task_features/services/StreamService.hpp"

struct SharedData;

namespace GUI::Features {

class DistributionService : public StreamService<DistributionService, Distribution> {
public:
  // GUI 线程: 参数快照 + 取消在跑
  void RequestCompute(SharedData &data);

  static constexpr const char *kName = "DistributionWorker";
};

} // namespace GUI::Features
