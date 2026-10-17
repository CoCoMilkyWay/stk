// DistService — Dist 的单 worker 线程编排 (骨架见 StreamService.hpp)
//
// Request: 选中特征列 (+ ts_valid 门控列, 按特征 valid_type) + ReadScope; 非 L_min / 无选择 静默忽略
#pragma once

#include "gui/task_features/services/StreamService.hpp"
#include "shared/Dist.hpp"

namespace GUI::Features {

class DistService : public StreamService<DistService, Dist> {
public:
  // GUI 线程: 参数快照 + 取消在跑
  void RequestCompute(SharedData &data);

  static constexpr const char *kName = "DistWorker";
  static Dist &target(SharedData &data);
};

} // namespace GUI::Features
