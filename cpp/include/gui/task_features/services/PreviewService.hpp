// PreviewService — Preview 的单 worker 线程编排 (骨架见 StreamService.hpp)
//
// Request: 全 L_1min 非 META 特征列 (+ ts_valid 门控列, 按 valid_type) + price 笼两列 + ReadScope; 不依赖特征选中.
// 触发: 进 Features 任务输入就绪 / 池 / 日期区间变了 / Compute 落了新池
#pragma once

#include "gui/task_features/services/StreamService.hpp"
#include "shared/Preview.hpp"

namespace GUI::Features {

class PreviewService : public StreamService<PreviewService, Preview> {
public:
  // GUI 线程: 参数快照 + 取消在跑 (全 L_1min 非 META 特征, 不看选中)
  void RequestCompute(SharedData &data);

  static constexpr const char *kName = "PreviewWorker";
  static Preview &target(SharedData &data);
};

} // namespace GUI::Features
