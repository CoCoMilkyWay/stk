// PreviewService — FeaturePreview 的单 worker 线程编排 (骨架见 StreamService.hpp)
//
// Request: 全 L1 非 META 特征列 + valid_type 表 + _meta 门控列 + ReadScope; 不依赖特征选中.
// 触发: 进 Features 任务输入就绪 / universe / 日期区间变了 / Compute 落了新库
#pragma once

#include "gui/task_features/services/StreamService.hpp"
#include "shared/FeaturePreview.hpp"

#include <vector>

namespace GUI::Features {

struct PreviewRequest {
  analysis::ReadScope scope;
  std::vector<size_t> feat_cols;          // 预览特征列 (metadata 下标, 升序)
  std::vector<L2::ValidType> valid_types; // 与 feat_cols 平行
  size_t meta_col = 0;                    // "_meta" 门控列下标
  size_t n_features = 0;                  // 该层特征总数 (cells 尺寸)
};

class PreviewService : public StreamService<PreviewService, PreviewRequest> {
public:
  // GUI 线程: 参数快照 + 取消在跑 (全 L1 非 META 特征, 不看选中)
  void RequestCompute(SharedData &data);

  static constexpr const char *kWorkerName = "PreviewWorker";
  static FeaturePreview &target(SharedData &data);
  void reset(FeaturePreview &pv, PreviewRequest &req);
};

} // namespace GUI::Features
