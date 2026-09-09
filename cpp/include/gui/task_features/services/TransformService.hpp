// TransformService — Transform 的单 worker 线程编排 (骨架见 StreamService.hpp)
//
// Request: UI 参数 + 列选择 (特征列 / NeutralRank 上下文列 / _meta 门控列) + 序列焦点 + ReadScope;
//          非 L1 / 无选择 静默忽略. 换焦点也是一次新构建 (链末输出经过 CS 截面, 单资产无法离线重放)
#pragma once

#include "gui/task_features/services/StreamService.hpp"
#include "shared/Transform.hpp"

#include <vector>

namespace GUI::Features {

struct TransformRequest {
  analysis::ReadScope scope;
  Transform::Params params;
  std::vector<size_t> columns; // [特征 (+ mcap, ind_l1) (+ _meta)]
  bool has_valid = false;
  uint32_t focus = 0; // 序列快照焦点 (子轴下标, 已 clamp)
};

class TransformService : public StreamService<TransformService, TransformRequest> {
public:
  // GUI 线程: 参数快照 + 取消在跑.
  // focus = 序列快照焦点 (UI 滑条槽位 == 子轴下标, 内部按子轴大小 clamp)
  void RequestCompute(SharedData &data, const Transform::Params &params, int focus);

  static constexpr const char *kWorkerName = "TransformWorker";
  static Transform &target(SharedData &data);
  void reset(Transform &tf, TransformRequest &req);
};

} // namespace GUI::Features
