// TransformService — Transform 的单 worker 线程编排 (骨架见 StreamService.hpp)
//
// Request: UI 参数 + 列选择 (特征列 / NeutralRank 上下文列 (+ ts_valid 门控列)) + 序列焦点 + ReadScope;
//          非 L_1min / 无选择 静默忽略. 换焦点也是一次新构建 (链末输出经过 CS 截面, 单资产无法离线重放)
#pragma once

#include "gui/task_features/services/StreamService.hpp"
#include "shared/Transform.hpp"

namespace GUI::Features {

class TransformService : public StreamService<TransformService, Transform> {
public:
  // GUI 线程: 参数快照 + 取消在跑.
  // focus = 序列快照焦点 (UI 滑条槽位 == 池轴下标, 内部按池轴大小 clamp)
  void RequestCompute(SharedData &data, const Transform::Params &params, int focus);

  static constexpr const char *kName = "TransformWorker";
  static Transform &target(SharedData &data);
};

} // namespace GUI::Features
