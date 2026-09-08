// TabTransform - 特征变换探索 (分批流式, 对仗 TabDist)
//
// UI 布局:
//   1. Compute | Cancel | Status (天进度) | 特征 | 完整性 (输入/输出有效率)
//   2. 变换链 (与计算顺序一致): 季节 → 平稳化 → 带通 → TS 归一 → 截面 (Tf → Method)
//      任何参数变了 = 新请求 (拖动中也刷, 按时间 gate 节流; 松手必刷)
//   3. 统计子集焦点滑条 + ADF/KPSS 逐天通过率热力条 (统计子集, 悬停详情, 点选焦点)
//   4. 序列视图 (最近一批): 原始 (+TOD 轮廓) | 链末输出
//   5. 输出分布 (全局 + 绘制子集资产 PDF, 焦点高亮) | 单日 PSD 均值 (周期轴, 带通光标拖动即调参)
//
// Threading: UI 渲染帧内持 transform.mutex; 计算在 TransformService 单 worker (批末发布快照)
#pragma once

#include "shared/Transform.hpp"

struct SharedData;

namespace GUI::Features {

class TransformService;

struct TransformUIState {
  Transform::Params params;    // UI 编辑中的参数 (请求时快照)
  bool dirty = false;          // 参数改了但还没发请求
  float last_req_time = -1.0f; // 上次发请求的 ImGui 时间 (秒), 拖动节流用
  bool need_autofit = false;   // epoch/focus 变那帧 autofit 一次, 流式逐批不 fit
  uint64_t last_epoch = 0;     // 数据版本 (reset/clear/每批发布 +1), 跨构建单调, 不依赖 status 转移
  int last_focus = -1;

  int focus = 0;         // 统计子集焦点槽位 (0..n_stat-1)
  int hovered_stat = -1; // 热力条悬停槽位
};

void RenderTabTransform(TransformService *service, SharedData &data, TransformUIState &ui);
void StopTabTransform(TransformService *service, SharedData &data);

} // namespace GUI::Features
