// TabTransform - 特征变换探索 (分批流式, 对仗 TabDist)
//
// UI 布局:
//   1. Compute | Cancel | Status (天进度) | 特征 | 完整性 (输入/输出有效率)
//   2. 变换链 (与计算顺序一致): 季节 → 平稳化 → 带通 → TS 归一 → 截面 (Tf → Method)
//      任何参数变了 = 新请求 (拖动中也刷, 按时间 gate 节流; 松手必刷)
//   3. 资产焦点滑条 (全 universe 子轴, 无上限) + ADF/KPSS 逐天通过率热力条 (悬停详情, 点选焦点)
//   4. 序列视图 (焦点资产全程, 逐批增长): 原始 (+TOD 轮廓) | 链末输出
//      快照只存焦点资产 → 换焦点 = 自动重跑构建 (与改参数同一 dirty/节流通道)
//      两图轴永远互锁 (缩放/平移同步); autofit = 两图数据并集同视野
//   5. 输出分布 (全局 + 绘制子集资产 PDF, 焦点高亮) | 单日 PSD 均值 (周期轴, 带通光标拖动即调参)
//      | 单日 ACF/PACF 均值 (Bartlett 参考带)
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
  // 待 autofit (epoch/focus 变即置位; 序列图双击也置位 → 两图并集 fit). 粘滞: 不按帧清,
  // 等该图有数据 且 BeginPlot 真成功那帧才消费 —— reset 只 +epoch 不填数据 (首次构建
  // series/lines 全空), 子窗被裁剪时 BeginPlot 失败会静默丢掉 SetNextAxesToFit, 按帧清都会漏.
  // 各图数据就绪时机不同 (series.n_days / total 样本数 / psd_n / acf_n), 各自独立记账.
  // PSD 只 fit y: x 是带通光标所在轴, 拖动 → 重算 → x refit 会与光标共振, x 固定只由用户缩放.
  bool fit_series = false;
  bool fit_pdf = false;
  bool fit_psd = false;
  bool fit_acf = false;
  uint64_t last_epoch = 0; // 数据版本 (reset/clear/每批发布 +1), 跨构建单调, 不依赖 status 转移
  int last_focus = -1;

  int focus = 0;         // 焦点槽位 == 子轴下标 (0..A_sub-1)
  int hovered_stat = -1; // 热力条悬停槽位

  // 序列两图 (原始/输出) 轴互锁: 链接值所有权在 UI (ImPlot::SetupAxisLinks),
  // 任一图缩放/平移都写回这里, 另一图跟随; autofit 时写两图数据并集
  double series_x_min = 0.0, series_x_max = 1.0;
  double series_y_min = -1.0, series_y_max = 1.0;
};

void RenderTabTransform(TransformService *service, SharedData &data, TransformUIState &ui);
void StopTabTransform(TransformService *service, SharedData &data);

} // namespace GUI::Features
