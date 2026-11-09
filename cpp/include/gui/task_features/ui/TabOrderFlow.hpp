// TabOrderFlow - OrderFlow Visualization Tab
// Lifecycle:
//   - RenderTabOrderFlow: 首帧启动 OrderFlowService worker (幂等)
//   - 切 tab 不停 worker (流式继续, 回来即全); 切出任务由 service 析构 join
#pragma once

struct SharedData;

namespace GUI::Features {

class OrderFlowService;
struct Preview;

// Render OrderFlow tab - starts service on first call (pv: legend Cat2 标注 / flag 轴缩放, 与 Table 页同口径)
void RenderTabOrderFlow(OrderFlowService *service, SharedData &data, const Preview &pv);

} // namespace GUI::Features
