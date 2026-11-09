#pragma once
#include <memory>

namespace GUI::Tools {

// System 页: 静态硬件信息 + 动态负载曲线. 只负责画, 探测与采样在 misc/system_metrics (与左下 IconBar 同一份);
// 页不显示时不采样 (一次系统调用都没有)
struct SystemState {
  struct Panel; // 曲线历史 (TabSystem.cpp)
  std::unique_ptr<Panel> panel;
  bool live = false; // 页显示中 = 采样中 (左栏标 [live])
  SystemState();
  ~SystemState();
};

// 页切进: 清空曲线 (关着的这段没有采样, 留着旧数据会被误读成"刚刚发生的") 并开始采样
void ShowSystem(SystemState &s);
void DrawSystem(SystemState &s);

} // namespace GUI::Tools
