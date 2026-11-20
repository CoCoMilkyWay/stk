// Tab Client - 客户端窗口复现页: 实时渲染客户端窗口树 + GDI 绘制层 (色块 / 文字) + 标准控件子项, 支持检查与鼠键透传
//   完全券商无关, 只经 live::Mirror 驱动 (Broker::mirror); 后台线程收 gdiwatch 增量流与窗口树变化, 发布不可变 MirrorFrame 给 GUI 60fps 绘制
#pragma once

#include "gui/task_live/LiveHub.hpp"

#include <memory>
#include <string>

namespace GUI::Live {

struct ClientState {
  struct Impl;
  std::shared_ptr<Impl> impl;

  bool live = true;        // 实时跟随 (gdiwatch + 窗口树变更)
  bool interact = false;   // 鼠键透传到客户端 (默认关, 防误点)
  bool show_gdi = true;    // GDI 色块与文字
  bool show_detail = true; // 标准控件子项 (列表/表头/树)
  bool show_box = false;   // 叠加窗口边框
  bool fit = true;         // 画布自适应缩放 (关 = 1:1)
  bool only_vis = true;    // 右栏窗口树只列可见窗口
  bool inspector = true;   // 显示右栏检查器
  std::string top_hwnd;    // 空 = 全顶层叠画; 否则只画该顶层窗口
  std::string sel_hwnd;    // 选中的窗口 (高亮并查其 GDI 图元)
  int sel_item = -1;       // 检查器里悬停的 GDI 图元下标
  char filter[64] = {};    // 窗口树过滤词
  float split = 0.72f;     // 左右分栏比

  ClientState();
  ~ClientState();
  ClientState(ClientState &&) noexcept;
  ClientState &operator=(ClientState &&) noexcept;
  void stop();
};

void RenderTabClient(LiveHub &hub, ClientState &st);

} // namespace GUI::Live
