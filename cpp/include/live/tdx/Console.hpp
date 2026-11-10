// 通达信适配的调试台 (Live 的 Client 页, 经 Broker::console 画): 看清、摸清客户端界面, 给自动化找控件 / 消息 / 内存表
//   页头: shim 在线状态 (离线可点「接入」注入) / 不抬窗开关与拦截次数 / 刷新窗口树 / ping
//   上栏 布局图: 按屏幕坐标、按 Win32 z 序还原窗口树 (标准控件按类画, 自绘窗口按 shim 绘制层画出真实文字与色块);
//     交互开时鼠标 / 键盘 / 滚轮经 shim 投给客户端 (只给屏幕坐标, 目标窗口由 shim 定), 关时点选; Ctrl+滚轮缩放, 中键拖拽平移
//   下栏 工具: 对选中控件读文本 / 点击 / 闪烁 / 取矩形 / 设文本, 原始消息 post / send, Spy 抓消息 (可回放 / 定位)
//        列表: 窗口树 (含自绘窗口里的绘制元素), 可折叠、与布局图同一悬停 / 选中
//        录制: 消息 / 交易请求 / 窗口树 / 绘制层的变化逐条落 output/run/live/tdx/rec/<时刻>.jsonl, 页面留尾巴; 可接入闪电下单窗口的请求对象
//        探内存: 列出堆里全部明文表 (首行 0|…|), 或按文本 (GBK / UTF-16LE) 找处
//   后台线程 (拉树 / 推送 / 输入 / 动作) 只在页面近 1s 内画过时转; 录制与探内存跑完为止. 线程各持核心的 shared_ptr, 析构只置停
#pragma once

#include <memory>

namespace live::tdx::console {

struct Core;
struct View;

class Console {
public:
  Console();
  ~Console();
  Console(const Console &) = delete;
  Console &operator=(const Console &) = delete;

  void draw(); // GUI 线程每帧

private:
  std::shared_ptr<Core> c_;
  std::unique_ptr<View> v_;
};

} // namespace live::tdx::console
