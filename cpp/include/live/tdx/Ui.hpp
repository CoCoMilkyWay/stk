// 通达信适配: 客户端界面自动化的公共件 — 界面锁、接入、窗口节点、按绘制文字点按钮、对话框控件、交易功能树与查询页
//   一切经 shim 控制通道; 窗口句柄一律是 shim 给的 "0x%08x" 串, 原样回传
#pragma once

#include "live/Broker.hpp"

#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace live::tdx::ui {

// 客户端界面自动化 (刷新 / 下单 / 两融条款 / 调仓) 一次只许一路; who = 占着的那路 (空 = 空闲).
// 同一线程可重入 (调仓整批占着, 逐单再进), who 记最外层
class Gate {
public:
  class Hold {
  public:
    Hold(Gate &g, const char *who);
    ~Hold();
    Hold(const Hold &) = delete;
    Hold &operator=(const Hold &) = delete;

  private:
    Gate &g_;
    std::string outer_;
  };
  std::string who() const;

private:
  std::recursive_mutex mu_;
  mutable std::mutex who_mu_;
  std::string who_;
};

inline constexpr unsigned kWmVscroll = 0x115, kWmCommand = 0x111;
inline constexpr int kSbPagedown = 3, kSbTop = 6;
inline constexpr int kPageCount = 166, kPageAll = 454; // 查询页: 列表上方「第1--N条」/「共N条」、「查询全部」按钮

// 节点记录 (enum / tree / find / rect)
struct Node {
  int depth = 0;
  std::string hwnd;
  long id = 0;
  std::string cls, title;
  int rect[4] = {};
  bool vis = false, en = false;
};

// 接入客户端: shim 不在线即编译 + 注入; 再开 noraise 不抬窗 (重扫模块, 补钩 shim 注入后才加载的 DLL)、
// gdi 绘制层 (首次装入口钩子并重绘全部窗口). 启动时与客户端重启后各一次
void attach();

std::vector<Node> enum_(const std::string &target = ""); // 只展开一层; 空 = 本进程全部顶层窗口
std::vector<Node> tree(const std::string &target = "", double timeout = 5);
std::vector<Node> find_wnd(const std::string &query); // class=<类名> [title=<标题>]
void post(const std::string &h, unsigned msg, long wp, const std::string &lp = "0");
void send(const std::string &h, unsigned msg, long wp, long lp);
std::string title(const std::string &h);
std::string text(const std::string &h, double timeout = 5);
void settext(const std::string &h, const std::string &s);
unsigned regmsg(const char *name);
void clickat(const std::string &h, int x, int y);

// 对话框 h 的直接子控件, 按 {名: 控件 id} 各取唯一一个 → {名: hwnd}
std::map<std::string, std::string> ctls(const std::string &h, const std::vector<std::pair<std::string, long>> &ids);
// 点对话框 dlg 上 id 为 cid 的按钮: 向 dlg 投 WM_COMMAND(cid, 按钮)
void command(const std::string &dlg, long cid);

// 等 fn() 为真即回其值; 超时 → Fail (what() 超时时才求值, 好报出当时的状态)
template <class F>
auto wait(const std::function<std::string()> &what, F &&fn, double timeout) {
  const auto end = std::chrono::steady_clock::now() + std::chrono::duration<double>(timeout);
  while (true) {
    auto r = fn();
    if (r)
      return r;
    if (std::chrono::steady_clock::now() >= end)
      fail(strf("%gs 内未等到%s", timeout, what().c_str()));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}
inline std::function<std::string()> said(std::string s) {
  return [s = std::move(s)] { return s; };
}

// 在 cls / title (非空才比) 匹配的可见窗口的绘制层里找文字恰为 text 的项, 向该窗口投递其中心点的左键单击
// (自绘控件谁画谁处理点击); 须唯一命中
void click_text(const std::string &cls, const std::string &title, const std::string &text);
void click_tool(const std::string &text); // 点交易栏上画着 text 的按钮 (页签「成交」「持仓」「撤单」与「刷新」)
void show_bar();                          // 交易栏收起了即点顶栏「交易」, 等功能树出来; 用交易栏的事务开头各调一次
void click_func(const std::string &name); // 点交易功能树上的 name: 树只给可见项, 滚到顶后逐页往下找, 翻不动即到底
// 功能树点 func, 等表头含 col 的查询页出来 → 其 hwnd; 页一开客户端即重查 (大表要卡一阵), 故等得长
std::string open_page(const std::string &func, const std::string &col, double timeout = 180);
std::string count_label(const std::string &dlg); // 查询页的条数标签 (客户端卡着时 enum 也读不到, 须事先取好)
// 条数标签上的列表条数, 重查中标签清空 → 无; 客户端卡着时 gettext 一直等到它缓过来
std::optional<int> page_count(const std::string &lbl, double timeout = 5);

std::string bare(const std::string &s);   // 去掉全部空白 (各表与标题里名称的空格数不一, 如「鲁  泰Ａ」「鲁 泰Ａ」)
std::string squash(const std::string &s); // 空白段并成一个空格、去头尾

} // namespace live::tdx::ui
