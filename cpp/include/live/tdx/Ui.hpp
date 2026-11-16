// 通达信适配: 客户端界面里通达信特有的件 — 界面锁、主窗、交易栏与功能树、查询页、弹框; 通用 Win32 操作见 Win.hpp
#pragma once

#include "live/tdx/Win.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
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
    // 有别路在等即先让它占、做完再接着占 (刷新一轮在两步之间让下单等先走); 只许最外层调.
    // 让出期间界面任人改, 接着的一步须自己切到要的页
    void yield();

  private:
    Gate &g_;
    std::string outer_;
  };
  std::string who() const;

private:
  std::recursive_mutex mu_;
  mutable std::mutex who_mu_;
  std::string who_;
  std::atomic<int> waiting_{0};      // 在等的路数
  std::atomic<uint64_t> granted_{0}; // 等过才占到的次数
};

inline constexpr int kPageCount = 166, kPageAll = 454; // 查询页: 列表上方「第1--N条」/「共N条」、「查询全部」按钮
inline constexpr long kMsgText = 7013, kMsgYes = 7015; // 确认框 / 提示框: 正文 Static、确认按钮

win::Hwnd main_wnd(); // 通达信主窗 (须唯一)

// ---------- 交易栏 ----------

void click_tool(const std::string &text); // 点交易栏上画着 text 的按钮 (页签「成交」「持仓」「撤单」与「刷新」)
void show_bar();                          // 交易栏收起了即点顶栏「交易」, 等功能树出来; 用交易栏的事务开头各调一次
void click_func(const std::string &name); // 点交易功能树上的 name: 树只给可见项, 滚到顶后逐页往下找, 翻不动即到底

// 交易栏页签 (「成交」「持仓」「撤单」) 的页: page = 页 (对话框), count = 页上「共N条」/「第1--N条」的条数标签 (有才给)
struct Tab {
  win::Hwnd page;
  std::optional<win::Hwnd> count;
};
// 切到页签 tab (已在该页即不点) 并等页出来, 好让随后的「刷新」落在这页上. 认页: 页上可见的列表 (持仓 / 成交 = 表头含某列,
// 撤单 = 委托列表控件); 头一回认出即记下 (页 / 条数标签, 客户端重开即重认), 认不出的页签此后只点不等.
// 没等到页 → 无 (调用方照点「刷新」)
std::optional<Tab> open_tab(const std::string &tab);

// ---------- 查询页 ----------

// 功能树点 func, 等表头含 col 的查询页出来 → 其 hwnd; 页一开客户端即重查 (大表要卡一阵), 故等得长.
// count = 条数标签的控件 id (各查询页多为 kPageCount, 按日查的页另有其号)
win::Hwnd open_page(const std::string &func, const std::string &col, long count = kPageCount, double timeout = 180);
win::Hwnd count_label(const win::Hwnd &dlg, long count = kPageCount); // 查询页的条数标签 (客户端卡着时 enum 也读不到, 须事先取好)
// 条数标签上的列表条数, 重查中标签清空 → 无; 客户端卡着时 gettext 一直等到它缓过来
std::optional<int> page_count(const win::Hwnd &lbl, double timeout = 5);
std::optional<int> count_in(std::string_view text); // 「第1--N条」/「共N条」→ N; 不是这形 → 无

// ---------- 弹框 ----------

// 做 act, 等其后新出现、标题在 titles 内的可见对话框 (须唯一); 超时报出期间冒出的新窗口及正文
win::Hwnd popup(const std::vector<std::string> &titles, double timeout, const std::function<void()> &act);
std::string body(const win::Hwnd &box); // 确认 / 提示框正文, 没有该控件 → 空串
void yes(const win::Hwnd &box);         // 点确认

} // namespace live::tdx::ui
