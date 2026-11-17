// 通达信适配: 客户端界面里通达信特有的件 — 主窗、交易栏与功能树、交易栏下的页与查一次、条数标签、弹框; 通用 Win32 操作见 Win.hpp
//   页: 交易栏下宿主 (AfxWnd100 59649) 的对话框子窗口. 页签页 (持仓 / 成交 / 撤单) 与功能树开的查询页同一形:
//     列表 (表头列名认页) + 查询键 (Button 1140 或 144) + 条数标签 (Static 1243 或 166); 开过的页留着 (隐藏), 再开即显示.
//   查一次: 切到页 (切页签 / 点功能树, 已在该页也算) 或点页上的查询类按键, 客户端即重查该页.
//     查着时页的查询键禁用, 末个应答处理完才恢复 (持仓页一查两问, 其间一直禁用); 查起在页显示之后 40~200ms.
//     查着时客户端不理切页签、点功能树; 页一显示即把焦点挪过几处控件, 早于查起.
//     故界面动作一律经 query / press: 动作前须空闲 (显示着的页查询键可用), 动作后等到查询键禁用过且此刻可用 (win::disables),
//     即这一问查完、焦点已定、应答已在内存里、条数标签写着这一问的条数.
#pragma once

#include "live/tdx/Win.hpp"

#include <functional>
#include <string>
#include <vector>

namespace live::tdx::ui {

inline constexpr double kQueryWait = 30; // 一问最长等多久 [s] (查询全部另给)

win::Hwnd main_wnd(); // 通达信主窗 (须唯一)

// ---------- 交易栏 ----------

void show_bar();                          // 交易栏收起了即点顶栏「交易」, 等功能树出来; 用交易栏的事务开头各调一次
void click_func(const std::string &name); // 点交易功能树上的 name: 树只给可见项, 滚到顶后逐页往下找, 翻不动即到底

// ---------- 页 ----------

// 列表上方的「共N条」/「第1--N条」(Static)
struct Count {
  win::Hwnd h;
  int read() const; // 写着的条数; 别的字 → Fail
};
struct Page {
  win::Hwnd dlg, key; // 页 / 查询键
  Count count;
};
// act 让客户端查一次: 等表头含 col 的页显示着、且其查询键 act 后禁用过且此刻可用. act 前等空闲; 各至多 timeout 秒.
// tick: 等查完期间约每秒调一次 (报进度)
Page query(const std::string &col, const std::function<void()> &act, double timeout = kQueryWait, const std::function<void()> &tick = {});
Page open_tab(const std::string &tab);                                                       // 切页签「持仓」「成交」「撤单」
Page open_page(const std::string &func, const std::string &col, double timeout = kQueryWait); // 功能树点 func, 认页的列 col
// 点页 p 上 id 为 key 的按键 (查询 / 查询全部), 等这一问查完 (同 query)
void press(const Page &p, long key, double timeout = kQueryWait, const std::function<void()> &tick = {});

// ---------- 弹框 ----------

// 做 act, 等其后新出现、标题在 titles 内的可见对话框 (须唯一); 超时报出期间冒出的新窗口及正文
win::Hwnd popup(const std::vector<std::string> &titles, double timeout, const std::function<void()> &act);
std::string body(const win::Hwnd &box); // 确认 / 提示框正文, 没有该控件 → 空串
void yes(const win::Hwnd &box);         // 点确认

} // namespace live::tdx::ui
