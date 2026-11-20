// 通达信适配: 客户端界面里通达信特有的件 — 主窗、交易栏与功能树、交易栏下的页与查一次、条数标签、弹框; 通用 Win32 操作见 Win.hpp
//   页: 交易栏下宿主 (AfxWnd100 59649) 的对话框子窗口. 页签页 (持仓 / 成交 / 撤单) 与功能树开的查询页同一形:
//     列表 (表头列名认页) + 查询键 (Button 1140 或 144) + 条数标签 (Static 1243 或 166); 开过的页留着 (隐藏), 再开即显示.
//   查一次: 切到页 (切页签 / 点功能树, 已在该页也算) 或点页上的查询类按键, 客户端即重查该页.
//     查着时页的查询键禁用, 末个应答处理完才恢复 (持仓页一查两问, 其间一直禁用); 查起在页显示之后 40~200ms.
//     查着时客户端不理切页签、点功能树; 页一显示即把焦点挪过几处控件, 早于查起.
//     故界面动作一律经 query: 动作前须空闲 (显示着的页查询键可用), 动作后等到查询键禁用过且此刻可用 (win::disables),
//     即这一问查完、焦点已定、条数标签写着这一问的条数; 随即记下其间的请求 (req::flush), 插件的入参从此可供直发照搬.
//   账户数据不经界面读 (一律直发, Tables.hpp); 界面只用于引导 (让插件发一次各功能) 与下单
#pragma once

#include "live/tdx/Win.hpp"

#include <functional>
#include <string>
#include <vector>

namespace live::tdx::ui {

inline constexpr double kQueryWait = 30; // 一问最长等多久 [s]

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
// 一页: page = 交易栏页签 (持仓 / 成交 / 撤单) 或功能树上的功能名; 认页 = 表头含 col 且不含 without (nullptr = 不看):
//   开过的页一直留着 (隐藏), 人在客户端里点开过的页也算, 故认页的列须在所有可能开着的页里唯一 (只靠 col 分不开的另给 without)
struct Open {
  const char *page, *col, *without = nullptr;
};
// act 让客户端查一次: 等认得出的那页 (见 Open) 显示着、且其查询键 act 后禁用过且此刻可用. act 前等空闲; 各至多 timeout 秒
Page query(const Open &o, const std::function<void()> &act, double timeout = kQueryWait);
Page open(const Open &o, double timeout = kQueryWait); // 页签即向宿主投其命令, 否则点功能树; 经 query 等查完

// ---------- 弹框 ----------

// 做 act, 等其后新出现、标题在 titles 内的可见对话框 (须唯一); 先冒出别的对话框 (如客户端的 MessageBox 提示) 即 Fail, 报其标题与正文;
//   正文含 again 之一的 (客户端一时没就绪) 例外: 关掉它、隔 kAgainGap 秒再做 act, 都算在 timeout 内
inline constexpr double kAgainGap = 0.3;
win::Hwnd popup(const std::vector<std::string> &titles, double timeout, const std::function<void()> &act, const std::vector<std::string> &again = {});
std::string body(const win::Hwnd &box); // 确认 / 提示框 / 系统 MessageBox 正文, 没有该控件 → 空串
void yes(const win::Hwnd &box);         // 点确认
// before 之后新出的可见对话框一律关掉 (WM_CLOSE = 取消 / 关提示), 等至多 3s; 关不完只记一笔, 不 Fail (失败的收尾里用)
void close_new(const win::Tops &before);

} // namespace live::tdx::ui
