// 通达信适配: 客户端界面里通达信特有的件 — 主窗、交易栏与功能树、条数标签、页签、查询页、弹框; 通用 Win32 操作见 Win.hpp
#pragma once

#include "live/tdx/Win.hpp"

#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace live::tdx::ui {

inline constexpr long kPageCount = 166; // 查询页列表上方条数标签的控件 id (按日查的页另有其号)

win::Hwnd main_wnd(); // 通达信主窗 (须唯一)

// ---------- 交易栏 ----------

void click_tool(const std::string &text); // 点交易栏上画着 text 的按钮 (页签「成交」「持仓」「撤单」与「刷新」)
void show_bar();                          // 交易栏收起了即点顶栏「交易」, 等功能树出来; 用交易栏的事务开头各调一次
void click_func(const std::string &name); // 点交易功能树上的 name: 树只给可见项, 滚到顶后逐页往下找, 翻不动即到底

// ---------- 条数标签 ----------

// 列表上方的「共N条」/「第1--N条」(Static): 查前清空, 客户端处理完随后那次查询的应答即写回, 作应答到了的判据
struct Count {
  win::Hwnd h;
  void clear() const;
  // 写着的条数; 清空着 (重查中) → 无; 别的字 → Fail. 客户端卡着时 gettext 一直等到它缓过来 (至多 timeout 秒)
  std::optional<int> read(double timeout = 5) const;
};

// ---------- 页签 ----------

// 交易栏页签 (「成交」「持仓」「撤单」) 的页: page = 页 (对话框), count = 页上的条数标签 (有才给)
struct Tab {
  win::Hwnd page;
  std::optional<Count> count;
};
// 切到页签 tab (已在该页即不点) 并等页出来, 好让随后的「刷新」落在这页上. 认页: 页上可见的列表 (持仓 / 成交 = 表头含某列,
// 撤单 = 委托列表控件); 头一回认出即记下 (客户端重开即重认), 认不出的页签此后只点不等; 没有条数标签即每回重找.
// 没等到页 → 无 (调用方照点「刷新」)
std::optional<Tab> open_tab(const std::string &tab);

// ---------- 查询页 ----------

// 查询页: dlg = 页 (对话框), count = 其条数标签 (客户端卡着时 enum 也读不到, 故开页时即取好)
struct Page {
  win::Hwnd dlg;
  Count count;
};
// 功能树点 func, 等表头含 col、id 为 count 的条数标签所在的查询页出来; 页一开客户端即重查 (大表要卡一阵), 故等得长
Page open_page(const std::string &func, const std::string &col, long count = kPageCount, double timeout = 180);
void query_all(const Page &p); // 点「查询全部」

// ---------- 弹框 ----------

// 做 act, 等其后新出现、标题在 titles 内的可见对话框 (须唯一); 超时报出期间冒出的新窗口及正文
win::Hwnd popup(const std::vector<std::string> &titles, double timeout, const std::function<void()> &act);
std::string body(const win::Hwnd &box); // 确认 / 提示框正文, 没有该控件 → 空串
void yes(const win::Hwnd &box);         // 点确认

} // namespace live::tdx::ui
