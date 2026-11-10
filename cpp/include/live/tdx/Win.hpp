// 通达信适配: 客户端进程里的通用 Win32 操作 (与通达信无关的一层; 调试台只用这层) — 接入、窗口节点与图层、消息、对话框控件、按绘制文字点
//   一切经 shim 控制通道; 窗口句柄一律是 shim 给的 "0x%08x" 串 (Hwnd), 原样回传
#pragma once

#include "nlohmann/json.hpp"

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace live::tdx::win {

using Hwnd = std::string;

inline constexpr unsigned kWmVscroll = 0x115, kWmCommand = 0x111;
inline constexpr int kSbPagedown = 3, kSbTop = 6;

// 节点记录 (enum / tree / find / rect): 先序, depth 定父子, 兄弟序即 z 序 (先 = 上层); rect 为屏幕坐标
struct Node {
  int depth = 0;
  Hwnd hwnd;
  long id = 0;
  std::string cls, title;
  int rect[4] = {};
  unsigned style = 0, exstyle = 0;
  bool vis = false, en = false;
};
Node node_of(const nlohmann::json &j);
std::vector<Node> nodes_of(const std::vector<nlohmann::json> &recs);

// 图层记录 (detail / gdidump / gdiwatch): 一窗口一份, off = 客户区左上相对窗口左上;
//   项为客户区坐标, 后出的在上; v 在绘制层为色 0x00BBGGRR, 在 detail 为 st 位
struct Item {
  int l = 0, t = 0, r = 0, b = 0;
  unsigned v = 0;
  std::optional<std::string> text;
};
struct Layer {
  Hwnd hwnd;
  int off[2] = {};
  std::vector<Item> items;
};
Layer layer_of(const nlohmann::json &j);

// 接入客户端: shim 不在线即编译 + 注入; 再开 noraise 不抬窗 (重扫模块, 补钩 shim 注入后才加载的 DLL)、
// gdi 绘制层 (首次装入口钩子并重绘全部窗口). 启动时与客户端重启后各一次
void attach();

// ---------- 窗口 ----------

std::vector<Node> enum_(const Hwnd &target = ""); // 只展开一层; 空 = 本进程全部顶层窗口
std::vector<Node> tree(const Hwnd &target = "", double timeout = 5);
std::vector<Node> find_wnd(const std::string &query); // class=<类名> [title=<标题>]
using Tops = std::map<Hwnd, Node>;
Tops tops();                                        // 可见顶层窗口
std::vector<Layer> layers(const Hwnd &target = ""); // 绘制层; 空 = 全部窗口

// ---------- 消息 ----------

void post(const Hwnd &h, unsigned msg, long wp, const std::string &lp = "0");
void send(const Hwnd &h, unsigned msg, long wp, long lp);
std::string title(const Hwnd &h);
std::string text(const Hwnd &h, double timeout = 5);
void settext(const Hwnd &h, const std::string &s);
unsigned regmsg(const char *name);
void clickat(const Hwnd &h, int x, int y);

// ---------- 控件 ----------

// 对话框 h 的直接子控件, 按 {名: 控件 id} 各取唯一一个 → {名: hwnd}
std::map<std::string, Hwnd> ctls(const Hwnd &h, const std::vector<std::pair<std::string, long>> &ids);
// 点对话框 dlg 上 id 为 cid 的按钮: 向 dlg 投 WM_COMMAND(cid, 按钮)
void command(const Hwnd &dlg, long cid);
// 在 cls / title (非空才比) 匹配的可见窗口的绘制层里找文字恰为 text 的项, 向该窗口投递其中心点的左键单击
// (自绘控件谁画谁处理点击); 须唯一命中
void click_text(const std::string &cls, const std::string &title, const std::string &text);

// ---------- 文字 ----------

std::string bare(const std::string &s);   // 去掉全部空白 (各表与标题里名称的空格数不一, 如「鲁  泰Ａ」「鲁 泰Ａ」)
std::string squash(const std::string &s); // 空白段并成一个空格、去头尾

} // namespace live::tdx::win
