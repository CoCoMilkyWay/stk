// 通达信适配: 客户端进程里的通用 Win32 操作 (与通达信无关的一层) — 接入、窗口节点与图层、消息、对话框控件、禁用计数、按绘制文字点、输入透传
//   一切经 shim 控制通道; 窗口句柄一律是 shim 给的 "0x%08x" 串 (Hwnd), 原样回传
//   节点 / 图层即 live::WinNode / DrawItem / DrawLayer (live/Mirror.hpp, 画面复现同一份)
#pragma once

#include "live/Mirror.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace live::tdx::win {

using Hwnd = std::string;
// 节点记录 (enum / tree / find / rect): 先序, depth 定父子, 兄弟序即 z 序 (先 = 上层); rect 为屏幕坐标; parent: 顶层为 0x00000000
using Node = live::WinNode;
// 图层记录 (detail / gdidump / gdiwatch): 一窗口一份, off = 客户区左上相对窗口左上;
//   项为客户区坐标, 后出的在上; v 在绘制层为色 0x00BBGGRR, 在 detail 为 st 位
using Item = live::DrawItem;
using Layer = live::DrawLayer;

inline constexpr unsigned kWmVscroll = 0x115, kWmCommand = 0x111, kWmClose = 0x10;
inline constexpr int kSbPagedown = 3, kSbTop = 6;

Layer layer_of(const nlohmann::json &j);
std::pair<int, int> center(const Item &it); // 项的中心点 (向下取整)

// 接入客户端: shim 不在线即编译 + 注入; 再开 noraise 不抬窗 (重扫模块, 补钩 shim 注入后才加载的 DLL)、
// gdi 绘制层 (首次装入口钩子并重绘全部窗口)、detours 修补客户端里旧版 Detours 的坏跳板 (不修则其 MessageBoxA 一弹即崩).
// 启动时与客户端重启后各一次
void attach();

// ---------- 窗口 ----------

std::vector<Node> enum_(const Hwnd &target = ""); // 只展开一层; 空 = 本进程全部顶层窗口
std::vector<Node> tree(const Hwnd &target = "");  // 递归展开; 空 = 本进程全部
// 本进程全部窗口里类名为 cls (id / title 给了才比) 的各节点, 先序; 不读别的节点的标题、不经界面线程, 比整棵 tree 省得多
std::vector<Node> find(const std::string &cls, std::optional<long> id = std::nullopt, const std::string &title = "");
Node node(const Hwnd &h); // 一个窗口的节点记录 (窗口已销毁: 类名为空、不可见)
using Tops = std::map<Hwnd, Node>;
Tops tops(); // 可见顶层窗口
// h 被禁用 (EnableWindow FALSE) 的累计次数: 禁用一闪即过也记得住. 头一回问到某线程的窗口起, 该线程上的才记
long disables(const Hwnd &h);
int64_t treegen(); // 窗口树代数: 本进程窗口有建 / 毁 / 显隐 / 挪动 / 改标题即 +1

// ---------- 绘制与控件 ----------

std::vector<Layer> gdidump(const Hwnd &target = ""); // 绘制层 (空 = 全部窗口)
Layer detail(const Hwnd &h, double timeout = 5);     // 标准控件的子项 (列表行 / 格、表头、树项、菜单项、勾选态)

// ---------- 消息与输入 ----------

void post(const Hwnd &h, unsigned msg, long wp, const std::string &lp = "0");
long send(const Hwnd &h, unsigned msg, long wp, long lp); // → 返回值
std::string title(const Hwnd &h);
std::string text(const Hwnd &h, double timeout = 5);
void settext(const Hwnd &h, const std::string &s);
unsigned regmsg(const char *name);
void clickat(const Hwnd &h, int x, int y);
// 输入透传 (画面复现页): 屏幕坐标的鼠标消息 / Win32 虚拟键 / Unicode 码点, 一律投递 (不动真实光标与键盘)
void mouse(const char *kind, int x, int y, unsigned mk = 0, int delta = 0);
void key(unsigned vk);
void ch(unsigned cp);

// ---------- 对话框控件 ----------

// 对话框 h 的直接子控件, 按 {名: 控件 id} 各取唯一一个 → {名: hwnd}
std::map<std::string, Hwnd> ctls(const Hwnd &h, const std::vector<std::pair<std::string, long>> &ids);
// 点对话框 dlg 上 id 为 cid 的按钮: 向 dlg 投 WM_COMMAND(cid, 按钮)
void command(const Hwnd &dlg, long cid);
// 在类名为 cls (title 非空才比) 的可见窗口的绘制层里找文字恰为 text 的项, 向该窗口投递其中心点的左键单击
// (自绘控件谁画谁处理点击); 须唯一命中. 一处没有 = 窗口正重绘 (绘制层刚清), 等一会儿画回来
void click_text(const std::string &cls, const std::string &title, const std::string &text);

// ---------- 文字 ----------

std::string bare(const std::string &s);   // 去掉全部空白 (各表与标题里名称的空格数不一, 如「鲁  泰Ａ」「鲁 泰Ａ」)
std::string squash(const std::string &s); // 空白段并成一个空格、去头尾

} // namespace live::tdx::win
