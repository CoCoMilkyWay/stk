// 通达信适配: 客户端进程里的通用 Win32 操作 — 复用 live::WinNode / DrawItem / DrawLayer, 零重复定义
//   一切经 shim 控制通道; 窗口句柄一律是 shim 给的 "0x%08x" 串 (Hwnd), 原样回传
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
using Node = live::WinNode;
using Item = live::DrawItem;
using Layer = live::DrawLayer;

inline constexpr unsigned kWmVscroll = 0x115, kWmCommand = 0x111, kWmClose = 0x10;
inline constexpr int kSbPagedown = 3, kSbTop = 6;

Layer layer_of(const nlohmann::json &j);
std::pair<int, int> center(const Item &it);

// 接入客户端: shim 不在线即编译 + 注入; 开 noraise / gdi 绘制层 / 修旧版 Detours 坏跳板
void attach();

// ---------- 窗口 ----------

std::vector<Node> enum_(const Hwnd &target = "");
std::vector<Node> tree(const Hwnd &target = "");
std::vector<Node> find(const std::string &cls, std::optional<long> id = std::nullopt, const std::string &title = "");
Node node(const Hwnd &h);
using Tops = std::map<Hwnd, Node>;
Tops tops();
long disables(const Hwnd &h);
int64_t treegen();

// ---------- 绘制与控件 ----------

std::vector<Layer> gdidump(const Hwnd &target = "");
Layer detail(const Hwnd &h, double timeout = 5);

// ---------- 消息与输入 ----------

void post(const Hwnd &h, unsigned msg, long wp, const std::string &lp = "0");
void send(const Hwnd &h, unsigned msg, long wp, long lp);
std::string title(const Hwnd &h);
std::string text(const Hwnd &h, double timeout = 5);
void settext(const Hwnd &h, const std::string &s);
unsigned regmsg(const char *name);
void clickat(const Hwnd &h, int x, int y);
void mouse(const char *kind, int x, int y, unsigned mk = 0, int delta = 0);
void key(unsigned vk);
void ch(unsigned cp);

// ---------- 对话框控件 ----------

std::map<std::string, Hwnd> ctls(const Hwnd &h, const std::vector<std::pair<std::string, long>> &ids);
void command(const Hwnd &dlg, long cid);
void click_text(const std::string &cls, const std::string &title, const std::string &text);

// ---------- 文字 ----------

std::string bare(const std::string &s);
std::string squash(const std::string &s);

} // namespace live::tdx::win
