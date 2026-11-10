// 通达信调试台各块共用的模型 (页面见 live/tdx/Console.hpp)
//   Core: 后台线程与页面共用 (持 mu): 发布的窗口树 / 子项 / 绘制层快照、页面开关、队列、各输出
//   View: 页面独有 (GUI 线程): 每帧从 Core 取的快照、输入框与勾选、缩放、选中 / 悬停
//   选中 / 悬停的对象 Pick: 窗口, 或绘制元素 (自绘窗口绘制层里的一段文字, 当作该窗口的子节点:
//     hwnd = 窗口#客户区左,上 (重画不变), host = 所画窗口, cls = "绘", rect 为屏幕坐标)
#pragma once

#include "live/tdx/Win.hpp"

#include "imgui.h"
#include "nlohmann/json.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace live::tdx::console {

using Clock = std::chrono::steady_clock;
using nlohmann::json;
using ojson = nlohmann::ordered_json;
using namespace std::chrono_literals;

inline constexpr auto kActive = 1s;     // 页面多久没画即停转
inline constexpr auto kSoon = 120ms;    // 交互后隔多久补拉一次树
inline constexpr size_t kRecCap = 2000; // 录制页留的条数
inline constexpr auto kRecGap = 250ms;  // 录制一轮的间隔

// detail 子项 st 位 (同 shim.c ST_*)
inline constexpr unsigned kStSel = 1, kStCheck = 2, kStKids = 4, kStOpen = 8, kStGray = 16, kStSep = 32, kStSub = 64, kStRow = 128;

// 一棵窗口树 + 派生: hwnd → 下标、父子 (兄弟序即 z 序, 先 = 上层)
struct Tree {
  std::vector<win::Node> nodes;
  std::unordered_map<std::string, size_t> at;
  std::vector<std::vector<size_t>> kids;
  std::vector<size_t> roots;
  std::string err; // 拉树失败的原因 (nodes 空)
  const win::Node *find(const std::string &hwnd) const;
};
std::shared_ptr<const Tree> make_tree(std::vector<win::Node> nodes, std::string err = "");

using Layers = std::map<std::string, win::Layer>; // hwnd → detail 子项
struct Gdi {
  win::Layer layer;
  Clock::time_point at; // 到达时刻
};
using Gdis = std::map<std::string, std::shared_ptr<const Gdi>>; // hwnd → 绘制层

struct Pick : win::Node {
  std::string host; // 非空 = 绘制元素
};

struct SpyRec {
  std::string msg, hwnd, cls, wparam, lparam;
};

struct RecRow {
  std::string kind, line;
};

struct Core {
  std::mutex mu;
  std::condition_variable cv;
  bool stop = false;
  Clock::time_point seen{}; // 页面上次画
  bool active(Clock::time_point now) const { return now - seen < kActive; }

  // ---------- 发布 ----------
  std::shared_ptr<const Tree> tree = make_tree({});
  std::shared_ptr<const Layers> detail = std::make_shared<Layers>();
  std::shared_ptr<const Gdis> gdi = std::make_shared<Gdis>();
  std::optional<long> pid; // ping; 无 = 离线
  bool noraise = false;
  long blocked = 0;
  std::string status; // 接入 / 状态查询的上一条错误

  // ---------- 页面开关 ----------
  bool auto_upd = true;
  int fps = 2;
  bool spy_auto = false;

  // ---------- 触发与队列 ----------
  bool want_tree = true;                  // 推送 / 手动: 拉一次树
  std::optional<Clock::time_point> soon;  // 交互后: 到点拉一次树并查不抬窗
  std::deque<std::function<void()>> acts; // 动作 (按钮), 依次跑
  struct Input {
    std::string line;
    int drop = 0; // 0 不可丢 / 1 拖拽移动 / 2 滚轮: 同种上一条没回来就丢
  };
  std::deque<Input> inputs; // 投给客户端的输入, 串行保序
  bool dropping[3] = {};

  // ---------- 输出 ----------
  std::string sel_out, msg_out, spy_st;
  std::vector<SpyRec> spy;

  // ---------- 录制 ----------
  bool rec_on = false, rec_halt = false;
  std::string rec_stamp, rec_file, rec_st;
  long rec_n = 0;
  Clock::time_point rec_t0{};
  std::deque<RecRow> rec_tail;
  uint64_t rec_ver = 0;
  std::string req_st = "先开始录制，再打开闪电下单窗口并选中其顶层窗口";

  // ---------- 探内存 ----------
  bool probing = false;
  std::string probe_out;
  uint64_t probe_ver = 0;
};

struct View {
  // 每帧快照
  std::shared_ptr<const Tree> tree;
  std::shared_ptr<const Layers> detail;
  std::shared_ptr<const Gdis> gdi;
  std::optional<long> pid;
  bool noraise = false;
  long blocked = 0;
  std::string status, sel_out, msg_out, spy_st, req_st;
  std::vector<SpyRec> spy;

  // 页头 / 布局图
  bool auto_upd = true, interact = true, gdi_on = true, only_vis = false, hide_zero = true;
  int fps = 2;
  std::string filter;
  std::optional<float> scale; // 无 = 待适配 (取刚好放下)
  bool reset = false;         // 复位: 下一帧滚回左上
  float top_h = 0;            // 上栏高, 0 = 待定
  int held = 0;               // 交互中按住的键位 (MK_LBUTTON 1 / MK_RBUTTON 2)
  struct Pan {
    ImVec2 at, scroll;
    bool moved = false;
  };
  std::optional<Pan> pan;

  // 选中 / 悬停 (悬停: 上一帧定下的, 本帧各处重新报)
  std::optional<Pick> sel;
  std::string hover, hover_next;
  bool hover_map = false, hover_map_next = false; // 悬停来自布局图 (列表随之滚到该行)
  std::set<std::string> collapsed;

  // 工具
  std::string setval, m_target, m_msg = "0x0111", m_wp = "0", m_lp = "0", spy_filter = "msg=0x0111";
  bool spy_auto = false;

  // 录制
  bool show_msg = true, show_req = true, show_win = true, show_gdi = false;
  std::string excl, mark, req_off;
  uint64_t rec_ver = ~0ull;
  std::deque<RecRow> rec_tail;
  bool rec_on = false;
  std::string rec_line;

  // 探内存
  std::string words;
  uint64_t probe_ver = ~0ull;
  std::string probe_out;
  bool probing = false;
};

// ---------- 后台 ----------
void act(Core &c, std::function<void()> fn);                     // 排一个动作
void input(Core &c, std::string line, int drop = 0);             // 排一条输入
std::string shown(const std::function<std::vector<json>()> &fn); // 跑一条 shim 命令 → 输出框文字 (记录或 {err})
void noraise(Core &c, const std::string &arg);                   // 不抬窗 (arg 空 = 只读状态)
void spy(Core &c, const std::string &action, const std::string &filter);

// ---------- 页面公用 ----------
std::string trim(std::string_view s);            // 去头尾空白
std::string cut(const std::string &s, size_t n); // 前 n 个码点
std::string lower(std::string s);
bool std_class(const std::string &cls);                    // 按类画的标准控件 (表外即自绘窗口)
bool needs_detail(const win::Node &n);                      // 可见、矩形非空、该类要拉 detail 子项
const win::Layer *gdi_of(const View &v, const win::Node &n); // 绘制层开且该窗口有内容
std::vector<Pick> elems_of(const View &v, const win::Node &n);
std::optional<Pick> pick_of(const View &v, const std::string &id);
void select(View &v, const Pick &p);
void act_sel(Core &c, View &v, const std::string &kind, const std::string &arg = ""); // 对选中的: 窗口经 shim, 绘制元素就地

// 过滤: text 过滤词; match 词中; keep 列表里留不留
struct Filter {
  std::string text;
  bool only_vis, hide_zero;
  bool match(const win::Node &n) const;
  bool keep(const win::Node &n) const;
};
Filter filter_of(const View &v);

bool input_text(const char *id, std::string &s, const char *hint = nullptr, ImGuiInputTextFlags flags = 0);
void out_box(const char *id, const std::string &s, float lines); // 只读多行 (可选中复制)

// ---------- 各块 ----------
void draw_map(Core &c, View &v);
void draw_list(Core &c, View &v);
void draw_tools(Core &c, View &v);
void draw_rec(const std::shared_ptr<Core> &c, View &v);   // 录制线程跑完为止 (不随页面停)
void draw_probe(const std::shared_ptr<Core> &c, View &v); // 探内存线程同上

} // namespace live::tdx::console
