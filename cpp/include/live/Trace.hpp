// 实盘: 交互日志 — Broker 的流程 (刷新 / 下单 / 拉取) 与适配里同客户端的交互 (shim 命令 / 请求层 / 外部进程), 一行一事件落
//   output/run/live/trace/<启动时刻>.jsonl (每次启动一个文件, 只留最近 kKeep 个). 只记事件与定位所需的字段, 供事后据此还原一次失败.
//   行: {t 本地时刻 HH:MM:SS.mmm, th 线程号, at 段路径 (外/内), ev 种类, 其余为该事件的字段}
//   段 Span: 段内有事件才落盘 (首个事件前补记 begin + 段的字段, 出段记 end + ms, 因异常出段另带 fail: true), 空转的轮询不留痕;
//     段外不记 (shim 程序的命令转发、探内存、录制都在段外; 其在线用例各成一段「用例 <名>」)
//   同一线程相邻几条除 ms 外全同的事件只记首条, 其后记一行 rep {n 又几次, ms 首条到末条}
//   live::fail 经此: 先在当时的段路径下记一行 fail {why}, 再抛 live::Fail
//   事件分两层: ev 交互细节 (只落这里); note 事务里的一条 (落这里, 并经 Hooks 交给事务: 页面尾日志 + 事务日志文件)
//   实时位置 Track: 线程在 Watch 作用域内时, 其段栈 (各段名 + 进入时刻)、实时一行 (live) 与最近发给客户端的命令 (sent) 另镜像进一个 Track,
//     别的线程 (GUI) 随时可读作进度 (不落盘, 与段内有无事件无关); Watch 可带 Hooks, 本线程的 note / 进段 / live 即同步回调
//   段: 刷新 {why 起因, n 轮号} › 接入 (› 引导) / 读 (成交 / 持仓 / 挂单 / 融资负债 / 信用资产一趟直发, 对不上即再读一遍);
//     下单 {委托} › 接入 / 切换 / 开窗 / 填单 / 确认 / 回报;
//     两融条款 › 接入 / 引导 (两表一并翻页);  资金流水 {from} › 接入 / 引导 / 第 n 窗 {lo, hi};
//     界面件: 点「文字」/ 功能树「项」/ 开页「页」{col} / 弹框 {titles}
//     事务 (LiveHub): 事务名 › 刷新 / 试算 / 行情 / 核对 + 上面各段
//   种类:
//     shim {cmd, ms, ret 应答记录 | n 条数 + kb (应答大于 kRetCap 字节)}   run {argv, rc, ms, err / out 尾部}   外部进程
//     build {dll, cc} / inject {dll, port, proc} / online {port}           shim 编译、注入、注入后上线
//     done {page, ms}                    一问查完 (页的查询键动作后禁用过且已恢复), ms 自动作起
//     ask {func, cols, rows, pos}        请求层直发一问 (req::ask; 一趟几问即几条) 收到应答: 出参数 / 行数 / 定位串
//     req_arm {vt, slots, dict, fields, funcs}  请求层钩上 (虚表 / 槽数 / 协议字典文件与规模)   req_lost {n}  事件环满挤掉的调用数
//     rows {tab, n}                      两融条款一表翻页到底的行数
//     texts {wins, seen}                 点文字不恰一处: 候选窗口数、各候选窗口上画着的文字
//     popup {title, hwnd}                等到的弹框                        form {px, dflt, unit, cap0, cap, n}  填单的价、默认价、单位、最大量、股数
//     gate {ms}                          界面锁被占着, 等了 ms             unsynced {try}  持仓与成交对不上
//     acct {assets, line, debt, net, avail, cash}  读到的信用账户      placed {qty, px, msg}  委托回报
//     terms {n}  两融条款只数            flows {n}  资金流水总条数
//     note 各条 (事务里): order / quote / job / acct / plan / terms / window / flows / ok / err {…, text}
#pragma once

#include "nlohmann/json.hpp"

#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace live::trace {

using Kw = nlohmann::ordered_json;

inline constexpr size_t kKeep = 20;     // 留几个日志文件
inline constexpr size_t kRetCap = 1024; // shim 应答至多这么多字节照录, 再大只记条数与大小

bool on(); // 当前线程在段内 (段外的事件不记; 拼字段较贵的调用方先问)

void ev(const char *kind, Kw kw = Kw::object()); // 交互细节一条
// 事务里的一条: 文字给页面 (并入字段 text), 字段落盘; 挂着 Hooks 即同步交给事务
void note(const char *kind, std::string text, Kw kw = Kw::object());
void live(std::string line); // 实时进度一行 (覆盖上一行; 镜像进 Track, 交给 Hooks, 不落盘)

inline long ms_since(std::chrono::steady_clock::time_point t0) {
  return static_cast<long>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count());
}

// loud = false: 段内一概不记 (连同嵌套的段), 仍镜像进 Track
class Span {
public:
  explicit Span(std::string name, Kw kw = Kw::object(), bool loud = true);
  ~Span();
  Span(const Span &) = delete;
  Span &operator=(const Span &) = delete;

private:
  int unwinding_;
};

// 一路此刻在哪: 段栈 (外 → 内; 空 = 闲) + 实时一行 + 最近发给客户端的命令 (适配给的一句, 如「shim 命令名 首个参数」)
struct Spot {
  struct Seg {
    std::string name;
    std::chrono::steady_clock::time_point t0;
  };
  std::vector<Seg> segs;
  std::string live;
  std::string cmd;
};

// 一路的实时位置: 跟随的线程写, 别的线程读
class Track {
public:
  Spot now() const;
  void set(Spot s);

private:
  mutable std::mutex mu_;
  Spot s_;
};

// 事务的钩子 (都在跟随的线程里同步调; 缺省空): 记一条 (文字 + 落盘字段) / 进一段 (段名) / 实时一行
struct Hooks {
  std::function<void(const char *kind, std::string text, const Kw &kw)> on_note;
  std::function<void(std::string_view name)> on_span;
  std::function<void(std::string line)> on_live;
};

// 当前线程在作用域内进的段与 live / 命令镜像进 t, 并交给 hooks (可空); 出作用域即清空. 一个线程同时只跟一个
class Watch {
public:
  explicit Watch(Track &t, const Hooks *hooks = nullptr);
  ~Watch();
  Watch(const Watch &) = delete;
  Watch &operator=(const Watch &) = delete;
};

void sent(std::string_view cmd); // 适配向客户端发出一条命令时调: 跟随着的线程记进其 Track

} // namespace live::trace
