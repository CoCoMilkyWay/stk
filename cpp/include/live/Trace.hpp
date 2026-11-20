// 实盘: 统一的交互与事务日志 + 实时进度镜像 (唯一真相源, 零双写)
//   落盘 output/run/live/trace/<启动时刻>.jsonl (每次启动一个文件, 留最近 kKeep 个):
//     行 = {t HH:MM:SS.mmm, th 线程号, at 段路径 (外/内), ev 种类, ...}
//     段 Span: 段内有事件才落 begin/end (+ms, 异常出段带 fail: true); 同线程相邻同事件并成 rep {n, ms}
//   实时位置 Track (UI 随时免锁镜像读, 与是否落盘无关):
//     Watch 作用域内, 本线程的段栈 (各段名 + 进入时刻)、live 进度一行、最近 shim 命令自动镜像进 Track;
//     可选同时挂载 Journal: 本线程的 note(kind, text, kw) 既落盘 jsonl 又推入页面尾日志, 且 span 进出通知回调
//   live::fail 经此: 先在当前段下记 fail {why}, 再抛 live::Fail (外部不可控边界的唯一收口)
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

bool on();          // 当前线程在段内 (段外事件不记)
std::string file(); // 当前进程的 trace 文件路径 (空 = 还没落过笔)

// 底层交互事件 (只落盘 jsonl, 不进事务页面日志)
void ev(const char *kind, Kw kw = Kw::object());
// 业务事件 (落盘 jsonl + 若挂了 Journal 则同步进页面尾日志)
void note(const char *kind, std::string text, Kw kw = Kw::object());
// 当前实时进度一行 (覆盖上行, 镜像进 Track, 不落盘)
void live(std::string line);

inline long ms_since(std::chrono::steady_clock::time_point t0) {
  return static_cast<long>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count());
}

// loud = false: 段内 (含嵌套段) 不落盘, 仍更新 Track
class Span {
public:
  explicit Span(std::string name, Kw kw = Kw::object(), bool loud = true);
  ~Span();
  Span(const Span &) = delete;
  Span &operator=(const Span &) = delete;

private:
  int unwinding_;
};

// 一路此刻在哪: 段栈 (外 → 内; 空 = 闲) + 实时一行 + 最近发出的 shim 命令
struct Spot {
  struct Seg {
    std::string name;
    std::chrono::steady_clock::time_point t0;
  };
  std::vector<Seg> segs;
  std::string live;
  std::string shim;
};

class Track {
public:
  Spot now() const;
  void set(Spot s);

private:
  mutable std::mutex mu_;
  Spot s_;
};

// 事务监听钩子 (均在事务线程同步回调; 缺省空)
struct Hooks {
  std::function<void(const char *kind, std::string text)> on_note;
  std::function<void(std::string_view name)> on_span;
  std::function<void(std::string line)> on_live;
};

// 当前线程在作用域内镜像进 Track (及可选 Hooks); 出作用域清空. 单线程不嵌套
class Watch {
public:
  explicit Watch(Track &t, const Hooks *hooks = nullptr);
  ~Watch();
  Watch(const Watch &) = delete;
  Watch &operator=(const Watch &) = delete;
};

void shim_sent(std::string_view cmd);

} // namespace live::trace
