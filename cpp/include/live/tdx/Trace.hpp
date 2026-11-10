// 通达信适配: 交互日志 — 与客户端的交互 (shim 命令 / 读内存 / 外部进程) 与 Broker 的流程, 一行一事件落
//   output/run/live/tdx/log/<启动时刻>.jsonl (每次启动一个文件, 只留最近 kKeep 个). 只记事件与定位所需的字段, 供事后据此还原一次失败.
//   行: {t 本地时刻 HH:MM:SS.mmm, th 线程号, at 段路径 (外/内), ev 种类, 其余为该事件的字段}
//   段 Span: 段内有事件才落盘 (首个事件前补记 begin + 段的字段, 出段记 end + ms, 因异常出段另带 fail: true), 空转的轮询不留痕;
//     段外不记 (调试台的轮询、推送与手动操作都在段外)
//   同一线程相邻几条除 ms 外全同的事件只记首条, 其后记一行 rep {n 又几次, ms 首条到末条}
//   适配里的 fail / need 经此 (遮住 live::fail): 先在当时的段路径下记一行 fail {why}, 再抛 live::Fail
//   种类: begin / end / rep / fail
//     shim {cmd, ms, ret 应答记录 | n 条数 + kb (应答大于 kRetCap 字节)}   run {argv, rc, ms, err / out 尾部}
//     build / inject / online   shim 编译、注入、注入后上线                attach {pid} / ptrace {scope}   接进程、放开 ptrace
//     gen {tab, rows, at}       盯着的表换代 (at = 选中各块的地址)        wait {tab, ms, by: gen | full}  等客户端重查后的换代
//     ask {tab, rows, ms}       act 后等到新应答                          torn {tab, n}  连读 n 次才一致
//     rows {func, n, got}       两融整表拼表进度 (页上条数 / 内存行数)    popup {title, hwnd}  等到的弹框
//     gate {by, ms}             界面锁被 by 占着, 等了 ms                  back  接回客户端    unsynced {try}  持仓与成交对不上
//     put {what, ...}           发布挂单 / 信用账户                        placed {qty, px, msg}  委托回报
#pragma once

#include "nlohmann/json.hpp"

#include <chrono>
#include <string>

namespace live::tdx {

[[noreturn]] void fail(std::string why);
inline void need(bool ok, const char *why) {
  if (!ok)
    fail(why);
}
inline void need(bool ok, const std::string &why) {
  if (!ok)
    fail(why);
}

namespace trace {

using Kw = nlohmann::ordered_json;

inline constexpr size_t kKeep = 20;     // 留几个日志文件
inline constexpr size_t kRetCap = 1024; // shim 应答至多这么多字节照录, 再大只记条数与大小

bool on(); // 当前线程在段内 (段外的事件不记; 拼字段较贵的调用方先问)
void ev(const char *ev, Kw kw = Kw::object());

inline long ms_since(std::chrono::steady_clock::time_point t0) {
  return static_cast<long>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count());
}

class Span {
public:
  explicit Span(std::string name, Kw kw = Kw::object());
  ~Span();
  Span(const Span &) = delete;
  Span &operator=(const Span &) = delete;

private:
  int unwinding_;
};

} // namespace trace

} // namespace live::tdx
