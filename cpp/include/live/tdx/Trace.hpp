// 通达信适配: 交互日志 — 与客户端的交互 (shim 命令 / 读内存 / 外部进程) 与 Broker 的流程, 一行一事件落
//   output/run/live/tdx/log/<启动时刻>.jsonl (每次启动一个文件, 只留最近 kKeep 个). 只记事件与定位所需的字段, 供事后据此还原一次失败.
//   行: {t 本地时刻 HH:MM:SS.mmm, th 线程号, at 段路径 (外/内), ev 种类, 其余为该事件的字段}
//   段 Span: 段内有事件才落盘 (首个事件前补记 begin + 段的字段, 出段记 end + ms, 因异常出段另带 fail: true), 空转的轮询不留痕;
//     段外不记 (调试台的轮询、推送与手动操作都在段外)
//   同一线程相邻几条除 ms 外全同的事件只记首条, 其后记一行 rep {n 又几次, ms 首条到末条}
//   适配里的 fail / need 经此 (遮住 live::fail): 先在当时的段路径下记一行 fail {why}, 再抛 live::Fail
//   段: 刷新 {why 起因, n 轮号} › 成交 / 持仓 / 挂单 / 信用账户;  盯持仓 › 接入;  下单 {委托} › 切换 / 开窗 / 填单 / 确认 / 回报;
//     两融条款 › <功能名> (另一线程 <功能名>条数标签);  <独占者> (调仓整批等);  界面件: 点「文字」/ 功能树「项」/ 开页「功能」{col} / 弹框 {titles}
//   种类:
//     shim {cmd, ms, ret 应答记录 | n 条数 + kb (应答大于 kRetCap 字节)}   run {argv, rc, ms, err / out 尾部}   外部进程
//     build {dll, cc} / inject {dll, port, proc} / online {port}           shim 编译、注入、注入后上线
//     attach {pid} / ptrace {scope}      接进程堆、放开 ptrace             torn {tab, n}  连读 n 次才一致
//     gen {tab, rows, addr}              盯着的表换代 (addr = 选中各块的地址)
//     wait {tab, ms, by: gen | full}     等客户端重查后的换代              ask {tab, rows, addr, ms}  act 后等到新应答
//     rows {n, got}                      两融整表拼表 (页上条数 / 内存行数) 每变一次
//     texts {wins, seen}                 点文字不恰一处: 候选窗口数、各候选窗口上画着的文字
//     popup {title, hwnd}                等到的弹框                        form {px, dflt, unit, cap0, cap, n}  填单的价、默认价、单位、最大量、股数
//     gate {by, ms}                      界面锁被 by 占着, 等了 ms         unsynced {try}  持仓与成交对不上
//     put {what, …}                      发布信用账户                      placed {qty, px, msg}  委托回报    terms {n}  两融条款只数
//     back {down} / down {why}           盯持仓: 接回客户端 / 掉线期间原因变了 (原因不变的重试不记)
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
void ev(const char *kind, Kw kw = Kw::object());

inline long ms_since(std::chrono::steady_clock::time_point t0) {
  return static_cast<long>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count());
}

// loud = false: 段内一概不记 (连同嵌套的段)
class Span {
public:
  explicit Span(std::string name, Kw kw = Kw::object(), bool loud = true);
  ~Span();
  Span(const Span &) = delete;
  Span &operator=(const Span &) = delete;

private:
  int unwinding_;
};

} // namespace trace

} // namespace live::tdx
