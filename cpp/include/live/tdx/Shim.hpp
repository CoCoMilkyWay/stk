// 通达信适配: shim (注入客户端进程的 Win32 消息桥) 的构建、注入与双通道 (长连接命令 ⇄ 长推流订阅)
//   源码 src/live/tdx/win32/ (shim.c / inject.c), 产物落 output/run/live/tdx/shim/:
//   按源码哈希 H 命名 shim_<H>.dll, 控制端口 = 20000 + H % 10000 (源码变即换 DLL、换端口).
//   交叉编译 clang + mingw 运行时优先, i686-w64-mingw32-gcc 次之; 注入经 wine inject.exe.
//   协议: 一行 `cmd arg...\n` → 0..N 行 JSON 记录 + 末行 `ok` / `err <原因>`; 长连接复用 (断线自动重连),
//   推送流 (treewatch / gdiwatch) 走独立 Stream 连接 (无变更 1s 心跳 null).
#pragma once

#include "nlohmann/json.hpp"

#include <string>
#include <type_traits>
#include <vector>

namespace live::tdx::shim {

bool online();
void ensure(double timeout = 5); // 编译 + 注入直到在线 (已在线即跳过); 失败 → Fail
void reset();                    // 主动断开复用的长连接 (客户端换进程时)

// 一条命令 → 记录; 末行非 ok 或 timeout 秒内没应答 → Fail.
// 经目标界面线程的命令在客户端卡着时 shim 隔 3s 回 busy, 这里接着再问到缓过来或 timeout.
std::vector<nlohmann::json> call(const std::string &line, double timeout = 5);

// 推流长连接 (treewatch / gdiwatch): 独立 socket, 逐行收 JSON (心跳 null 跳过); 断线 → Fail
class Stream {
public:
  explicit Stream(const std::string &cmd);
  ~Stream();
  Stream(const Stream &) = delete;
  Stream &operator=(const Stream &) = delete;

  // 等下一条非心跳记录 (至多 timeout 秒): 收到 → true, 超时无变更 → false, 断线 → Fail
  bool next(nlohmann::json &out, double timeout = 0.5);
  void close();

private:
  int fd_ = -1;
  std::string buf_;
};

inline std::string arg(const std::string &s) { return s; }
inline std::string arg(const char *s) { return s; }
template <class T>
  requires std::is_integral_v<T>
std::string arg(T v) { return std::to_string(v); }

template <class... A>
std::string line(const char *name, const A &...a) {
  std::string s = name;
  ((s += ' ', s += arg(a)), ...);
  return s;
}
template <class... A>
std::vector<nlohmann::json> cmd(const char *name, const A &...a) { return call(line(name, a...)); }
template <class... A>
std::vector<nlohmann::json> cmd_wait(double timeout, const char *name, const A &...a) { return call(line(name, a...), timeout); }

} // namespace live::tdx::shim
