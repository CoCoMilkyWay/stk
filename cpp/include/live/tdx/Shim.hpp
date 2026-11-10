// 通达信适配: shim (注入客户端进程的 Win32 消息桥) 的构建、注入与控制通道
//   源码 src/live/tdx/win32/ (shim.c 消息桥, 命令表见其文件头 / inject.c 远程加载), 产物落 output/run/live/tdx/shim/:
//   按源码哈希 H 命名 shim_<H>.dll, 控制端口 = 20000 + H % 10000 (源码一变即换 DLL、换端口, 旧进程里的旧版互不干扰).
//   交叉编译 clang + mingw 运行时优先, i686-w64-mingw32-gcc 次之.
//   注入: DLL 拷进 wine 的 C:\windows\temp (不依赖 Z: 盘映射)、写端口文件, inject.exe 远程 LoadLibrary
//   (WINEPREFIX 取环境变量或 ~/.wine, 目标进程名 SHIM_TARGET_PROC 或 tdxw.exe).
//   协议: 一行 `cmd arg...\n` → 0..N 行 JSON 记录 + 末行 ok / err <原因>; 一次一条连接, shim 串行应答.
//     长连接 (treewatch / gdiwatch): 连上先推全量、之后推增量, 无变更 1s 一行心跳 null, 不收尾.
#pragma once

#include "nlohmann/json.hpp"

#include <functional>
#include <string>
#include <type_traits>
#include <vector>

namespace live::tdx::shim {

int port();
bool online();
void ensure(double timeout = 5); // 编译 + 注入直到在线 (已在线即跳过); 失败 → Fail

// 一条命令 → 记录; 末行非 ok 或 timeout 秒内没应答 → Fail.
// 要经目标界面线程的命令 (gettext 等) 在客户端卡着时会一直等到它缓过来, 故调用方按需给长 timeout
std::vector<nlohmann::json> call(const std::string &line, double timeout = 5);
// 长连接: 每行 (记录或心跳 null) 交给 on, on 回 false 即断开; timeout 秒内没一行 / 被关 / err → Fail
void watch(const std::string &line, const std::function<bool(const nlohmann::json &)> &on, double timeout = 3);

inline std::string arg(const std::string &s) { return s; }
inline std::string arg(const char *s) { return s; }
template <class T>
  requires std::is_integral_v<T>
std::string arg(T v) { return std::to_string(v); }

template <class... A>
std::vector<nlohmann::json> cmd(const char *name, const A &...a) {
  std::string line = name;
  ((line += ' ', line += arg(a)), ...);
  return call(line);
}
template <class... A>
std::vector<nlohmann::json> cmd_wait(double timeout, const char *name, const A &...a) {
  std::string line = name;
  ((line += ' ', line += arg(a)), ...);
  return call(line, timeout);
}

} // namespace live::tdx::shim
