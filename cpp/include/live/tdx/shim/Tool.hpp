// shim 独立程序 (src/live/tdx/shim/) 的各子命令; 用法见 Main.cpp 文件头
#pragma once

#include <string>
#include <vector>

namespace live::tdx::tool {

int probe(const std::vector<std::string> &args);              // tables | text <词…> | obj | ref | class
int rec(double secs);                                         // 录制 secs 秒
int vt(double secs, const std::vector<std::string> &targets); // 虚表追踪 secs 秒; 目标 = RTTI 类名或 0x虚表
int test(const std::vector<std::string> &names);              // 空 = 全部不慢的; list = 列出

} // namespace live::tdx::tool
