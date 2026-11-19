#pragma once

// 网络接口 (sina / tushare / bigquant / agent 后端) 的诊断, 一律一行写 stderr:
//   fail  = "[源] 对象: 原因"                          一次性故障 (放弃 / 断言前)
//   retry = "[源] 对象: 原因 (第 n 次失败, Ns 后重试)"  首次必报, 之后至多每分钟一行
//   原因只留一句 (错误码 / 消息); 回包正文先经 clip 压成一行, 不带源码位置与整段回包
#include <algorithm>
#include <cstdio>
#include <string>
#include <string_view>

namespace netlog {

inline constexpr size_t kClip = 200; // clip 的截断长度 [字节]

// 正文 → 一行 (换行 / 制表 → 空格); 超 kClip 截在 UTF-8 字边界并补 "…"
inline std::string clip(std::string_view s) {
  size_t n = std::min(s.size(), kClip);
  while (n < s.size() && (static_cast<unsigned char>(s[n]) & 0xC0) == 0x80)
    --n;
  std::string o(s.substr(0, n));
  std::ranges::replace_if(o, [](char c) { return c == '\n' || c == '\r' || c == '\t'; }, ' ');
  return n < s.size() ? o + "…" : o;
}

inline void fail(std::string_view src, std::string_view what, std::string_view why) {
  const std::string s = "[" + std::string(src) + "] " + std::string(what) + ": " + std::string(why) + "\n";
  std::fputs(s.c_str(), stderr);
}

// attempt = 第几次失败 − 1 (从 0 起), wait = 重试前等的秒数
inline void retry(std::string_view src, std::string_view what, std::string_view why, int attempt, int wait) {
  if (attempt == 0 || attempt * wait / 60 != (attempt - 1) * wait / 60)
    fail(src, what, std::string(why) + " (第 " + std::to_string(attempt + 1) + " 次失败, " + std::to_string(wait) + "s 后重试)");
}

} // namespace netlog
