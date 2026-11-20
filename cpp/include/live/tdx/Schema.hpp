// 通达信适配: 客户端协议字典 — TcApi 缓存 <客户端目录>/T0002/wt_cache/TcApi_Cache/40-BRANCH-<营业部>-2.TCS
//   格式: [二进制头][INI (WTGN_<序>=<功能号>,<名称>)][\0]
//         [u16 字段数][字段数 × 35B {u16 字段号, u8 类型, u8, u16 宽度, char 名称[29] GBK}]
//         [功能定义: u16 流, 每条 {0x4bd, 入参数, 功能号, 入参…, 出参数, 功能号+1, 出参…}]
//   通过 /proc/<pid>/maps 定位客户端目录, 经 shim memhas 在进程内直查哪份字典段在堆里 (零 ptrace / sudo)
#pragma once

#include <map>
#include <string>
#include <vector>

namespace live::tdx {

struct Schema {
  struct Func {
    std::vector<int> in, out;
  };
  std::string file;
  std::map<int, std::string> fields; // 字段号 → 名称
  std::map<int, Func> funcs;         // 功能号 → 入参 / 出参
  std::map<int, std::string> titles; // 功能号 → 名称
  std::string name(int fid) const;   // 字典里没有 → "?"
};

Schema schema(int pid); // 读不到 / 不恰一份在堆里 → Fail

} // namespace live::tdx
