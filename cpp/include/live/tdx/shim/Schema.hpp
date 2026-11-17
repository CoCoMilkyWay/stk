// 客户端的协议字典: TcApi 缓存 <客户端目录>/T0002/wt_cache/TcApi_Cache/40-BRANCH-<营业部>-2.TCS, 登录时整份读进内存. 格式:
//   [二进制头][INI 文本 (\r\n 分行; WTGN_<序>=<功能号>,<名称> 列功能)][\0]
//   [u16 字段数][字段数 × 35 字节 {u16 字段号, u8 类型, u8 ?, u16 宽度, char 名称[29] (GBK, \0 结尾)}]
//   [功能定义: u16 流, 每条 {0x4bd, 入参数, 功能号, 入参字段号…, 出参数, 功能号 + 1, 出参字段号…}]
// 请求层 job 上写的字段 (槽 7 / 9) 是入参, 应答的列序即出参序 (明文表行尾另多空列).
// 认哪份: 客户端目录下各份里, 字典段原样在客户端堆里的那份 (恰一份; 别的营业部 / 旧账户的缓存不在堆里)
#pragma once

#include "live/tdx/Mem.hpp"

#include <map>
#include <string>
#include <vector>

namespace live::tdx::tool {

struct Schema {
  struct Func {
    std::vector<int> in, out;
  };
  std::string file;
  std::map<int, std::string> fields; // 字段号 → 名称
  std::map<int, Func> funcs;         // 功能号 → 入参 / 出参
  std::map<int, std::string> titles; // 功能号 → 名称 (WTGN 段只列一部分)
  std::string name(int fid) const;   // 字典里没有 → "?"
};

Schema schema(const mem::Heap &h); // 读不到 / 不恰一份在堆里 → Fail

} // namespace live::tdx::tool
