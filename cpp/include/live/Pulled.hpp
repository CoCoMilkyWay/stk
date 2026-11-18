// 实盘: 拉来的表的本地缓存 (两融条款 / 资金流水账本同形), 原子替换:
//   首行 pulled,<拉于 YYYYMMDD>[,键,值]…; 次行表头; 其后一行一条 (各格不含逗号、无引号)
#pragma once

#include "misc/date.hpp"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace disk {
enum Node : std::uint16_t;
}

namespace live {

struct Pulled {
  misc::Ymd pulled = 0;                    // 拉于 (0 = 还没有)
  std::map<std::string, std::string> meta; // 首行其余的 键 → 值
  std::vector<std::vector<std::string>> rows;
};

Pulled load_pulled(disk::Node file, const char *head);                // 没有文件 → 空; 各行列数同表头
void save_pulled(disk::Node file, const char *head, const Pulled &s); // 格里有逗号 → Fail

} // namespace live
