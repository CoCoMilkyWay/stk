// 实盘: 客户端适配表 (live/<客户端>/ 各一行); 上层按表列出、按下标建 Broker, 不认具体适配
#pragma once

#include "live/Broker.hpp"

#include <memory>
#include <span>

namespace live {

struct Client {
  const char *name;  // 连接按钮上的名字
  const char *about; // 接什么、先做什么、怎么读写
  std::unique_ptr<Broker> (*make)();
};
std::span<const Client> clients();

} // namespace live
