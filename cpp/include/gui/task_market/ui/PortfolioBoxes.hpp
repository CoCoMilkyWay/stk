// Alpha→Factors 页 1. 区右侧的两个组合小框 + 当前组合 (Market Lines 页的因子 / 策略两组读它).
//   小框 1: 选组合 (<portfolio_dir>/<name>.json) / New / Rename / Del;  小框 2: 当前组合的条目 "(池, 持有期): 名", Add = 加入 Factors 表里高光的
//   权重 (持有期 = Factors 页 Hold), Del = 删选中条目; 失效条目 (因子文件里找不到该规范串等) 红. 组合选择在内存, 内容即文件.
#pragma once

#include "gui/factors/File.hpp" // FactorRow
#include "gui/task_market/services/PortfolioService.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace GUI::Market {

struct PortfolioUIState {
  std::string dir;                     // names 对应的目录 (config.portfolio_dir 变 → 重列)
  std::vector<std::string> names;      // 目录下的组合 (主干名)
  std::string name;                    // 当前组合; 空 = 无
  std::vector<PortfolioEntry> entries; // 当前组合内容 (= 文件)
  std::vector<PortfolioItem> items;    // 解析结果 (SyncPortfolio 在内容变 / 因子重扫后重解析; 与 entries 同序, 可能落后一帧)
  uint64_t content_gen = 0;            // 换组合 / 改内容 +1
  uint64_t items_gen = 0;              // items 每改一次 +1 (解析 / 建请求时标失效)
  int sel = -1;                        // 小框 2 选中条目
  char name_buf[64] = "";              // New / Rename 的名字
  std::string msg;                     // 上次操作结果
  bool confirm_delete = false;
};

// 条目标签 "(池, 持有期): 名"
std::string EntryLabel(const PortfolioEntry &e);
// GUI 线程每帧 (Alpha / Market 都调): 目录变 → 重列, 当前组合不在了 → 选第一个 (载入内容, content_gen +1)
void SyncPortfolioDir(PortfolioUIState &ui, const std::string &dir);
// Factors 页 1. 区右侧两框. view_row = Factors 表高光行 (nullptr = 无); (pool, lv, hold) = Factors 页当前池 / 层 / Hold
void RenderPortfolioBoxes(PortfolioUIState &ui, const FactorRow *view_row, const std::string &pool, LevelId lv, int hold);

} // namespace GUI::Market
