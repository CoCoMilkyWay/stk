// Beta→Style: 风格篮子的参数优选 (StyleService; 原料 = all.bar 库), 选定的子池写 config/pools.json.
//   右表 = 风格 (features/Panel/PoolSpec.hpp kStyleDefs, 名后计 落地 / 选中 的池数),
//     点一行 = 打开 (至多一个) = 按左上参数网格 (端 / 只数 / 周期 / 权重 / ST / 板块 / 融资 / 过滤表各条件 各维多选, 笛卡尔积) 逐组选池回测,
//     其下列全A 两条 + 各组 × 权重的线 (全程切片为空的组不列):
//     勾选 | 中文名 (各段按维, 不起作用的段灰; 落地蓝 / 新选橙 / 去掉红) | 年化 | 夏普 | 回撤 | 超额 (对全A 同权重) | IR | 年换手, 底色 = 组内名次;
//     有选池态 (落地 / 新选 / 去掉) 的线恒在前, 各自按表头排序 (缺省夏普降序); 只画可见行;
//   左 = 网格条 + 状态 + 改动 (+新 / −去) [确认…] 弹窗列出改动, 写 config/pools.json = 落地 / [撤销] 回落地; 下 = 各线净值 (log 可切):
//     线色 = 夏普组内名次 (绿好 → 红差; 好的画在上), 悬停网格条某一维 = 按该维取值着色 (两值 = 蓝 / 橙, 多值 = 色环等分; 按钮字色即图例);
//     细线 = 未选 (合计点数超上限按步长抽点), 粗线 = 有选池态 (缺省按态着色), 灰 = 全A, 悬停 (图上最近一条 / 表行) = 白粗线 + 绩效提示, 粗线 / 悬停线尾标名
//   右表与 Market 各页同宽 (sheet_width)
#pragma once

#include "features/Panel/PoolSpec.hpp"             // Spec / Grid
#include "gui/task_beta/services/StyleService.hpp" // StyleRequest

#include <cstdint>
#include <string>
#include <vector>

namespace GUI::Beta {

// 打开的风格 + 参数网格 (进请求) + 选池的落地 (= pools.json) / 改动中的选中 + 悬停 / 视图 + 结果派生
struct StyleUIState {
  int open = -1; // 打开的风格 (pool::Style; −1 = 无)
  pool::Grid grid;
  StyleRequest sent;              // 上次发出的 (gen 0 = 未发)
  bool loaded = false;            // 读过 config/pools.json
  std::vector<pool::Spec> landed; // = 文件 (池表序)
  std::vector<pool::Spec> chosen; // 改动中的选中 (落地序, 新选的接在后; 确认 = 写文件)
  uint64_t pick_gen = 1;          // landed / chosen 每改一次 +1
  int hot = -1;                   // 上帧悬停的线 (StyleResult::lines 下标): 右表行 / 图上最近一条
  int dim = -1;                   // 上帧悬停的网格维 (−1 = 无): 线按该维的取值着色
  bool log_y = true;
  uint32_t fit_gen = 0; // 上次 fit 的结果 (gen)
  // 结果派生: 名 / 名次 / 画序随结果, 选池态随选中, 表序随三者 + 表头排序
  uint32_t of_gen = 0;            // 派生所属的结果 (gen)
  uint64_t of_pick = 0;           // 选池态所属的选中 (pick_gen)
  int of_sort = -2;               // 表序所属的排序: 列 · 2 + 降序 (−1 = 原序; −2 = 未排)
  std::vector<std::string> names; // [线] 中文名
  std::vector<uint8_t> marks;     // [线] 选池态
  std::vector<float> ranks;       // [线 · 绩效列] 组内名次 (0 = 最好 → 1 = 最差; NaN = 不可算)
  std::vector<uint32_t> paint;    // 画序 (夏普升序: 好的画在上)
  std::vector<uint32_t> order;    // 右表行序 (有选池态的在前, 其余按表头排序)
};

// GUI 线程每帧: 首次读 config/pools.json (落地 = 选中)
void SyncStylePools(StyleUIState &ui);
// 整页 (左 = 网格条 / 状态 / 改动 + 净值图, 右 = 风格表); 打开的风格 / 网格变 → 发请求
void RenderTabStyle(StyleService &svc, StyleUIState &s);

} // namespace GUI::Beta