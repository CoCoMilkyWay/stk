// Beta→Style: 风格篮子的参数优选 (StyleService; 原料 = all.bar 库), 选定的子池写 output/config/beta/pools.json.
//   右表 = 风格 (features/Panel/PoolSpec.hpp kStyleDefs, 名后计 落地 / 选中 的池数),
//     点一行 = 打开 (至多一个) = 按左上参数网格 (端 / 只数 / 周期 / 权重 / 各切片维 各维多选, 笛卡尔积) 逐组选池回测,
//     其下列全A 两条 (行情页同一条) + 各组 × 权重的线 (全程切片为空的组不列):
//     中文名 (各段按维, 不起作用的段灰; 落地蓝 / 新选橙 / 去掉红) | 年化 | 夏普 | 回撤 | 超额 (对全A 同权重) | IR | 年换手, 底色 = 组内名次;
//     点行 = 选中 ↔ 去掉 (选中的行底色亮; 图上点线同一份); 有选池态 (落地 / 新选 / 去掉) 的线恒在前, 各自按表头排序 (缺省夏普降序); 只画可见行;
//   左 = 网格条 (开关按钮: 左键翻转 / 右键只留它; 不起作用的取值字灰, 与线名灰段同一判据) + 状态 + 改动 (+新 / −去) [确认…] 弹窗列出改动,
//     写 output/config/beta/pools.json = 落地 / [撤销] 回落地 + log + 窗滑条 (整年, 缺省全程); 下 = 各线滚动净值 (窗 = 全程即净值):
//     窗 = 末 W 年: 右表绩效 / 名次 / 线色 / 表序只看窗内 (各窗绩效服务一次算好, 拖动即时), 图上每日 = 该日净值 / W 年前净值, 竖线 = 右表窗起;
//     线色 = 夏普组内名次 (绿好 → 红差; 好的画在上), 悬停网格条某一维 = 按该维取值着色 (两值 = 蓝 / 橙, 多值 = 色环等分; 按钮字色即图例),
//       悬停到其中一个取值 = 该取值的线提到最上 (不透明加粗), 其余淡出;
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
  int open = -1;                  // 打开的风格 (pool::Style; −1 = 无)
  pool::Grid grid;                // 风格维 = 打开的风格
  StyleRequest sent;              // 上次发出的 (gen 0 = 未发)
  bool loaded = false;            // 读过 output/config/beta/pools.json
  std::vector<pool::Spec> landed; // = 文件 (池表序)
  std::vector<pool::Spec> chosen; // 改动中的选中 (落地序, 新选的接在后; 确认 = 写文件)
  uint64_t pick_gen = 1;          // landed / chosen 每改一次 +1
  int hot = -1;                   // 上帧悬停的线 (StyleResult::lines 下标): 右表行 / 图上最近一条
  int dim = -1;                   // 上帧悬停的网格维 (−1 = 无): 线按该维的取值着色
  int val = -1;                   // 上帧悬停的该维取值 (−1 = 无): 该取值的线提到最上, 其余淡出
  bool log_y = true;
  int win = 0;          // 窗 (年; 0 = 全程 = StyleResult::years): 右表绩效只算末 win 年, 图 = 滚动 win 年净值
  uint32_t fit_gen = 0; // 上次 fit 的结果 (gen)
  int fit_win = 0;      // 上次 fit 的窗 (年)
  // 结果派生: 名随结果, 名次 / 画序 / 基日随 (结果, 窗), 选池态随选中, 表序随以上 + 表头排序
  uint32_t of_gen = 0;            // 派生所属的结果 (gen)
  int of_win = 0;                 // 名次 / 画序 / 基日所属的窗 (年)
  uint64_t of_pick = 0;           // 选池态所属的选中 (pick_gen)
  int of_sort = -2;               // 表序所属的排序: 列 · 2 + 降序 (−1 = 原序; −2 = 未排)
  std::vector<std::string> names; // [线] 中文名
  std::vector<uint8_t> marks;     // [线] 选池态
  std::vector<float> ranks;       // [线 · 绩效列] 窗内组内名次 (0 = 最好 → 1 = 最差; NaN = 不可算)
  std::vector<uint32_t> paint;    // 画序 (窗内夏普升序: 好的画在上)
  std::vector<int32_t> base;      // [日] 滚动净值的基日 (= 以该日收尾的窗起 − 1; −1 = 窗盖到首日之前, 基 = 1)
  std::vector<uint32_t> order;    // 右表行序 (有选池态的在前, 其余按表头排序)
};

// GUI 线程每帧: 首次读 output/config/beta/pools.json (落地 = 选中)
void SyncStylePools(StyleUIState &ui);
// 整页 (左 = 网格条 / 状态 / 改动 + 净值图, 右 = 风格表); 打开的风格 / 网格变 → 发请求
void RenderTabStyle(StyleService &svc, StyleUIState &s);

} // namespace GUI::Beta