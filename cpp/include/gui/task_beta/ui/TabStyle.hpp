// Beta→Style: 风格篮子的参数优选 (StyleService; 原料 = all.bar 库), 选定的子池写 output/config/beta/pools.json.
//   右表 = 风格 (features/Panel/PoolSpec.hpp kStyleDefs, 名后计 落地 / 选中 的池数),
//     点一行 = 打开 (至多一个) = 按左上参数网格 (只数 / 周期 / 权重 / 划分维 (端 / ST / 板块 / 融资) / 过滤维 各维多选, 笛卡尔积; 集合维见 PoolSpec.hpp Grid) 逐组选池回测,
//     其下列全A 两条 (行情页同一条, 随网格勾选的权重) + 各组的线 (全程切片为空的组不列):
//     中文名 (各段按维, 不起作用的段灰; 落地蓝 / 新选橙 / 去掉红) | 平均年化 | 中位年化 | 平均夏普 | 中位夏普 (滚动一年窗) | 夏普分位 (累计中位夏普逐日截面分位的平均) | 最大回撤 | 年换手 (区间整段), 底色 = 组内名次;
//     点行 = 选中 ↔ 去掉 (选中的行底色亮; 图上点线同一份); 有选池态 (落地 / 新选 / 去掉) 的线恒在前, 各自按表头排序 (缺省夏普分位降序); 只画可见行;
//   左 = 网格条 (开关按钮: 左键翻转 / 右键只留它, 过滤维为下拉多选; 不起作用的取值字灰, 与线名灰段同一判据) + 状态 + 改动 (+新 / −去) [确认…] 弹窗列出改动,
//     写 output/config/beta/pools.json = 落地 / [撤销] 回落地; 中 = 各线累计中位夏普 (每日 = 自首窗到该日 各滚动一年窗夏普的中位; 末日值 = 右表中位夏普):
//     任一日的截面 = 到该日为止的名次; 一时的极端行情只占少数窗, 压不动中位; 只计满一年的窗 (年化平均在对数空间平均);
//     底 = 区间条 (同 Market 行情页; 底线 = 全A 等权全程净值): 选回测区间, 右表 / 图只算区间内 (只重算绩效, 篮子账不重跑);
//     [含晚起] 开关: 缺省不列晚于区间起点与同次序最早建仓 (风格池比同风格, 全选池比全A) 一个月仍未建仓的组 (切片中途才有成分, 如纯科创板), 名次只在列出的线内排;
//     线色 = 中位夏普组内名次 (绿好 → 红差; 好的画在上), 悬停网格条某一维 = 按该维取值着色 (两值 = 蓝 / 橙, 多值 = 色环等分; 按钮字色即图例),
//       悬停到其中一个取值 = 该取值的线提到最上 (不透明加粗), 其余淡出;
//     细线 = 未选 (合计点数超上限按步长抽点), 粗线 = 有选池态 (缺省按态着色), 灰 = 全A, 悬停 (图上最近一条 / 表行) = 白粗线 + 绩效提示 + 表行亮, 粗线 / 悬停线尾标名;
//       图上悬停时右表滚到该行居中, 悬停止回原位
//   右表与 Market 各页同宽 (sheet_width)
#pragma once

#include "features/Panel/PoolSpec.hpp"             // Spec / Grid
#include "gui/task_beta/services/StyleService.hpp" // StyleRequest
#include "gui/util/Chart.hpp"                      // DayRange
#include "misc/date.hpp"

#include <array>
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
  int synced = -1;                // 右表已滚到的行 (表行下标; 图上悬停的线, 换线 / 行序变才再滚)
  float back_y = -1.0f;           // 图上悬停起时右表的滚动位置 (悬停止回到它; −1 = 未在悬停)
  int dim = -1;                   // 上帧悬停的网格维 (−1 = 无): 线按该维的取值着色
  int val = -1;                   // 上帧悬停的该维取值 (−1 = 无): 该取值的线提到最上, 其余淡出
  misc::Ymd lo = 0, hi = 0;       // 回测区间 [lo, hi] (0 = 不限; 进请求)
  DayRange range;                 // 区间条 (d0 / d1 每帧由 lo / hi 在结果日轴上换算) + 拖动态
  bool late = false;              // 列晚起的组 (晚于区间起点与同次序最早建仓一个月仍未建仓, 如纯科创板; 缺省不列: 起点不同, 绩效不可比)
  uint32_t fit_gen = 0;           // 上次 fit 的结果 (gen)
  // 结果派生: 名随结果, 列出的线 / 名段宽 / 名次 / 画序随 (结果, 晚起), 选池态随选中, 表序随以上 + 表头排序
  uint32_t of_gen = 0;                     // 派生所属的结果 (gen)
  int of_late = -1;                        // 列出的线所属的 late (−1 = 未派生)
  uint64_t of_pick = 0;                    // 选池态所属的选中 (pick_gen)
  int of_sort = -2;                        // 表序所属的排序: 列 · 2 + 降序 (−1 = 原序; −2 = 未排)
  std::vector<std::string> names;          // [线] 中文名
  std::vector<uint8_t> marks;              // [线] 选池态
  size_t n_late = 0;                       // 晚起的线数
  std::vector<uint32_t> shown;             // 列出的线 (表 / 图只看这些; late 关 = 去掉晚起的)
  std::array<float, pool::kParts> slots{}; // [名段] 列出的线里该段最宽 (字号倍数): 表里线名各段占此宽, 上下对齐
  std::vector<float> ranks;                // [线 · 绩效列] 列出的线内的名次 (0 = 最好 → 1 = 最差; NaN = 不可算 / 未列)
  std::vector<uint32_t> paint;             // 画序 (列出的线, 中位夏普升序: 好的画在上)
  std::vector<uint32_t> order;             // 右表行序 (列出的线; 有选池态的在前, 其余按表头排序)
};

// GUI 线程每帧: 首次读 output/config/beta/pools.json (落地 = 选中)
void SyncStylePools(StyleUIState &ui);
// 整页 (左 = 网格条 / 状态 / 改动 + 累计中位夏普图 + 区间条, 右 = 风格表); 打开的风格 / 网格变 → 发请求.
// 返回 本帧写了 pools.json (调用方请求进程内重启)
bool RenderTabStyle(StyleService &svc, StyleUIState &s);

} // namespace GUI::Beta