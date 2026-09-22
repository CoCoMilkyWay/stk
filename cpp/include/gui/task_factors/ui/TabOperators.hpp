// Factors→Operators: OpTable 全部算子一张表. 静态列 = 表属性, 动态列 = worker 逐行发布的三后端耗时
// (stream 为 golden; cpu / gpu 没过对拍就以红色 error + Δ 顶替时间, 过了但比上游慢则 ms 标红).
// 顶部一行参数 + Run / Cancel.
#pragma once

#include "gui/task_factors/services/OperatorsService.hpp"

#include <cstdint>
#include <string>

namespace GUI::Factors {

struct OperatorsUIState {
  OperatorsRequest req; // 页面正在编辑的参数 (Run 时快照给 service)
  // 表列宽贴合: 发布代变了 (限速) → 连发几帧 TableSetColumnWidthAutoAll (对仗 TabFeature)
  uint64_t fit_epoch = ~0ull;
  double fit_last_time = 0.0;
  int fit_frames = 0;
  // 排序 (tristate)
  int sort_column = -1;
  bool sort_ascending = true;
};

// 返回值: 1 = Run 按下, -1 = Cancel 按下, 0 = 无 (由 TaskFactors 转成 service 调用)
int RenderTabOperators(OperatorsService &svc, OperatorsUIState &ui);

// 算子表落地 JSON (给人看, 一行一算子): <factor_dir>/operators.json. 算子库与 universe / 日期区间无关
// (合成张量), 故不像 features.json 那样按 universe 分目录, 全局一份, 每次跑完覆盖.
// 全部行按 OpTable 表序 (不受页面排序影响); 表头 = 本轮张量参数 + 总体状态, 行 = 静态列 + 对拍/耗时
// (未跑完的行只落静态列 + status). 一轮结束 (Running → Done / Cancelled) 时由 TaskFactors 调
void SaveOperatorTableJson(const std::string &factor_dir, OperatorsService &svc);

// 载入落盘快照 (Save 的逆, 进页时由 TaskFactors 调一次): 只吃整轮跑完 (status/done/total 齐备, 逐行
// status = done) 且静态列逐行对得上当前 OpTable (e_name/元数/三维分类/operand/operator/参数实际值;
// c_name 与 note 不比) 的文件, 命中则经 AdoptSnapshot 摆成 Done 并把页面张量参数跟成快照的形状 —— 进页不用算.
// 返回 false = 没文件 / 解析不了 / 校验不过 (后两者已把文件删掉), 由调用方起算一轮.
bool LoadOperatorTableJson(const std::string &factor_dir, OperatorsService &svc, OperatorsUIState &ui);

} // namespace GUI::Factors
