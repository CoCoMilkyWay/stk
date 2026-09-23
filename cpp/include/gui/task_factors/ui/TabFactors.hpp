// Factors→Factors: <factor_dir>/<universe>/ 下一因子一文件的整体面板. 顶部 = 作用域 / 后端 / 金额档 / 持有期 + Run / Cancel /
// Rescan + 状态; 中间 = 加因子 (表达式即时校验); 下面 = 表 (一行一文件: BROKEN 红 + 原因, 有效行显示规范串 / 结构规模 /
// 选定持有期的 Stat 列 / 耗时). 单因子详情面板另做.
#pragma once

#include "gui/task_factors/services/FactorsService.hpp"

#include <cstdint>
#include <string>

namespace GUI::Factors {

struct FactorsUIState {
  int backend = 0;  // 0 = CPU, 1 = GPU
  int amt_idx = 0;  // FeatureTable::amts 下标
  int hold_idx = 0; // FeatureTable::labels 下标 (表列显示哪个持有期)
  char add_buf[1024] = "";
  std::string add_checked; // 上次即时校验的输入 (变了才重校验)
  std::string add_err;     // 即时校验结果 (空 = 合法 / 空输入)
  std::string add_msg;     // 上次 Add 的结果
  // 表列宽贴合 (对仗 TabOperators)
  uint64_t fit_epoch = ~0ull;
  double fit_last_time = 0.0;
  int fit_frames = 0;
  int sort_column = -1;
  bool sort_ascending = true;
};

// 当前 config 决定的作用域 (GUI 线程每帧从 SharedData 取): 显示 + 与文件里 stat 的作用域对比着色
struct FactorsUIContext {
  std::string factor_dir; // <factor_dir>/<universe>
  std::string universe, start_date, end_date;
  bool axis_ready = false; // 资产轴就绪 (数据库扫过) 才能读特征库评估
};

// 返回值: 1 = Run (评估), 2 = Rescan (含 Add 成功后), -1 = Cancel, 0 = 无
int RenderTabFactors(FactorsService &svc, FactorsUIState &ui, const FactorsUIContext &ctx);

} // namespace GUI::Factors
