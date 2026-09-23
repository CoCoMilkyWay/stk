// Factors→Factors: <factor_dir>/<universe>/ 下一因子一文件的整体面板. 顶部 = 作用域 / 后端 / 金额档 / 持有期 + Run / Cancel /
// Rescan + 状态; 中间 = 加因子构建器 (从根往里逐槽下拉选算子 / 特征, 参数手填, 即时校验); 下面 = 表 (一行一文件: BROKEN 红 +
// 原因, 有效行显示规范串 / 结构规模 / 选定持有期的 Stat 列 / 耗时; 点规范串载入构建器). 单因子详情面板另做.
#pragma once

#include "gui/task_factors/services/FactorsService.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace GUI::Factors {

// 构建器的一个槽: 未选 / 特征叶 / 算子 (带 arity 个子槽 + 参数). 从外到内填: 先选根算子, 再逐槽往里选
struct BuildNode {
  int op = -1;                 // factor::expr::kOps 下标; -1 = 叶
  std::string feat;            // 叶: 特征 code (空 = 未选)
  std::vector<BuildNode> args; // 算子: arity 个子槽
  factor::Param p;             // 算子参数 (选中时按 Check.hpp 默认表填, 之后手改)
};

struct FactorsUIState {
  int backend = 0;        // 0 = CPU, 1 = GPU
  int amt_idx = 0;        // FeatureTable::amts 下标
  int hold_idx = 0;       // FeatureTable::labels 下标 (表列显示哪个持有期)
  BuildNode build;        // 加因子构建器的根
  char name_buf[64] = ""; // 因子名 = 文件名主干, 必填
  char note_buf[256] = "";
  char filter_buf[64] = ""; // 下拉里的过滤框 (打开时清空)
  std::string add_err;      // 即时校验结果 (空 = 合法 / 未填完)
  std::string add_msg;      // 上次 Add / Save / 删除 的结果
  // 两种模式: edit_file 空 = 添加模式 (Add 新文件, 查重); 非空 = 编辑模式 (表格选中行高光, Save 覆盖 / 删除该文件).
  // 选中行 → 载入构建器 + name + note; 取消选定 → 回添加模式但构建器内容保留 (当模板)
  std::string edit_file;
  int popup = 0;         // 本帧要开的弹窗: 1 Save 确认, 2 删除确认, 3 不能加 (表格里置, EndTable 后开)
  std::string popup_msg; // 弹窗正文
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
