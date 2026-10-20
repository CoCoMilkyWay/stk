// Factors→Factors: <factor_dir>/<pool>/ 下一因子一文件的整体面板. 顶部 = 作用域 / 后端 / 持有期 + Run / Cancel /
// Rescan + 状态, 下一行 Search (高光因子的模板参数搜索: 进度 / 每持有期的终选); 中间 = 加因子构建器 (从根往里逐槽下拉选算子 / 特征,
// 参数手填或勾成自由参数, 即时校验); 下面 = 表 (一行一权重, 多权重因子折成父行 + 可展开的子行: BROKEN 红 + 原因, 有效行显示规范串 /
// 结构规模 / 选定持有期的 Stat 列 / 耗时; 首列勾选 → 模板载入构建器编辑, 点行高光 → 给 Inspect 页).
#pragma once

#include "gui/task_factors/services/FactorsService.hpp"
#include "gui/task_factors/services/SearchService.hpp"

#include <cstdint>
#include <string>
#include <vector>

struct AssetItem;

namespace GUI::Factors {

// 构建器的一个槽: 未选 / 特征叶 / 算子 (带 arity 个子槽 + 参数). 从外到内填: 先选根算子, 再逐槽往里选
struct BuildNode {
  int op = -1;                 // factor::expr::kOps 下标; -1 = 叶
  std::string feat;            // 叶: 特征 code (空 = 未选)
  std::vector<BuildNode> args; // 算子: arity 个子槽
  factor::Param p;             // 算子参数 (选中时按 Check.hpp 默认表填, 之后手改)
  uint8_t free = 0;            // 自由参数位 (expr::kFreeD / K / K2): 源串打裸键名, 由 Search 搜
};

struct FactorsUIState {
  int backend = -1;          // 0 = CPU, 1 = GPU; -1 = 未定, 首帧按 GPU 可用性定 (有 GPU 默认 GPU)
  int hold_idx = 0;          // FeatureTable::labels 下标 (表列显示哪个持有期; 多权重因子的父行取该 h 下 score 最高的权重)
  BuildNode build;           // 加因子构建器的根
  char name_buf[64] = "";    // name_en = 文件名主干, 必填
  char name_cn_buf[64] = ""; // name_cn, 必填, 纯汉字 ≤10 字 (≤30 字节)
  char note_buf[256] = "";
  char filter_buf[64] = ""; // 下拉里的过滤框 (打开时清空)
  std::string add_err;      // 即时校验结果 (空 = 合法 / 未填完)
  std::string add_msg;      // 上次 Add / Save / 删除 的结果
  // 两种模式: edit_file 空 = 添加模式 (Add 新文件, 查重); 非空 = 编辑模式 (表格首列勾选的那个文件, 同时只勾一个; Save 覆盖 / 删除该文件).
  // 勾选 → 模板载入构建器 + name_en + name_cn + note; 取消勾选 → 回添加模式但构建器内容保留 (当模板)
  std::string edit_file;
  // 点行高光 = 单权重展示 (Inspect 页) 的对象 (FactorRow::key), 与 edit_file 互不干涉; 再点同一行 → 取消.
  // 多权重因子: 父行 = 选定 Hold 下 score 最高的权重, 点父行即高光它; 展开 (expanded 里的文件) 后可点各权重子行
  // 首次扫出表后默认高光第一个有效权重 (InspectAutoRequest 置, 只一次: 之后用户取消高光不再自动补)
  std::string view_file;
  bool view_defaulted = false;
  std::vector<std::string> expanded; // 展开的多权重因子文件
  // Search (对象 = 高光行所在文件的模板, 搜索窗内全部持有期): 点下时把行拷给 search_row (TaskFactors 据此建请求)
  FactorRow search_row;
  // Search all (当前池 / 层下全部可搜模板文件逐个搜, 全部搜完整目录 Run 一次): 点下时把各文件首个可搜行拷进队列, TaskFactors 逐个出队;
  // Cancel = 取消在跑 + 清空队列. search_all_n > 0 期间视同 Search 在跑 (队列项之间服务短暂空闲, 不许插别的请求 / 换池换层)
  std::vector<FactorRow> search_queue;
  int search_all_n = 0;  // 本轮文件总数; 0 = 没在 Search all
  int popup = 0;         // 本帧要开的弹窗: 1 Save 确认, 2 删除确认, 3 不能加 (表格里置, EndTable 后开)
  std::string popup_msg; // 弹窗正文
  // 表列宽贴合 (对仗 TabOperators)
  uint64_t fit_epoch = ~0ull;
  double fit_last_time = 0.0;
  int fit_frames = 0;
  int sort_column = -1;
  bool sort_ascending = true;
};

// 当前作用域 (GUI 线程每帧从 SharedData 取): 显示 + 与文件里 stat 的作用域 (池) 对比着色; 评估期按因子定 (所用库落盘日交集), 不在这里
struct FactorsUIContext {
  int seg = 0;                                    // 因子层 (FactorLevel; 本页已生效的, 跟页顶 / 特征面板选择走)
  std::string factor_dir;                         // factor_dir_of(pool, seg) = <factor_dir>/<池>/<层名>
  std::string pool;                               // 本页已生效的池名
  double sell_impact = 0.0;                       // Config::sell_impact (Inspect 期限结构净线的平仓侧常数; 纯显示平移, 不进请求, 改了即时生效)
  bool axis_ready = false;                        // 资产轴就绪 (数据库扫过) 才能读特征库评估
  const std::vector<AssetItem> *assets = nullptr; // 全局资产轴 (data.asset.items): Inspect 右下按全局 id 显示代码 / 名称
};

// Factors / Inspect 两页所有文字提示走这一个: 固定宽度处折行 (SetTooltip 不折行, 长句会被视口裁掉).
// 写法约定: 首行一句话说用途; 多个方面要讲时一方面一行, "• " 开头, 同类行对仗
void tip(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

// 返回值: 1 = Run (评估), 2 = Rescan (含 Add 成功后), -1 = Cancel, 3 = Search 高光行 (ui.search_row), 4 = Search all (ui.search_queue 已填),
// -3 = 取消 Search (Search all 的队列已清), 0 = 无
int RenderTabFactors(FactorsService &svc, SearchService &ss, FactorsUIState &ui, const FactorsUIContext &ctx);

} // namespace GUI::Factors
