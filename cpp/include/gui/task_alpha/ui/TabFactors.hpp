// Alpha→Factors: output/factors/<池>/<层>/ 下一因子一文件的整体面板. 顶部 = 作用域 / 后端 / 持有期 + Run / Cancel /
// Rescan + 状态, 下一行 Search (高光因子的模板参数搜索: 进度 / 每持有期的终选); 中间 = 加因子构建器 (从根往里逐槽下拉选算子 / 特征,
// 参数手填或勾成自由参数, 即时校验); 下面 = 表 (一行一权重, 多权重因子折成父行 + 可展开的子行: BROKEN 红 + 原因, 有效行显示规范串 /
// 结构规模 / 选定持有期的 Stat 列 / 耗时; 首列勾选 → 模板载入构建器编辑, 点行选中 (Ctrl 多选 / Shift 片选) → Run / Search 批量的对象, 锚点给 Inspect 页).
#pragma once

#include "gui/engine/Table.hpp"
#include "gui/engine/services/RunService.hpp"
#include "gui/engine/services/SearchService.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct AssetItem;

namespace GUI::Engine {
struct PortfolioUIState; // PortfolioBoxes.hpp
}

namespace GUI::Alpha {

using namespace Engine;

// 构建器的一个槽: 未选 / 特征叶 / 算子 (带 arity 个子槽 + 参数). 从外到内填: 先选根算子, 再逐槽往里选
struct BuildNode {
  int op = -1;                 // factor::expr::kOps 下标; -1 = 叶
  std::string feat;            // 叶: 特征 code (空 = 未选)
  std::vector<BuildNode> args; // 算子: arity 个子槽
  factor::Param p;             // 算子参数 (选中时按 Check.hpp 默认表填, 之后手改)
  uint8_t free = 0;            // 自由参数位 (expr::kFreeD / K / K2 / H / W): 源串打裸键名, 由 Search 搜
  std::string tag[3];          // 自由 d / k / w 的绑定名 (expr::tag_ix 下标; 空 = 独立), 源串打 键:名
};

// 一次 Search 的对象: 模板行 + 持有期键 (search_hold) + 落盘前是否清空文件全部 weights (SearchSpec::fresh)
struct SearchJob {
  FactorRow row;
  HoldId hold = kHoldT1;
  bool fresh = false;
};

struct FactorsUIState {
  int hold_idx = 0;            // FeatureTable::y.holds 下标 (表列显示哪个持有期; 多权重因子的父行取该 h 下 score 最高的权重)
  bool hold_defaulted = false; // 本作用域已按多数 *h 定过默认 Hold (DefaultHold; 换池 / 换层由 TaskAlpha 复位)
  BuildNode build;             // 加因子构建器的根
  char name_buf[64] = "";      // name_en = 文件名主干, 必填
  char name_cn_buf[64] = "";   // name_cn, 必填, 汉字 / [A-Za-z0-9#] ≤10 字 (≤30 字节)
  char note_buf[256] = "";
  char filter_buf[64] = ""; // 下拉里的过滤框 (打开时清空)
  std::string add_err;      // 即时校验结果 (空 = 合法 / 未填完)
  std::string add_msg;      // 上次 Add / Save / 删除 的结果
  // 两种模式: edit_file 空 = 添加模式 (Add 新文件, 查重); 非空 = 编辑模式 (表格首列勾选的那个文件, 同时只勾一个; Save 覆盖 / 删除该文件).
  // 勾选 → 模板载入构建器 + name_en + name_cn + note; 取消勾选 → 回添加模式但构建器内容保留 (当模板)
  std::string edit_file;
  // 点行高光 = 单权重展示 (Inspect 页) 的对象 (FactorRow::key), 与 edit_file 互不干涉; 再点同一行 → 取消.
  // 多权重因子: 父行 = 选定 Hold 下 score 最高的权重, 点父行即高光它; 展开 (expanded 里的文件) 后可点各权重子行
  // 扫出表后默认高光当前 Hold 下 rIC 最高的有效权重 (InspectAutoRequest 置, 每作用域一次: 之后用户取消高光不再自动补; 换池 / 换层由 TaskAlpha 复位)
  std::string view_file;
  bool view_defaulted = false;
  // 多选 (行 key, 选序): 恒含锚点 view_file (view_file 空 → 空); 单击 = 只选本行, Ctrl 单击 = 本行进出, Shift 单击 = 锚点到本行 (显示序) 片选 (同按 Ctrl 并进已选).
  // 选中行所在文件 = Run / Search 的批量对象
  std::vector<std::string> picked;
  std::vector<std::string> expanded;    // 展开的多权重因子文件
  std::vector<std::string> shown_files; // 表的显示序 (按文件去重, 每帧画表时更新): Run / Search / Search all 逐文件按它的先后
  // Run (对象 = 选中行所在文件, 只评它们的全部权重) / Run all (整目录): 点 Run 时把文件名记进 run_files (TaskAlpha 据此建请求 only_files)
  std::vector<std::string> run_files;
  // Search 队列 (一个文件的各档搜完且落过盘即只 Run 该文件, 结果立刻进表), 点下时把 (各文件首个可搜行, 档) 拷进队列, TaskAlpha 逐个出队:
  //   · Search     选中行所在文件 × 当前 Hold (须在 Search/Config kSearchHolds), 只换该 Hold 的权重
  //   · Search all 当前池 / 层下全部可搜文件 × 本层 kSearchHolds 内各档, 不看 Hold 选择; 每文件首档 fresh (该文件 weights 整份覆盖), 它没落盘 (中途放弃)
  //                则 fresh 顺延给同文件下一档
  // Cancel = 取消在跑的 Search + 清空队列. search_n > 0 期间视同 Search 在跑 (队列项之间 / 单文件 Run 时服务空闲, 不许插别的请求 / 换池换层)
  std::vector<SearchJob> search_queue;
  SearchJob search_cur;  // 在跑的那一项 (fresh 顺延用)
  int search_n = 0;      // 本轮队列项总数; 0 = 队列没在跑
  int popup = 0;         // 本帧要开的弹窗: 1 Save 确认, 2 删除确认, 3 不能加, 4 换收益口径确认 (表格里置, EndTable 后开)
  std::string popup_msg; // 弹窗正文
  FactorRow type_row;    // type 列点下的行 (换收益口径确认弹窗的对象; 确认后清空该文件结果, 按新口径重搜)
  // 表列宽贴合 (对仗 TabOperators)
  uint64_t fit_epoch = ~0ull;
  double fit_last_time = 0.0;
  int fit_frames = 0;
  int sort_column = -1;
  bool sort_ascending = true;
};

// 当前作用域 (GUI 线程每帧从 SharedData 取): 显示 + 与文件里 stat 的作用域 (池 / 评估年数) 对比着色; 评估期按因子定 (所用库落盘日交集的最近 years 年), 不在这里
struct FactorsUIContext {
  FactorLevelId lv = 0;                           // 因子层 (本页已生效的, 跟页顶 / 特征面板选择走) = ft->lv
  std::shared_ptr<const FeatureTable> ft;         // 该层字段表 (各服务请求同一份)
  Engine::FactorDir dir;                          // factor_dir_of(pool, lv) = output/factors/<池>/<层名>
  std::string pool;                               // 本页已生效的池名
  int years = 0;                                  // 本页已生效的评估年数 (评估期只留最近几年)
  Fee fee;                                        // Config 费率 (Inspect 期限结构净线的冲击 + 税佣; 纯显示平移, 不进请求, 改了即时生效)
  bool axis_ready = false;                        // 资产轴就绪 (数据库扫过) 才能读特征库评估
  const std::vector<AssetItem> *assets = nullptr; // 全局资产轴 (data.asset.items): Inspect 右上按全局 id 显示代码 / 名称
};

// 默认 Hold = 本作用域表里多数权重的 *h (每作用域一次; 都是手填权重 → 不动); 本作用域的表还没扫出 → 下帧再试. Factors / Inspect 两页每帧先调
void DefaultHold(RunService &svc, FactorsUIState &ui, const FactorsUIContext &ctx);
// hold_points 分色一行 (时点 / 价各一色), 从当前光标处画; 返回排版宽. draw = false 只量不画 (调用方定宽占位用)
float hold_points_line(const HoldPoints &p, bool draw = true);
// 紧凑切换控件 (替代下拉): "label ◂ value ▸", 方形箭头循环切, 值占位定宽 (最宽一项, 居中) → 按钮位置不随值跳;
// 只有两项时省掉箭头, 定宽按钮点一下 toggle. 返回是否切了. 整体一个 Group, 调用方紧接着 IsItemHovered() 对整个控件生效
bool cycler(const char *label, int &idx, const char *const *values, int n);
// 持有期切换 "Hold ◂ 周期名 ▸" (cycler, idx = ft.y.holds 下标); 不足两项只显示名字. 返回是否切了
bool hold_cycler(int &hold_idx, const FeatureTable &ft);
// Hold 提示的公共尾段 = define/Label.hpp【协议表 1 / 2】的 UI 摘要. 细节: 开仓 = 本层行收盘 C(层) 出信号, 日层 = 当日收盘建仓;
// 次分钟VWAP = 之后首个有成交连续竞价分钟 (各分钟层); 收盘 / 开盘撮合价 = 集合竞价单一价;
// 仓位感知 (factor/Stat/Pos.hpp): 新进须本侧可成交, 交接的续持者封板照持 (名义价估值); 平仓不可成交 / 停牌 → 顺延到首个可成交价点
inline constexpr const char *kHoldRulesTip = "• 新进成交不了 → 不建仓; 已持有的续持不需成交 (封板照持); 平仓成交不了 → 顺延到可成交时";
// 单个持有期的 stat 面板 (Inspect 顶部右栏): 关键指标按组分列 (rIC | LS | 覆盖 | 形态), 每格 指标名 + 值; 定义 / 格式 / 着色区间与因子表同一份,
// 悬停值给说明. h = e.hold[] 之一; 占满当前可用宽
void hold_stat_panel(const EvalStat &e, const factor::stat::HoldStat &h);

// 返回值: 1 = Run 选中行所在文件 (ui.run_files), 5 = Run all (整目录), 2 = Rescan (含 Add 成功后), -1 = Cancel,
// 4 = Search / Search all (ui.search_queue 已填), -3 = 取消 Search (队列已清), 0 = 无.
// backend = EngineHub::backend (首帧按 GPU 可用性定); pui: 1. 区右侧组合两小框 (PortfolioBoxes RenderPortfolioBoxes)
int RenderTabFactors(RunService &svc, SearchService &ss, FactorsUIState &ui, int &backend, PortfolioUIState &pui, const FactorsUIContext &ctx);

} // namespace GUI::Alpha