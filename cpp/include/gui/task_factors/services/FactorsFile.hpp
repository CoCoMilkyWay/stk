// FactorsFile — 因子文件 (一因子一文件) 的读写: 扫描 ScanFactorFile → 行 FactorRow; 构建器 Add / Update / Delete; Search 落权重与结局 WriteFactorSearch;
// Run 回写 WriteFactorStats. 计算服务 (Run / Search / Inspect / Portfolio) 只经这里碰文件.
//
// 因子文件 (独立落盘, 格式破碎也**绝不删**: 表里标 BROKEN + 原因, 人手动处理):
//   { "type":   "alpha",                                      // 必有: alpha (选股 / 择时, 走 Stat) | beta (风险暴露, 未实现, 留位)
//     "name_cn": "三十分钟动量",                                // 必有: 中文名, 纯汉字 1..10 字 (英文名 = 文件名主干 name_en)
//     "expr":   "CsRank(TsMeanRoll(TsLog(amt), d))",         // 必有: **模板**, 裸键名 d / k / k2 = 待搜参数 (Expr.hpp 自由参数), 数字 = 固定
//     "note":   "…",                                          // 可选, 人 / agent 写的一句话
//     "weights": [                                            // 可选: 参数搜索 (SearchService) 写的全部权重, 每项一个具体表达式, 一行一个
//       { "hold": 30,                                         //   搜索时的持有期 (0 = 手填)
//         "sign": -1,                                         //   可选, ±1 (缺 = +1): −1 = 取反才是好因子, 按反向评估 (Stat 契约【方向 Dir】)
//         "expr": "CsRank(TsMeanRoll(TsLog(amt), d=35))" }, … ], //   模板的一个实例 (结构同模板, 固定参数相同, 自由参数落值)
//     "search": [                                             // 可选: Search 的结局, 每持有期至多一项, 一行一个 (换模板即丢)
//       { "hold": 30, "start_date", "end_date", "days",       //   评估期 (所用库落盘日交集)
//         "top": { "expr", "sign", "mono", "ls", "ric",        //   可选: 无显著时的代表点 (SearchTop), 有终选则无
//                  "g", "se", "mu", "peak" } }, … ],
//     —— 以下 Run 写 (评估派生, 换掉 / 清空 weights 时一起删; StatJson scope_json / eval_json) ——
//     "run": { scope…, "frame", "max_impact", "n_full" },     // 全部 stats 共用的口径; 回写时变了 → 别的权重 stat 清成 null
//     "hold_cols": [...], "acc_cols": [...],                  // holds / acc 数组的列名 (StatJson), 与代码不符 → 全部 stats 忽略
//     "stats": [                                              // 与 weights 同长同序 (没评的位置 null), 一行一个, 数值表密排
//       { "time", "valid_pct", "rank_pct", "eval_ms",
//         "holds": [[HoldStat]…],                             //   全部持有期 (毛收益口径), 载入时显示
//         "costs": [{amt, acc: [[CostAcc]…]}…],               //   每金额段 (amt 0 = 固定段) × 每持有期换手成本累加器 (年换手 / Inspect 净线从它算)
//         "ic_pdf": [{lo, hi, y}…], "alpha_pdf": {lo, hi, y} }, … ] } //   可选 (缺 = 因子表小图空): 每持有期逐行 rIC 分布 / alpha 分布 (MiniPdf)
//   有效 = type 合法 且 name_cn 合法 且 expr 是字串且 parse 过 (算子 / 元数 / 参数值域 / 特征存在且可作输入)
//   且根是归一算子 (Expr.hpp root_frame → 口径 CS / TS, 决定 Stat 怎么算; Stat/Contract.hpp【口径 Frame】).
//   表里一行 = 一个权重 (FactorRow::widx); 模板含自由参数而无 weights → 一行 "未搜索" (不评); 模板本身具体而无 weights → 没搜过则视作唯一权重
//   (hold 0), 搜过 (有 search) 则同 "搜过无显著" 一行 (不评).
// 出错策略: 因子文件 / 表达式不合法是正常输入 → 行 error; 回写的文件扫描时已解析过 → 不合即 assert.
#pragma once

#include "factor/Search/Contract.hpp"                     // Score (因子表 score / util 与 Search 同一份目标)
#include "gui/task_factors/services/FactorsTable.hpp"     // FeatureTable / EvalStat
#include "gui/task_factors/services/OperatorsService.hpp" // RowStatus

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace GUI::Factors {

// 因子类型 (文件 type 键): alpha 走 Stat 评估 (口径由根算子定); beta 只留位, 扫描到即标 error
enum class FactorKind : uint8_t { Alpha,
                                  Beta };
inline constexpr const char *kind_name(FactorKind k) { return k == FactorKind::Alpha ? "alpha" : "beta"; }

// 一趟 Search 的结局 (文件 search[] 一项): 评估期 + 无显著时的代表点. 统计已按 sign 定向 (Search/Contract.hpp【方向】)
struct SearchTop {
  std::string expr; // 具体规范串 (不含 sign)
  int sign = 1;
  double mono = 0.0, ls = 0.0, ric = 0.0;
  double g = 0.0, se = 0.0; // 定向目标 / 其标准误 (Contract【目标】【噪声】)
  double mu = 0.0;          // 曲面在它处的后验均值 (peak 才有)
  bool peak = false;        // 真 = mono 过门槛的峰, 差在显著 (μ ≤ SE); 假 = 没有点 mono 过门槛, 取 mono 最高的点
};
struct SearchMark {
  int hold = 0;
  misc::Ymd start_date = 0, end_date = 0; // 评估期首末
  int days = 0;
  std::optional<SearchTop> top; // 无显著且有可用候选才有
};

// 行身份 (view_file / dup_of): "<file>#<widx>"
inline std::string row_key(std::string_view file, int widx) { return std::string(file) + "#" + std::to_string(widx); }
// 一行 = 一个因子文件的一个权重 (weights[widx]); 文件级问题 (BROKEN / 未搜索 / 无显著) 的文件只出一行 widx = −1, 无可评估表达式.
// 文件级字段 (file / name_* / expr_raw / tmpl / note / kind) 同文件各行相同
struct FactorRow : EvalStat {
  std::string file;    // 文件名 (不含目录) = 因子 (父) 身份 (edit_file / Search / 删除)
  int widx = -1;       // weights 下标; −1 = 文件级行
  int hold_src = 0;    // 该权重搜索时的持有期 (0 = 手填: 文件 expr 本身具体、无 weights 且没搜过, 视作唯一权重)
  int sign = 1;        // 该权重的方向 (weights[].sign; dir_of → Stat 方向); expr 不带方向
  std::string key;     // row_key(file, widx)
  std::string name_en; // = 文件名主干 (去 .json)
  std::string name_cn; // 文件 name_cn 键 (BROKEN 行可能为空)
  FactorKind kind = FactorKind::Alpha;
  factor::stat::Frame frame = factor::stat::Frame::CS; // 有效 alpha 行的口径 (root_frame, 模板与权重同根)
  std::string expr_raw;                                // 文件 expr 原串 (可能不规范 / 不合法)
  std::string tmpl;                                    // 文件 expr 规范串 (模板, 自由参数打裸键名); 空 = 解析失败
  std::string expr;                                    // 该权重的具体规范串 (评估对象, 按 sign 的方向评); 空 = 无 (文件级行)
  std::string note;
  std::vector<SearchMark> searched; // 文件 search[] (不合的项略去)
  std::string error;                // 非空 = BROKEN (文件格式 / 表达式 / 该权重 / 组 id 数据), 文件原样留着
  std::string dup_of;               // 非空 = 与该行 (key) 同 hold 同方向同一具体规范串
  int n_ops = 0, n_feats = 0, n_slots = 0;
  RowStatus status = RowStatus::Pending; // Done = 本轮算过 (evaluate) 或本轮不算 (只扫描)
  bool has_stat = false;                 // EvalStat 有内容 (本轮算的或文件载入的)
  bool stat_from_file = false;           // stat 来自文件 (非本进程算的); EvalStat::eval_ms = time 列
};

// 因子表的目标分量 (Search/Contract.hpp【目标】, 与 Search 打分同一份 score_of): 该行 stat 在持有期 hold 下的 每期真实毛 LS / rIC / score.
// 无 stat / stat 无该持有期 / n < 3 → ok = false
factor::search::Score RowScore(const FactorRow &r, int hold);

// 一个因子文件 → 行 (每权重一行; 文件级问题一行 widx −1) 追加进 out; 解析 / 校验失败 → error (文件不动). 跨行查重 (dup_of) 不在这里.
// RunService::scan 逐文件调; Portfolio 按 (文件, 具体规范串) 找回条目的权重也走它
void ScanFactorFile(const std::filesystem::path &p, const FeatureTable &ft, std::vector<FactorRow> &out);

// 英文名 = 文件名主干: 非空, ≤64, 仅 [A-Za-z0-9_]
bool ValidFactorName(std::string_view name);
// 中文名 (文件 name_cn 键): 合法 UTF-8, 纯汉字 (CJK 统一表意文字 U+4E00..U+9FFF), 1..10 字
bool ValidFactorNameCn(std::string_view name);

// 新因子文件: 两个名字合法 (调用方保证, 断言) 且模板合法 (parse 过 + root_frame 过) →
// <factor_dir>/<name>.json = {"type": "alpha", "name_cn": …, "expr": canon, "note"?}.
// 返回文件名; 表达式不合法 / 同名已存在 → 空串 + err. GUI 线程调 (随后 Request(evaluate=false) 重扫)
std::string AddFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view name, std::string_view name_cn,
                          std::string_view expr_src, std::string_view note, std::string &err);

// 编辑已有文件 (file = 表格里的文件名, 含 .json): expr 换成 canon (模板规范串变了 → 丢 weights: 都是旧模板的实例), name_cn / note 覆盖
// (note 空 = 删键), 其他键保留 (BROKEN 非对象文件从空重建); new_name ≠ 主干 → 改名. 返回新文件名; 表达式不合法 / 改名目标已存在 → "" + err.
// 删除: 直接删文件. 两者只在服务空闲时调 (evaluate 会写回文件)
std::string UpdateFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view file, std::string_view new_name,
                             std::string_view name_cn, std::string_view expr_src, std::string_view note, std::string &err);
void DeleteFactorFile(const std::string &factor_dir, std::string_view file);

// 参数搜索落盘 (SearchService worker 调, 服务空闲时): weights 里持有期 = mk.hold 的项换成 ws (每项 {hold, sign, expr}, 无 stat; 可空 = 清掉该 h),
// search 里该 h 的项换成 mk; 不可搜档 (search_hold 假) 的项一并丢掉, 别的项原样留; fresh → 原有 weights / search 全丢, 只剩本趟的;
// weights 为空 → 删 weights 键. 全部 stat 清掉 (调用方随后 Run 重算, 因子表 score / util 等全部由 stat 来), 其他键保留.
// 文件须存在且是对象 (扫描时已解析过)
struct SearchWeight {
  int hold = 0;
  int sign = 1;     // ±1
  std::string expr; // 具体规范串 (模板实例, 不含 sign)
};
void WriteFactorSearch(const std::string &factor_dir, std::string_view file, bool fresh, const SearchMark &mk, const std::vector<SearchWeight> &ws);

// Run 回写 (一个文件一次): rs = 该文件本轮评完的权重行 (同一 run 口径), 只动 stats[widx] + 顶层 run / 表头, 其他键 (expr / note / 人加的任何东西)
// 原样保留. stats 与 weights 同长同序 (没评的位置 null); run / 表头与已有的不同 (换了区间 / 池 / 后端…) → 别的权重的 stat 不再对应 run,
// 一并清成 null (下次 Run 重算). 手填权重 (文件无 weights, hold_src = 0) 首次回写时补出 weights = [{hold 0, expr}]
void WriteFactorStats(const std::filesystem::path &path, const std::vector<const FactorRow *> &rs);

} // namespace GUI::Factors
