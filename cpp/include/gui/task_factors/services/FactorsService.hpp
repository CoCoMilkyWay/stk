// FactorsService — Factors→Factors 表的单 worker: 扫 <factor_dir>/<pool>/*.json 因子文件 → 解析 / 校验模板与权重表达式
// (factor/Expr.hpp) → (Run 时) 读特征库把因子输入 + 标签装成 [T = days·seg][A] 平面 (seg = 因子层, 见 FactorLevel) → 每权重 Expr → Dag →
// CPU / GPU evaluator 算根平面 → Stat 评估 → 发布一行 + 回写该权重的 stat (stats[i]; expr / weights 不动).
//
// 因子文件 (一因子一文件, 独立落盘, 格式破碎也**绝不删**: 表里标 BROKEN + 原因, 人手动处理):
//   { "type":   "alpha",                                      // 必有: alpha (选股 / 择时, 走 Stat) | beta (风险暴露, 未实现, 留位)
//     "name_cn": "三十分钟动量",                                // 必有: 中文名, 纯汉字 1..10 字 (英文名 = 文件名主干 name_en)
//     "expr":   "CsRank(TsMeanRoll(TsLog(amt), d))",         // 必有: **模板**, 裸键名 d / k / k2 = 待搜参数 (Expr.hpp 自由参数), 数字 = 固定
//     "note":   "…",                                          // 可选, 人 / agent 写的一句话
//     "weights": [                                            // 可选: 参数搜索 (SearchService) 写的全部权重, 每项一个具体表达式, 一行一个
//       { "hold": 30,                                         //   搜索时的持有期 (0 = 手填)
//         "sign": -1,                                         //   可选, ±1 (缺 = +1): −1 = 取反才是好因子, 评估根下插 TsNeg (Expr negated_canon)
//         "expr": "CsRank(TsMeanRoll(TsLog(amt), d=35))" }, … ], //   模板的一个实例 (结构同模板, 固定参数相同, 自由参数落值)
//     —— 以下本服务写 (评估派生, 换掉 / 清空 weights 时一起删) ——
//     "run": { scope…, "frame", "max_impact", "n_full" },     // 全部 stats 共用的口径; 回写时变了 → 别的权重 stat 清成 null
//     "hold_cols": [...], "acc_cols": [...],                  // holds / acc 数组的列名 (StatJson), 与代码不符 → 全部 stats 忽略
//     "stats": [                                              // 与 weights 同长同序 (没评的位置 null), 一行一个, 数值表密排
//       { "time", "valid_pct", "rank_pct", "eval_ms",
//         "holds": [[HoldStat]…],                             //   全部持有期 (毛收益口径), 载入时显示
//         "turn": [[CostAcc]…],                               //   每持有期两腿换手量 (只量; 因子表年换手)
//         "costs": [{amt, acc: [[CostAcc]…]}…],               //   每金额段 × 每持有期换手成本累加器 (Inspect 净线从它算)
//         "ic_pdf": [{lo, hi, y}…], "alpha_pdf": {lo, hi, y} }, … ] } //   可选 (缺 = 因子表小图空): 每持有期逐行 rIC 分布 / alpha 分布 (MiniPdf)
//   有效 = type 合法 且 name_cn 合法 且 expr 是字串且 parse 过 (算子 / 元数 / 参数值域 / 特征存在且可作输入)
//   且根是归一算子 (Expr.hpp root_frame → 口径 CS / TS, 决定 Stat 怎么算; Stat/Contract.hpp【口径 Frame】).
//   表里一行 = 一个权重 (FactorRow::widx); 模板含自由参数而无 weights → 一行 "未搜索" (不评); 模板本身具体而无 weights → 视作唯一权重 (hold 0).
//   同一具体规范串多行 (跨文件 / 同文件) → 后者标 dup (黄), 仍算 (同文件的在共享 DAG 里是同一个根, Stat 也只算一次).
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑; UI 持 mutex 读 rows;
// 进度走原子. evaluate = false 只扫描 (进页 / Add / Rescan), = true 扫描 + 装载 + 评估.
//
// 装载: 逐因子文件 (各文件评估期 / A 轴不同, 见 FactorLevel) 把该文件用到的特征 (去重, 跨库跨层 level_asof 投影) + 标签库 ts_valid / cs_valid
// + 全部持有期标签列读进宿主 (逐天并行, 门控 fmeta::valid ∧ isfinite → 值 + 掩码); 标签存 fp16 位 (与落盘同格式, Stat 直接吃).
// 装载前按宿主内存预算断言放得下 (FactorsLoad assert_fits; 放不下 = 该因子层太细 / 评估期太长).
// 两张门控列的分工 (features/MetaFlag.hpp): 特征输入按 ts_valid 门控 (TS 节点要跨进出池的历史); 标签按 cs_valid 门控 (只被 Stat 吃,
// rank(y) 得在池内排); cs_valid 的有效位另成一张截面池掩码平面 g, 原样递给 CS 节点的算子与 Stat (factor/Contract.hpp【截面池 g】:
// 池内统计 / 池外就近取值全在算子内部, 评估器不做二次门控).
// 评估 (Run 的并行方案, 与 search 的解耦): 一个文件的全部权重合成一张共享 DAG (factor::build_forest, 公共子式只算一次), 按拓扑序
// 顺序走节点; CPU 每个节点内部切满所有核 (factor::cpu::run_node + 常驻 ForkJoin, 结果与单线程逐位一致), 根算完立刻全核 Stat, 再按槽计划
// 释放 (内存 = 峰值活槽 × T·A × 5B, 确定); GPU 同一张 DAG, 输入上传一次, 中间量常驻显存 (DevPool), Stat 走常驻会话.
// Stat 的持有期维 hd.n = n_hold ≤ kMaxHold, x 的 rank 只算一次. 标签是毛价格收益 (不含冲击 / 税佣), HoldStat 是毛口径;
// 换手成本 (lb_cost_* 每金额段, 买入按 Config::max_impact 封顶) 用同一份秩再攒一遍 CostAcc (FactorsProbe cost_tiers, 两后端同一份宿主代码:
// CPU 用刚算的 rx, GPU 把 r16(x) 拷回), 落 stat.costs; 换手量 (与金额段无关, 无段的层也攒) 落 stat.turn; 净线由 Inspect plot_term 在二级上现算, 因子表 score (排序 / 选代表权重) 由 RowScore 从该 h 的 HoldStat 算,
// Config::fixed_impact 只进公式不进文件. 因子表的全部数据都来自 stat —— Search 只内部用同一份目标挑参数, 不往文件写结论.
// time 列 (eval_ms) = 该因子子树全部节点 wall 之和 (共享节点算给每个用它的因子 = 单独算它要多久).
// 出错策略: 因子文件 / 表达式不合法是正常输入 → 行 error; 特征库 / 契约不一致 → assert.
#pragma once

#include "factor/Search/Contract.hpp"                     // Score (因子表 score / util 与 Search 同一份目标)
#include "gui/task_factors/services/FactorsTable.hpp"     // FeatureTable / FactorScope / EvalStat
#include "gui/task_factors/services/OperatorsService.hpp" // RowStatus
#include "gui/util/Service.hpp"

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

struct SharedData;

namespace GUI::Factors {

enum class Rc : uint8_t; // FactorsLoad.hpp
struct FactorFlow;       // FactorsEngine.hpp

// 因子服务的进度面 + 本轮说明 + 库判废收尾 (Run / Search / Inspect / Portfolio 同一份)
struct FactorWorker : Published {
  std::string message; // 本轮说明 (如 "特征库无数据"), 空 = 无; UI 持 mutex 读

protected:
  bool abort_out(Rc rc, FactorFlow &F); // 中断收尾 (rc 须 aborted): Stale → 弃作用域块 + 置 message; 恒 false
};

struct RunSpec {
  bool evaluate = false; // false = 只扫描解析 (不读库)
  std::string only_file; // 非空 = 只评这个因子文件的权重 (Search 写完后), 其余行保留文件里的 stat
};
using FactorsRequest = Request<RunSpec>;
// 只扫描恒成; evaluate 要作用域可用 (MakeFactorScope). ft = 该层字段表
bool MakeFactorsRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, bool evaluate, bool gpu,
                        FactorsRequest &req);

// ---- 行 ----
// 因子类型 (文件 type 键): alpha 走 Stat 评估 (口径由根算子定); beta 只留位, 扫描到即标 error
enum class FactorKind : uint8_t { Alpha,
                                  Beta };
inline constexpr const char *kind_name(FactorKind k) { return k == FactorKind::Alpha ? "alpha" : "beta"; }
// 行身份 (view_file / dup_of): "<file>#<widx>"
inline std::string row_key(std::string_view file, int widx) { return std::string(file) + "#" + std::to_string(widx); }
// 一行 = 一个因子文件的一个权重 (weights[widx]); 文件级问题 (BROKEN / 模板未搜索) 的文件只出一行 widx = −1, 无可评估表达式.
// 文件级字段 (file / name_* / expr_raw / tmpl / note / kind) 同文件各行相同
struct FactorRow : EvalStat {
  std::string file;    // 文件名 (不含目录) = 因子 (父) 身份 (edit_file / Search / 删除)
  int widx = -1;       // weights 下标; −1 = 文件级行
  int hold_src = 0;    // 该权重搜索时的持有期 (0 = 手填: 文件 expr 本身具体且无 weights, 视作唯一权重)
  int sign = 1;        // 该权重的方向 (weights[].sign); −1 → expr 是根下插了 TsNeg 的
  std::string key;     // row_key(file, widx)
  std::string name_en; // = 文件名主干 (去 .json)
  std::string name_cn; // 文件 name_cn 键 (BROKEN 行可能为空)
  FactorKind kind = FactorKind::Alpha;
  factor::stat::Frame frame = factor::stat::Frame::CS; // 有效 alpha 行的口径 (root_frame, 模板与权重同根)
  std::string expr_raw;                                // 文件 expr 原串 (可能不规范 / 不合法)
  std::string tmpl;                                    // 文件 expr 规范串 (模板, 自由参数打裸键名); 空 = 解析失败
  std::string expr;                                    // 该权重的具体规范串 (评估对象, 已含 sign); 空 = 无 (文件级行)
  std::string note;
  std::string error;  // 非空 = BROKEN (文件格式 / 表达式 / 该权重 / 组 id 数据), 文件原样留着
  std::string dup_of; // 非空 = 与该行 (key) 同 hold 同一具体规范串
  int n_ops = 0, n_feats = 0, n_slots = 0;
  RowStatus status = RowStatus::Pending;  // Done = 本轮算过 (evaluate) 或本轮不算 (只扫描)
  bool has_stat = false;                  // hold[][] 有内容 (本轮算的或文件载入的)
  bool stat_from_file = false;            // stat 来自文件 (非本进程算的); EvalStat::eval_ms = time 列
  MiniPdf ic_pdf[factor::stat::kMaxHold]; // [hold] 逐行 rIC (ok 行) 分布, 与 Inspect 左下左半同一组样本
  MiniPdf alpha_pdf;                      // alpha 分布 (截尾, 与 Inspect 左下右半同一份 alpha_dist; 与持有期无关)
  const MiniPdf *find_ic_pdf(int hold_m) const {
    const factor::stat::HoldStat *h = find_hold(hold_m);
    return h ? &ic_pdf[h - hold] : nullptr;
  }
};

// 因子表的目标分量 (Search/Contract.hpp【目标】, 与 Search 打分同一份 score_of): 该行 stat 在持有期 hold 下的 每期真实毛 LS / rIC / score.
// 无 stat / stat 无该持有期 / n < 3 → ok = false
factor::search::Score RowScore(const FactorRow &r, int hold);

// 一个因子文件 → 行 (每权重一行; 文件级问题一行 widx −1) 追加进 out; 解析 / 校验失败 → error (文件不动). 跨行查重 (dup_of) 不在这里.
// FactorsService::scan 逐文件调; Portfolio 按 (文件, 具体规范串) 找回条目的权重也走它
void ScanFactorFile(const std::filesystem::path &p, const FeatureTable &ft, std::vector<FactorRow> &out);

// 进度面 = 自身 (FactorWorker): Scanning → (evaluate) Loading 期 done/total = 天 → Running 期 = 共享 DAG 算子节点
class FactorsService : public Service<FactorsService, FactorsRequest>, public FactorWorker {
  friend Service<FactorsService, FactorsRequest>;

public:
  static constexpr const char *kName = "FactorsWorker";
  FactorsService(); // 两者都在 .cpp: FactorFlow 不完整
  ~FactorsService();

  int broken() const { return broken_.load(std::memory_order_relaxed); }

  // UI 持 mutex 读
  std::vector<FactorRow> rows;
  FactorsRequest current;

private:
  static constexpr Phase kStart = Phase::Scanning;
  Progress &board() { return *this; }
  bool run(FactorsRequest &req);
  void scan(const FactorsRequest &req, std::vector<FactorRow> &out);
  bool evaluate(const FactorsRequest &req, FactorFlow &F); // false = 取消 / 判废
  // 一个因子文件的全部待评权重 (idx = rows 下标, 同文件同模板 → 同叶 → 同评估期 / A 轴): 管线 eval_job (共享森林, 作用域块 F.c 补装)
  // 走引擎 E (FactorsEngine Engine<Cpu | Gpu>, 本轮一个). Ok (含该文件 Empty / Broken: 行已标) / Cancel / Stale
  template <class E>
  Rc evaluate_file(const FactorsRequest &req, FactorFlow &F, E &eng, const std::vector<size_t> &idx, const std::vector<factor::expr::Expr> &exprs,
                   const std::vector<factor::stat::Frame> &frames);

  std::unique_ptr<FactorFlow> flow_; // 首个 evaluate 懒建, 随 worker 活 (作用域块每轮用完即弃)
  std::atomic<int> broken_{0};
};

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

// 参数搜索落盘 (SearchService worker 调, 服务空闲时): weights 里持有期 = hold 的项换成 ws (每项 {hold, sign, expr}, 无 stat), 别的项原样留;
// 全部 stat 清掉 (调用方随后 Run 重算, 因子表 score / util 等全部由 stat 来), 其他键保留. 文件须存在且是对象 (扫描时已解析过)
struct SearchWeight {
  int hold = 0;
  int sign = 1;     // ±1
  std::string expr; // 具体规范串 (模板实例, 不含 sign)
};
void WriteFactorWeights(const std::string &factor_dir, std::string_view file, int hold, const std::vector<SearchWeight> &ws);

} // namespace GUI::Factors
