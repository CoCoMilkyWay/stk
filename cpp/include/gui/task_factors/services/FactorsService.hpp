// FactorsService — Factors→Factors 表的单 worker: 扫 <factor_dir>/<universe>/*.json 因子文件 → 解析 / 校验表达式
// (factor/Expr.hpp) → (Run 时) 读特征库把因子输入 + 标签装成 [T = days·kSegLen][A] 平面 → 每因子 Expr → Dag →
// CPU / GPU evaluator 算根平面 → Stat 评估 → 发布一行 + 回写该因子文件 (params / stat 键; expr 不动).
//
// 因子文件 (一因子一文件, 独立落盘, 格式破碎也**绝不删**: 表里标 BROKEN + 原因, 人手动处理):
//   { "type":   "alpha",                                      // 必有: alpha (选股 / 择时, 走 Stat) | beta (风险暴露, 未实现, 留位)
//     "name_cn": "三十分钟动量",                                // 必有: 中文名, 纯汉字 1..10 字 (英文名 = 文件名主干 name_en)
//     "expr":   "CsRank(TsMeanRoll(TsLog(amt), d=30))",      // 必有, 规范串或任意合法写法; 本服务永不改写它
//     "note":   "…",                                          // 可选, 人 / agent 写的一句话
//     "params": [ {}, {"d": 30}, {} ],                        // 可选, 按算子节点前序逐节点覆盖 expr 字面值 (搜索结果落此)
//     "stat":   { scope…, "frame", "valid_pct", "eval_ms", "max_impact", "n_full",
//                 "holds": [HoldStat…],                               // 本服务写 (全部持有期, 毛收益口径), 载入时显示
//                 "costs": [{amt, cover, acc: [CostAcc…]}…] } }        // 每金额档 × 每持有期换手成本累加器 (看板 h* / util / 净线从它算)
//   有效 = type 合法 且 name_cn 合法 且 expr 是字串且 parse 过 (算子 / 元数 / 参数值域 / 特征存在且可作输入) 且 params (若有) 覆盖成功
//   且根是归一算子 (Expr.hpp root_frame → 口径 CS / TS, 决定 Stat 怎么算; Stat/Contract.hpp【口径 Frame】).
//   同一规范串多文件 → 后者标 dup (黄), 仍算 (共享 DAG 里是同一个根, Stat 也只算一次).
//
// 线程模型 (对仗 OperatorsService): GUI 线程 Request 覆盖挂起请求 + 取消在跑 + 懒起 worker; UI 持 mutex 读 rows;
// 进度走原子. evaluate = false 只扫描 (进页 / Add / Rescan), = true 扫描 + 装载 + 评估.
//
// 装载: 一次把所有有效因子用到的特征 (去重) + ts_valid / cs_valid + 全部持有期标签列读进宿主 (逐天并行 load_day_columns,
// 门控 fmeta::valid ∧ isfinite → 值 + 掩码); 标签存 fp16 位 (与落盘同格式, Stat 直接吃).
// 两张门控列的分工 (features/MetaFlag.hpp): 特征输入按 ts_valid 门控 (TS 节点要跨进出池的历史); 标签按 cs_valid 门控 (只被 Stat 吃,
// rank(y) 得在池内排); cs_valid 的有效位另成一张截面池掩码平面 g, 原样递给 CS 节点的算子与 Stat (factor/Contract.hpp【截面池 g】:
// 池内统计 / 池外就近取值全在算子内部, 评估器不做二次门控).
// 评估 (Run 的并行方案, 与 search 的解耦): 所有有效因子合成一张共享 DAG (factor::build_forest, 公共子式只算一次), 按拓扑序
// 顺序走节点; CPU 每个节点内部切满所有核 (factor::cpu::run_node + 常驻 ForkJoin, 结果与单线程逐位一致), 根算完立刻全核 Stat, 再按槽计划
// 释放 (内存 = 峰值活槽 × T·A × 5B, 确定); GPU 同一张 DAG, 输入上传一次, 中间量常驻显存 (DevPool), Stat 走常驻会话.
// Stat 的持有期维 hd.n = n_hold ≤ kMaxHold, x 的 rank 只算一次. 标签是毛价格收益 (不含冲击 / 税佣), HoldStat 是毛口径;
// 换手成本 (lb_cost_* 每档, 买入按 Config::max_impact 封顶) 用同一份秩再攒一遍 CostAcc (FactorsLoad cost_tiers, 两后端同一份宿主代码:
// CPU 用 Workspace::ws, GPU 把 r16(x) 拷回), 落 stat.costs; 净线 / h* / util 由消费端 (看板 RowScore / Inspect plot_term) 在二级上现算,
// Config::sell_impact 只进公式不进文件. 看板的全部数据都来自 stat —— Search 只内部用同一份目标挑参数, 不往文件写结论.
// time 列 (eval_ms) = 该因子子树全部节点 wall 之和 (共享节点算给每个用它的因子 = 单独算它要多久).
// 出错策略: 因子文件 / 表达式不合法是正常输入 → 行 error; 特征库 / 契约不一致 → assert.
#pragma once

#include "codec/L2_DataType.hpp" // L2::ValidType
#include "factor/Exec.hpp"
#include "factor/Expr.hpp"
#include "factor/Search/Contract.hpp" // Score / Objective (看板 h* / util 与 Search 同一份目标)
#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/OperatorsService.hpp" // RowStatus
#include "shared/Analysis.hpp"                            // ReadScope
#include "shared/Feature.hpp"                             // Feature::Metadata

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

struct SharedData;

namespace GUI::Factors {

// ---- 字段表视图 (GUI 线程从 Feature::Metadata 建一次, worker 只读) ----
struct FeatCol {
  std::string code;
  uint32_t col = 0; // L1 列下标
  L2::ValidType vt = L2::ValidType::ALL;
  bool allowed = false; // 可作因子输入 (TS / CS 特征); 标签 / ts_valid / cs_valid 不许 (前视)
};
struct LabelCol {
  int hold = 0;                         // 持有期键 (Stat/Contract.hpp)
  uint32_t long_col = 0, short_col = 0; // 列下标
};
struct CostCol {
  int amt = 0;                        // 下单金额档 (万)
  uint32_t buy_col = 0, sell_col = 0; // lb_cost_buy_<amt>w / lb_cost_sell_<amt>w 列下标
};
struct FeatureTable {
  std::vector<FeatCol> cols;    // 下标 = L1 列
  uint32_t ts_col = 0;          // ts_valid 门控列 (特征输入)
  uint32_t cs_col = 0;          // cs_valid 门控列 (标签 + 截面门控平面)
  std::vector<LabelCol> labels; // 按 hold 升序; long / short 都齐才收
  std::vector<CostCol> costs;   // 建仓冲击成本特征, 按 amt 升序 (Run / Inspect 每档攒 CostAcc; Search 只用选定一档)
  const FeatCol *find(std::string_view code) const;
  factor::expr::FeatureLookup lookup() const; // 给 Expr::parse 注入
};
// LB 列按 code 识别 (LabelReturn 生成的字段行): lb_<long|short>_<name> 标签 (name = <n>m / close / t<N> → 持有期键, Stat/Contract.hpp);
// lb_cost_<buy|sell>_<amt>w 建仓冲击成本
FeatureTable BuildFeatureTable(const Feature::Metadata &meta);

// 本地时间 "YYYY-MM-DD HH:MM" (stat.time / search.time 落盘用)
std::string now_string();

// ---- 请求 (GUI 线程解析 config, worker 不碰) ----
struct FactorsRequest {
  std::string factor_dir; // <config.factor_dir>/<universe>
  bool evaluate = false;  // false = 只扫描解析
  // 以下 evaluate 时才用
  analysis::ReadScope scope;
  std::string universe, start_date, end_date;
  bool gpu = false;
  double max_impact = 0.0; // Config::max_impact (买入冲击封顶, 成本装载时生效)
};
// evaluate 请求需要资产轴 (读特征库); 轴未就绪 (数据库未扫) → false
bool MakeFactorsRequest(const SharedData &data, bool evaluate, bool gpu, FactorsRequest &req);

// ---- 行 ----
// 因子类型 (文件 type 键): alpha 走 Stat 评估 (口径由根算子定); beta 只留位, 扫描到即标 error
enum class FactorKind : uint8_t { Alpha,
                                  Beta };
inline constexpr const char *kind_name(FactorKind k) { return k == FactorKind::Alpha ? "alpha" : "beta"; }
struct StatScope {
  std::string universe, start_date, end_date, backend, time;
  int days = 0, T = 0, A = 0;
  double max_impact = 0.0; // 买入冲击封顶 (Config::max_impact, 成本装载时生效)
  double n_full = 0.0;     // 满仓一腿只数 = 池内平均有效票数 / kGroups (util 分母; 池宽决定, 与因子无关)
};
// 一个金额档的换手成本 = Stat 契约【换手成本】的 CostAcc, 每持有期一份 (Run / Inspect / Search 同一份代码: Stat/Host.hpp cost_row):
// 顶 / 底组按真实换手扣, 每 (t, h) 行只有新进者 (t−h 不在该组) 付建仓冲击 + sell_impact; 均摊成本 long_cost / short_cost 随 h 变 (h 越长留仓越多).
// 买入冲击按 Config::max_impact 封顶后进 (装载时). 吃不满深度的格 (成本 NaN) 不计入 → 高档均值偏低, cover 把它亮出来.
// Run 每因子每档都攒 (落 stat.costs), 看板 h* / util 列 + Inspect 期限结构净线都从它算
struct CostTier {
  int amt = 0;                                            // 万 (= FeatureTable::costs[c].amt)
  factor::stat::CostAcc acc[factor::stat::kMaxHold] = {}; // [hd.n], 与 hold 同序
  float cover = 0.f;                                      // Σ 成本有效 / Σ rx 有效 (格数比, 不分组不分 h)
};
struct FactorRow {
  std::string file;    // 文件名 (不含目录) = 行的身份 (view_file / edit_file / dup_of 都用它)
  std::string name_en; // = 文件名主干 (去 .json)
  std::string name_cn; // 文件 name_cn 键 (BROKEN 行可能为空)
  FactorKind kind = FactorKind::Alpha;
  factor::stat::Frame frame = factor::stat::Frame::CS; // 有效 alpha 行的口径 (root_frame)
  std::string expr_raw;                                // 文件里的原串 (可能不规范 / 不合法)
  std::string expr;                                    // 规范串 (含当前参数); 空 = 解析失败
  std::string note;
  std::string error;  // 非空 = BROKEN (文件格式 / 表达式 / params / 组 id 数据), 文件原样留着
  std::string dup_of; // 非空 = 与该文件同一规范串
  int n_ops = 0, n_feats = 0, n_slots = 0;
  RowStatus status = RowStatus::Pending; // Done = 本轮算过 (evaluate) 或本轮不算 (只扫描)
  bool has_stat = false;                 // hold[][] 有内容 (本轮算的或文件载入的)
  bool stat_from_file = false;           // stat 来自文件 (非本进程算的)
  StatScope scope;                       // stat 的作用域
  double eval_ms = 0;                    // time 列: 子树算子节点 wall 之和 (与 hold 无关: 因子平面只算一遍, Stat 按持有期复用它)
  float valid_pct = 0.f;
  int n_hold = 0;
  factor::stat::HoldStat hold[factor::stat::kMaxHold]; // [hold], 毛收益口径
  std::vector<CostTier> costs;                         // [amt] = FeatureTable::costs 序, 每档 [n_hold] CostAcc (净线 / h* / util 从它算)
  const factor::stat::HoldStat *find_hold(int hold_m) const {
    for (int j = 0; j < n_hold; ++j)
      if (hold[j].hold == hold_m)
        return &hold[j];
    return nullptr;
  }
  const CostTier *find_cost(int amt) const {
    for (const CostTier &c : costs)
      if (c.amt == amt)
        return &c;
    return nullptr;
  }
};

// 看板的目标分量 (Search/Contract.hpp【目标】, 与 Search 打分同一份 score_of): 该行 stat 在金额档 amt 下的 h* / util / 年化净多空 / score.
// 无 stat / stat 无该档 (旧文件) → best < 0. hold 窗 / ric_pow 取契约默认; n_full 来自 stat 作用域
factor::search::Score RowScore(const FactorRow &r, int amt, double sell_impact);

enum class FactorsStatus : uint8_t { Idle,
                                     Scanning,
                                     Loading,
                                     Running,
                                     Done,
                                     Cancelled };

class FactorsService {
public:
  FactorsService() = default;
  ~FactorsService() { Stop(); }
  FactorsService(const FactorsService &) = delete;
  FactorsService &operator=(const FactorsService &) = delete;

  void SetFeatureTable(FeatureTable &&t) { feats_ = std::move(t); } // worker 未起时 (进页) 调一次
  const FeatureTable &feats() const { return feats_; }

  // GUI 线程
  void Request(const FactorsRequest &req);
  void RequestCancel() { cancel_.store(true, std::memory_order_relaxed); }
  void Stop();

  // 进度 (原子, 免锁): Loading 期 done/total = 天; Running 期 = 因子
  FactorsStatus status() const { return status_.load(std::memory_order_acquire); }
  int done() const { return done_.load(std::memory_order_relaxed); }
  int total() const { return total_.load(std::memory_order_relaxed); }
  int broken() const { return broken_.load(std::memory_order_relaxed); }
  uint64_t epoch() const { return epoch_.load(std::memory_order_relaxed); }

  // UI 持锁读
  std::mutex mutex;
  std::vector<FactorRow> rows;
  FactorsRequest current;
  std::string message; // 本轮说明 (如 "特征库无数据"), 空 = 无

private:
  void worker_loop();
  void scan(const FactorsRequest &req, std::vector<FactorRow> &out);
  bool evaluate(const FactorsRequest &req, factor::exec::ForkJoin &ex); // false = 取消

  FeatureTable feats_;
  std::thread thread_;
  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<FactorsRequest> pending_;
  std::atomic<bool> cancel_{false};
  std::atomic<bool> stop_{false};
  std::atomic<FactorsStatus> status_{FactorsStatus::Idle};
  std::atomic<int> done_{0}, total_{0}, broken_{0};
  std::atomic<uint64_t> epoch_{0};
};

// 英文名 = 文件名主干: 非空, ≤64, 仅 [A-Za-z0-9_]
bool ValidFactorName(std::string_view name);
// 中文名 (文件 name_cn 键): 合法 UTF-8, 纯汉字 (CJK 统一表意文字 U+4E00..U+9FFF), 1..10 字
bool ValidFactorNameCn(std::string_view name);

// 新因子文件: 两个名字合法 (调用方保证, 断言) 且表达式合法 (parse 过 + root_frame 过) →
// <factor_dir>/<name>.json = {"type": "alpha", "name_cn": …, "expr": canon, "note"?, "params": [...]}.
// 返回文件名; 表达式不合法 / 同名已存在 → 空串 + err. GUI 线程调 (随后 Request(evaluate=false) 重扫)
std::string AddFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view name, std::string_view name_cn,
                          std::string_view expr_src, std::string_view note, std::string &err);

// 编辑已有文件 (file = 表格里的文件名, 含 .json): expr 换成 canon (规范串变了 → 丢 stat), name_cn / note 覆盖 (note 空 = 删键),
// params 重建, 其他键保留 (BROKEN 非对象文件从空重建); new_name ≠ 主干 → 改名. 返回新文件名; 表达式不合法 / 改名目标已存在 → "" + err.
// 删除: 直接删文件. 两者只在服务空闲时调 (evaluate 会写回文件)
std::string UpdateFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view file, std::string_view new_name,
                             std::string_view name_cn, std::string_view expr_src, std::string_view note, std::string &err);
void DeleteFactorFile(const std::string &factor_dir, std::string_view file);

// 参数搜索落盘 (SearchService worker 调, 服务空闲时): params 换成 e 的 (expr 键不动, 规范串仍由 expr + params 还原), 旧 stat 删掉
// (是别的参数算的; 调用方随后 Run 重算, 看板 h* / util 等全部由 stat 来), 其他键保留. 文件须存在且是对象 (扫描时已解析过)
void WriteFactorParams(const std::string &factor_dir, std::string_view file, const factor::expr::Expr &e);

} // namespace GUI::Factors
