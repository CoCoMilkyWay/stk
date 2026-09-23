// FactorsService — Factors→Factors 表的单 worker: 扫 <factor_dir>/<universe>/*.json 因子文件 → 解析 / 校验表达式
// (factor/Expr.hpp) → (Run 时) 读特征库把因子输入 + 标签装成 [T = days·kSegLen][A] 平面 → 每因子 Expr → Dag →
// CPU / GPU evaluator 算根平面 → Stat 评估 → 发布一行 + 回写该因子文件 (params / stat 键; expr 不动).
//
// 因子文件 (一因子一文件, 独立落盘, 格式破碎也**绝不删**: 表里标 BROKEN + 原因, 人手动处理):
//   { "expr":   "CsRank(TsMeanRoll(TsLog(amt), d=30))",      // 必有, 规范串或任意合法写法; 本服务永不改写它
//     "note":   "…",                                          // 可选, 人 / agent 写的一句话
//     "params": [ {}, {"d": 30}, {} ],                        // 可选, 按算子节点前序逐节点覆盖 expr 字面值 (搜索结果落此)
//     "stat":   { scope…, "valid_pct", "eval_ms",
//                 "amts": [ {"amt": 5, "holds": [HoldStat…]}, … ] } }   // 本服务写 (全部 金额档 × 持有期), 载入时显示
//   有效 = expr 是字串且 parse 过 (算子 / 元数 / 参数值域 / 特征存在且可作输入) 且 params (若有) 覆盖成功.
//   同一规范串多文件 → 后者标 dup (黄), 仍算 (共享 DAG 里是同一个根, Stat 也只算一次).
//
// 线程模型 (对仗 OperatorsService): GUI 线程 Request 覆盖挂起请求 + 取消在跑 + 懒起 worker; UI 持 mutex 读 rows;
// 进度走原子. evaluate = false 只扫描 (进页 / Add / Rescan), = true 扫描 + 装载 + 评估.
//
// 装载: 一次把所有有效因子用到的特征 (去重) + _meta + 全部 (amt × hold) 标签列读进宿主 (逐天并行 load_day_columns, 门控
// fmeta::valid ∧ isfinite → 值 + 掩码); 标签存 fp16 位 (与落盘同格式, Stat 直接吃).
// 评估 (Run 的并行方案, 与 search 的解耦): 所有有效因子合成一张共享 DAG (factor::build_forest, 公共子式只算一次), 按拓扑序
// 顺序走节点; CPU 每个节点内部切满所有核 (factor::cpu::run_node_par, 结果与单线程逐位一致), 根算完立刻全核 Stat, 再按槽计划
// 释放 (内存 = 峰值活槽 × T·A × 5B, 确定); GPU 同一张 DAG, 输入上传一次, 中间量常驻显存 (DevPool), Stat 走常驻会话.
// Stat 的"持有期"维展平为 (amt, hold) 对: hd.n = n_amt × n_hold ≤ kMaxHold, x 的 rank 只算一次.
// time 列 (eval_ms) = 该因子子树全部节点 wall 之和 (共享节点算给每个用它的因子 = 单独算它要多久).
// 出错策略: 因子文件 / 表达式不合法是正常输入 → 行 error; 特征库 / 契约不一致 → assert.
#pragma once

#include "codec/L2_DataType.hpp" // L2::ValidType
#include "factor/Expr.hpp"
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
  bool allowed = false; // 可作因子输入 (TS / CS 特征); 标签 / _meta 不许 (前视)
};
struct LabelCol {
  int hold = 0;                              // 持有期 (分钟)
  std::vector<uint32_t> long_col, short_col; // [amt_idx] 列下标
};
struct FeatureTable {
  std::vector<FeatCol> cols; // 下标 = L1 列
  uint32_t meta_col = 0;
  std::vector<int> amts;        // 标签金额档 (万), 文件序
  std::vector<LabelCol> labels; // 按 hold 升序; 每档金额都齐才收
  const FeatCol *find(std::string_view code) const;
  factor::expr::FeatureLookup lookup() const; // 给 Expr::parse 注入
};
// 标签列按 code 命名 lb_long_<h>m_<amt>w / lb_short_<h>m_<amt>w 识别 (LabelReturn 生成的字段行)
FeatureTable BuildFeatureTable(const Feature::Metadata &meta);

// ---- 请求 (GUI 线程解析 config, worker 不碰) ----
struct FactorsRequest {
  std::string factor_dir; // <config.factor_dir>/<universe>
  bool evaluate = false;  // false = 只扫描解析
  // 以下 evaluate 时才用
  analysis::ReadScope scope;
  std::string universe, start_date, end_date;
  bool gpu = false;
};
// evaluate 请求需要资产轴 (读特征库); 轴未就绪 (数据库未扫) → false
bool MakeFactorsRequest(const SharedData &data, bool evaluate, bool gpu, FactorsRequest &req);

// ---- 行 ----
constexpr int kMaxAmt = 4; // 金额档数上限 (n_amt × n_hold ≤ factor::stat::kMaxHold 运行期断言)
struct StatScope {
  std::string universe, start_date, end_date, backend, time;
  int days = 0, T = 0, A = 0;
};
struct FactorRow {
  std::string file;     // 文件名 (不含目录)
  std::string expr_raw; // 文件里的原串 (可能不规范 / 不合法)
  std::string expr;     // 规范串 (含当前参数); 空 = 解析失败
  std::string note;
  std::string error;  // 非空 = BROKEN (文件格式 / 表达式 / params / 组 id 数据), 文件原样留着
  std::string dup_of; // 非空 = 与该文件同一规范串
  int n_ops = 0, n_feats = 0, n_slots = 0;
  RowStatus status = RowStatus::Pending; // Done = 本轮算过 (evaluate) 或本轮不算 (只扫描)
  bool has_stat = false;                 // hold[][] 有内容 (本轮算的或文件载入的)
  bool stat_from_file = false;           // stat 来自文件 (非本进程算的)
  StatScope scope;                       // stat 的作用域
  double eval_ms = 0;                    // time 列: 子树算子节点 wall 之和 (与 amt / hold 无关: 因子平面只算一遍, Stat 按组合复用它)
  float valid_pct = 0.f;
  int n_amt = 0, n_hold = 0;
  int amt[kMaxAmt] = {};                                        // 金额档 (万), 升序
  factor::stat::HoldStat hold[kMaxAmt][factor::stat::kMaxHold]; // [amt][hold]
  const factor::stat::HoldStat *find_hold(int amt_w, int hold_m) const {
    for (int i = 0; i < n_amt; ++i)
      if (amt[i] == amt_w)
        for (int j = 0; j < n_hold; ++j)
          if (hold[i][j].hold == hold_m)
            return &hold[i][j];
    return nullptr;
  }
};

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
  bool evaluate(const FactorsRequest &req); // false = 取消

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

// 因子名 = 文件名主干: 非空, ≤64, 仅 [A-Za-z0-9_]
bool ValidFactorName(std::string_view name);

// 新因子文件: 名字合法 (调用方保证, 断言) 且表达式合法 (parse 过) → <factor_dir>/<name>.json = {"expr": canon, "note"?, "params": [...]}.
// 返回文件名; 表达式不合法 / 同名已存在 → 空串 + err. GUI 线程调 (随后 Request(evaluate=false) 重扫)
std::string AddFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view name, std::string_view expr_src,
                          std::string_view note, std::string &err);

// 编辑已有文件 (file = 表格里的文件名, 含 .json): expr 换成 canon (规范串变了 → 丢 stat), note 覆盖 (空 = 删键), params 重建,
// 其他键保留 (BROKEN 非对象文件从空重建); new_name ≠ 主干 → 改名. 返回新文件名; 表达式不合法 / 改名目标已存在 → "" + err.
// 删除: 直接删文件. 两者只在服务空闲时调 (evaluate 会写回文件)
std::string UpdateFactorFile(const std::string &factor_dir, const FeatureTable &feats, std::string_view file, std::string_view new_name,
                             std::string_view expr_src, std::string_view note, std::string &err);
void DeleteFactorFile(const std::string &factor_dir, std::string_view file);

} // namespace GUI::Factors
