// SearchService — Factors 页 Optimize 的单 worker: 对表格高光的那一个因子 (view_file) 搜它表达式的**全部参数项** (d / k / k2;
// factor/Search/Grid.hpp 定槽与坐标), 目标 = 单一金额档真实换手成本下的 年化净 LS · 资金利用率 · rIC, h* 只在 30m ~ T+1 里选
// (factor/Search/Contract.hpp【目标】: mono / 换手 / 显著性 / 样本外 / 活跃度硬阈值有意不进, 搜索要快), 严格好于基线才回写该因子文件的
// params 键 (FactorsService WriteFactorParams; stat 删掉, UI 随后自动 Run 重算). 分数只在搜索内部用: 看板的 h* / util 列由 Run 落的 stat
// (每档 CostAcc) 用同一份 score_of 现算, 文件里不留搜索结论.
//
// 搜索流程 (一趟 = 一批无依赖候选沿全部数据块流过一遍; 趟与趟之间才有依赖):
//   尺度          先装块 0: 满仓只数 n_full = 池内平均有效票数 / kGroups (util 分母, FactorsLoad n_full_of); 基线 DAG 在 CPU 上走到各阈值槽 (ANY / GE0) 的输入节点,
//                 取池内有效格标准差 σ 作该槽坐标单位 (Grid set_scale); σ 不是正有限数的槽剔除 (输入常量, 阈值无意义)
//   趟 0 粗网格   全部槽的格点笛卡尔积 (步长由 kLayerMax 预算反推, 含基线 = 当前参数), 每候选自选 h* (窗内 argmax 年化净 LS · util)
//   分支          粗网格 score 最高的 kBranches 个互不相邻格点各开一支, 支内 h 固定为该格点的 h*
//   趟 1..        每支围自己的 top-1 开细化网格 (步长从粗步长一半起逐层减半, 点数 ≤ kLayerMax), 全部活支的新点拼成一趟;
//                 一支一层相对增益 < kMinGain 即停
//   终            全部评过的候选按 score (自选 h*) 取最优: 不严格高于基线 score → 文件不动; 否则写 params
//
// 评估 (Search 算子, 两后端语义同 Stat):
//   分块流式  按天切块, 块 i = 交易日 [i·D, (i+1)·D), 装载时前面多带 kCarryDays = 5 日 (= kMaxD = max_lag 的行数), 统计从 t_lo 起
//             (首块无 carry); 块大小 D 按显存 / 内存预算定, 全装得下就一块. 块内候选评估与整张量逐位同 (ROLL 窗 ≤ kMaxD, EXPAND 段界 reset;
//             唯 TsMeanEma 全历史递推在块首重启, carry 5 日后差 (1−k)^1275, 可忽略); 跨块 Result::merge. 块数据内存装得下就整轮缓存, 否则每趟重读.
//   候选批    一趟的候选按批合成共享 DAG (factor::build_forest: 参数不同的节点以下公共前缀只算一次).
//             CPU: 每节点全核 (run_node), 根算完立刻全核 Search (rank_x + eval_rows: 行统计 + 换手成本), 不留根平面;
//             GPU: keep_roots 森林, 一批 C 张根平面同在显存, 一次 search_eval (核内 8 候选共用标签行); C 按显存定, 上限 search_max_batch.
//   成本      Cost 取 FeatureTable::costs[cost_idx] 一档 (如 5w) 的 buy / sell 平面, 只有顶 / 底组的新进者 (t−h 不在该组) 付建仓冲击 +
//             平仓常数 Config::sell_impact (契约【成本】)
//
// 线程模型 (对仗 InspectService): GUI 线程 Request 覆盖挂起请求 + 取消在跑 + 懒起 worker; UI 持 mutex 读 result (每趟结束发布一次, 最终再发布);
// 进度走原子: Loading 期 done/total = 块内天; Running 期 = 候选 × 块. 服务间互斥由 UI 保证 (Factors Run 与 Optimize 不同时跑: 都写因子文件).
// 出错策略: 因子行 / 作用域不合是正常输入 → result.message; 契约不一致 → assert.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable / FactorRow
#include "shared/Analysis.hpp"                          // ReadScope

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

struct SharedData;

namespace GUI::Factors {

struct SearchRequest {
  std::string factor_dir;                              // <factor_dir>/<universe> (落盘)
  std::string file, expr;                              // 因子文件名 + 规范串 (Factors 表的有效行)
  factor::stat::Frame frame = factor::stat::Frame::CS; // 口径 (TS 时根 TsRankRoll 的 d 不搜)
  analysis::ReadScope scope;
  std::string universe, start_date, end_date;
  bool gpu = false;
  int cost_idx = 0;         // FeatureTable::costs 下标 (单一金额档)
  double sell_impact = 0.0; // Config::sell_impact
  double max_impact = 0.0;  // Config::max_impact (买入冲击封顶, 装载时生效)
};
// 资产轴未就绪 / 区间无月份 → false. row 须是有效 alpha 行 (error 空)
bool MakeSearchRequest(const SharedData &data, const FactorRow &row, bool gpu, int cost_idx, SearchRequest &req);

// 评过的一个候选 (UI 列表)
struct SearchCand {
  std::string expr;      // 规范串
  std::vector<double> u; // 坐标 (与 SearchResult::slots 对齐)
  int pass = 0;          // 第几趟评的 (0 = 粗)
  int best_hi = -1;      // 自选 h* (hd 下标); < 0 = 无一持有期 n ≥ 3
  double score = -std::numeric_limits<double>::infinity();
  double ls_net = 0.0, util = 0.0, ric = 0.0; // h* 处 (ls_net 年化; util 资金利用率)
};

struct SearchResult {
  std::string file, expr0;        // 对象 + 基线规范串
  std::string best_expr;          // 当前最优规范串 (每趟刷新); 空 = 还没评出
  std::vector<std::string> slots; // 可搜槽名 ("TsDeltaRoll.d" …)
  factor::stat::Holds hd;         // hd.h[hi]
  std::vector<SearchCand> cands;  // 全部评过的, score 降序
  int n_pass = 0;                 // 跑完的趟数
  int n_chunk = 0, chunk_days = 0, batch = 0;
  std::string backend;
  double seconds = 0.0;
  double n_full = 0.0;                                          // 满仓一腿只数 (util 分母; 块 0 池内平均有效票数 / kGroups)
  double base_score = -std::numeric_limits<double>::infinity(); // 基线 score
  bool wrote = false;                                           // 已写 params (UI 据此重扫 + Run)
  std::string message;                                          // 结论 / 没算成的原因
};

enum class SearchStatus : uint8_t { Idle,
                                    Loading,
                                    Running,
                                    Done,
                                    Cancelled };

class SearchService {
public:
  SearchService() = default;
  ~SearchService() { Stop(); }
  SearchService(const SearchService &) = delete;
  SearchService &operator=(const SearchService &) = delete;

  void SetFeatureTable(FeatureTable &&t) { feats_ = std::move(t); } // worker 未起时 (进页) 调一次
  const FeatureTable &feats() const { return feats_; }

  // GUI 线程
  void Request(const SearchRequest &req);
  void RequestCancel() { cancel_.store(true, std::memory_order_relaxed); }
  void Stop();

  SearchStatus status() const { return status_.load(std::memory_order_acquire); }
  int pass() const { return pass_.load(std::memory_order_relaxed); } // 当前趟 (0 = 粗)
  int done() const { return done_.load(std::memory_order_relaxed); } // Loading: 块内天; Running: 候选 × 块
  int total() const { return total_.load(std::memory_order_relaxed); }
  int chunk() const { return chunk_.load(std::memory_order_relaxed); }   // 本趟已过的块
  int chunks() const { return chunks_.load(std::memory_order_relaxed); } // 块数
  uint64_t epoch() const { return epoch_.load(std::memory_order_relaxed); }

  // UI 持锁读
  std::mutex mutex;
  SearchResult result;

private:
  struct Flow;
  void worker_loop();
  bool run(const SearchRequest &req, Flow &F); // false = 取消

  FeatureTable feats_;
  std::thread thread_;
  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<SearchRequest> pending_;
  std::atomic<bool> cancel_{false};
  std::atomic<bool> stop_{false};
  std::atomic<SearchStatus> status_{SearchStatus::Idle};
  std::atomic<int> pass_{0}, done_{0}, total_{0}, chunk_{0}, chunks_{0};
  std::atomic<uint64_t> epoch_{0};
};

} // namespace GUI::Factors
