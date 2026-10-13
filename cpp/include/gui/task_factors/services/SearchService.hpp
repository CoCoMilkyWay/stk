// SearchService — Factors 页 Optimize 的单 worker: 对表格高光的那一个因子文件 (模板 expr, Expr.hpp 自由参数) 搜它的**全部自由参数**
// (factor/Search/Grid.hpp 定槽与坐标), 搜索窗内每个持有期 h (Search/Contract.hpp hold_in_window: 30m ~ T+1) 各自独立搜, 目标 = 该 h 的
// 每期真实毛 LS (持仓口径, 已含触发 / 利用率) · rIC (Contract【目标】: 只评排序能力, 不跨 h 比, 不看成本), 每 h 终选 ≤ kPicks 个持仓重合最少的
// 优质权重 (Contract【多权重】), 全部 h 的 picks 整个替换该因子文件的 weights (FactorsService WriteFactorWeights; UI 随后自动 Run 重算每权重的 stat).
// 分数只在搜索内部用: 看板排序 / 选代表权重由 Run 落的 stat 用同一份 score_of 现算, 文件里不留搜索结论.
//
// 搜索流程 (一趟 = 一批无依赖候选沿全部数据块流过一遍, 全部 h 的标签同在 (hd.n = H), 因子平面只算一遍; 趟与趟之间才有依赖):
//   尺度          先装块 0: 满档只数 n_full = 池内平均有效票数 / kGroups (Labels::cap, FactorsLoad n_full_of); 模板按占位值 (Expr::placeholder)
//                 建 DAG 在 CPU 上走到各阈值槽 (ANY / GE0) 的输入节点, 取池内有效格标准差 σ 作该槽坐标单位 (Grid set_scale);
//                 σ 不是正有限数的槽 → 整个搜索放弃 (自由参数没法落值)
//   趟 0 粗网格   全部槽的格点笛卡尔积 (围 Grid 的中心铺, 步长由 kLayerMax 预算反推), 评一次得全部 h 的分
//   分支          每 h: 粗网格 score 降序贪心取全部互不相邻格点各开一支 (= 该 h 的全部局部峰)
//   趟 1..        每 h 每支围自己的 top-1 开细化网格 (步长从粗步长一半起逐层减半, 点数 ≤ kLayerMax), 全部 h 全部活支的新点去重拼成一趟;
//                 一支一层相对增益 < kMinGain 即停
//   趟 corr       每 h 的高原集 (各支最优里 score ≥ (1 − kPlateau)·max) 取并集再评一趟, 顺带抽样 kCorrRows 行的 r16 回宿主, 同 h 两两算
//                 截面相关 (Stat/Host lag_row) 沿抽样行取均值
//   终            每 h pick_uncorrelated → ≤ kPicks 权重; 一个 h 都没有正分候选 → 文件不动; 否则 weights = 全部 h 的 picks
//
// 评估 (Search 算子, 两后端语义同 Stat; 装 / 评搜索窗内全部标签组):
//   分块流式  按天切块, 块 i = 交易日 [i·D, (i+1)·D), 装载时前面多带 kCarryDays = 5 日 (= kMaxD = max_lag 的行数), 统计从 t_lo 起
//             (首块无 carry); 块大小 D 按显存 / 内存预算定, 全装得下就一块. 块内候选评估与整张量逐位同 (ROLL 窗 ≤ kMaxD, EXPAND 段界 reset;
//             唯 TsMeanEma 全历史递推在块首重启, carry 5 日后差 (1−k)^1275, 可忽略); 跨块 Result::merge. 块数据内存装得下就整轮缓存, 否则每趟重读.
//   候选批    一趟的候选按批合成共享 DAG (factor::build_forest: 参数不同的节点以下公共前缀只算一次).
//             CPU: 每节点全核 (run_node), 根算完立刻全核 Search (rank_x + accum_rows), 不留根平面;
//             GPU: keep_roots 森林, 一批 C 张根平面同在显存, 一次 search_eval (核内 8 候选共用标签行); C 按显存定, 上限 search_max_batch.
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
  std::string file, tmpl;                              // 因子文件名 + 模板规范串 (FactorRow::tmpl, 须含自由参数)
  factor::stat::Frame frame = factor::stat::Frame::CS; // 口径
  analysis::ReadScope scope;
  std::string universe, start_date, end_date;
  bool gpu = false;
};
// 资产轴未就绪 / 区间无月份 → false. row 须是文件级有效的行 (tmpl 非空, 文件级无 error)
bool MakeSearchRequest(const SharedData &data, const FactorRow &row, bool gpu, SearchRequest &req);

// 评过的一个候选在一个 h 下的分 (UI 列表)
struct SearchCand {
  std::string expr;      // 具体规范串
  std::vector<double> u; // 坐标 (与 SearchResult::slots 对齐)
  int pass = 0;          // 第几趟评的 (0 = 粗)
  bool ok = false;       // n ≥ 3
  double score = -std::numeric_limits<double>::infinity();
  double ls = 0.0, ric = 0.0; // 每期真实毛 LS (ls_period) / rIC
};
// 一个 h 的终选权重
struct SearchPick {
  std::string expr;
  double score = 0.0, ls = 0.0, ric = 0.0;
  double corr = 0.0; // 与同 h 其他 pick 的最大截面相关 (单个 pick 时 0)
};
// 一个持有期的搜索状态 / 结果
struct SearchHold {
  int hold = 0;
  int n_branch = 0, n_plateau = 0;                             // 分支数 (粗网格局部峰) / 高原集大小
  double max_score = -std::numeric_limits<double>::infinity(); // 各支最优的最高分
  std::vector<SearchCand> cands;                               // 全部评过的, score 降序
  std::vector<SearchPick> picks;                               // 终选 (空 = 该 h 无正分候选)
};

struct SearchResult {
  std::string file, tmpl;         // 对象 + 模板规范串
  std::vector<std::string> slots; // 可搜槽名 ("TsDeltaRoll.d" …)
  std::vector<SearchHold> holds;  // 搜索窗内持有期, 升序
  int n_pass = 0;                 // 跑完的趟数 (含 corr 趟)
  int n_chunk = 0, chunk_days = 0, batch = 0;
  std::string backend;
  double seconds = 0.0;
  double n_full = 0.0; // 满档一腿只数 (Labels::cap; 块 0 池内平均有效票数 / kGroups)
  int n_weights = 0;   // 写入的权重数
  bool wrote = false;  // 已写 weights (UI 据此重扫 + Run)
  std::string message; // 结论 / 没算成的原因
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
