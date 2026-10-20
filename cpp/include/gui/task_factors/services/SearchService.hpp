// SearchService — Factors 页 Search 的单 worker: 对表格高光的那一个因子文件 (模板 expr, Expr.hpp 自由参数) 搜它的**全部自由参数**
// (factor/Search/Grid.hpp 定槽与坐标), 搜索窗内每个持有期 h (Search/Contract.hpp hold_in_window: 30m ~ T+1) 各自独立搜, 目标 = 该 h 的
// 每期真实毛 LS (持仓口径, 已含触发 / 利用率) · rIC (Contract【目标】: 只评排序能力, 不跨 h 比, 不看成本), 每 h 终选 ≤ kPicks 个持仓重合最少的
// 优质权重 (Contract【多权重】), 全部 h 的 picks 整个替换该因子文件的 weights (FactorsService WriteFactorWeights; UI 随后自动 Run 重算每权重的 stat).
// 分数只在搜索内部用: 因子表排序 / 选代表权重由 Run 落的 stat 用同一份 score_of 现算, 文件里不留搜索结论.
//
// 搜索流程 (一趟 = 一批无依赖候选沿全部数据块流过一遍, 全部 h 的标签同在 (hd.n = H), 因子平面只算一遍; 趟与趟之间才有依赖):
//   尺度          先装块 0: 满仓只数 n_full = 池内平均有效资产数 / kGroups (Labels::cap, FactorsLoad YPlanes::n_full); 模板按占位值 (Expr::placeholder)
//                 建 DAG 在 CPU 上走到各阈值槽 (ANY / GE0) 的输入节点, 取池内有效格标准差 σ 作该槽坐标单位 (Grid set_scale);
//                 σ 不是正有限数的槽 → 整个搜索放弃 (自由参数没法落值)
//   趟 0 粗网格   全部槽的格点笛卡尔积 (围 Grid 的中心铺, 步长由 kLayerMax 预算反推), 评一次得全部 h 的分 (自动定向: Contract【方向】)
//   分支          每 h: 粗网格的全部局部峰各开一支 (Grid pick_branches)
//   趟 1..        每 h 每支坐标模式搜索 (Grid【模式搜索】, 步长从粗步长一半起; 每轮 2P 邻点), 全部 h 全部活支的新点去重拼成一趟;
//                 走到同 h 别支的点 → 并; 不升且低于 (1 − kPlateau)·当前最高 → 剪
//   趟 corr       每 h 的高原集 (各支最优里 score ≥ (1 − kPlateau)·max) 取并集再评一趟, 顺带抽样 kCorrRows 行的 r16 回宿主, 同 h 两两算
//                 截面相关 (Stat/Host lag_row) 沿抽样行取均值
//   终            每 h pick_uncorrelated → ≤ kPicks 权重; 一个 h 都没有正分候选 → 文件不动; 否则 weights = 全部 h 的 picks
//
// 评估 (Search 算子, 两后端语义同 Stat; 装 / 评搜索窗内全部标签组):
//   分块流式  按天切块, 块 i = 交易日 [i·D, (i+1)·D), 装载时前面多带 carry_days = ⌈kMaxD / seg⌉ 日 (行数 ≥ kMaxD ≥ max_lag), 统计从 t_lo 起
//             (首块无 carry); 块大小 D 按显存 / 内存预算定, 全装得下就一块. 块内候选评估与整张量逐位同 (ROLL 窗 ≤ kMaxD, EXPAND 段界 reset;
//             唯 TsMeanEma 全历史递推在块首重启, carry 5 日后差 (1−k)^1275, 可忽略); 跨块 Result::merge. 块数据内存装得下就整轮缓存, 否则每趟重读.
//   候选批    一趟的候选按批合成共享 DAG (factor::build_forest: 参数不同的节点以下公共前缀只算一次).
//             CPU: 每节点全核 (run_node), 根算完立刻全核 Search (rank_x + accum_rows), 不留根平面;
//             GPU: keep_roots 森林, 一批 C 张根平面同在显存, 一次 search_eval (核内 8 候选共用标签行); C 按显存定, 上限 search_max_batch.
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑; UI 持 mutex 读 result (每趟结束发布一次, 最终再发布);
// 进度走原子: Loading 期 done/total = 块内天; Running 期 = 候选 × 块. 服务间互斥由 UI 保证 (Factors Run 与 Search 不同时跑: 都写因子文件).
// 出错策略: 因子行 / 作用域不合是正常输入 → result.message; 契约不一致 → assert.
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable / FactorRow
#include "gui/util/Service.hpp"

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Factors {

struct SearchSpec {
  std::string file, tmpl;                              // 因子文件名 + 模板规范串 (FactorRow::tmpl, 须含自由参数)
  factor::stat::Frame frame = factor::stat::Frame::CS; // 口径
};
using SearchRequest = Request<SearchSpec>; // scope.factor_dir 落盘 (log / weights); scope.max_impact 不看 (不评成本)
// 资产轴未就绪 / 标签库未建 → false. row 须是文件级有效的行 (tmpl 非空, 文件级无 error)
bool MakeSearchRequest(const SharedData &data, const std::string &pool, int seg, const FactorRow &row, bool gpu, SearchRequest &req);

// 评过的一个候选在一个 h 下的分 (UI 列表)
struct SearchCand {
  std::string expr;      // 具体规范串
  std::vector<double> u; // 坐标 (与 SearchResult::slots 对齐)
  int pass = 0;          // 第几趟评的 (0 = 粗)
  bool ok = false;       // n ≥ 3
  double score = -std::numeric_limits<double>::infinity();
  double ls = 0.0, ric = 0.0; // 每期真实毛 LS (ls_period) / rIC (已按 sign 定向)
  int sign = 1;               // 方向 (Contract score_signed)
};
// 一个 h 的终选权重
struct SearchPick {
  std::string expr;
  int sign = 1;
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

// 一块的装载量
struct SearchChunk {
  int days = 0;    // 装载天数 (含前带 carry)
  double mb = 0.0; // 宿主装载量 (特征 + 门控 + 标签; CPU 另含预处理标签)
};

struct SearchResult {
  std::string file, tmpl;         // 对象 + 模板规范串
  std::vector<std::string> slots; // 可搜槽名 ("TsDeltaRoll.d" …)
  std::vector<SearchHold> holds;  // 搜索窗内持有期, 升序
  int n_pass = 0;                 // 跑完的趟数 (含 corr 趟)
  int n_chunk = 0, chunk_days = 0, batch = 0;
  int n_assets = 0;                    // 资产数
  std::vector<SearchChunk> chunk_plan; // 分块计划 (装载前发布)
  std::string backend;
  double seconds = 0.0;
  double n_full = 0.0; // 满仓一腿只数 (Labels::cap; 块 0 池内平均有效资产数 / kGroups)
  int n_weights = 0;   // 写入的权重数
  bool wrote = false;  // 已写 weights (UI 据此重扫 + Run)
  std::string message; // 结论 / 没算成的原因
};

// 搜索阶段 (UI 显示): 定尺度 (装块 0 + 阈值槽 σ) → 粗网格 → 局部模式搜索 → 相关性
enum class SearchStage : uint8_t { Scale,
                                   Coarse,
                                   Refine,
                                   Corr };

// 进度面 = 自身 (Published): Loading 期 done/total = 块内天; Running 期 = 候选 × 块
class SearchService : public Service<SearchService, SearchRequest>, public Published {
  friend Service<SearchService, SearchRequest>;

public:
  static constexpr const char *kName = "SearchWorker";
  SearchService(); // 两者都在 .cpp: Flow 不完整
  ~SearchService();

  void SetFeatureTable(std::shared_ptr<const FeatureTable> t) { feats_ = std::move(t); } // 首个 Request 前 (进页) 调一次
  const FeatureTable &feats() const { return *feats_; }

  SearchStage stage() const { return stage_.load(std::memory_order_relaxed); }
  int level() const { return level_.load(std::memory_order_relaxed); }   // 局部搜索轮 (Refine 期)
  int alive() const { return alive_.load(std::memory_order_relaxed); }   // 存活分支数 (Refine 期)
  int pass() const { return pass_.load(std::memory_order_relaxed); }     // 当前趟 (0 = 粗)
  int chunk() const { return chunk_.load(std::memory_order_relaxed); }   // 本趟已过的块
  int chunks() const { return chunks_.load(std::memory_order_relaxed); } // 块数

  // UI 持 mutex 读
  SearchResult result;

private:
  struct Flow;
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return *this; }
  bool run(SearchRequest &req);
  bool evaluate(const SearchRequest &req, Flow &F); // false = 取消

  std::shared_ptr<const FeatureTable> feats_;
  std::unique_ptr<Flow> flow_; // 首个请求懒建, 随 worker 活
  std::atomic<SearchStage> stage_{SearchStage::Scale};
  std::atomic<int> level_{0}, alive_{0};
  std::atomic<int> pass_{0}, chunk_{0}, chunks_{0};
};

} // namespace GUI::Factors
