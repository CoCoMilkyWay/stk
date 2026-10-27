// SearchService — Factors 页 Search 的单 worker: 对表格高光的那一个因子文件 (模板 expr, Expr.hpp 自由参数) 搜它的**全部自由参数**
// (factor/Search/Space.hpp 定槽 / 坐标 / 区间), 只搜调用方给定的一个持有期 h (Factors 页当前 Hold; 搜哪个周期由上层定), 目标 = 该 h 的
// 每期真实毛 LS (持仓口径, 已含触发 / 利用率) · rIC (Contract【目标】: 只评排序能力, 不跨 h 比, 不看成本), 按冗余规则终选持仓重合少的
// 显著权重 (Contract【多权重】, 个数不设上限), picks 替换该因子文件里同 h 的 weights, 别的 h 的权重不动 (FactorsService WriteFactorWeights;
// UI 随后自动 Run 重算每权重的 stat).
// 分数只在搜索内部用: 因子表排序 / 选代表权重由 Run 落的 stat 用同一份 score_of 现算, 文件里不留搜索结论.
// 唯一的预算量 = 第 0 趟点数 (Space kInit); 停机 / 高原 / 去重全由各候选的标准误 (Contract【噪声】) 判, 无比例 / 步长 / 个数常数.
//
// 搜索流程 (一趟 = 一批无依赖候选沿全部数据块流过一遍, 只装 h 一组标签 (hd.n = 1); 趟与趟之间才有依赖):
//   准备          先装块 0: 满仓只数 n_full = 全期池内平均有效资产数 / kGroups (Labels::cap, 各块共用, 与 Run 同一个; 一块 = 块 0 的 YPlanes::n_full,
//                 多块 = FactorsEngine plan_job 预扫全期门控); 槽区间按 A = n_full · kGroups 定 (Space slots_of).
//                 阈值槽 (ANY / GE0) 由深到浅: 中性具体式 (Space neutral) 在 CPU 上走到它的输入节点, 取块 0 抽样行的池内有效值作分位表
//                 (Space set_qtab); 输入是常量 → 整个搜索放弃
//   趟 0 采样     R_d 低差异点 (Space design), 评一次得分 (自动定向: Contract【方向】)
//   每趟之后      已评点拟合曲面 (Search/Surface.hpp, 噪声 = 各点 SE) → 峰 → pick_diverse 得活跃峰 (= 当前终选集)
//   趟 1..        每个活跃峰 c, 每槽 ± 方向: 步长从 ℓ_j/2 起 (夹到区间端), 落到已评点就减半再试; 新点的 μ + sd − μ_c > SE_c (可能显著胜过峰) 才评;
//                 取整回到 c / 不合法 / 本趟已提 / 不值得 → 该方向止. 新点去重拼成一趟; 没有新点 → 停
//   终            最后一次曲面的 pick_diverse 即 weights; 没有显著候选 → 文件不动
//   r16 抽样      每趟顺带把每个候选的 kCorrRows 抽样行 r16 回宿主常驻 (约 kCorrRows × A × 2 字节 / 候选), 截面相关按需两两现算 (Stat/Host lag_row),
//                 不另开一趟
//
// 评估 (Search 算子, 两后端语义同 Stat; 只装 / 评给定持有期的标签组):
//   分块流式  按天切块, 块 i = 交易日 [i·D, (i+1)·D), 装载时前面多带 carry = ⌈(模板回看 + max_lag) / seg⌉ 日 (Space lookback: 各路径 ROLL 窗长之和,
//             自由 d 取 kMaxD), 统计从 t_lo 起 (首块无 carry); 块大小 D 按显存 / 内存预算定, 全装得下就一块. 块内候选评估与整张量逐位同
//             (EXPAND 段界 reset; 唯 EXPO 全历史递推在块首重启, 差 (1−k)^carry 行, 可忽略); 跨块 Result::merge. 块数据内存装得下就整轮缓存, 否则每趟重读.
//   候选批    一趟的候选按批合成共享 DAG (factor::build_forest: 参数不同的节点以下公共前缀只算一次); 批 = 引擎 batch_of (FactorsEngine).
//             CPU: 每节点全核 (run_node), 根算完立刻全核 Stat (rank_x + accum_rows, 与 Run 同一条; 抽样行拷 rx), 不留根平面; 批 = kBatch;
//             GPU: keep_roots 森林, 一批 C 张根平面同在显存, 一次 search_eval (核内 8 候选共用标签行); C 按显存定, 上限 search_max_batch.
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑; UI 持 mutex 读 result (每趟结束发布一次, 最终再发布);
// 进度走原子: Loading 期 done/total = 块内天; Running 期 = 候选 × 块. 服务间互斥由 UI 保证 (Factors Run 与 Search 不同时跑: 都写因子文件).
// 出错策略: 因子行 / 作用域不合是正常输入 → message; 契约不一致 → assert.
#pragma once

#include "factor/Search/Contract.hpp" // Score
#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable / FactorRow
#include "gui/util/Service.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Factors {

struct SearchSpec {
  std::string file, tmpl;                              // 因子文件名 + 模板规范串 (FactorRow::tmpl, 须含自由参数)
  factor::stat::Frame frame = factor::stat::Frame::CS; // 口径
  int hold = 0;                                        // 搜的持有期键 (字段表标签组之一)
};
using SearchRequest = Request<SearchSpec>; // scope.factor_dir 落盘 (log / weights); scope.max_impact 不看 (不评成本)
// 资产轴未就绪 / 标签库未建 → false. row 须是文件级有效的行 (tmpl 非空, 文件级无 error); hold = 字段表标签组的键; ft = 该层字段表
bool MakeSearchRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, const FactorRow &row, int hold, bool gpu,
                       SearchRequest &req);

// 评过的一个候选的分 (UI 列表)
struct SearchCand {
  std::string expr;         // 具体规范串
  std::vector<double> u;    // 坐标 (与 SearchResult::slots 对齐)
  int pass = 0;             // 第几趟评的 (0 = 采样)
  factor::search::Score sc; // 分 (Contract score_signed: ok / score / ls / ric 已按 sign 定向 / sign / se)
};
// 终选权重
struct SearchPick {
  std::string expr;
  factor::search::Score sc;
  double corr = 0.0; // 与其他 pick 的最大截面相关 (单个 pick 时 0)
};

// 一块的装载量
struct SearchChunk {
  int days = 0;    // 装载天数 (含前带 carry)
  double mb = 0.0; // 宿主装载量 (特征 + 门控 + 标签; CPU 另含预处理标签)
};

struct SearchResult {
  std::string file, tmpl;         // 对象 + 模板规范串
  std::vector<std::string> slots; // 可搜槽名 ("TsDeltaRoll.d" …)
  int hold = 0;                   // 搜的持有期键
  int n_peak = 0;                 // 最后一次曲面的峰数
  std::vector<SearchCand> cands;  // 全部评过的, score 降序
  std::vector<SearchPick> picks;  // 终选 (空 = 无显著候选)
  int n_pass = 0;                 // 跑完的趟数
  int n_chunk = 0, chunk_days = 0, batch = 0;
  int n_assets = 0;                    // 资产数
  std::vector<SearchChunk> chunk_plan; // 分块计划 (装载前发布)
  std::string backend;
  double seconds = 0.0;
  double n_full = 0.0; // 满仓一腿只数 (Labels::cap, 各块共用; 全期池内平均有效资产数 / kGroups)
  int n_weights = 0;   // 写入的权重数
  bool wrote = false;  // 已写 weights (UI 据此重扫 + Run)
};

// 搜索阶段 (UI 显示): 准备 (装块 0 + 阈值槽分位表) → 采样 (趟 0) → 细化 (趟 1..)
enum class SearchStage : uint8_t { Prepare,
                                   Design,
                                   Refine };

// 进度面 = 自身 (FactorWorker; message = 结论 / 没算成的原因): Loading 期 done/total = 块内天; Running 期 = 候选 × 块
class SearchService : public FactorWorker<SearchService, SearchRequest> {
  friend Service<SearchService, SearchRequest>;

public:
  static constexpr const char *kName = "SearchWorker";
  SearchService(); // 两者都在 .cpp: FactorFlow 不完整
  ~SearchService();

  SearchStage stage() const { return stage_.load(std::memory_order_relaxed); }
  int alive() const { return alive_.load(std::memory_order_relaxed); }   // 活跃峰数 (Refine 期)
  int pass() const { return pass_.load(std::memory_order_relaxed); }     // 当前趟 (0 = 采样)
  int chunk() const { return chunk_.load(std::memory_order_relaxed); }   // 本趟已过的块
  int chunks() const { return chunks_.load(std::memory_order_relaxed); } // 块数

  // UI 持 mutex 读
  SearchResult result;

private:
  struct Searcher; // 一轮搜索的状态 + 步骤 (.cpp)
  static constexpr Phase kStart = Phase::Loading;
  bool run(SearchRequest &req);
  bool eval(const SearchRequest &req, FactorFlow &F); // false = 取消 / 判废

  std::atomic<SearchStage> stage_{SearchStage::Prepare};
  std::atomic<int> alive_{0};
  std::atomic<int> pass_{0}, chunk_{0}, chunks_{0};
};

} // namespace GUI::Factors
