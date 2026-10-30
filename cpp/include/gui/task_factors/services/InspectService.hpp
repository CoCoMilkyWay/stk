// InspectService — Factors→Inspect 页的单 worker: 对 Factors 表里高光的那一个因子 (view_file) 读特征库 → 一棵 Dag → CPU
// evaluator → Stat, **沿时间落一级量** (逐行 rIC + 分层 / 多空净值线; RunService 全表 Run 只留二级 HoldStat) 给页面画分层累计 / 多空 / IC 分布.
// 装载层与 RunService 共用 (FactorsLoad.hpp); 算子 / 流程层 (factor/Stat/Cpu.hpp run / EvalCpu.hpp run_node) 与搜索共用, 并行由
// 常驻 ForkJoin 出 (misc/exec.hpp).
//
// 与 RunService 的分工: Factors 页 = 全部因子一轮排名 (回写文件); Inspect = 一个因子深看 (不写文件, 结果只在内存).
//
// 计算 flow (worker 私有 FactorFlow + 本服务的平面缓冲, 常驻). 只有 S / Y 跨请求缓存 (作用域不变就复用), 其后每次请求整条重算;
// 整条走管线 eval_job (FactorsEngine.hpp, 与 Run / Portfolio 同一份), R / C / L / T / N 是探针 (InspectProbes); N 落线进净值引擎 fold_book
// (FactorsBook.hpp, 与 Portfolio / 池线同一份):
//   S  块       FactorsLoad Block, key = block_key (层 | Domain::key | 日段 | YSpec: 持有期 / 金额段 / 封顶 | B)
//               → Y = 截面门控, 毛标签 [H], 成本 [C]; X = 特征平面 (Life::Worker: 评估域内读过的全留, 只增). 池等权线 (pool_line, 每持有期一条) 随 Y 一代算一次
//               (块钩子, whole: 整期一块; 装不下内存预算 → 评估期只留最近的日, fit_recent)
//   Y  标签     随 S                                       → 毛标签秩: 块上的 Block::ry (Engine<Cpu> 会话首次 prep_label, 随块常驻); 没有净标签集
//   X  因子                                                → 缺的特征补读 (ensure_block) → Engine<Cpu>::walk (run_node, 全核) → 根平面 (HostPool 常驻)
//   R  rank_x (引擎)                                       → RxP 落 InspectPlanes::rx (随结果发布: 右上资产轴 / K 线着色);
//                                                            RxAllP 另跑一遍 g = xm 的 rank_x → rx_all, 池外段也有秩可显
//   C  成本                                                → end_codes(rx) → CostTier[C ≥ 1] (FactorsTable.hpp): 每金额段 × 每持有期一份换手成本 CostAcc (FactorsProbe
//                                                            CostP → cost_rows, 与 Run 同一份; Stat 契约【换手成本】: 端成员 = 端码 ∧ 标签有效位, 逐格 Δw,
//                                                            同一价点才与 t−h 交接; 一遍 [T][A], 无排序). 期限结构的净线 = 毛二级 − 两端均摊成本 (含 Config::fixed_impact), 全在 UI 现算
//                                                            (成本不进标签, 不被截面去均值吃掉; fixed_impact 只进均摊公式, 不进请求)
//   L  评估 + 尾部                                         → 一级 Row (逐行, 不落) + 二级 HoldStat[H] (只毛, accum_rows 一遍 → finish_all); 逐行 rIC 落 InspectPlanes::ric;
//                                                            尾部 (两端组内更极端的 kTailOfGroup 截的分层 / 多空, 只在 Row::ok 行; 不进 Stat 契约 / Search, 见 FactorsTable TailRow)
//                                                            是 label_row 的逐格扩展 (FactorsProbe TailExt), 与标签侧同一遍 A 轴
//   T  换手                                                → 各 LS 线两端的换手量 CostAcc: 顶底 = C 的段 0 (各段同一份 Δw); 各级尾部 = Stat cost_rows 一个固定段
//                                                            (端成员 = 该级端码 ∧ 标签有效位, 满仓 = cap · kTailOfGroup[j]); 图例年换手 = turn_annual, UI 现算
//   N  净值                                                → 落线探针 NavP: 每行由 Row + 尾部算各线这一批的收益 (FactorsBook RowLines), 全部持有期一次 fold_book
//                                                            (Stat 契约【净值】: 平仓行实现 1/h̄), stride = S → nav 日收益 (分层 / 尾部 / LS 的绝对 + 超额, 只毛;
//                                                            可成交 / 顺延在标签里)
// 结果一次发布: 页面的 Hold / 超额|绝对 / 冲击 全是纯显示切片, 不进请求; 只有换因子 / Compute (弃缓存) / 作用域变才进 worker.
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑; 新请求开跑不清旧 result: 页面继续整页画旧结果
// (旧因子, 头部文字一并), 新结果发布时一次性覆盖 (UI 持 mutex 读; 一级平面大 (秩 2B × T × A), UI 不拷, 持锁派生成日级序列后放手;
// 发布 = 与 worker 的 InspectPlanes 交换, 不分配). 进度走原子: Loading 期 done/total = 天; Running 期 = 算子节点 + 标签集.
// 后端: 只 CPU (单因子一棵 Dag; GPU 留后).
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsBook.hpp"   // BookLines
#include "gui/task_factors/services/FactorsFile.hpp"   // FeatureTable / FactorRow / StatScope / AlphaDist
#include "gui/task_factors/services/FactorsWorker.hpp" // FactorWorker
#include "shared/Analysis.hpp"                         // ReadScope / AggPdf

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Factors {

// 请求 = 一个因子 (FactorRef, 与 Portfolio 条目同一份) + 权重行 key; reload = 弃缓存整体重读 (特征库重算过 / 手动 Compute).
// scope.max_impact 装载时生效 → 进作用域缓存键; scope.gpu 不看 (只 CPU)
struct InspectSpec {
  FactorRef f;
  std::string row; // 权重行 key (FactorRow::key = 文件#权重号)
};
using InspectRequest = Request<InspectSpec>;
// 请求 / 结果 key: 层 | 权重行 | 具体规范串 | 方向 | 池
inline std::string inspect_key(LevelId lv, const std::string &pool, const std::string &row, const std::string &expr, factor::stat::Dir dir) {
  return std::string(factor_level(lv).name) + "|" + row + "|" + expr + (dir == factor::stat::kRev ? "|-" : "|+") + "|" + pool;
}
// 资产轴未就绪 / 标签库未建 → false. row 须是有效 alpha 行 (error 空); ft = 该层字段表
bool MakeInspectRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, const FactorRow &row, bool reload, bool gpu,
                        InspectRequest &req);

// 分层累计净值 (净值引擎 fold_book, 全部持有期一遍, 复利). 分层线 [0, kGroups) = 20 组, layer_tail(端, j) = 该端组内第 j 级尾部 (口径同 Row::grp);
// LS 线 0 = 顶底 (ls_row), 1 + j = 第 j 级尾部多空; LS = ½ (顶端 − 底端), 绝对 / 超额只差两端只数不同时的基准项.
// 净值线 (InspectResult::nav) = 分层绝对 [0, kNavLayers) | 分层超额 (− Row::mkt) [kNavLayers, 2 kNavLayers) | LS 绝对 [2 kNavLayers, + kNavLs)
// | LS 超额 [2 kNavLayers + kNavLs, kNavLines); 值 = 日收益 (当日平仓的各批收益 / h 之和)
inline constexpr int kNavLayers = factor::stat::kGroups + factor::stat::kEnds * kTails;
inline constexpr int kNavLs = 1 + kTails;
inline constexpr int kNavLines = 2 * (kNavLayers + kNavLs);
inline constexpr int layer_tail(int e, int j) { return factor::stat::kGroups + e * kTails + j; }

// Inspect 的一级平面: worker 写进自己那份, 发布时与结果整体交换 (旧缓冲回收给 worker, 下次原地写)
struct InspectPlanes {
  std::vector<float> ric;       // 逐行 rIC [hd.n][T] (Row::ok 行; 其余 NaN): IC 时序
  std::vector<uint16_t> rx;     // r16(x) [T][A] (CS = 截面秩, TS = 自身分位; 只池内 g ∧ xm, kRankNone 无效): 右上资产轴排序 / K 线着色
  std::vector<uint16_t> rx_all; // 同上但不看池 (g = xm): 池外段 (出 CS 池 / 当日无成交) 的秩, K 线淡色显示; 评估不用
  void clear() { ric.clear(), rx.clear(), rx_all.clear(); }
};

// EvalStat (与 Run 行同一份): scope / eval_ms / valid% / rank% / 二级 hold[n_hold] (只毛口径) / costs [C] = FeatureTable::y.costs 序
// (期限结构净线: 毛二级 − 两端均摊换手成本, UI 现算)
struct InspectResult : EvalStat {
  std::string key; // = inspect_key; 空 = 无结果
  std::string row, expr;
  LevelId lv = 0; // 因子层: T = days · seg, K 线 / 横轴按层
  factor::stat::Frame frame = factor::stat::Frame::CS;
  std::vector<misc::Ymd> dates;                                       // [days]
  factor::stat::Holds hd;                                             // hd.k[hi]
  analysis::ReadScope read_scope;                                     // 特征库作用域 (Domain::rs: 所用库 + A 轴 + 月份): 右下 K 线按它读标签库 (main); axis 做 A 轴 ↔ 全局 id
  BookLines pool_index;                                               // [hd.n][T] 池等权线的逐行收益 (= pool_line, 每持有期一条, 作用域级): 右上 K 线叠加当前 Hold 那条,
                                                                      //   UI compound 成点位后按视野左端个股价重锚
  float pool_ann[factor::stat::kMaxHold] = {};                        // [hi] 池等权线的算术年化 (Σ 逐行收益 / days × kDaysPerYear; 与 pool_index 同代): 右下期限结构的池指数线
  InspectPlanes pl;                                                   // 一级平面 (rIC / 秩)
  BookLines nav;                                                      // 分层累计的日收益 [hd.n · kNavLines][days] (线 hi · kNavLines + i, 见 kNavLines)
  factor::stat::CostAcc ls_turn[kTails][factor::stat::kMaxHold] = {}; // [尾部 j][hi] LS 线 1 + j 两端换手量 (固定段; 见 flow T); 取用 ls_turn_of
  analysis::AggPdf ic_agg[factor::stat::kMaxHold];                    // [hi] 逐行 rIC (ok 行) 分布全分辨率 (EvalStat::ic_pdf 是它的迷你版; 左下左半)
  AlphaDist alpha_dist;                                               // alpha 分布 (全体 + 各组, 左下右半图)
  std::string error;                                                  // 非空 = 没算成 (评估域不成 / 特征叶值域越界), 其余字段空

  const float *ric_at(int hi) const { return pl.ric.data() + static_cast<size_t>(hi) * scope.T; }
  const float *nav_at(int hi, int line) const { return nav.at(hi * kNavLines + line); }
  // LS 线 line (0 = 顶底 = 成本段 0, 1 + j = 第 j 级尾部) 在持有期 hi 的两端换手量
  const factor::stat::CostAcc &ls_turn_of(int line, int hi) const { return line == 0 ? costs.front().acc[hi] : ls_turn[line - 1][hi]; }
};

// 进度面 = 自身 (FactorWorker): Loading 期 done/total = 天; Running 期 = 算子节点 + 成本 + 评估
class InspectService : public FactorWorker<InspectService, InspectRequest> {
  friend Service<InspectService, InspectRequest>;

public:
  static constexpr const char *kName = "InspectWorker";
  InspectService(); // 两者都在 .cpp: FactorFlow 不完整
  ~InspectService();

  // UI 持 mutex 读 (不拷平面)
  InspectResult result;
  uint64_t result_gen = 0; // result 每换一次 +1 (UI 派生缓存键; epoch 随进度跳, 不能当键)

private:
  static constexpr Phase kStart = Phase::Loading;
  bool run(InspectRequest &req);
  bool eval(const InspectRequest &req, FactorFlow &F); // false = 取消 / 判废

  // worker 私有 (跨请求存活): 块在 FactorFlow (Y 常驻, X = 评估域内读过的特征, 只增)
  InspectPlanes pl_;                            // 一级平面, 发布时与 result.pl 交换
  BookLines pool_;                              // 池等权线 (pool_line), 随 Y 一代
  float pool_ann_[factor::stat::kMaxHold] = {}; // pool_ 各持有期的算术年化, 同代
  uint64_t pool_gen_ = 0;                       // pool_ 算时的 c.gen
};

} // namespace GUI::Factors
