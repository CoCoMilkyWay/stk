// InspectService — Factors→Inspect 页的单 worker: 对 Factors 表里高光的那一个因子 (view_file) 读特征库 → 一棵 Dag → CPU
// evaluator → Stat, **保留一级 Row** (FactorsService 全表 Run 只留二级 HoldStat, 画不了沿时间的东西) 给页面画分层累计 / 多空 / IC 分布.
// 装载层与 FactorsService 共用 (FactorsLoad.hpp); 算子 / 流程层 (factor/Stat/Cpu.hpp run / EvalCpu.hpp run_node) 与搜索共用, 并行由
// 常驻 ForkJoin 出 (misc/exec.hpp).
//
// 与 FactorsService 的分工: Factors 页 = 全部因子一轮排名 (回写文件); Inspect = 一个因子深看 (不写文件, 结果只在内存).
//
// 计算 flow (worker 私有 Flow; 缓冲常驻, 稳态零分配). 只有 S / Y 跨请求缓存 (作用域不变就复用), 其后每次请求整条重算:
//   S  作用域   key = 特征库目录 | 子轴 hash | 月份 | 封顶  → dates, 截面门控, 毛标签 [H], 成本 [C], universe 指数; 特征平面缓存 (作用域内读过的全留, 只增)
//   Y  标签     随 S                                       → 毛 LabelsHost {ry[H], Labels} (FactorsLoad prep_labels, 一次常驻); 没有净标签集
//   X  因子                                                → 缺的特征补读 → Dag 节点 (run_node, 全核) → 根平面 (Pool 常驻)
//   R  rank_x                                              → 直接秩进 InspectPlanes::rx (随结果发布: 右上标的轴 / K 线着色);
//                                                            之前多跑一遍 g = xm 的 rank_x → rx_all, 池外段也有秩可显
//   C  成本                                                → leg_codes(rx) → CostTier[C] (FactorsService.hpp): 每金额档 × 每持有期一份换手成本 CostAcc (FactorsLoad
//                                                            cost_tiers, 与 Run 同一份; Stat 契约【换手成本】: t ≥ h 的行逐格比 t−h 腿码, 两腿新进者
//                                                            累建仓冲击; 一遍 [T][A], 无排序无标签). 期限结构的净线 = 毛二级 − 两腿均摊成本 (含 Config::sell_impact), 全在 UI 现算
//                                                            (成本不进标签, 不被截面去均值吃掉; sell_impact 只进均摊公式, 不进请求)
//   L  评估 + 尾部                                         → 一级 Row[H][T] + 二级 HoldStat[H] (只毛, accum_rows 一遍 → finish_all) + TailRow[H][T] (两腿组内更极端的
//                                                            kTailOfGroup 截的分层 / 多空, 只在 Row::ok 行; Inspect 专属, 不进 Stat 契约 / Search),
//                                                            尾部是 label_row 的逐格扩展 (TailExt), 与标签侧同一遍 A 轴
// 结果一次发布: 页面的 Hold / 超额|绝对 / 冲击 全是纯显示切片, 不进请求; 只有换因子 / Compute (弃缓存) / 作用域变才进 worker.
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑; 新请求开跑不清旧 result: 页面继续整页画旧结果
// (旧因子, 头部文字一并), 新结果发布时一次性覆盖 (UI 持 mutex 读; 一级 Row 大 (~130B × H × T), UI 不拷, 持锁派生成日级序列后放手;
// 发布 = 与 Flow 的 InspectPlanes 交换, 不分配). 进度走原子: Loading 期 done/total = 天; Running 期 = 算子节点 + 标签集.
// 后端: 只 CPU (单因子一棵 Dag; GPU 留后).
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsLoad.hpp"    // XDist
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable / FactorRow / StatScope
#include "gui/util/Service.hpp"
#include "shared/Analysis.hpp" // ReadScope / AggPdf

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Factors {

struct InspectRequest {
  std::string file, expr; // 权重行 key (FactorRow::key = 文件#权重号) + 具体规范串 (Factors 表的有效行; expr 已校验, worker 再 parse 必成功)
  factor::stat::Frame frame = factor::stat::Frame::CS;
  analysis::ReadScope scope;
  std::string universe;
  misc::Ymd start_date = 0, end_date = 0;
  bool reload = false;     // 弃缓存整体重读 (特征库重算过 / 手动 Compute)
  double max_impact = 0.0; // Config::max_impact (买入冲击封顶, 装载时生效 → 进作用域缓存键)
  std::string key() const { return file + "|" + expr + "|" + universe + "|" + std::to_string(start_date) + "|" + std::to_string(end_date); }
};
// 资产轴未就绪 / 区间无月份 → false. row 须是有效 alpha 行 (error 空)
bool MakeInspectRequest(const SharedData &data, const FactorRow &row, bool reload, InspectRequest &req);

// 尾部分层 (Inspect 专属, 不进 Stat 契约): 两腿的组 (顶 / 底) 内更极端的一截, 占该组 kTailOfGroup[j] (= 整截面 kTailOfGroup[j] / kGroups: 2.5% / 1% / 0.25%),
// j 越大越极端. 阈值按 r16 定 (tail_thresholds), 与 grp_of 的组边界同口径 (q = 1 即整组). 一级 [H][T], 只在 Row::ok 行有值, 其余全 0
inline constexpr int kTails = 3;
inline constexpr float kTailOfGroup[kTails] = {0.5f, 0.2f, 0.05f};
static_assert(kTailOfGroup[0] < 1.f && kTailOfGroup[1] < kTailOfGroup[0] && kTailOfGroup[2] < kTailOfGroup[1] && kTailOfGroup[2] > 0.f);
struct TailRow {
  float sum[factor::stat::kLegs][kTails] = {};    // 两腿尾部做多超额 e 的持仓收益 Σe / max(cnt, cap_j) (cap_j = 满档 · kTailOfGroup[j]; 与 Row::grp 同口径)
  uint16_t cnt[factor::stat::kLegs][kTails] = {}; // 两腿尾部样本数
  float ls[kTails] = {};                          // 尾部多空 = ls_of(两腿尾部, Row::mkt, cap_j) (与 Row::ls 同式; 尾空 → 该腿 0)
};

// Inspect 的一级平面: worker 写进 Flow 那份, 发布时与结果整体交换 (旧缓冲回收给 Flow, 下次原地写)
struct InspectPlanes {
  std::vector<factor::stat::Row> rows; // 一级 [hd.n][T], 只毛口径
  std::vector<TailRow> tails;          // 一级尾部 [hd.n][T], 与 rows 同布局, 只毛口径
  std::vector<uint16_t> rx;            // r16(x) [T][A] (CS = 截面秩, TS = 自身分位; 只池内 g ∧ xm, kRankNone 无效): 右上标的轴排序 / K 线着色
  std::vector<uint16_t> rx_all;        // 同上但不看池 (g = xm): 池外段 (出 CS 池 / 当日无成交) 的秩, K 线淡色显示; 评估不用
  void clear() { rows.clear(), tails.clear(), rx.clear(), rx_all.clear(); }
};

struct InspectResult {
  std::string key; // = InspectRequest::key(); 空 = 无结果
  std::string file, expr;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  StatScope scope;
  std::vector<misc::Ymd> dates; // [days]
  int n_hold = 0;
  factor::stat::Holds hd;                   // hd.h[hi]
  std::vector<CostTier> costs;              // [C] = FeatureTable::costs 序 (期限结构净线: 毛二级 − 两腿均摊换手成本, UI 现算)
  analysis::ReadScope read_scope;           // 特征库作用域 (目录 + 子轴 + 月份): 右下 K 线按它读库; uni 做子轴 ↔ 全局 id
  std::vector<float> uni_index;             // [T] universe 等权指数 (= Loaded::index, 作用域级, 起点 1): 右上 K 线叠加, UI 按视野左端个股价重锚
  InspectPlanes pl;                         // 一级平面 (Row / 尾部 / 秩)
  std::vector<factor::stat::HoldStat> hold; // 二级 [hd.n], 只毛口径
  XDist xdist;                              // 根输入值分布 (全体 + 各组, 左下右半图)
  float valid_pct = 0.f, rank_pct = 0.f;    // 池内有效率 / 可秩率 (FactorsLoad valid_pct_of / rank_pct_of)
  double eval_ms = 0.0;
  std::string error; // 非空 = 没算成 (特征叶值域越界等), 其余字段空

  const factor::stat::Row *rows_at(int hi) const { return pl.rows.data() + static_cast<size_t>(hi) * scope.T; }
  const TailRow *tails_at(int hi) const { return pl.tails.data() + static_cast<size_t>(hi) * scope.T; }
};

// 进度面 = 自身 (Published): Loading 期 done/total = 天; Running 期 = 算子节点 + 成本 + 评估
class InspectService : public Worker<InspectService, InspectRequest>, public Published {
  friend Worker<InspectService, InspectRequest>;

public:
  static constexpr const char *kName = "InspectWorker";
  InspectService(); // 两者都在 .cpp: Flow 不完整
  ~InspectService();

  void SetFeatureTable(FeatureTable &&t) { feats_ = std::move(t); } // 首个 Request 前 (进页) 调一次
  const FeatureTable &feats() const { return feats_; }

  // UI 持 mutex 读 (不拷 rows)
  InspectResult result;
  uint64_t result_gen = 0; // result 每换一次 +1 (UI 派生缓存键; epoch 随进度跳, 不能当键)
  std::string message;     // 本轮说明 (如 "特征库无数据"), 空 = 无

private:
  struct Flow;
  static constexpr Phase kStart = Phase::Loading;
  Progress &board() { return *this; }
  bool run(InspectRequest &req);
  bool evaluate(const InspectRequest &req, Flow &F); // false = 取消

  FeatureTable feats_;
  std::unique_ptr<Flow> flow_; // 首个请求懒建, 随 worker 活 (S / Y 缓存跨请求)
};

} // namespace GUI::Factors
