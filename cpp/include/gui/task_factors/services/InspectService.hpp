// InspectService — Factors→Inspect 页的单 worker: 对 Factors 表里高光的那一个因子 (view_file) 读特征库 → 一棵 Dag → CPU
// evaluator → Stat, **保留一级 Row** (FactorsService 全表 Run 只留二级 HoldStat, 画不了沿时间的东西) 给页面画分层累计 / 多空 / IC 分布.
// 装载层与 FactorsService 共用 (FactorsLoad.hpp); 算子 / 流程层 (factor/Stat/Cpu.hpp run / EvalCpu.hpp run_node) 与搜索共用, 并行由
// 常驻 ForkJoin 出 (misc/exec.hpp).
//
// 与 FactorsService 的分工: Factors 页 = 全部因子一轮排名 (回写文件); Inspect = 一个因子深看 (不写文件, 结果只在内存).
//
// 计算 flow (worker 私有 Flow; 缓冲常驻, 稳态零分配). 只有 S / Y 跨请求缓存 (作用域不变就复用), 其后每次请求整条重算:
//   S  作用域   FactorsLoad ScopeCache, key = scope_key (层 | EvalScope::key | 日段 | YSpec: 持有期 / 金额段 / 封顶 / 指数)
//               → Y = 截面门控, 毛标签 [H], 成本 [C], 池等权指数, 可成交 / 估值价 (book); X = 特征平面 (作用域内读过的全留, 只增)
//               (装载前 assert_fits: 放不下 = 因子层太细 / 评估期太长)
//   Y  标签     随 S                                       → 毛标签秩: Engine<Cpu>::stat_session (Stat/Cpu.hpp StatSession, 一次常驻); 没有净标签集
//   X  因子                                                → 缺的特征补读 (ensure_x) → Engine<Cpu>::walk (run_node, 全核) → 根平面 (HostPool 常驻)
//   R  rank_x                                              → 直接秩进 InspectPlanes::rx (随结果发布: 右上资产轴 / K 线着色);
//                                                            之前多跑一遍 g = xm 的 rank_x → rx_all, 池外段也有秩可显
//   C  成本                                                → leg_codes(rx) → CostTier[C] (FactorsService.hpp): 每金额段 × 每持有期一份换手成本 CostAcc (FactorsLoad
//                                                            cost_tiers, 与 Run 同一份; Stat 契约【换手成本】: t ≥ h 的行逐格比 t−h 腿码, 两腿新进者
//                                                            累建仓冲击; 一遍 [T][A], 无排序无标签). 期限结构的净线 = 毛二级 − 两腿均摊成本 (含 Config::fixed_impact), 全在 UI 现算
//                                                            (成本不进标签, 不被截面去均值吃掉; fixed_impact 只进均摊公式, 不进请求)
//   L  评估 + 尾部                                         → 一级 Row[H][T] + 二级 HoldStat[H] (只毛, accum_rows 一遍 → finish_all) + TailRow[H][T] (两腿组内更极端的
//                                                            kTailOfGroup 截的分层 / 多空, 只在 Row::ok 行; Inspect 专属, 不进 Stat 契约 / Search),
//                                                            尾部是 label_row 的逐格扩展 (TailExt), 与标签侧同一遍 A 轴
//   N  净值                                                → 每持有期 FactorsBook run_book (换仓点分批 1/h, 买不进空着, 卖不出延长; Y 带 book 平面) → nav 日收益
//                                                            (分层 / 尾部 / LS 的绝对 + 超额, 只毛)
// 结果一次发布: 页面的 Hold / 超额|绝对 / 冲击 全是纯显示切片, 不进请求; 只有换因子 / Compute (弃缓存) / 作用域变才进 worker.
//
// 线程模型 (gui/util/Service.hpp, 随对象): GUI 线程 Request 覆盖挂起请求 + 取消在跑; 新请求开跑不清旧 result: 页面继续整页画旧结果
// (旧因子, 头部文字一并), 新结果发布时一次性覆盖 (UI 持 mutex 读; 一级 Row 大 (~130B × H × T), UI 不拷, 持锁派生成日级序列后放手;
// 发布 = 与 Flow 的 InspectPlanes 交换, 不分配). 进度走原子: Loading 期 done/total = 天; Running 期 = 算子节点 + 标签集.
// 后端: 只 CPU (单因子一棵 Dag; GPU 留后).
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsLoad.hpp"    // AlphaDist
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable / FactorRow / StatScope
#include "gui/util/Service.hpp"
#include "shared/Analysis.hpp" // ReadScope / AggPdf

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Factors {

struct InspectSpec {
  std::string file, expr; // 权重行 key (FactorRow::key = 文件#权重号) + 具体规范串 (Factors 表的有效行; expr 已校验, worker 再 parse 必成功)
  factor::stat::Frame frame = factor::stat::Frame::CS;
  bool reload = false; // 弃缓存整体重读 (特征库重算过 / 手动 Compute)
  std::string key(const FactorScope &sc) const { return std::to_string(sc.seg) + "|" + file + "|" + expr + "|" + sc.pool; }
};
using InspectRequest = Request<InspectSpec>; // scope.max_impact 装载时生效 → 进作用域缓存键; scope.gpu 不看 (只 CPU)
// 资产轴未就绪 / 标签库未建 → false. row 须是有效 alpha 行 (error 空)
bool MakeInspectRequest(const SharedData &data, const std::string &pool, int seg, const FactorRow &row, bool reload, InspectRequest &req);

// 尾部分层 (Inspect 专属, 不进 Stat 契约): 两腿的组 (顶 / 底) 内更极端的一截, 占该组 kTailOfGroup[j] (= 整截面 kTailOfGroup[j] / kGroups: 2.5% / 1% / 0.25%),
// j 越大越极端. 阈值按 r16 定 (tail_thresholds), 与 grp_of 的组边界同口径 (q = 1 即整组). 一级 [H][T], 只在 Row::ok 行有值, 其余全 0
inline constexpr int kTails = 3;
inline constexpr float kTailOfGroup[kTails] = {0.5f, 0.2f, 0.05f};
static_assert(kTailOfGroup[0] < 1.f && kTailOfGroup[1] < kTailOfGroup[0] && kTailOfGroup[2] < kTailOfGroup[1] && kTailOfGroup[2] > 0.f);
struct TailRow {
  float sum[factor::stat::kLegs][kTails] = {};    // 两腿尾部做多超额 e 的持仓收益 Σe / max(cnt, cap_j) (cap_j = 满仓 · kTailOfGroup[j]; 与 Row::grp 同口径)
  uint16_t cnt[factor::stat::kLegs][kTails] = {}; // 两腿尾部样本数
  float ls[kTails] = {};                          // 尾部多空 = ls_of(两腿尾部, Row::mkt, cap_j) (与 Row::ls 同式; 尾空 → 该腿 0)
};

// 分层累计净值 (FactorsBook 引擎, 每持有期一遍, 复利). 分层线 [0, kGroups) = 20 组, layer_tail(腿, j) = 该腿组内第 j 级尾部 (全按做多);
// LS 线 0 = 顶底 (G20 多 + G1 空, 两腿超额之和), 1 + j = 第 j 级尾部. 净值线 (InspectResult::nav) = 分层绝对 [0, kNavLayers) | 分层超额 (− 引擎基准:
// 批的有秩 ∧ 买得进 资产收益等权均值, 分钟 / 收盘型即 Row::mkt[kLong]) [kNavLayers, 2 kNavLayers) | LS [2 kNavLayers, kNavLines); 值 = 日收益 (当日各行 (1 + 增量) 连乘 − 1)
inline constexpr int kNavLayers = factor::stat::kGroups + factor::stat::kLegs * kTails;
inline constexpr int kNavLs = 1 + kTails;
inline constexpr int kNavLines = 2 * kNavLayers + kNavLs;
inline constexpr int layer_tail(int l, int j) { return factor::stat::kGroups + l * kTails + j; }

// Inspect 的一级平面: worker 写进 Flow 那份, 发布时与结果整体交换 (旧缓冲回收给 Flow, 下次原地写)
struct InspectPlanes {
  std::vector<factor::stat::Row> rows; // 一级 [hd.n][T], 只毛口径
  std::vector<TailRow> tails;          // 一级尾部 [hd.n][T], 与 rows 同布局, 只毛口径
  std::vector<uint16_t> rx;            // r16(x) [T][A] (CS = 截面秩, TS = 自身分位; 只池内 g ∧ xm, kRankNone 无效): 右上资产轴排序 / K 线着色
  std::vector<uint16_t> rx_all;        // 同上但不看池 (g = xm): 池外段 (出 CS 池 / 当日无成交) 的秩, K 线淡色显示; 评估不用
  void clear() { rows.clear(), tails.clear(), rx.clear(), rx_all.clear(); }
};

// EvalStat (与 Run 行同一份): scope / eval_ms / valid% / rank% / 二级 hold[n_hold] (只毛口径) / costs [C] = FeatureTable::y.costs 序
// (期限结构净线: 毛二级 − 两腿均摊换手成本, UI 现算)
struct InspectResult : EvalStat {
  std::string key; // = InspectSpec::key(scope); 空 = 无结果
  std::string file, expr;
  int seg = 0; // 因子层: T = days · seg, K 线 / 横轴按层
  factor::stat::Frame frame = factor::stat::Frame::CS;
  std::vector<misc::Ymd> dates;   // [days]
  factor::stat::Holds hd;         // hd.h[hi]
  analysis::ReadScope read_scope; // 特征库作用域 (EvalScope::rs: 所用库 + A 轴 + 月份): 右下 K 线按它读标签库 (main); axis 做 A 轴 ↔ 全局 id
  std::vector<float> pool_index;  // [T] 池等权指数 (= YPlanes::index[kWeightEq] 篮子 0, 作用域级, 起点 1): 右上 K 线叠加, UI 按视野左端个股价重锚
  InspectPlanes pl;               // 一级平面 (Row / 尾部 / 秩)
  std::vector<float> nav;         // [hd.n][kNavLines][days] 分层累计的日收益 (见 kNavLines)
  AlphaDist alpha_dist;           // alpha 分布 (全体 + 各组, 左下右半图)
  std::string error;              // 非空 = 没算成 (特征叶值域越界等), 其余字段空

  const factor::stat::Row *rows_at(int hi) const { return pl.rows.data() + static_cast<size_t>(hi) * scope.T; }
  const TailRow *tails_at(int hi) const { return pl.tails.data() + static_cast<size_t>(hi) * scope.T; }
  const float *nav_at(int hi, int line) const { return nav.data() + (static_cast<size_t>(hi) * kNavLines + static_cast<size_t>(line)) * scope.days; }
};

// 进度面 = 自身 (Published): Loading 期 done/total = 天; Running 期 = 算子节点 + 成本 + 评估
class InspectService : public Service<InspectService, InspectRequest>, public Published {
  friend Service<InspectService, InspectRequest>;

public:
  static constexpr const char *kName = "InspectWorker";
  InspectService(); // 两者都在 .cpp: Flow 不完整
  ~InspectService();

  void SetFeatureTable(std::shared_ptr<const FeatureTable> t) { feats_ = std::move(t); } // 首个 Request 前 (进页) 调一次
  const FeatureTable &feats() const { return *feats_; }

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

  std::shared_ptr<const FeatureTable> feats_;
  std::unique_ptr<Flow> flow_; // 首个请求懒建, 随 worker 活 (S / Y 缓存跨请求)
};

} // namespace GUI::Factors
