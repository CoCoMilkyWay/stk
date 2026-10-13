// InspectService — Factors→Inspect 页的单 worker: 对 Factors 表里高光的那一个因子 (view_file) 读特征库 → 一棵 Dag → CPU
// evaluator → Stat, **保留一级 Row** (FactorsService 全表 Run 只留二级 HoldStat, 画不了沿时间的东西) 给页面画分层累计 / 多空 / IC 分布.
// 装载层与 FactorsService 共用 (FactorsLoad.hpp); 算子 / 流程层 (factor/Stat/Cpu.hpp run / EvalCpu.hpp run_node) 与搜索共用, 并行由
// 常驻 ForkJoin 出 (factor/Exec.hpp).
//
// 与 FactorsService 的分工: Factors 页 = 全部因子一轮排名 (回写文件); Inspect = 一个因子深看 (不写文件, 结果只在内存).
//
// 计算 flow (worker 私有 Flow; 缓冲常驻, 稳态零分配). 只有 S / Y 跨请求缓存 (作用域不变就复用), 其后每次请求整条重算:
//   S  作用域   key = 特征库目录 | 子轴 hash | 月份 | 封顶  → dates, 截面门控, 毛标签 [H], 成本 [C], universe 指数; 特征平面缓存 (作用域内读过的全留, 只增)
//   Y  标签     随 S                                       → 毛 {v[kLegs], m, ry[H]} (ry = prep_label 一次常驻); 没有净标签集
//   X  因子                                                → 缺的特征补读 → Dag 节点 (run_node, 全核) → 根平面 (Pool 常驻)
//   R  rank_x                                              → 直接秩进 InspectPlanes::rx (随结果发布: 右上标的轴 / K 线着色);
//                                                            之前多跑一遍 g ≡ 1 的 rank_x → rx_all, 池外段也有秩可显
//   C  成本                                                → CostTier[C] (FactorsService.hpp): 每金额档 × 每持有期一份换手成本 CostAcc (FactorsLoad
//                                                            cost_tiers, 与 Run 同一份; Stat 契约【换手成本】: t ≥ h 的行逐格比 t−h 组别, 两腿新进者
//                                                            累建仓冲击; 一遍 [T][A], 无排序无标签). 期限结构的净线 = 毛二级 − 两腿均摊成本 (含 Config::sell_impact), 全在 UI 现算
//                                                            (成本不进标签, 不被截面去均值吃掉; sell_impact 只进均摊公式, 不进请求)
//   L  评估 + 尾部                                         → 一级 Row[H][T] + 二级 HoldStat[H] (只毛, eval_rows 一遍) + TailRow[H][T] (本文件: 两腿组内更极端的
//                                                            kTailOfGroup 截的分层 / 多空, 只在 Row::ok 行; Inspect 专属, 不进 Stat 契约 / Search), 尾部走 eval_rows 的 on_row 同遍
// 结果一次发布: 页面的 Hold / 超额|绝对 / 冲击 全是纯显示切片, 不进请求; 只有换因子 / Compute (弃缓存) / 作用域变才进 worker.
//
// 线程模型 (对仗 FactorsService): GUI 线程 Request 覆盖挂起请求 + 取消在跑 + 懒起 worker; 新请求开跑不清旧 result: 页面继续整页画旧结果
// (旧因子, 头部文字一并), 新结果发布时一次性覆盖 (UI 持 mutex 读; 一级 Row 大 (~130B × H × T), UI 不拷, 持锁派生成日级序列后放手;
// 发布 = 与 Flow 的 InspectPlanes 交换, 不分配). 进度走原子: Loading 期 done/total = 天; Running 期 = 算子节点 + 标签集.
// 后端: 只 CPU (单因子一棵 Dag; GPU 留后).
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable / FactorRow / StatScope
#include "shared/Analysis.hpp"                          // ReadScope / AggPdf

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

struct SharedData;

namespace GUI::Factors {

struct InspectRequest {
  std::string file, expr; // 权重行 key (FactorRow::key = 文件#权重号) + 具体规范串 (Factors 表的有效行; expr 已校验, worker 再 parse 必成功)
  factor::stat::Frame frame = factor::stat::Frame::CS;
  analysis::ReadScope scope;
  std::string universe, start_date, end_date;
  bool reload = false;     // 弃缓存整体重读 (特征库重算过 / 手动 Compute)
  double max_impact = 0.0; // Config::max_impact (买入冲击封顶, 装载时生效 → 进作用域缓存键)
  std::string key() const { return file + "|" + expr + "|" + universe + "|" + start_date + "|" + end_date; }
};
// 资产轴未就绪 / 区间无月份 → false. row 须是有效 alpha 行 (error 空)
bool MakeInspectRequest(const SharedData &data, const FactorRow &row, bool reload, InspectRequest &req);

// 尾部分层 (Inspect 专属, 不进 Stat 契约): 两腿的组 (顶 / 底) 内更极端的一截, 占该组 kTailOfGroup[j] (= 整截面 kTailOfGroup[j] / kGroups: 2.5% / 1% / 0.25%),
// j 越大越极端. 阈值按 r16 定 (tail_thresholds), 与 grp_of 的组边界同口径 (q = 1 即整组). 一级 [H][T], 只在 Row::ok 行有值, 其余全 0
inline constexpr int kTails = 3;
inline constexpr float kTailOfGroup[kTails] = {0.5f, 0.2f, 0.05f};
static_assert(kTailOfGroup[0] < 1.f && kTailOfGroup[1] < kTailOfGroup[0] && kTailOfGroup[2] < kTailOfGroup[1] && kTailOfGroup[2] > 0.f);
struct TailRow {
  float sum[factor::stat::kLegs][kTails] = {};    // 两腿尾部做多超额 e 的和 (Σlv − cnt·mkt[kLong], 与 Row::grp 同口径)
  uint16_t cnt[factor::stat::kLegs][kTails] = {}; // 两腿尾部样本数
  float ls[kTails] = {};                          // 尾部多空 = ls_of(两腿尾部, Row::mkt) (与 Row::ls 同式; 尾空 → 该腿 0)
};

// Inspect 的一级平面: worker 写进 Flow 那份, 发布时与结果整体交换 (旧缓冲回收给 Flow, 下次原地写)
struct InspectPlanes {
  std::vector<factor::stat::Row> rows; // 一级 [hd.n][T], 只毛口径
  std::vector<TailRow> tails;          // 一级尾部 [hd.n][T], 与 rows 同布局, 只毛口径
  std::vector<uint16_t> rx;            // r16(x) [T][A] (CS = 截面秩, TS = 自身分位; 只池内 g ∧ xm, kRankNone 无效): 右上标的轴排序 / K 线着色
  std::vector<uint16_t> rx_all;        // 同上但不看池 (g ≡ 1, 只要 xm): 池外段 (出 CS 池 / 当日无成交) 的秩, K 线淡色显示; 评估不用
  void clear() { rows.clear(), tails.clear(), rx.clear(), rx_all.clear(); }
};

// 根输入值分布 (Inspect 专属, 左下右半图; 与左半 rIC 分布对仗, 同一套 KLL → AggPdf): 样本 = 池内有效格 (rx 有效 ∧ 根输入有效), 组号 = grp_of(rx)
// (与 Stat 同一组). 与持有期无关
struct XDist {
  analysis::AggPdf pdf;                               // 截尾 [lo, hi] 内样本的 PDF (255 点全落在主体上, 重尾不压扁); 只画图, 矩不用它
  uint64_t n = 0;                                     // 全体样本数 (不截尾, 下同)
  float mean = 0.f, sd = 0.f, skew = 0.f, kurt = 0.f; // 全体矩
  float lo = 0.f, hi = 0.f;                           // 截尾范围 = 全体 ICDF 首末内点 (≈ 0.4% / 99.6% 分位)
  float bound[factor::stat::kGroups - 1] = {};        // G_k | G_{k+1} 边界 = 逐行 (G_k 最大值 + G_{k+1} 最小值) / 2 的行平均 (TS 口径组间交叠, 仅示意)
  float share[factor::stat::kGroups] = {};            // 组占全体比例
  float gmean[factor::stat::kGroups] = {};            // 组均值
  std::vector<float> series;                          // 逐行截面均值 (有样本的行, 时序)
};

struct InspectResult {
  std::string key; // = InspectRequest::key(); 空 = 无结果
  std::string file, expr;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  StatScope scope;
  std::vector<std::string> dates; // [days] YYYYMMDD
  int n_hold = 0;
  factor::stat::Holds hd;                   // hd.h[hi]
  std::vector<CostTier> costs;              // [C] = FeatureTable::costs 序 (期限结构净线: 毛二级 − 两腿均摊换手成本, UI 现算)
  analysis::ReadScope read_scope;           // 特征库作用域 (目录 + 子轴 + 月份): 右下 K 线按它读库; uni 做子轴 ↔ 全局 id
  std::vector<float> uni_index;             // [T] universe 等权指数 (= Loaded::index, 作用域级, 起点 1): 右上 K 线叠加, UI 按视野左端个股价重锚
  InspectPlanes pl;                         // 一级平面 (Row / 尾部 / 秩)
  std::vector<factor::stat::HoldStat> hold; // 二级 [hd.n], 只毛口径
  XDist xdist;                              // 根输入值分布 (全体 + 各组, 左下右半图)
  float valid_pct = 0.f;
  double eval_ms = 0.0;
  std::string error; // 非空 = 没算成 (特征叶值域越界等), 其余字段空

  const factor::stat::Row *rows_at(int hi) const { return pl.rows.data() + static_cast<size_t>(hi) * scope.T; }
  const TailRow *tails_at(int hi) const { return pl.tails.data() + static_cast<size_t>(hi) * scope.T; }
};

enum class InspectStatus : uint8_t { Idle,
                                     Loading,
                                     Running,
                                     Done,
                                     Cancelled };

class InspectService {
public:
  InspectService() = default;
  ~InspectService() { Stop(); }
  InspectService(const InspectService &) = delete;
  InspectService &operator=(const InspectService &) = delete;

  void SetFeatureTable(FeatureTable &&t) { feats_ = std::move(t); } // worker 未起时 (进页) 调一次
  const FeatureTable &feats() const { return feats_; }

  // GUI 线程
  void Request(const InspectRequest &req);
  void RequestCancel() { cancel_.store(true, std::memory_order_relaxed); }
  void Stop();

  InspectStatus status() const { return status_.load(std::memory_order_acquire); }
  int done() const { return done_.load(std::memory_order_relaxed); }
  int total() const { return total_.load(std::memory_order_relaxed); }
  uint64_t epoch() const { return epoch_.load(std::memory_order_relaxed); } // result 变了 +1

  // UI 持锁读 (不拷 rows)
  std::mutex mutex;
  InspectResult result;
  std::string message; // 本轮说明 (如 "特征库无数据"), 空 = 无

private:
  struct Flow;
  void worker_loop();
  bool evaluate(const InspectRequest &req, Flow &F); // false = 取消

  FeatureTable feats_;
  std::thread thread_;
  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<InspectRequest> pending_;
  std::atomic<bool> cancel_{false};
  std::atomic<bool> stop_{false};
  std::atomic<InspectStatus> status_{InspectStatus::Idle};
  std::atomic<int> done_{0}, total_{0};
  std::atomic<uint64_t> epoch_{0};
};

} // namespace GUI::Factors
