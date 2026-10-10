// InspectService — Factors→Inspect 页的单 worker: 对 Factors 表里高光的那一个因子 (view_file) 读特征库 → 一棵 Dag → CPU
// evaluator → Stat, **保留一级 Row** (FactorsService 全表 Run 只留二级 HoldStat, 画不了沿时间的东西) 给页面画分层累计 / 多空 / IC 分布.
// 装载层与 FactorsService 共用 (FactorsLoad.hpp); 算子 / 流程层 (factor/Stat/Cpu.hpp run / EvalCpu.hpp run_node) 与搜索共用, 并行由
// 常驻 ForkJoin 出 (factor/Exec.hpp).
//
// 与 FactorsService 的分工: Factors 页 = 全部因子一轮排名 (回写文件); Inspect = 一个因子深看 (不写文件, 结果只在内存).
//
// 计算 flow (worker 私有 Flow, 分级缓存, 请求来了从第一个 key 不匹配的级重算, 上游全部复用; 缓冲常驻, 稳态零分配):
//   S  作用域   key = 特征库目录 | 子轴 hash | 月份        → dates, 截面门控, 毛标签 [H], 成本 [C]; 特征平面缓存 (作用域内读过的全留, 只增)
//   Y  标签集   key = S + sell_impact                      → [c = 0..C] {lv, sv, m, ry[H]}: c = 0 毛, c ≥ 1 扣 costs[c−1] + sell_impact 的净
//   X  因子     key = S + expr                             → 缺的特征补读 → Dag 节点 (run_node, 全核) → 根平面 (Pool 常驻)
//   R  rank_x   key = X                                    → ws[T][A]
//   L  立方体   key = R × Y                                → Row[c][H][T] + HoldStat[c][H] (eval_rows 每标签集一遍, ws 共用)
// 结果一次发布整个立方体: 页面的 Hold / 冲击档 / 超额|绝对 全是纯显示切片, 不进请求; 只有换因子 / Compute (弃缓存) / 作用域变才进 worker.
//
// 线程模型 (对仗 FactorsService): GUI 线程 Request 覆盖挂起请求 + 取消在跑 + 懒起 worker; 新请求开跑不清旧 result: 页面继续整页画旧结果
// (旧因子, 头部文字一并), 新结果发布时一次性覆盖 (UI 持 mutex 读; 一级 Row 大 (~140B × (C+1) × H × T), UI 不拷, 持锁派生成日级序列后放手;
// 发布 = 与 Flow 的立方体缓冲交换, 不分配). 进度走原子: Loading 期 done/total = 天; Running 期 = 算子节点 + 标签集.
// 后端: 只 CPU (单因子一棵 Dag; GPU 留后).
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable / FactorRow / StatScope
#include "shared/Analysis.hpp"                          // ReadScope

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
  std::string file, expr; // 因子文件名 + 规范串 (Factors 表的有效行; expr 已校验, worker 再 parse 必成功)
  factor::stat::Frame frame = factor::stat::Frame::CS;
  analysis::ReadScope scope;
  std::string universe, start_date, end_date;
  double sell_impact = 0.0; // 平仓固定冲击 (Config::sell_impact), 净标签集 (c ≥ 1) 用
  bool reload = false;      // 弃缓存整体重读 (特征库重算过 / 手动 Compute)
  std::string key() const { return file + "|" + expr + "|" + universe + "|" + start_date + "|" + end_date + "|" + std::to_string(sell_impact); }
};
// 资产轴未就绪 / 区间无月份 → false. row 须是有效 alpha 行 (error 空)
bool MakeInspectRequest(const SharedData &data, const FactorRow &row, bool reload, InspectRequest &req);

struct InspectResult {
  std::string key; // = InspectRequest::key(); 空 = 无结果
  std::string file, expr;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  StatScope scope;
  std::vector<std::string> dates; // [days] YYYYMMDD
  int n_hold = 0;
  factor::stat::Holds hd;                   // hd.h[hi]
  std::vector<int> impacts;                 // [C + 1] 冲击档 (万): [0] = 0 毛, 其后 = FeatureTable::costs 序
  double sell_impact = 0.0;                 // 净档的平仓冲击
  std::vector<factor::stat::Row> rows;      // 一级 [C + 1][hd.n][T]
  std::vector<factor::stat::HoldStat> hold; // 二级 [C + 1][hd.n]
  float valid_pct = 0.f;
  double eval_ms = 0.0;
  std::string error; // 非空 = 没算成 (特征叶值域越界等), 其余字段空

  int n_impact() const { return static_cast<int>(impacts.size()); }
  int impact_index(int amt) const { // 找不到 (字段表换过) → 0 毛
    for (int c = 0; c < n_impact(); ++c)
      if (impacts[static_cast<size_t>(c)] == amt)
        return c;
    return 0;
  }
  const factor::stat::Row *rows_at(int c, int hi) const { return rows.data() + (static_cast<size_t>(c) * hd.n + hi) * scope.T; }
  const factor::stat::HoldStat *hold_at(int c) const { return hold.data() + static_cast<size_t>(c) * hd.n; } // [hd.n]
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
