// InspectService — Factors→Inspect 页的单 worker: 对 Factors 表里高光的那一个因子 (view_file) 读特征库 → 一棵 Dag → CPU
// evaluator → Stat, **保留一级 Row[H][T]** (FactorsService 全表 Run 只留二级 HoldStat, 画不了沿时间的东西) 给页面画
// 分层累计 / 多空 / IC 时序. 装载层与 FactorsService 共用 (FactorsLoad.hpp).
//
// 与 FactorsService 的分工: Factors 页 = 全部因子一轮排名 (回写文件); Inspect = 一个因子深看 (不写文件, 结果只在内存).
//
// 缓存 (worker 私有, 跨请求存活): 同一作用域 (特征库目录 + 子轴 + 月份) 下标签平面 + 标签 rank (ry) 只装一次; 特征平面
// 只补当前因子缺的, 不再用的释放 (内存 = 标签 + 本因子的特征). 换作用域 / reload = true → 整体重读.
//
// 线程模型 (对仗 FactorsService): GUI 线程 Request 覆盖挂起请求 + 取消在跑 + 懒起 worker; 新请求开跑即清旧 result (页面不显示
// 别的因子的图); 结束一次性发布 result (UI 持 mutex 读; 一级 Row 大 (~140B × H × T), UI 不拷, 持锁派生成日级序列后放手).
// 进度走原子: Loading 期 done/total = 天; Running 期 = 算子节点.
// 后端: 只 CPU (单因子一棵 Dag, 全核 run_node_par 够快; GPU 留后).
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/task_factors/services/FactorsService.hpp" // FeatureTable / FactorRow / StatScope / kMaxAmt
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
  bool reload = false; // 弃缓存整体重读 (特征库重算过 / 手动 Compute)
  std::string key() const { return file + "|" + expr + "|" + universe + "|" + start_date + "|" + end_date; }
};
// 资产轴未就绪 / 区间无月份 → false. row 须是有效 alpha 行 (error 空)
bool MakeInspectRequest(const SharedData &data, const FactorRow &row, bool reload, InspectRequest &req);

struct InspectResult {
  std::string key; // = InspectRequest::key(); 空 = 无结果
  std::string file, expr;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  StatScope scope;
  std::vector<std::string> dates; // [days] YYYYMMDD
  int n_amt = 0, n_hold = 0;
  int amt[kMaxAmt] = {};
  factor::stat::Holds hd;              // hd.h[ai * n_hold + hi]
  std::vector<factor::stat::Row> rows; // 一级 [hd.n][T]
  factor::stat::HoldStat hold[kMaxAmt][factor::stat::kMaxHold];
  float valid_pct = 0.f;
  double eval_ms = 0.0;
  std::string error; // 非空 = 没算成 (特征叶值域越界等), 其余字段空
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
  struct Cache;
  void worker_loop();
  bool evaluate(const InspectRequest &req, Cache &cache); // false = 取消

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
