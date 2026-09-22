// OperatorsService — Factors→Operators 表的单 worker: 一张合成张量喂给 OpTable 全部算子,
// 逐算子跑 naive (参考) / stream (实盘流式) / gpu (CUDA, 编了才有), 对拍 + 计时, 算完一行发布一行.
//
// 线程模型 (对仗 task_features 的 StreamService, 但不读特征库, 故不套它):
//   - GUI 线程: Request(req) 覆盖挂起请求 + 取消在跑 + 唤醒 (worker 懒起); RequestCancel 只中断
//   - worker:   取最新请求 → 全行复位 Pending → 逐算子 Running → 算 → 持锁写回 Done, epoch++
//   - UI:       持 mutex 读 rows 快照; 进度走原子, 免锁
//
// 对拍口径与 op_check 完全一致 (同一份 factor/Check.hpp): PLAIN profile, 配方按算子, k 按 kKParams,
// 容差 tol_of(name, gpu, PLAIN). 差别只在 op_check 全扫 profile × d, 这里只跑页面给的一组 (d, seed).
// 计时 = 各后端整段 wall time (steady_clock); GPU 含 cudaMalloc + H2D/D2H (GpuRun 接口如此), 首次调用
// 前做一次热身 (CUDA 上下文初始化不计入).
#pragma once

#include "factor/Check.hpp"
#include "factor/Contract.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

namespace GUI::Factors {

// 页面参数: 张量形状 + 窗长 + 种子. T = days · kSegLen (段界对齐是 EXPAND 的前提, 故按段数给)
struct OperatorsRequest {
  int days = 10;
  int A = 128;
  int d = 20;
  unsigned seed = 1;
  int T() const { return days * factor::kSegLen; }
};

enum class RowStatus : uint8_t { Pending,
                                 Running,
                                 Done };

// 一行 = OpTable 一个算子: 静态列直接来自表, 动态列由 worker 发布
struct OperatorRow {
  // 静态
  const char *name = nullptr;
  bool is_cs = false;
  int arity = 0;
  factor::Win win = factor::Win::POINT; // CS 无窗, 恒 POINT
  factor::Strat strat = factor::Strat::POINT;
  const char *params = nullptr;  // OpTable 参数列: 本算子读取的 Param 字段名, 如 "d,k"
  const char *formula = nullptr; // LaTeX (OpTable 公式列, 符号规范见 OpTable.hpp 头注)
  const char *note = nullptr;    // 备注 (退化条件 / 参数含义)
  // 动态
  factor::Param param; // 本轮实际喂的参数 (d 来自请求, k/k2 来自 Check.hpp kKParams); 复位时就填, 不等跑到
  RowStatus status = RowStatus::Pending;
  factor::check::Diff stream;                      // stream vs naive
  factor::check::Diff gpu;                         // gpu vs naive (gpu_ms < 0 时无意义)
  double naive_ms = 0, stream_ms = 0, gpu_ms = -1; // gpu_ms < 0 = 无 GPU 后端
};

enum class OperatorsStatus : uint8_t { Idle,
                                       Running,
                                       Done,
                                       Cancelled };

class OperatorsService {
public:
  OperatorsService(); // 由 OpTable 展开静态行 (顺序 = 表序: TS 逐点 / TS 有窗 / CS)
  ~OperatorsService() { Stop(); }
  OperatorsService(const OperatorsService &) = delete;
  OperatorsService &operator=(const OperatorsService &) = delete;

  // GUI 线程
  void Request(const OperatorsRequest &req);
  void RequestCancel() { cancel_.store(true, std::memory_order_relaxed); }
  void Stop(); // join (幂等)

  // 进度 (原子, 免锁)
  OperatorsStatus status() const { return status_.load(std::memory_order_acquire); }
  int done() const { return done_.load(std::memory_order_relaxed); }
  int total() const { return static_cast<int>(rows.size()); }
  int failed() const { return failed_.load(std::memory_order_relaxed); }
  uint64_t epoch() const { return epoch_.load(std::memory_order_relaxed); }
  bool gpu_available() const { return gpu_.load(std::memory_order_relaxed); }

  // UI 持锁读; current = 正在跑 / 上次跑完的参数快照
  std::mutex mutex;
  std::vector<OperatorRow> rows;
  OperatorsRequest current;

private:
  // 每行一个跑手: 由 OpTable 宏实例化的模板, 只算动态列 (静态列 worker 不碰)
  using RunFn = void (*)(const OperatorsRequest &, OperatorRow &);
  std::vector<RunFn> runners_;

  void worker_loop();

  std::thread thread_;
  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<OperatorsRequest> pending_;
  std::atomic<bool> cancel_{false};
  std::atomic<bool> stop_{false};
  std::atomic<OperatorsStatus> status_{OperatorsStatus::Idle};
  std::atomic<int> done_{0};
  std::atomic<int> failed_{0};
  std::atomic<uint64_t> epoch_{0};
  std::atomic<bool> gpu_{false};
  bool gpu_warmed_ = false; // 仅 worker 线程读写
};

} // namespace GUI::Factors
