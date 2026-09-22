// OperatorsService — Factors→Operators 表的单 worker: 一张合成张量喂给 OpTable 全部算子,
// 逐算子跑 cpu (挖掘向量) / stream (实盘流式) / gpu (CUDA, 编了才有), 对拍 + 计时, 算完一行发布一行.
//
// 线程模型 (对仗 task_features 的 StreamService, 但不读特征库, 故不套它):
//   - GUI 线程: Request(req) 覆盖挂起请求 + 取消在跑 + 唤醒 (worker 懒起); RequestCancel 只中断
//   - worker:   取最新请求 → 全行复位 Pending → 逐算子 Running → 算 → 持锁写回 Done, epoch++
//   - UI:       持 mutex 读 rows 快照; 进度走原子, 免锁
//
// 对拍口径与 op_check 完全一致 (同一份 factor/Check.hpp): PLAIN profile, 配方按算子,
// d/k 按每算子默认表 (kDParams / kKParams), 容差 tol_of(name, gpu, PLAIN). 差别只在
// op_check 全扫 profile × d, 这里每算子只跑一组默认参数.
// 计时: cpu / stream = 整段 wall time (steady_clock); gpu = 纯 kernel (cudaEvent, 不含
// cudaMalloc + H2D/D2H —— 搬运是 GpuRun 对拍接口的成本, 不是算子的), 首次调用前做一次热身.
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

// 页面参数: 张量形状 [times × A]. times = 时间轴长度 (期 = 分钟, 以后可选秒), 必须整段
// (段 = 交易日 = kSegLen 分钟, 段界对齐是 EXPAND 的前提, UI 负责取整).
// A 默认 5000 = GPU 后端设计点 (一线程一资产, 太小喂不满卡, 吞吐不公允).
// d/k 每算子自带默认 (Check.hpp kDParams / kKParams); seed 由 Request() 取时间戳
// (不做复现, 同一轮内所有算子共享同一张量).
struct OperatorsRequest {
  int times = 10 * factor::kSegLen;
  int A = 5000;
  unsigned seed = 1; // Request() 时间戳填充, UI 不编辑
};

enum class RowStatus : uint8_t { Pending,
                                 Running,
                                 Done };

// 一行 = OpTable 一个算子: 静态列直接来自表 (三维分类: T 窗 × A 域 × 核类), 动态列由 worker 发布
struct OperatorRow {
  // 静态
  const char *name = nullptr;
  int arity = 0;
  factor::Win win = factor::Win::POINT;      // T 窗 (CS 组恒 POINT)
  factor::Scope scope = factor::Scope::SELF; // A 域
  factor::Kern kern = factor::Kern::MAP;     // 核类
  const char *params = nullptr;              // OpTable 参数列: 本算子读取的 Param 字段名, 如 "d,k"
  const char *formula = nullptr;             // LaTeX (OpTable 公式列, 符号规范见 OpTable.hpp 头注)
  const char *note = nullptr;                // 备注 (退化条件 / 参数含义)
  // 动态
  factor::Param param; // 本轮实际喂的参数 (d/k/k2 来自 Check.hpp 每算子默认表); 复位时就填, 不等跑到
  RowStatus status = RowStatus::Pending;
  factor::check::Diff stream;                    // stream vs cpu
  factor::check::Diff gpu;                       // gpu vs cpu (gpu_ms < 0 时无意义)
  double cpu_ms = 0, stream_ms = 0, gpu_ms = -1; // gpu_ms = 纯 kernel; < 0 = 无 GPU 后端
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
  bool gpu_warmed_ = false; // 仅 worker 线程读写
};

} // namespace GUI::Factors
