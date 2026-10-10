#pragma once

// =============================================================================
// 执行器: 算子 / 流程层唯一的并行抽象 (CPU 侧)
// =============================================================================
//   核与 flow 只写两句话: "把 [0,T) 行切给 fn(t0, t1, tid)" (rows) / "n 个任务给 fn(i, tid)" (tasks); 谁出线程由调用方选:
//     Serial    单线程内联, 零开销. 搜索场景: 因子间并行由上层每线程一个因子, 算子内严禁再起线程.
//     ForkJoin  常驻线程池 fork-join. 交互 / 全表 Run: 节点内 / 行间切满核. 调用线程自己当 tid 0 干活, worker 只在
//               dispatch 期间醒; 一次 dispatch 的固定开销 = 一轮 futex 唤醒 + 一次屏障 (~0.1ms @ 72 线程), 不分配.
//   两者结果逐位一致: 切法不改语义 (行独立 / 段对齐 / A 列块都是无关联切分), 由各核保证.
//   threads() 给调用方按 tid 备暂存 (Scratch[tid]); ForkJoin(1) 退化为 Serial 行为 (无 worker).
//   出错策略: 只 assert.
// =============================================================================

#include <algorithm>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

namespace factor::exec {

struct Serial {
  int threads() const { return 1; }
  // 行区间: fn(t0, t1, tid). unit = 条带边界对齐单位 (单线程无意义, 接口对齐 ForkJoin)
  template <class Fn>
  void rows(int T, Fn &&fn, int unit = 1) const {
    assert(T >= 0 && unit >= 1 && T % unit == 0);
    if (T > 0)
      fn(0, T, 0);
  }
  // 任务: fn(i, tid)
  template <class Fn>
  void tasks(int n, Fn &&fn) const {
    for (int i = 0; i < n; ++i)
      fn(i, 0);
  }
};

class ForkJoin {
public:
  explicit ForkJoin(int threads) : n_(std::max(1, threads)) {
    th_.reserve(static_cast<size_t>(n_ - 1));
    for (int tid = 1; tid < n_; ++tid)
      th_.emplace_back([this, tid] { worker(tid); });
  }
  ~ForkJoin() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      stop_ = true;
      ++gen_;
    }
    cv_.notify_all();
    for (std::thread &t : th_)
      t.join();
  }
  ForkJoin(const ForkJoin &) = delete;
  ForkJoin &operator=(const ForkJoin &) = delete;

  int threads() const { return n_; }

  // 静态条带: [0,T) 按 unit 的倍数均分给 threads 份, fn(t0, t1, tid); 空条带不调. T 须是 unit 的倍数
  template <class Fn>
  void rows(int T, Fn &&fn, int unit = 1) {
    assert(T >= 0 && unit >= 1 && T % unit == 0);
    if (T == 0)
      return;
    const int units = T / unit;
    const int per = (units + n_ - 1) / n_;
    auto job = [&](int tid) {
      const int t0 = tid * per * unit, t1 = std::min(T, (tid + 1) * per * unit);
      if (t0 < t1)
        fn(t0, t1, tid);
    };
    dispatch(job);
  }

  // 动态抢单: fn(i, tid), 任务重轻不均 (IO / A 列块) 用这个
  template <class Fn>
  void tasks(int n, Fn &&fn) {
    if (n <= 0)
      return;
    std::atomic<int> next{0};
    auto job = [&](int tid) {
      for (int i = next.fetch_add(1, std::memory_order_relaxed); i < n; i = next.fetch_add(1, std::memory_order_relaxed))
        fn(i, tid);
    };
    dispatch(job);
  }

private:
  template <class Job>
  void dispatch(Job &job) {
    assert(!busy_ && "ForkJoin 不可重入 (fn 里不得再 dispatch 同一个池)");
    if (n_ == 1) {
      job(0);
      return;
    }
    busy_ = true;
    {
      std::lock_guard<std::mutex> lock(mu_);
      call_ = [](void *ctx, int tid) { (*static_cast<Job *>(ctx))(tid); };
      ctx_ = &job;
      pending_.store(n_ - 1, std::memory_order_relaxed);
      ++gen_;
    }
    cv_.notify_all();
    job(0);
    while (pending_.load(std::memory_order_acquire) != 0) // 本线程干完等尾巴, 自旋让出 (尾巴通常只差几十 µs)
      std::this_thread::yield();
    busy_ = false;
  }

  void worker(int tid) {
    uint64_t seen = 0;
    while (true) {
      void (*call)(void *, int);
      void *ctx;
      {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [&] { return stop_ || gen_ != seen; });
        if (stop_)
          return;
        seen = gen_;
        call = call_, ctx = ctx_;
      }
      call(ctx, tid);
      pending_.fetch_sub(1, std::memory_order_acq_rel);
    }
  }

  const int n_;
  std::vector<std::thread> th_;
  std::mutex mu_;
  std::condition_variable cv_;
  uint64_t gen_ = 0;
  bool stop_ = false;
  bool busy_ = false;
  void (*call_)(void *, int) = nullptr;
  void *ctx_ = nullptr;
  std::atomic<int> pending_{0};
};

} // namespace factor::exec
