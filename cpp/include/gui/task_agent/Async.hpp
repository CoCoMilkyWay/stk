#pragma once

// =============================================================================
// Async — 通用单线程异步管道: Chat (问大模型) 与 Lab (问数据 / 引擎) 共用同一份骨架
// =============================================================================
//   GUI 线程: send(Req) 投一件活 (值传递, 无共享可变态) → poll() 逐个取走 Tick (末个 Step::End 带 Out)
//             → live(Live&) 看在跑那件此刻的样子 → busy() / cancel()
//   worker:   取走 Req → fn(req, Pipe) 直通跑完 → 同一把锁内落 busy=false 并入队 Step::End
// =============================================================================

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

namespace GUI::Agent {

// Live 同步策略: 缺省整份赋; 大流式缓冲 (如 Chat 的思考/正文增量) 可重载 sync_live 只追加新增字节
template <class Live>
void sync_live(Live &dst, const Live &src, uint64_t) { dst = src; }

template <class Req, class Step, class Out, class Live>
class Async {
public:
  struct Tick {
    Step step;
    Out out{};
  };
  struct Pipe {
    Async &host;
    const std::atomic<bool> &cancel;
    void step(Step s) const {
      std::lock_guard<std::mutex> lock(host.mutex_);
      host.ticks_.push_back({s, {}});
    }
    template <class Fn>
    void edit(Fn &&fn) const {
      std::lock_guard<std::mutex> lock(host.mutex_);
      fn(host.live_, host.gen_);
    }
  };
  using RunFn = std::function<Out(Req &, const Pipe &)>;

  explicit Async(RunFn fn) : fn_(std::move(fn)) { thread_ = std::thread(&Async::loop, this); }
  ~Async() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stop_ = true;
    }
    cancel_.store(true, std::memory_order_relaxed);
    cv_.notify_all();
    thread_.join();
  }
  Async(const Async &) = delete;
  Async &operator=(const Async &) = delete;

  void send(Req req) {
    assert(!busy() && "worker 正忙, 先等收尾");
    busy_.store(true, std::memory_order_release);
    cancel_.store(false, std::memory_order_relaxed);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      live_ = {}, ++gen_;
      ticks_.clear(); // 上一件已被放下的残留作废
      pending_ = std::move(req);
    }
    cv_.notify_all();
  }

  void cancel() { cancel_.store(true, std::memory_order_relaxed); }
  bool busy() const { return busy_.load(std::memory_order_acquire); }

  std::optional<Tick> poll() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ticks_.empty())
      return std::nullopt;
    Tick t = std::move(ticks_.front());
    ticks_.pop_front();
    return t;
  }

  void live(Live &dst) const {
    std::lock_guard<std::mutex> lock(mutex_);
    sync_live(dst, live_, gen_);
  }

private:
  void loop() {
    Pipe pipe{*this, cancel_};
    for (;;) {
      Req req;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return stop_ || pending_.has_value(); });
        if (stop_)
          return;
        req = std::move(*pending_);
        pending_.reset();
      }
      Out out = fn_(req, pipe);
      // 先落 busy 再交 End (同一把锁内): 取走 End 后随即 send 不撞忙
      std::lock_guard<std::mutex> lock(mutex_);
      busy_.store(false, std::memory_order_release);
      live_ = {}, ++gen_;
      ticks_.push_back({Step::End, std::move(out)});
    }
  }

  RunFn fn_;
  std::thread thread_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::optional<Req> pending_;
  Live live_{};
  uint64_t gen_ = 0;
  std::deque<Tick> ticks_;
  bool stop_ = false;
  std::atomic<bool> busy_{false}, cancel_{false};
};

} // namespace GUI::Agent
