#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <utility>

// ============================================================================
// Progress — 全部后台计算的进度面 (流式分析目标 / 因子服务 / K 线流 同一份)
// ============================================================================
//   原子, UI 免锁读. epoch = 变化计数: 每个动词 +1, 跨轮单调不归零 (UI 以此判 "数据变了")
//
//   worker 骨架 (gui/util/Service.hpp):   begin(kStart)  ……  end(完成?)
//   run 内:                              begin(阶段, 总量)    阶段切换 (Loading → Running), 计数归零
//                                        publish(n)           done += n; 只改了数据不推进度 → publish()
//   回收:                                idle()               回到未算
//
// Published = 进度 + 发布快照锁 (worker 短锁写, UI 渲染帧内持锁读)
// PrefixPub = 免锁单调前缀发布字 (流式 K 线)
// PingPong  = 免锁双槽整体发布 + 进度面 (OrderFlow 盘口 / 资产状态)
// Mailbox   = 整件交接 (worker post → GUI 帧首 take, 所有权转到 GUI 线程; Database 流水线)
// ============================================================================
enum class Phase : uint8_t { Idle,
                             Scanning,
                             Loading,
                             Running,
                             Done,
                             Cancelled };

inline const char *PhaseName(Phase p) {
  constexpr const char *kNames[] = {"idle", "scanning", "loading", "running", "done", "cancelled"};
  return kNames[static_cast<size_t>(p)];
}

struct Progress {
  std::atomic<Phase> phase{Phase::Idle};
  std::atomic<size_t> done{0}, total{0};
  std::atomic<uint64_t> epoch{0};

  void begin(Phase p, size_t n = 0) {
    done.store(0, std::memory_order_relaxed);
    total.store(n, std::memory_order_relaxed);
    phase.store(p, std::memory_order_release);
    epoch.fetch_add(1, std::memory_order_release);
  }
  void publish(size_t n = 0) {
    done.fetch_add(n, std::memory_order_release);
    epoch.fetch_add(1, std::memory_order_release);
  }
  void end(bool completed) {
    phase.store(completed ? Phase::Done : Phase::Cancelled, std::memory_order_release);
    epoch.fetch_add(1, std::memory_order_release);
  }
  void idle() {
    done.store(0, std::memory_order_relaxed);
    total.store(0, std::memory_order_relaxed);
    phase.store(Phase::Idle, std::memory_order_release);
    epoch.fetch_add(1, std::memory_order_release);
  }

  bool busy() const {
    const Phase p = phase.load(std::memory_order_acquire);
    return p == Phase::Scanning || p == Phase::Loading || p == Phase::Running;
  }
};

struct Published : Progress {
  mutable std::mutex mutex;
};

// PrefixPub — 单调前缀发布字 (流式 K 线): 数组按满容量预留后只 push_back → data() 稳定,
// worker 写完一天 release pub, GUI acquire 读 pub 只画前缀. 换代: 先零计数再置 gen (GUI 见到新 gen 时旧计数必已归零)
struct PrefixPub {
  // pub word: [gen:16][days:16][points:32]
  static constexpr uint64_t pack(uint32_t gen, size_t days, size_t points) {
    return (static_cast<uint64_t>(gen & 0xFFFF) << 48) | (static_cast<uint64_t>(days & 0xFFFF) << 32) | static_cast<uint64_t>(points & 0xFFFFFFFF);
  }
  static constexpr void unpack(uint64_t w, uint32_t &gen, size_t &days, size_t &points) {
    gen = static_cast<uint32_t>(w >> 48);
    days = static_cast<size_t>((w >> 32) & 0xFFFF);
    points = static_cast<size_t>(w & 0xFFFFFFFF);
  }
  std::atomic<uint64_t> pub{0};
};

// PingPong — 双槽整体发布 (单写 worker / 单读 GUI, 免锁): worker 等 writable() → 写 back() → commit();
// GUI 帧首 ack() 翻面. pending 未 ack 期间 worker 不碰任何槽. 自带进度面 (worker 骨架的 board)
template <class Slot>
struct PingPong : Progress {
  Slot slot[2];
  std::atomic<int> front{0};
  std::atomic<bool> pending{false};

  const Slot &front_slot() const { return slot[front.load(std::memory_order_acquire)]; }
  bool writable() const { return !pending.load(std::memory_order_acquire); }
  Slot &back() {
    assert(writable() && "背槽未 ack");
    return slot[1 - front.load(std::memory_order_acquire)];
  }
  void commit() { pending.store(true, std::memory_order_release); }
  void ack() { // GUI: 先翻面后清 pending (worker 等 pending 清)
    if (!pending.load(std::memory_order_acquire))
      return;
    front.store(1 - front.load(std::memory_order_relaxed), std::memory_order_release);
    pending.store(false, std::memory_order_release);
  }
  void clear() {
    slot[0].clear();
    slot[1].clear();
    front.store(0, std::memory_order_relaxed);
    pending.store(false, std::memory_order_relaxed);
    idle();
  }
};

// Mailbox — 单件交接 (单写 worker / 单读 GUI): worker 一轮收尾 post 一件, 未取走的旧件被覆盖 (最新覆盖旧的, 与 Worker 请求同义);
// GUI 帧首 take 拿走所有权, 在 GUI 线程换进共享状态 (AssetInfo / 扫描产物只许在 GUI 线程替换). 锁每轮各碰一次, 不在热路径
template <class T>
class Mailbox {
public:
  void post(T v) {
    std::lock_guard<std::mutex> lock(mutex_);
    slot_ = std::move(v);
  }
  std::optional<T> take() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::optional<T> out = std::move(slot_);
    slot_.reset();
    return out;
  }

private:
  std::mutex mutex_;
  std::optional<T> slot_;
};
