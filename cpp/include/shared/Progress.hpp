#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>

// ============================================================================
// Progress — 全部后台计算的进度面 (流式分析目标 / 因子服务 / K 线流 同一份)
// ============================================================================
//   原子, UI 免锁读. epoch = 变化计数: 每个动词 +1, 跨轮单调不归零 (UI 以此判 "数据变了")
//
//   worker 骨架 (gui/util/Worker.hpp):   begin(kStart)  ……  end(完成?)
//   run 内:                              begin(阶段, 总量)    阶段切换 (Loading → Running), 计数归零
//                                        publish(n)           done += n; 只改了数据不推进度 → publish()
//   回收:                                idle()               回到未算
//
// Published = 进度 + 发布快照锁 (worker 短锁写, UI 渲染帧内持锁读)
// PrefixPub = 免锁单调前缀发布字 (流式 K 线)
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
