#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>

// 进程代 (epoch): 进程内重启 (GUI::ReinitAllTasks) 前移一代. 读盘派生的进程级缓存一律是 Epochal<T>:
// 每代首次 get() 现建 (默认构造 = 读盘), 代内共享一份 → 盘上状态变了, 换一代即全部按盘重读. 业务无关.
namespace misc {

inline std::atomic<uint64_t> g_epoch{1};

inline uint64_t epoch() { return g_epoch.load(std::memory_order_acquire); }
// 调用方保证读缓存的线程全部已停: 上一代 get() 给出的引用此后作废
inline void next_epoch() { g_epoch.fetch_add(1, std::memory_order_acq_rel); }

template <class T>
class Epochal {
public:
  T &get() {
    const uint64_t e = epoch();
    if (built_.load(std::memory_order_acquire) == e)
      return *v_;
    std::lock_guard<std::mutex> lock(mu_);
    if (built_.load(std::memory_order_relaxed) != e) {
      v_.reset();        // 旧代先拆再建: 不让两代的资源 (线程池 / 大表) 同时在
      v_.reset(new T()); // 不经 make_unique: T 可只把构造开给 Epochal<T> (friend)
      built_.store(e, std::memory_order_release);
    }
    return *v_;
  }

private:
  std::mutex mu_;
  std::atomic<uint64_t> built_{0};
  std::unique_ptr<T> v_;
};

} // namespace misc
