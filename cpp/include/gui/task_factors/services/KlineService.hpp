// KlineService — Factors→Inspect 右下图的单 worker: 对选中的一个标的, 沿 Inspect 结果的交易日逐日流式读 L1 分钟 OHLC (选列读, 每日只碰
// 5 个小文件) → 追加 + 单调前缀发布, GUI 画已发布前缀 (发布协议对仗 shared/OrderFlow.hpp 的 Kline: pub word [gen|days|points]; 数组按
// 满容量预留后只 push_back → data() 稳定). 每天之间轮询新请求: 拖标的随时打断, 新代从头流.
// 请求 = (gen, 特征库作用域 (目录 + 子轴), 日期表, 子轴资产下标, 列号). gen 由 GUI 递增自持, 与发布面配对; 新请求覆盖旧的.
// 线程模型: GUI 线程 Request 覆盖挂起请求 + 懒起 worker. worker 只在 "GUI 请求代 ≠ 发布代" 时重建数组 (此刻 GUI 不读前缀), 免锁.
#pragma once

#include "shared/Analysis.hpp" // ReadScope

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

namespace GUI::Factors {

struct KlineRequest {
  uint32_t gen = 0;
  analysis::ReadScope scope;    // features_dir + uni (reader 的期望子轴)
  std::vector<misc::Ymd> dates; // = InspectResult::dates; x = 日下标 × kSegLen + 分钟
  int sub = -1;                 // 子轴资产下标
  uint32_t col[5] = {};         // L1 列号: open high low close ts_valid (FeatureTable 解析)
};

// worker 写 / GUI 前缀读 (免锁 SPSC)
struct KlineStream {
  // pub word: [gen:16][days:16][points:32]; gen 换代时 days / points 归零
  static constexpr uint64_t pack(uint32_t gen, size_t days, size_t points) {
    return (static_cast<uint64_t>(gen & 0xFFFF) << 48) | (static_cast<uint64_t>(days & 0xFFFF) << 32) | static_cast<uint64_t>(points & 0xFFFFFFFF);
  }
  static constexpr void unpack(uint64_t w, uint32_t &gen, size_t &days, size_t &points) {
    gen = static_cast<uint32_t>(w >> 48);
    days = static_cast<size_t>((w >> 32) & 0xFFFF);
    points = static_cast<size_t>(w & 0xFFFFFFFF);
  }
  std::vector<double> x, open, high, low, close; // 有效分钟 (ts_valid), x = day · kSegLen + m
  std::atomic<uint64_t> pub{0};
  std::atomic<double> y_min{0.0}, y_max{0.0}; // 已发布 OHLC 范围 (points > 0 才有意义)
};

class KlineService {
public:
  KlineService();
  ~KlineService(); // Stop (join); 在 .cpp 定义 (unique_ptr<Impl> 要完整类型)
  KlineService(const KlineService &) = delete;
  KlineService &operator=(const KlineService &) = delete;

  void Request(KlineRequest req); // GUI 线程: 覆盖挂起请求 + 懒起 worker
  void Stop();

  const KlineStream &stream() const { return s_; }

private:
  struct Impl; // worker 私有: reader + 选列缓冲
  void worker_loop();
  void begin(KlineRequest &&req);
  bool step(); // 装一天并发布; 返回是否还有下一天

  KlineStream s_;
  std::thread thread_;
  std::mutex req_mutex_;
  std::condition_variable req_cv_;
  std::optional<KlineRequest> pending_;
  std::atomic<bool> stop_{false};
  // ---- worker 私有 ----
  std::unique_ptr<Impl> impl_;
  KlineRequest cur_;
  size_t next_day_ = 0;
  bool active_ = false;
  double y_min_ = 0.0, y_max_ = 0.0;
};

} // namespace GUI::Factors
