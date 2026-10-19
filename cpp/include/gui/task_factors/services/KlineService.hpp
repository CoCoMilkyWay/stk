// KlineService — Factors→Inspect 右下图的单 worker: 对选中的一个资产, 沿 Inspect 结果的交易日逐日流式读因子层 OHLC (选列读, 每日只碰
// 4~5 个小文件) → 追加 + 单调前缀发布, GUI 画已发布前缀 (发布协议 = shared/Progress.hpp PrefixPub, 与 shared/OrderFlow.hpp 的 Kline 同一份).
// 每天之间查 cancel_: 拖资产随时打断, 新代从头流.
// 请求 = (gen, 特征库作用域 (目录 + 库轴), 日期表, 库轴资产下标, 列号). gen 由 GUI 递增自持, 与发布面配对; 新请求覆盖旧的.
// 线程模型 (gui/util/Service.hpp, 随对象). worker 只在 "GUI 请求代 ≠ 发布代" 时重建数组 (此刻 GUI 不读前缀), 免锁.
#pragma once

#include "gui/util/Service.hpp"
#include "shared/Analysis.hpp" // ReadScope
#include "shared/Progress.hpp" // PrefixPub

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

namespace GUI::Factors {

struct KlineRequest {
  uint32_t gen = 0;
  analysis::ReadScope scope;    // 主库 lib (目录 + 库轴, reader 的期望)
  std::vector<misc::Ymd> dates; // = InspectResult::dates; x = 日下标 × VR + 行 (VR = 该层每日有效行数)
  int sub = -1;                 // 库轴资产下标
  size_t level = 0;             // 特征库层 (因子层)
  uint32_t col[5] = {};         // 该层列号: open high low close ts_valid (FeatureTable 解析)
};

// worker 写 / GUI 前缀读 (免锁 SPSC); 进度 done/total = 天
struct KlineStream : Progress, PrefixPub {
  std::vector<double> x, open, high, low, close; // 有效行 (ts_valid), x = day · VR + t
  std::atomic<double> y_min{0.0}, y_max{0.0};    // 已发布 OHLC 范围 (points > 0 才有意义)
};

class KlineService : public Service<KlineService, KlineRequest> {
  friend Service<KlineService, KlineRequest>;

public:
  static constexpr const char *kName = "KlineLane";
  KlineService(); // 两者都在 .cpp: Impl 不完整
  ~KlineService();

  void Request(KlineRequest req); // GUI 线程: 断言后转 Service::Request

  const KlineStream &stream() const { return s_; }

private:
  struct Impl; // worker 私有: reader + 选列缓冲
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return s_; }
  bool run(KlineRequest &req);
  void reset(KlineRequest &&req);
  bool step(size_t d); // 装一天并发布; false = 库判废

  KlineStream s_;
  // ---- worker 私有 ----
  std::unique_ptr<Impl> impl_;
  KlineRequest cur_;
  double y_min_ = 0.0, y_max_ = 0.0;
};

} // namespace GUI::Factors
