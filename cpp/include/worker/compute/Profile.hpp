#pragma once

#include "features/Storage/FeatureLevels.hpp" // TierId
#include "misc/date.hpp"
#include "worker/compute/Layout.hpp"
#include "worker/compute/Progress.hpp"

#include <chrono>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

class StoreRing;

// ============================================================================
// 特征计算采样 (misc::perf::kEnabled = PROFILE 模式, 否则全空): 协调线程每 100ms tick, 取消 / 管线收尾 finish.
//   首日落盘起 (流水线已灌满, 不含 InitCores / 灌池) perf record 本进程, 满 misc::perf::kSeconds / 取消 / 管线收尾即停,
//   落 <kProfileDir>/<池>/<开采时刻 YYYYMMDD_HHMMSS>/ (每轮一目录, 前后两轮可对比):
//     meta.json   本轮配置 (池 / 档 / A 轴 / 日期轴 / 核布局) + 采样窗实测 (日环首末格数; 落盘天速; 逐核工作量速率 / 忙碌占比)
//     perf 报告   按段分组 (组名 = 段 tag, 组内线程 = StageLayout::thread_name), 组首 / 组尾两核 + 全段合并 (见 misc/perf.hpp)
//   忙碌占比口径同仪表盘 (1 − 干等 / 墙钟); perf 样本只计在 CPU 上的时间, 两者之差 = 缺页 / 抢占等非干等的阻塞.
// ============================================================================
namespace misc::perf {
class Capture;
}

inline constexpr const char *kProfileDir = "../../../../output/profile";

struct ProfileSpec {
  std::string pool;
  TierId tier;
  const char *unit; // 时序工作量单位 (TierFeed::kUnit)
  size_t assets;
  std::span<const misc::Ymd> dates;
};

class ComputeProfile {
public:
  ComputeProfile(ProfileSpec spec, const StageLayout &layout, const StoreRing &store, const ComputeStats &stats);
  ~ComputeProfile();
  ComputeProfile(const ComputeProfile &) = delete;
  ComputeProfile &operator=(const ComputeProfile &) = delete;

  void tick();   // 首日落盘起开采; 满 kSeconds 即 close, 报告在报告线程出, 管线照跑
  void finish(); // close + 等报告出齐 (幂等; 未开采则无产物). 取消即调: 采样窗止于取消时刻

private:
  void close(); // 停采 + 落 meta.json + 起报告线程 (幂等)
  // 核序 = layout.stages() 段序 × 段内序号
  struct Snap {
    std::chrono::steady_clock::time_point t;
    size_t io_done = 0;
    size_t slots = 0;
    std::vector<size_t> work, idle_us;
  };
  Snap snap() const;
  void write_meta(const Snap &end) const;

  ProfileSpec spec_;
  const StageLayout &layout_;
  const StoreRing &store_;
  const ComputeStats &stats_;
  std::string dir_;
  std::unique_ptr<misc::perf::Capture> capture_;
  Snap start_;
  bool done_ = false;
  std::thread report_;
};
