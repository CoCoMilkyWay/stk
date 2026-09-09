#include "shared/Analysis.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "misc/date.hpp"
#include "misc/profiler.hpp"
#include "shared/Config.hpp"

#include <numeric>
#include <random>
#include <thread>

namespace analysis {

ReadScope read_scope(const Config &cfg, size_t num_assets) {
  ReadScope s;
  s.months = misc::iter_months(cfg.start_date, cfg.end_date);
  s.uni = universe_axis(cfg, num_assets); // 与特征计算同一推导 (A 轴/文件列序/目录都由它定)
  s.features_dir = cfg.FeatureUniverseDir();
  return s;
}

DateList enumerate_dates(const FeatureRead &reader, const std::vector<std::string> &months) {
  TraceN("EnumDates"); // 首帧账目: 每月一次目录迭代 + 每天一次 stat
  DateList out;
  for (size_t m = 0; m < months.size(); ++m) {
    auto ds = reader.list_dates(months[m].substr(0, 4), months[m].substr(4, 2));
    for (auto &d : ds) {
      out.dates.push_back(std::move(d));
      out.month.push_back(static_cast<uint16_t>(m));
    }
  }
  return out;
}

void prepare_slots(std::vector<KLLcache> &slots, size_t n, size_t capacity, size_t resolution) {
  if (slots.size() == n) {
    clear_slots(slots);
    return;
  }
  slots.clear();
  slots.reserve(n);
  for (size_t i = 0; i < n; ++i)
    slots.emplace_back(capacity, resolution);
}

void clear_slots(std::vector<KLLcache> &slots) {
  for (auto &kll : slots)
    kll.clear();
}

std::vector<uint32_t> shuffled_order(size_t n) {
  std::vector<uint32_t> order(n);
  std::iota(order.begin(), order.end(), 0u);
  std::shuffle(order.begin(), order.end(), std::mt19937{0x5eed});
  return order;
}

void check_axis(const std::vector<uint32_t> &global_ids) {
  assert(!global_ids.empty() && "universe 为空");
  assert(std::is_sorted(global_ids.begin(), global_ids.end()) &&
         std::adjacent_find(global_ids.begin(), global_ids.end()) == global_ids.end() &&
         "global_ids 必须升序去重 (UniverseAxis::ids)");
  (void)global_ids;
}

ThreadLayout thread_layout(size_t A) {
  const size_t n_blocks = (A + kAssetBlock - 1) / kAssetBlock;
  const size_t n_hw = std::max<size_t>(1, std::thread::hardware_concurrency());
  const size_t n_threads = std::min(n_hw, std::max(kDaysPerBatch, n_blocks));
  return {n_threads, std::min(n_threads, kDaysPerBatch)};
}

} // namespace analysis
