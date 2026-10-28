// FeatureProfile —— 特征计算 perf 采样窗 + meta.json (产物布局见 feature_workers.hpp / misc/perf.hpp)
#include "worker/feature_workers.hpp"

#include "features/Storage/FeatureStore.hpp"
#include "misc/logging.hpp"
#include "misc/perf.hpp"
#include "nlohmann/json.hpp"
#include "shared/Config.hpp"

#include <cassert>
#include <ctime>
#include <filesystem>
#include <fstream>

namespace {

using json = nlohmann::json;

const ComputeStats::Core &core_of(const ComputeStats &stats, ComputeStage kind, int i) {
  const size_t k = static_cast<size_t>(i);
  switch (kind) {
  case ComputeStage::Prefetch:
    return stats.prefetch[k];
  case ComputeStage::TS:
    return stats.ts[k];
  case ComputeStage::CS:
    return stats.cs[k];
  case ComputeStage::IO:
    return stats.io[k];
  }
  assert(false && "unknown compute stage");
  return stats.ts[0];
}

const char *work_unit(ComputeStage kind, const char *ts_unit) {
  switch (kind) {
  case ComputeStage::Prefetch:
  case ComputeStage::IO:
    return "B";
  case ComputeStage::TS:
    return ts_unit;
  case ComputeStage::CS:
    return "beat";
  }
  assert(false && "unknown compute stage");
  return "";
}

// 本地墙钟 YYYYMMDD_HHMMSS (目录名: 字典序 = 时间序)
std::string stamp_now() {
  const std::time_t t = std::time(nullptr);
  std::tm tm{};
  localtime_r(&t, &tm);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y%m%d_%H%M%S", &tm);
  return buf;
}

} // namespace

FeatureProfile::FeatureProfile(ProfileSpec spec, const StageLayout &layout, const StoreRing &store, const ComputeStats &stats)
    : spec_(std::move(spec)), layout_(layout), store_(store), stats_(stats) {}

FeatureProfile::~FeatureProfile() { assert((done_ || !capture_) && "FeatureProfile: 开采后析构前须 finish"); }

FeatureProfile::Snap FeatureProfile::snap() const {
  Snap s{std::chrono::steady_clock::now(), store_.query_io_done(), {}, {}};
  for (const StageLayout::Stage &st : layout_.stages())
    for (int i = 0; i < st.threads; ++i) {
      const ComputeStats::Core &c = core_of(stats_, st.kind, i);
      s.work.push_back(c.work.load(std::memory_order_relaxed));
      s.idle_us.push_back(c.idle_us.load(std::memory_order_relaxed));
    }
  return s;
}

void FeatureProfile::tick() {
  if constexpr (!misc::perf::kEnabled)
    return;
  if (done_)
    return;
  if (!capture_) {
    if (store_.query_io_done() == 0)
      return;
    dir_ = std::string(config::PROFILE_DIR) + "/" + spec_.pool + "/" + stamp_now();
    capture_ = std::make_unique<misc::perf::Capture>(dir_);
    start_ = snap();
    return;
  }
  if (capture_->seconds() >= misc::perf::kSeconds)
    finish();
}

void FeatureProfile::finish() {
  if (done_)
    return;
  done_ = true;
  if (!capture_)
    return; // 首日落盘前就收尾 (区间极短 / 取消): 无稳态可采
  capture_->stop();
  write_meta(snap());
  std::vector<misc::perf::Group> groups;
  for (const StageLayout::Stage &st : layout_.stages()) {
    if (st.threads == 0)
      continue;
    misc::perf::Group g{st.tag, {}};
    for (int i = 0; i < st.threads; ++i)
      g.comms.push_back(StageLayout::thread_name(st, i));
    groups.push_back(std::move(g));
  }
  capture_->report(groups);
  // 进日志不进终端: 采样窗多在管线中途收, 终端正被仪表盘原地重画
  Logger::log("perf", std::to_string(capture_->seconds()) + "s → " + std::filesystem::absolute(dir_).string() + " (报告后台生成, 出齐见 report.done)");
}

void FeatureProfile::write_meta(const Snap &end) const {
  const double sec = std::chrono::duration<double>(end.t - start_.t).count();
  assert(sec > 0.0);
  json workers = json::array(), stages = json::array();
  size_t k = 0;
  for (const StageLayout::Stage &st : layout_.stages()) {
    double work_sum = 0.0, busy_sum = 0.0;
    for (int i = 0; i < st.threads; ++i, ++k) {
      const double work = static_cast<double>(end.work[k] - start_.work[k]) / sec;
      const double busy = 1.0 - static_cast<double>(end.idle_us[k] - start_.idle_us[k]) / (sec * 1e6);
      work_sum += work;
      busy_sum += busy;
      workers.push_back({{"thread", StageLayout::thread_name(st, i)},
                         {"stage", st.tag},
                         {"worker_id", st.first_worker + i},
                         {"cpus", layout_.cpus_of(st, i)},
                         {"work_per_s", work},
                         {"busy", busy}});
    }
    stages.push_back({{"tag", st.tag},
                      {"name", st.name},
                      {"threads", st.threads},
                      {"phys_cores", st.cores},
                      {"work_unit", work_unit(st.kind, spec_.unit)},
                      {"work_per_s", work_sum},
                      {"busy_mean", st.threads > 0 ? busy_sum / st.threads : 0.0}});
  }
  const json meta = {
      {"pool", spec_.pool},
      {"tier", tier_name(spec_.tier)},
      {"assets", spec_.assets},
      {"dates", {{"count", spec_.dates.size()}, {"first", spec_.dates.front()}, {"last", spec_.dates.back()}}},
      {"ring_slots", spec_.ring_slots},
      {"adopt_pct", spec_.adopt_pct},
      {"perf", {{"freq_hz", misc::perf::kFreq}, {"seconds", capture_->seconds()}}},
      {"window", {{"seconds", sec}, {"io_done", {start_.io_done, end.io_done}}, {"io_days_per_s", static_cast<double>(end.io_done - start_.io_done) / sec}}},
      {"stages", stages},
      {"workers", workers},
  };
  std::ofstream f(std::filesystem::path(dir_) / "meta.json");
  assert(f && "meta.json 写不开");
  f << meta.dump(2) << '\n';
}
