// FeatureProgress —— 特征计算仪表盘 (拉取式, 固定 5 行, 布局见 feature_workers.hpp)
#include "worker/feature_workers.hpp"

#include "features/Backend/FeatureStore.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace {

constexpr int kLines = 5;
constexpr int kBarWidth = 28;
constexpr int kRefreshMs = 100;
constexpr size_t kSlowMax = 4;     // 慢核行最多展开几个 (一行放得下)
constexpr size_t kSlowGapDays = 2; // 差距 ≥ 此天数才算慢核 (1 天是相位噪声)

// "12m34s" (与 progress_parallel 同款)
std::string fmt_duration(long long seconds) {
  std::ostringstream os;
  if (seconds >= 3600)
    os << seconds / 3600 << "h" << std::setw(2) << std::setfill('0') << (seconds % 3600) / 60 << "m";
  else
    os << seconds / 60 << "m" << std::setw(2) << std::setfill('0') << seconds % 60 << "s";
  return os.str();
}

// 压力色: 落后 gap 天, 钳到池深, 白→红 (满红 = 差距吃满整个池子)
void heat_color(std::ostringstream &os, size_t gap, size_t slots) {
  const float t = std::min(1.0f, static_cast<float>(gap) / static_cast<float>(slots));
  const int gb = static_cast<int>(255.0f * (1.0f - t));
  os << "\033[38;2;255;" << gb << ";" << gb << "m";
}

} // namespace

FeatureProgress::FeatureProgress(const GlobalFeatureStore &store, const TsSchedule &sched,
                                 const ComputeStats &stats, const std::vector<std::string> &dates)
    : store_(store), sched_(sched), stats_(stats), dates_(dates),
      start_time_(std::chrono::steady_clock::now()) {
  assert(!dates_.empty() && "日期轴为空, 不应启动进度渲染");
  assert(stats_.ts.size() == store_.query_ts_workers() && "stats 槽位数与 TS worker 数不符");

  // 预留画布
  for (int i = 0; i < kLines; ++i)
    std::cout << "\n";
  std::cout << std::flush;

  refresh_thread_ = std::thread([this] {
    while (running_.load(std::memory_order_acquire)) {
      render();
      std::this_thread::sleep_for(std::chrono::milliseconds(kRefreshMs));
    }
  });
}

FeatureProgress::~FeatureProgress() { stop(); }

void FeatureProgress::stop() {
  if (!running_.exchange(false, std::memory_order_release))
    return;
  if (refresh_thread_.joinable())
    refresh_thread_.join();
  render();
  std::cout << "\n"
            << std::flush;
}

long long FeatureProgress::elapsed_ms() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now() - start_time_)
      .count();
}

void FeatureProgress::render() {
  const size_t total = dates_.size();
  const size_t num_ts = store_.query_ts_workers();
  const size_t slots = store_.query_slots();
  const long long now_ms = elapsed_ms();
  const double elapsed_s = static_cast<double>(now_ms) / 1000.0;

  // ---- 采样 (原子读一轮快照; 各值间不要求一致, 展示用) ----
  const size_t io_days = stats_.io_days.load(std::memory_order_relaxed);
  const size_t cs_days = store_.query_cs_days_done();
  const size_t pf_days = stats_.prefetch_days.load(std::memory_order_relaxed);
  const size_t pf_bytes = stats_.prefetch_bytes.load(std::memory_order_relaxed);

  std::vector<size_t> frontier(num_ts), holding(num_ts, 0), done(num_ts), orders(num_ts);
  for (size_t w = 0; w < num_ts; ++w) {
    frontier[w] = store_.ts_frontier(static_cast<int>(w));
    done[w] = stats_.ts[w].done_today.load(std::memory_order_relaxed);
    orders[w] = stats_.ts[w].orders.load(std::memory_order_relaxed);
  }
  for (const std::atomic<int32_t> &o : sched_.owner) {
    const int32_t w = o.load(std::memory_order_relaxed);
    if (w >= 0 && static_cast<size_t>(w) < num_ts)
      ++holding[static_cast<size_t>(w)];
  }
  const size_t lead = *std::max_element(frontier.begin(), frontier.end());
  const size_t lag = *std::min_element(frontier.begin(), frontier.end());
  size_t sum_orders = 0;
  for (size_t w = 0; w < num_ts; ++w)
    sum_orders += orders[w];

  // ETA 标定: 首个落盘样本之后的增量
  if (first_io_ms_ < 0 && io_days > 0) {
    first_io_ms_ = now_ms;
    first_io_days_ = io_days;
  }
  const double per_day_s = (first_io_ms_ >= 0 && io_days > first_io_days_)
                               ? (now_ms - first_io_ms_) / 1000.0 / static_cast<double>(io_days - first_io_days_)
                               : 0.0;

  std::ostringstream out;
  out << "\033[" << kLines << "A\r";
  char buf[256];

  // ---- 行1: 总进度 (落盘天数 = 端到端真完成) ----
  {
    const float p = static_cast<float>(io_days) / static_cast<float>(total);
    const int filled = static_cast<int>(kBarWidth * p);
    out << "[";
    for (int j = 0; j < kBarWidth; ++j)
      out << (j < filled ? '#' : (j == filled && io_days < total ? '>' : ' '));
    out << "] " << std::setw(3) << static_cast<int>(p * 100) << "% "
        << io_days << "/" << total << " 天 | " << fmt_duration(now_ms / 1000);
    if (per_day_s > 0.0 && io_days < total) {
      out << ", ETA " << fmt_duration(static_cast<long long>(per_day_s * static_cast<double>(total - io_days)));
      snprintf(buf, sizeof(buf), " (%.1fs/天)", per_day_s);
      out << buf;
    }
    out << "\033[K\n";
  }

  // ---- 行2: 阶段行 (预取带日期/吞吐, 截面/落盘只有天数前沿) ----
  {
    const double mbps = elapsed_s > 0.0 ? pf_bytes / 1e6 / elapsed_s : 0.0;
    snprintf(buf, sizeof(buf), "预取 %zu/%zu %s %.0fMB/s (%.1fGB) | 截面 %zu/%zu | 落盘 %zu/%zu",
             pf_days, total, pf_days > 0 ? dates_[pf_days - 1].c_str() : "--------",
             mbps, pf_bytes / 1e9, cs_days, total, io_days, total);
    out << buf << "\033[K\n";
  }

  // ---- 行3: 时序汇总 (逐核速度浓缩为 Σ; 领跑者正在算 dates_[lead]) ----
  {
    const auto [hmin, hmax] = std::minmax_element(holding.begin(), holding.end());
    const double sum_mps = elapsed_s > 0.0 ? sum_orders / 1e6 / elapsed_s : 0.0;
    snprintf(buf, sizeof(buf), "时序 前沿 %zu..%zu/%zu (%s) Σ%.1fM/s (%.2fG单) 持仓 %zu..%zu",
             lag, lead, total, dates_[std::min(lead, total - 1)].c_str(),
             sum_mps, sum_orders / 1e9, *hmin, *hmax);
    out << buf << "\033[K\n";
  }

  // ---- 行4: 核压热力条 —— 每核1格, 块高 = 当日进度, 颜色 = 落后天数 ----
  {
    static const char *kBlocks[8] = {"▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"};
    out << "核压 [";
    for (size_t w = 0; w < num_ts; ++w) {
      if (holding[w] == 0) { // 无资产 (资产数 < worker 数才会发生)
        out << "\033[0m·";
        continue;
      }
      heat_color(out, lead - frontier[w], slots);
      out << kBlocks[std::min<size_t>(7, done[w] * 8 / holding[w])];
    }
    out << "\033[0m]\033[K\n";
  }

  // ---- 行5: 慢核明细 —— 差距 ≥2 天者按差距降序展开完整数值 ----
  {
    std::vector<std::pair<size_t, size_t>> slow; // (差距, 核号)
    for (size_t w = 0; w < num_ts; ++w)
      if (lead - frontier[w] >= kSlowGapDays)
        slow.emplace_back(lead - frontier[w], w);
    std::sort(slow.begin(), slow.end(), std::greater<>());

    out << "慢核 ";
    if (slow.empty()) {
      out << "—";
    } else {
      for (size_t k = 0; k < std::min(slow.size(), kSlowMax); ++k) {
        const auto [gap, w] = slow[k];
        heat_color(out, gap, slots);
        snprintf(buf, sizeof(buf), "%s%zu:-%zu天 [%zu/%zu] %.1fM/s", k ? " · " : "",
                 w, gap, done[w], holding[w],
                 elapsed_s > 0.0 ? orders[w] / 1e6 / elapsed_s : 0.0);
        out << buf;
      }
      out << "\033[0m";
      if (slow.size() > kSlowMax)
        out << " (+" << slow.size() - kSlowMax << ")";
    }
    out << "\033[K\n";
  }

  std::cout << out.str() << std::flush;
}
