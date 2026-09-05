// FeatureProgress —— 特征计算仪表盘 (拉取式, 固定 6 行, 布局见 feature_workers.hpp)
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
constexpr size_t kWindow = 30; // 带宽滑窗 (tick 数, ~3s)

// 块高: 8 档余裕 (空 = 压力大, 满 = 健康/闲)
const char *kBlocks[8] = {"▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"};

// "12m34s" (与 progress_parallel 同款)
std::string fmt_duration(long long seconds) {
  std::ostringstream os;
  if (seconds >= 3600)
    os << seconds / 3600 << "h" << std::setw(2) << std::setfill('0') << (seconds % 3600) / 60 << "m";
  else
    os << seconds / 60 << "m" << std::setw(2) << std::setfill('0') << seconds % 60 << "s";
  return os.str();
}

// 一格 = 一个压力指标 t ∈ [0,1] 的双重编码: 块高 = 1−t (满 = 闲/健康,
// 空 = 压力大), 颜色 = t 白→红; 完全闲置 (idle) = 绿 + 满块
void cell(std::ostringstream &os, float t, bool idle) {
  t = std::min(1.0f, std::max(0.0f, t));
  if (idle) {
    os << "\033[38;2;64;220;96m" << kBlocks[7];
    return;
  }
  const int gb = static_cast<int>(255.0f * (1.0f - t));
  os << "\033[38;2;255;" << gb << ";" << gb << "m"
     << kBlocks[static_cast<size_t>((1.0f - t) * 7.0f + 0.5f)];
}

} // namespace

FeatureProgress::FeatureProgress(const GlobalFeatureStore &store, const TsSchedule &sched,
                                 const ComputeStats &stats, const std::vector<std::string> &dates)
    : store_(store), sched_(sched), stats_(stats), dates_(dates),
      start_time_(std::chrono::steady_clock::now()) {
  assert(!dates_.empty() && "日期轴为空, 不应启动进度渲染");
  assert(stats_.ts.size() == store_.query_ts_workers() && "stats 槽位数与 TS worker 数不符");

  const size_t num_cores = stats_.ts.size() + 3; // [预取, ts..., 截面, 落盘]
  hist_work_.assign(num_cores * kWindow, 0);
  hist_idle_.assign(num_cores * kWindow, 0);
  hist_ms_.assign(kWindow, 0);

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
  const size_t num_cores = num_ts + 3;
  const long long now_ms = elapsed_ms();

  // ---- 采样 (原子读一轮快照; 各值间不要求一致, 展示用) ----
  const size_t io_days = stats_.io.work.load(std::memory_order_relaxed);
  const size_t cs_days = store_.query_cs_days_done();
  const size_t ts_days = store_.query_ts_days_done(); // 全员计满的日数 (最慢前沿)
  const size_t pf_days = stats_.prefetch_days.load(std::memory_order_relaxed);

  // 核序 = [预取, ts0..tsN-1, 截面, 落盘]: work + idle_ms 快照
  std::vector<size_t> work(num_cores), idle(num_cores);
  auto snap = [&](size_t idx, const ComputeStats::Core &c) {
    work[idx] = c.work.load(std::memory_order_relaxed);
    idle[idx] = c.idle_ms.load(std::memory_order_relaxed);
  };
  snap(0, stats_.prefetch);
  for (size_t w = 0; w < num_ts; ++w)
    snap(1 + w, stats_.ts[w]);
  snap(1 + num_ts, stats_.cs);
  snap(2 + num_ts, stats_.io);

  std::vector<size_t> frontier(num_ts), holding(num_ts, 0);
  for (size_t w = 0; w < num_ts; ++w)
    frontier[w] = store_.ts_frontier(static_cast<int>(w));
  for (const std::atomic<int32_t> &o : sched_.owner) {
    const int32_t w = o.load(std::memory_order_relaxed);
    if (w >= 0 && static_cast<size_t>(w) < num_ts)
      ++holding[static_cast<size_t>(w)];
  }
  const size_t lead = *std::max_element(frontier.begin(), frontier.end());
  const size_t lag = *std::min_element(frontier.begin(), frontier.end());

  // ---- 滑窗轧差: work → 当前速率 (速度展示), idle_ms → 忙碌占比 (压力) ----
  const size_t slot = tick_ % kWindow;
  const size_t oldest = tick_ < kWindow ? 0 : (tick_ + 1) % kWindow;
  const double win_s = tick_ == 0 ? 0.0
                                  : static_cast<double>(now_ms - hist_ms_[oldest]) / 1000.0;
  std::vector<double> rate(num_cores, 0.0);
  std::vector<float> busy(num_cores, 0.0f); // 1 = 满负荷, 0 = 全在干等
  for (size_t c = 0; c < num_cores; ++c) {
    if (win_s > 0.0) {
      rate[c] = static_cast<double>(work[c] - hist_work_[c * kWindow + oldest]) / win_s;
      const double idle_s = static_cast<double>(idle[c] - hist_idle_[c * kWindow + oldest]) / 1000.0;
      busy[c] = static_cast<float>(std::min(1.0, std::max(0.0, 1.0 - idle_s / win_s)));
    }
    hist_work_[c * kWindow + slot] = work[c];
    hist_idle_[c * kWindow + slot] = idle[c];
  }
  hist_ms_[slot] = now_ms;
  ++tick_;

  // 完全闲置 (绿): 滑窗内基本全在干等. 干完活线程已退出的核 idle 不再累计,
  // busy 会假读成 1, 由调用侧用 done 条件另行判绿.
  constexpr float kIdleBusy = 0.05f;

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

  // ---- 行2: 预取 —— 指标 = 忙碌占比; 基本全在门控干等 / 已读完 = 绿 ----
  {
    out << "预取 [";
    cell(out, busy[0], busy[0] < kIdleBusy || pf_days >= total);
    out << "\033[0m] ";
    snprintf(buf, sizeof(buf), "%zu/%zu %s %.0fMB/s (%.1fGB)",
             pf_days, total, pf_days > 0 ? dates_[pf_days - 1].c_str() : "--------",
             rate[0] / 1e6, work[0] / 1e9);
    out << buf << "\033[K\n";
  }

  // ---- 行3: 时序 —— 指标 = 落后领跑天数/池深 (领跑满/落后空红); 池边干等 = 绿 ----
  {
    out << "时序 [";
    for (size_t w = 0; w < num_ts; ++w) {
      const size_t gap = lead - frontier[w];
      cell(out, static_cast<float>(gap) / static_cast<float>(slots),
           busy[1 + w] < kIdleBusy && frontier[w] < total);
    }
    out << "\033[0m] ";
    double sum_rate = 0.0;
    size_t sum_orders = 0;
    for (size_t w = 0; w < num_ts; ++w) {
      sum_rate += rate[1 + w];
      sum_orders += work[1 + w];
    }
    const auto [hmin, hmax] = std::minmax_element(holding.begin(), holding.end());
    snprintf(buf, sizeof(buf), "前沿 %zu..%zu/%zu (%s) Σ%.1fM/s (%.2fG单) 持仓 %zu..%zu",
             lag, lead, total, dates_[std::min(lead, total - 1)].c_str(),
             sum_rate / 1e6, sum_orders / 1e9, *hmin, *hmax);
    out << buf << "\033[K\n";
  }

  // ---- 行4: 截面 —— 指标 = 距时序前沿的积压/池深 (空红 = 瓶颈); 追平前沿 = 绿 ----
  {
    const size_t backlog = std::min(ts_days - std::min(ts_days, cs_days), slots);
    out << "截面 [";
    cell(out, static_cast<float>(backlog) / static_cast<float>(slots),
         backlog == 0 || busy[1 + num_ts] < kIdleBusy);
    out << "\033[0m] ";
    snprintf(buf, sizeof(buf), "%zu/%zu (%s) %.1fk格/s",
             cs_days, total, dates_[std::min(cs_days, total - 1)].c_str(),
             rate[1 + num_ts] / 1e3);
    out << buf << "\033[K\n";
  }

  // ---- 行5: 落盘 —— 指标 = 忙碌占比; 基本全在等 CS_DONE / 已刷完 = 绿 ----
  {
    out << "落盘 [";
    cell(out, busy[2 + num_ts], busy[2 + num_ts] < kIdleBusy || io_days >= total);
    out << "\033[0m] ";
    snprintf(buf, sizeof(buf), "%zu/%zu", io_days, total);
    out << buf;
    if (rate[2 + num_ts] > 0.0) {
      snprintf(buf, sizeof(buf), " %.1fs/天", 1.0 / rate[2 + num_ts]);
      out << buf;
    }
    out << "\033[K\n";
  }

  std::cout << out.str() << std::flush;
}
