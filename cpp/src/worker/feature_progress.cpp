// FeatureProgress —— 特征计算仪表盘 (拉取式, 固定 6 行, 布局见 feature_workers.hpp)
#include "worker/feature_workers.hpp"

#include "features/Storage/FeatureStore.hpp"

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

// 块高: 8 级余裕 (空 = 压力大, 满 = 健康/闲)
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

FeatureProgress::FeatureProgress(const StoreRing &store, const TsSchedule &sched,
                                 const ComputeStats &stats, const std::vector<misc::Ymd> &dates, const char *unit)
    : store_(store), unit_(unit), sched_(sched), stats_(stats), dates_(dates),
      start_time_(std::chrono::steady_clock::now()) {
  assert(!dates_.empty() && "日期轴为空, 不应启动进度渲染");
  assert(stats_.ts.size() == sched_.frontier.size() && "stats 槽位数与 TS worker 数不符");

  const size_t num_cores = stats_.prefetch.size() + stats_.ts.size() + stats_.cs.size() + stats_.io.size(); // [预取..., ts..., 截面..., 落盘...]
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
  const size_t num_pf = stats_.prefetch.size();
  const size_t num_ts = sched_.frontier.size();
  const size_t num_cs = stats_.cs.size();
  const size_t num_io = stats_.io.size();
  const size_t ts_core = num_pf, cs_core = ts_core + num_ts, io_core = cs_core + num_cs;
  const size_t num_cores = io_core + num_io;
  const long long now_ms = elapsed_ms();

  // ---- 采样 (原子读一轮快照; 各值间不要求一致, 展示用) ----
  const size_t pf_days = store_.query_pf_done();
  const size_t cs_days = store_.query_cs_done();
  const size_t io_days = store_.query_io_done();

  // 核序 = [预取 × K_pf, ts0..tsN-1, 截面 × K_cs, 落盘 × K_io]: work + idle_us 快照
  std::vector<size_t> work(num_cores), idle(num_cores);
  auto snap = [&](size_t idx, const ComputeStats::Core &c) {
    work[idx] = c.work.load(std::memory_order_relaxed);
    idle[idx] = c.idle_us.load(std::memory_order_relaxed);
  };
  for (size_t k = 0; k < num_pf; ++k)
    snap(k, stats_.prefetch[k]);
  for (size_t w = 0; w < num_ts; ++w)
    snap(ts_core + w, stats_.ts[w]);
  for (size_t k = 0; k < num_cs; ++k)
    snap(cs_core + k, stats_.cs[k]);
  for (size_t k = 0; k < num_io; ++k)
    snap(io_core + k, stats_.io[k]);

  std::vector<size_t> frontier(num_ts), holding(num_ts, 0);
  for (size_t w = 0; w < num_ts; ++w)
    frontier[w] = sched_.frontier[w].load(std::memory_order_acquire);
  for (const std::atomic<int32_t> &o : sched_.owner) {
    const int32_t w = o.load(std::memory_order_relaxed);
    if (w >= 0 && static_cast<size_t>(w) < num_ts)
      ++holding[static_cast<size_t>(w)];
  }
  const size_t lead = *std::max_element(frontier.begin(), frontier.end());
  const size_t lag = *std::min_element(frontier.begin(), frontier.end());

  // ---- 滑窗轧差: work → 当前速率 (速度展示), idle_us → 忙碌占比 (压力) ----
  const size_t slot = tick_ % kWindow;
  const size_t oldest = tick_ < kWindow ? 0 : (tick_ + 1) % kWindow;
  const double win_s = tick_ == 0 ? 0.0
                                  : static_cast<double>(now_ms - hist_ms_[oldest]) / 1000.0;
  std::vector<double> rate(num_cores, 0.0);
  std::vector<float> busy(num_cores, 0.0f); // 1 = 满负荷, 0 = 全在干等
  for (size_t c = 0; c < num_cores; ++c) {
    if (win_s > 0.0) {
      rate[c] = static_cast<double>(work[c] - hist_work_[c * kWindow + oldest]) / win_s;
      const double idle_s = static_cast<double>(idle[c] - hist_idle_[c * kWindow + oldest]) / 1e6;
      busy[c] = static_cast<float>(std::min(1.0, std::max(0.0, 1.0 - idle_s / win_s)));
    }
    hist_work_[c * kWindow + slot] = work[c];
    hist_idle_[c * kWindow + slot] = idle[c];
  }
  hist_ms_[slot] = now_ms;
  ++tick_;

  // 完全闲置 (绿): 滑窗内基本全在干等. 干完活线程已退出的核 idle 不再累计,
  // busy 会假读成 1, 由 done 条件另行判绿, 也不进段统计.
  constexpr float kIdleBusy = 0.05f;
  std::vector<bool> done(num_cores, false);
  for (size_t k = 0; k < num_pf; ++k)
    done[k] = pf_days >= total;
  for (size_t w = 0; w < num_ts; ++w)
    done[ts_core + w] = frontier[w] >= total;
  for (size_t k = 0; k < num_cs; ++k)
    done[cs_core + k] = cs_days >= total;
  for (size_t k = 0; k < num_io; ++k)
    done[io_core + k] = io_days >= total;

  // 段忙碌 = 段内未完成核的 均 / 峰
  struct Load {
    float mean = 0.0f, peak = 0.0f;
  };
  auto load_of = [&](size_t c0, size_t n) {
    Load l;
    size_t k = 0;
    for (size_t c = c0; c < c0 + n; ++c) {
      if (done[c])
        continue;
      l.mean += busy[c];
      l.peak = std::max(l.peak, busy[c]);
      ++k;
    }
    if (k > 0)
      l.mean /= static_cast<float>(k);
    return l;
  };
  const Load pf_load = load_of(0, num_pf), ts_load = load_of(ts_core, num_ts), cs_load = load_of(cs_core, num_cs), io_load = load_of(io_core, num_io);
  auto rate_of = [&](size_t c0, size_t n) {
    double r = 0.0;
    for (size_t c = c0; c < c0 + n; ++c)
      r += rate[c];
    return r;
  };
  auto work_of = [&](size_t c0, size_t n) {
    size_t s = 0;
    for (size_t c = c0; c < c0 + n; ++c)
      s += work[c];
    return s;
  };
  auto load_text = [](const Load &l, size_t n) {
    char s[48];
    if (n == 1)
      snprintf(s, sizeof(s), "忙 %3.0f%%", l.mean * 100.0f);
    else
      snprintf(s, sizeof(s), "忙 均 %3.0f%% 峰 %3.0f%%", l.mean * 100.0f, l.peak * 100.0f);
    return std::string(s);
  };

  // ETA 标定: 首个落盘样本之后的增量
  if (first_io_ms_ < 0 && io_days > 0) {
    first_io_ms_ = now_ms;
    first_io_days_ = io_days;
  }
  const double per_day_s = (first_io_ms_ >= 0 && io_days > first_io_days_)
                               ? (now_ms - first_io_ms_) / 1000.0 / static_cast<double>(io_days - first_io_days_)
                               : 0.0;

  // 全程均值 (瞬时速率见各行; 此处为 start→now 的端到端口径)
  const size_t sum_orders = work_of(ts_core, num_ts);
  const double elapsed_s = static_cast<double>(now_ms) / 1000.0;
  const double global_rate = elapsed_s > 0.0 ? static_cast<double>(sum_orders) / elapsed_s : 0.0;
  const double global_day_s = io_days > 0 ? elapsed_s / static_cast<double>(io_days) : 0.0;

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
    if (global_rate > 0.0) {
      snprintf(buf, sizeof(buf), " | Σ%.1fM%s/s", global_rate / 1e6, unit_);
      out << buf;
    }
    if (global_day_s > 0.0) {
      snprintf(buf, sizeof(buf), " %.1fs/天", global_day_s);
      out << buf;
    }
    if (per_day_s > 0.0 && io_days < total)
      out << ", ETA " << fmt_duration(static_cast<long long>(per_day_s * static_cast<double>(total - io_days)));
    // 最忙段 (段均值): ~100% = 该段力竭, 即流水线瓶颈
    const std::pair<const char *, float> stage_load[] = {{"预取", pf_load.mean}, {"时序", ts_load.mean}, {"截面", cs_load.mean}, {"落盘", io_load.mean}};
    const auto &[busiest, busiest_load] = *std::max_element(std::begin(stage_load), std::end(stage_load),
                                                            [](const auto &a, const auto &b) { return a.second < b.second; });
    if (tick_ > 1) {
      snprintf(buf, sizeof(buf), " | 最忙 %s %.0f%%", busiest, busiest_load * 100.0f);
      out << buf;
    }
    out << "\033[K\n";
  }

  // 每核一格 = 忙碌占比 (见 feature_workers.hpp); 干等 / 本核已完成 = 绿
  auto cells = [&](size_t c0, size_t n) {
    for (size_t c = c0; c < c0 + n; ++c)
      cell(out, busy[c], busy[c] < kIdleBusy || done[c]);
    out << "\033[0m] ";
  };

  // ---- 行2: 预取 ----
  {
    out << "预取 [";
    cells(0, num_pf);
    snprintf(buf, sizeof(buf), "%s %zu/%zu %s %.0fMB/s (%.1fGB)", load_text(pf_load, num_pf).c_str(),
             pf_days, total, pf_days > 0 ? misc::dense(dates_[pf_days - 1]).c_str() : "--------",
             rate_of(0, num_pf) / 1e6, work_of(0, num_pf) / 1e9);
    out << buf << "\033[K\n";
  }

  // ---- 行3: 时序 (核间进度差看 前沿 最慢..领跑) ----
  {
    out << "时序 [";
    cells(ts_core, num_ts);
    const auto [hmin, hmax] = std::minmax_element(holding.begin(), holding.end());
    snprintf(buf, sizeof(buf), "%s 前沿 %zu..%zu/%zu (%s) %.2fG%s 持仓 %zu..%zu", load_text(ts_load, num_ts).c_str(),
             lag, lead, total, misc::dense(dates_[std::min(lead, total - 1)]).c_str(),
             static_cast<double>(sum_orders) / 1e9, unit_, *hmin, *hmax);
    out << buf << "\033[K\n";
  }

  // ---- 行4: 截面 ----
  {
    out << "截面 [";
    cells(cs_core, num_cs);
    snprintf(buf, sizeof(buf), "%s %zu/%zu (%s) %.1fk格/s", load_text(cs_load, num_cs).c_str(),
             cs_days, total, misc::dense(dates_[std::min(cs_days, total - 1)]).c_str(),
             rate_of(cs_core, num_cs) / 1e3);
    out << buf << "\033[K\n";
  }

  // ---- 行5: 落盘 ----
  {
    out << "落盘 [";
    cells(io_core, num_io);
    snprintf(buf, sizeof(buf), "%s %zu/%zu %s %.0fMB/s (%.1fGB)", load_text(io_load, num_io).c_str(),
             io_days, total, io_days > 0 ? misc::dense(dates_[io_days - 1]).c_str() : "--------",
             rate_of(io_core, num_io) / 1e6, work_of(io_core, num_io) / 1e9);
    out << buf << "\033[K\n";
  }

  std::cout << out.str() << std::flush;
}
