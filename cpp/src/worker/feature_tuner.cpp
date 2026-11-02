// 时序调参 —— 初始日环模拟 / 日环内存预算 / 运行中扩格 + 领养配额 (口径见 feature_workers.hpp)
#include "worker/feature_workers.hpp"

#include "features/Storage/FeatureStore.hpp"
#include "misc/cross_platform.hpp" // memory_usage
#include "misc/logging.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <numeric>
#include <string>
#include <utility>

namespace {

constexpr double kLeadSlack = 0.01; // 初始余量: 模拟总耗时容许超出无限深的比例
constexpr double kBusyFloor = 0.05; // 配额分母 β 的下限 (几乎全在干等的核)

std::string ms_text(size_t us) { return std::to_string(us / 1000) + "ms"; }

} // namespace

size_t RingBudget::affordable() const {
  assert(per_slot > 0);
  const MemoryUsage m = memory_usage();
  const size_t keep = static_cast<size_t>(static_cast<double>(m.total_bytes) * kMemMargin);
  const size_t avail = m.total_bytes - m.used_bytes;
  return avail > keep ? (avail - keep) / per_slot : 0;
}

size_t RingBudget::cap() const {
  assert(per_slot > 0);
  return static_cast<size_t>(static_cast<double>(memory_usage().total_bytes) * (1.0 - kMemMargin)) / per_slot;
}

// 逐日递推: 核 w 开工日 d 的时刻 = max(做完 d − 1, 全员做完 d − S − 1), 总耗时单调不增于 S ⇒ 二分
size_t ts_lead_days(const std::vector<std::vector<double>> &load, size_t max_lead) {
  assert(!load.empty());
  if (max_lead == 0)
    return 0;
  const size_t D = load.front().size();
  double bound = 0.0; // 无限深: 各核独立做完
  for (const std::vector<double> &l : load) {
    assert(l.size() == D);
    bound = std::max(bound, std::accumulate(l.begin(), l.end(), 0.0));
  }
  std::vector<double> cur(load.size()), all_done(D);
  auto fits = [&](size_t S) {
    std::fill(cur.begin(), cur.end(), 0.0);
    for (size_t d = 0; d < D; ++d) {
      const double gate = d > S ? all_done[d - S - 1] : 0.0;
      double last = 0.0;
      for (size_t w = 0; w < load.size(); ++w) {
        cur[w] = std::max(cur[w], gate) + load[w][d];
        last = std::max(last, cur[w]);
      }
      all_done[d] = last;
    }
    return D == 0 || all_done[D - 1] <= bound * (1.0 + kLeadSlack);
  };
  size_t lo = 1, hi = max_lead;
  if (!fits(hi))
    return hi;
  while (lo < hi) {
    const size_t mid = (lo + hi) / 2;
    if (fits(mid))
      hi = mid;
    else
      lo = mid + 1;
  }
  return lo;
}

TsTuner::TsTuner(const StoreRing &store, TsSchedule &sched, const ComputeStats &stats, size_t total, RingBudget budget, std::function<void()> grow)
    : store_(store), sched_(sched), stats_(stats), total_(total), budget_(budget), grow_(std::move(grow)),
      t0_(std::chrono::steady_clock::now()), edge_(stats.ts.size(), 0), pending_(stats.ts.size(), 0), idle0_(stats.ts.size(), 0) {
  assert(sched.frontier.size() == stats.ts.size() && "stats 槽位数与 TS worker 数不符");
}

// 前沿到底的核 (含无资产的核, 开工即报到底) 不参与: 它们不再是任何人的关键路径
void TsTuner::tick() {
  const long long now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0_).count();
  const size_t N = stats_.ts.size();
  std::vector<size_t> f(N), de(N);
  size_t f_min = total_;
  for (size_t w = 0; w < N; ++w) {
    f[w] = sched_.frontier[w].load(std::memory_order_acquire);
    if (f[w] < total_)
      f_min = std::min(f_min, f[w]);
  }
  if (f_min >= total_)
    return;

  bool downstream = false; // 最慢核自己撞池边 = 池深被下游卡住, 本 tick 的池边干等与时序偏差无关
  for (size_t w = 0; w < N; ++w) {
    const size_t e = stats_.ts[w].edge_us.load(std::memory_order_relaxed);
    de[w] = e - edge_[w];
    edge_[w] = e;
    if (f[w] == f_min && de[w] > 0)
      downstream = true;
  }
  if (!downstream)
    for (size_t w = 0; w < N; ++w) {
      if (f[w] >= total_)
        continue;
      if (f[w] == f_min) {
        loss_us_ += pending_[w];
        pending_[w] = 0;
      } else {
        pending_[w] += de[w];
        edge_win_us_ += de[w];
      }
    }

  if (f_min >= win_f_ + kWindowDays && now - win_us_ >= kWindowMinMs * 1000)
    close_window(now, f_min, f);
}

void TsTuner::close_window(long long now_us, size_t f_min, const std::vector<size_t> &f) {
  const size_t N = stats_.ts.size();
  const double wall = static_cast<double>(now_us - win_us_);

  // 扩格: 损失够大才扩, 每窗至多一格 (新格要下一窗才见效)
  if (static_cast<double>(loss_us_) >= kLossFrac * wall) {
    const bool room = store_.query_slots() < store_.query_cap() && budget_.affordable() >= 1;
    if (room)
      grow_();
    Logger::log("tuner", std::string(room ? "ring +1 → " : "ring 满 (内存 / 上限) ") + std::to_string(store_.query_slots()) +
                             " | 已确认损失 " + ms_text(loss_us_) + " / 窗 " + ms_text(static_cast<size_t>(wall)));
  }

  // 配额: 持仓权重 × 忙碌占比偏离的半量; 窗内无池边干等 = 偏差还没卡住谁, 不动
  std::vector<int64_t> hold(N, 0);
  for (size_t i = 0; i < sched_.owner.size(); ++i)
    hold[static_cast<size_t>(sched_.owner[i].load(std::memory_order_relaxed))] += sched_.weight[i];
  std::vector<double> beta(N, 0.0);
  double sum = 0.0;
  size_t active = 0;
  for (size_t w = 0; w < N; ++w) {
    const size_t idle = stats_.ts[w].idle_us.load(std::memory_order_relaxed);
    if (f[w] < total_ && hold[w] > 0) {
      beta[w] = std::clamp(1.0 - static_cast<double>(idle - idle0_[w]) / wall, kBusyFloor, 1.0);
      sum += beta[w];
      ++active;
    }
    idle0_[w] = idle;
  }
  const double mean = active > 0 ? sum / static_cast<double>(active) : 0.0;
  for (size_t w = 0; w < N; ++w) {
    const bool on = edge_win_us_ > 0 && beta[w] > 0.0;
    sched_.quota[w].store(on ? std::llround(0.5 * static_cast<double>(hold[w]) * (beta[w] - mean) / beta[w]) : 0, std::memory_order_relaxed);
  }

  win_us_ = now_us;
  win_f_ = f_min;
  loss_us_ = 0;
  edge_win_us_ = 0;
}
