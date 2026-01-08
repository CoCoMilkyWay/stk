// TransformService Implementation

#include "gui/task_features/services/TransformService.hpp"
#include "math/normalize/Normalize.hpp"
#include "math/stationary/ADF.hpp"
#include "math/stationary/FracDiff.hpp"
#include "math/stationary/IntDiff.hpp"
#include "math/stationary/KPSS.hpp"
#include "math/stationary/MADetrend.hpp"
#include "misc/profiler.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <thread>

namespace GUI::Features {

// ============================================================================
// TransformThreadPool Implementation
// ============================================================================

TransformThreadPool::TransformThreadPool(size_t num_threads) {
  threads_.reserve(num_threads);
  for (size_t i = 0; i < num_threads; ++i) {
    threads_.emplace_back(&TransformThreadPool::worker, this);
  }
}

TransformThreadPool::~TransformThreadPool() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
  }
  cv_.notify_all();
  for (auto &t : threads_) {
    if (t.joinable())
      t.join();
  }
}

void TransformThreadPool::worker() {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
      if (stop_ && tasks_.empty())
        return;
      if (!tasks_.empty()) {
        task = std::move(tasks_.back());
        tasks_.pop_back();
        ++active_;
      }
    }
    if (task) {
      task();
      --active_;
    }
  }
}

void TransformThreadPool::wait_all() {
  while (true) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (tasks_.empty() && active_ == 0)
        return;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
}

// ============================================================================
// TransformService Implementation
// ============================================================================

TransformService::TransformService(const std::string &features_dir)
    : features_dir_(features_dir) {
  size_t n_threads = std::max(1u, std::thread::hardware_concurrency());
  pool_ = std::make_unique<TransformThreadPool>(n_threads);
}

TransformService::~TransformService() {
  coro_stop_ = true;
  int wait = 0;
  while (coro_running_ && wait < 1000) {
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    ++wait;
  }
  coro_.reset();
}

void TransformService::RequestCompute() { compute_requested_ = true; }

void TransformService::StartCompute(CoroManager &coro, SharedData &data) {
  if (coro_running_)
    return;

  coro_stop_ = false;
  coro_ = coro.Spawn([this, &data]() -> asio::awaitable<void> {
    co_await ComputeLoop(data);
  });
  coro_running_ = true;
}

void TransformService::StopCompute(CoroManager &coro, SharedData & /*data*/) {
  if (!coro_running_)
    return;
  coro_stop_ = true;
  while (coro_running_) {
    coro.Poll();
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  coro_.reset();
}

asio::awaitable<void> TransformService::ComputeLoop(SharedData &data) {
  coro_running_ = true;
  auto &tf = data.transform;

  while (!coro_stop_) {
    // 等待计算请求
    if (!compute_requested_) {
      co_await asio::steady_timer(co_await asio::this_coro::executor, std::chrono::milliseconds(16)).async_wait(asio::use_awaitable);
      continue;
    }
    compute_requested_ = false;

    int feat_idx = data.feature.selection.primary_feature_idx;
    if (feat_idx < 0)
      continue;

    tf.compute.status = Transform::Compute::Status::Computing;
    tf.compute.cancel = false;

    do_compute(data);

    if (tf.compute.cancel) {
      tf.compute.status = Transform::Compute::Status::Cancelled;
    } else {
      tf.compute.status = Transform::Compute::Status::Done;
    }

    tf.input.update(feat_idx, data.feature.selection.selected_level, tf.selected_block, tf.config);
  }

  coro_running_ = false;
  co_return;
}

void TransformService::do_compute(SharedData &data) {
  TraceN("Transform_Compute");

  auto &tf = data.transform;
  const size_t n_assets = data.asset.items.size();
  tf.n_assets = n_assets;

  if (n_assets == 0)
    return;

  // 初始化结果
  tf.results.resize(n_assets);
  tf.compute.total = n_assets;
  tf.compute.done = 0;

  // 并行处理每个asset
  for (size_t a = 0; a < n_assets; ++a) {
    if (tf.compute.cancel)
      break;
    pool_->submit([this, &data, a]() { process_asset(data, a); });
  }

  // 等待所有任务完成
  pool_->wait_all();

  // Finalize
  if (!tf.compute.cancel) {
    finalize(data);
  }
}

void TransformService::process_asset(SharedData &data, size_t asset_idx) {
  auto &tf = data.transform;
  if (tf.compute.cancel)
    return;

  auto &result = tf.results[asset_idx];
  result.clear();

  // TODO: 从FeatureReader加载数据
  // 目前用假数据演示流程

  // 生成假数据 (正弦波 + 噪声)
  const size_t n = 1000;
  result.raw.resize(n);
  result.stationary.resize(n);
  result.normalized.resize(n);

  float base = static_cast<float>(asset_idx) * 0.1f;
  for (size_t i = 0; i < n; ++i) {
    float t = static_cast<float>(i) / 100.0f;
    result.raw[i] = base + std::sin(t) * 0.5f + (static_cast<float>(rand()) / RAND_MAX - 0.5f) * 0.2f;
  }

  // 应用平稳化
  switch (tf.config.stationary_method) {
  case Transform::StationaryMethod::NONE:
    std::copy(result.raw.begin(), result.raw.end(), result.stationary.begin());
    break;
  case Transform::StationaryMethod::MA_DETREND:
    math::stationary::ma_detrend({result.raw.data(), n},
                                 {result.stationary.data(), n},
                                 tf.config.ma_window);
    break;
  case Transform::StationaryMethod::INT_DIFF:
    math::stationary::int_diff({result.raw.data(), n},
                               {result.stationary.data(), n},
                               tf.config.diff_order);
    break;
  case Transform::StationaryMethod::FRAC_DIFF:
    math::stationary::frac_diff({result.raw.data(), n},
                                {result.stationary.data(), n},
                                tf.config.frac_d,
                                tf.config.frac_window);
    break;
  }

  // 应用归一化
  math::normalize::NormParams params;
  params.clip_k = tf.config.clip_k;
  params.winsor_pct = tf.config.winsor_pct;
  params.power_alpha = tf.config.power_alpha;

  math::normalize::normalize({result.stationary.data(), n},
                             {result.normalized.data(), n},
                             tf.config.norm_method,
                             params);

  // ADF检验 (on stationary series)
  math::stationary::ADFWorkspace adf_ws;
  auto adf_result = math::stationary::adf_test({result.stationary.data(), n}, 4, adf_ws);
  result.adf_stat = adf_result.statistic;
  result.adf_pval = adf_result.pvalue;
  result.adf_pass = adf_result.pvalue < 0.05f;

  // KPSS检验 (on stationary series)
  math::stationary::KPSSWorkspace kpss_ws;
  auto kpss_result = math::stationary::kpss_test({result.stationary.data(), n}, -1, kpss_ws); // -1 = auto bandwidth
  result.kpss_stat = kpss_result.statistic;
  result.kpss_pval = kpss_result.pvalue;
  result.kpss_pass = kpss_result.pvalue > 0.05f;

  // FFT (简化版: 功率谱)
  // TODO: 实现真实FFT
  result.fft_freq.resize(n / 2);
  result.fft_power.resize(n / 2);
  for (size_t i = 0; i < n / 2; ++i) {
    result.fft_freq[i] = static_cast<float>(i) / n;
    result.fft_power[i] = 1.0f / (1.0f + i * 0.1f); // 假的1/f谱
  }

  result.n_samples = n;
  result.valid = true;

  ++tf.compute.done;
}

void TransformService::finalize(SharedData &data) {
  TraceN("Transform_Finalize");

  auto &tf = data.transform;

  // 聚合FFT
  if (!tf.results.empty() && tf.results[0].valid) {
    size_t n_freq = tf.results[0].fft_freq.size();
    tf.avg_fft_freq = tf.results[0].fft_freq;
    tf.avg_fft_power.assign(n_freq, 0.0f);

    size_t valid_count = 0;
    for (const auto &r : tf.results) {
      if (r.valid && r.fft_power.size() == n_freq) {
        for (size_t i = 0; i < n_freq; ++i) {
          tf.avg_fft_power[i] += r.fft_power[i];
        }
        ++valid_count;
      }
    }

    if (valid_count > 0) {
      float inv = 1.0f / valid_count;
      for (auto &p : tf.avg_fft_power) {
        p *= inv;
      }
    }
  }

  // 初始化横截面 (时间=0)
  tf.update_cross_section(0);
}

} // namespace GUI::Features
