// TransformService Implementation

#include "gui/task_features/services/TransformService.hpp"
#include "math/normalize/Normalize.hpp"
#include "math/spectral/MultiResPSD.hpp"
#include "math/stationary/ADF.hpp"
#include "math/stationary/FracDiff.hpp"
#include "math/stationary/IntDiff.hpp"
#include "math/stationary/KPSS.hpp"
#include "math/stationary/MADetrend.hpp"
#include "misc/affinity.hpp"
#include "misc/profiler.hpp"
#include "shared/Feature.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <thread>

namespace GUI::Features {

// ============================================================================
// TransformWorkerPool Implementation
// ============================================================================

TransformWorkerPool::TransformWorkerPool(size_t num_workers) {
  workers_.reserve(num_workers);
  unsigned int n_cores = misc::Affinity::core_count();
  for (size_t i = 0; i < num_workers; ++i) {
    workers_.emplace_back(&TransformWorkerPool::worker_loop, this, i);
    unsigned int core_id = static_cast<unsigned int>(i) % n_cores;
    misc::Affinity::pin_thread(workers_.back().native_handle(), core_id);
  }
}

TransformWorkerPool::~TransformWorkerPool() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
  }
  cv_.notify_all();
  for (auto &t : workers_) {
    if (t.joinable())
      t.join();
  }
}

void TransformWorkerPool::bind(SharedData *data,
                               void (*compute_fn)(SharedData &, size_t, uint64_t, bool),
                               void (*on_all_done)(SharedData &)) {
  data_ = data;
  compute_fn_ = compute_fn;
  on_all_done_ = on_all_done;
}

void TransformWorkerPool::trigger(uint64_t gen) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    triggered_gen_ = gen;
  }
  cv_.notify_all();
}

void TransformWorkerPool::worker_loop(size_t worker_id) {
  TraceThread(("TransformWorker_" + std::to_string(worker_id)).c_str());
  uint64_t last_gen = 0;

  while (true) {
    uint64_t my_gen;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [&] {
        return stop_ || triggered_gen_ > last_gen;
      });
      if (stop_)
        return;
      my_gen = triggered_gen_;
    }
    last_gen = my_gen;

    if (!data_ || !compute_fn_)
      continue;

    auto &tf = data_->transform;
    size_t n_assets = tf.cache.n_assets;
    size_t n_workers = workers_.size();

    if (n_assets == 0)
      continue;

    // 检查是否被新 generation 取代
    auto is_stale = [&]() {
      return tf.compute.generation.load() != my_gen;
    };

    // 每个worker负责 [start, end) 范围的asset
    size_t per_worker = (n_assets + n_workers - 1) / n_workers;
    size_t start = worker_id * per_worker;
    size_t end = std::min(start + per_worker, n_assets);

    // ========== TS Phase ==========
    for (size_t a = start; a < end; ++a) {
      if (is_stale())
        break;
      compute_fn_(*data_, a, my_gen, false);
    }

    // ========== Barrier ==========
    // 无论是否 stale，都增加计数，保证 barrier 能完成
    ++tf.compute.ts_done;

    // 等待所有 worker 到达，但检查 generation 变化
    while (tf.compute.ts_done.load() < n_workers) {
      if (is_stale())
        break;
      std::this_thread::yield();
    }

    // 如果 generation 变化了，跳过 CS，开始下一轮
    if (is_stale())
      continue;

    // ========== CS Phase ==========
    for (size_t a = start; a < end; ++a) {
      if (is_stale())
        break;
      compute_fn_(*data_, a, my_gen, true);
    }

    // 如果 generation 变化了，跳过 on_all_done
    if (is_stale())
      continue;

    // 完成计数
    size_t done = ++tf.compute.done;
    if (done == n_workers && on_all_done_) {
      on_all_done_(*data_);
    }
  }
}

// ============================================================================
// TransformService Implementation
// ============================================================================

TransformService::TransformService(const std::string &features_dir)
    : features_dir_(features_dir), reader_(features_dir) {
  size_t n_threads = std::max(1u, std::thread::hardware_concurrency());
  pool_ = std::make_unique<TransformWorkerPool>(n_threads);
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

void TransformService::RequestCompute() {
  compute_requested_ = true;
}

void TransformService::StartCompute(CoroManager &coro, SharedData &data) {
  if (coro_running_)
    return;

  pool_->bind(&data, compute_asset_static, on_all_done_static);

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
  TraceThread("TransformCoro");
  coro_running_ = true;
  auto &tf = data.transform;

  while (!coro_stop_) {
    if (!compute_requested_) {
      co_await asio::steady_timer(co_await asio::this_coro::executor,
                                  std::chrono::milliseconds(16))
          .async_wait(asio::use_awaitable);
      continue;
    }
    compute_requested_ = false;

    int feat_idx = data.feature.selection.primary_feature_idx;
    int level = data.feature.selection.selected_level;
    if (feat_idx < 0 || level < 0)
      continue;

    // 生成 blocks (如果需要)
    if (tf.blocks.empty()) {
      std::vector<std::string> dates;
      for (const auto &d : data.asset.binary.dates) {
        std::string d_str = d;
        std::string start = data.config.start_date;
        std::string end = data.config.end_date;
        start.erase(std::remove(start.begin(), start.end(), '-'), start.end());
        end.erase(std::remove(end.begin(), end.end(), '-'), end.end());
        if (d_str >= start && d_str <= end)
          dates.push_back(d_str);
      }
      tf.generate_blocks(level, dates);
    }

    if (tf.blocks.empty())
      continue;

    // 检查是否需要重新加载数据
    bool need_load = tf.need_reload(level, feat_idx);

    if (need_load) {
      tf.compute.status = Transform::Compute::Status::Loading;
      load_block(data, level, feat_idx, tf.selected_block);
    }

    if (!tf.cache.valid())
      continue;

    // 触发新一轮计算
    invalidate_all(data);
    tf.compute.status = Transform::Compute::Status::Computing;
    pool_->trigger(tf.compute.generation.load());
  }

  coro_running_ = false;
  co_return;
}

// ============================================================================
// Invalidate All Assets
// ============================================================================

void TransformService::invalidate_all(SharedData &data) {
  auto &tf = data.transform;
  const size_t n_assets = tf.cache.n_assets;
  const size_t n_samples = tf.cache.n_samples;

  // 预分配 results (只在首次或大小变化时分配)
  if (tf.results.size() != n_assets) {
    tf.preallocate(n_assets, n_samples);
  }

  // 预分配 PSD 缓存
  if (tf.psd.asset_psd.size() != n_assets) {
    tf.psd.resize(n_assets);
  }

  // 递增 generation，触发新一轮
  ++tf.compute.generation;

  // 重置同步计数
  tf.compute.ts_done = 0;
  tf.compute.done = 0;
  tf.compute.n_workers = pool_->num_workers();
  tf.compute.total = n_assets;
}

// ============================================================================
// Block Loading (统一 L0/L1/L2)
// ============================================================================

void TransformService::load_block(SharedData &data, int level, int feature_idx, int block_idx) {
  auto &tf = data.transform;
  auto &cache = tf.cache;

  if (cache.matches(level, feature_idx, block_idx))
    return;

  cache.clear();

  const size_t A = data.asset.items.size();
  if (A == 0 || block_idx >= (int)tf.blocks.size())
    return;

  const auto &block = tf.blocks[block_idx];
  if (block.dates.empty())
    return;

  // 获取 feature offset (对仗: 三个 level 相同逻辑)
  size_t f_offset = 0;
  size_t valid_offset = 0;
  L2::ValidType valid_type = L2::ValidType::ALL;

  const auto &meta = level == 0   ? data.feature.metadata.features_l0
                     : level == 1 ? data.feature.metadata.features_l1
                                  : data.feature.metadata.features_l2;
  if (feature_idx >= 0 && feature_idx < (int)meta.size()) {
    valid_type = meta[feature_idx].valid_type;
  }

  if (level == 0) {
    f_offset = L0_FIELD_OFFSETS[feature_idx];
    if (valid_type == L2::ValidType::DEPTH) {
      valid_offset = L0_FIELD_OFFSETS[L0_FieldOffset::_depth_valid];
    } else {
      valid_offset = L0_FIELD_OFFSETS[L0_FieldOffset::_data_valid];
    }
  } else if (level == 1) {
    f_offset = L1_FIELD_OFFSETS[feature_idx];
    valid_offset = L1_FIELD_OFFSETS[L1_FieldOffset::_data_valid];
  } else {
    f_offset = L2_FIELD_OFFSETS[feature_idx];
    valid_offset = L2_FIELD_OFFSETS[L2_FieldOffset::_data_valid];
  }

  cache.raw.resize(A);
  cache.sparse.resize(A);

  size_t t_base = 0; // 累计时间偏移

  {
    TraceN("IO_Allocate");
    day_tensor_.preallocate_level(A, level);
  }

  // 统一流程：遍历 block.dates，逐天加载并拼接
  for (const auto &date : block.dates) {
    {
      TraceN("IO_Load");
      reader_.load_day_level(date, level, day_tensor_);
    }

    const size_t T_day = day_tensor_.T[level];
    if (T_day == 0)
      continue;

    const size_t F = day_tensor_.F[level];
    const feature_storage_t *base = day_tensor_.data[level].data();

    // 扩展 cache 容量
    for (size_t a = 0; a < A; ++a) {
      cache.raw[a].resize(t_base + T_day);
    }

    // 提取单特征，拼接到 cache
    {
      TraceN("IO_Extract");
      for (size_t a = 0; a < A; ++a) {
        for (size_t t = 0; t < T_day; ++t) {
          size_t idx = (t * F + f_offset) * A + a;
          float val = static_cast<float>(base[idx]);
          cache.raw[a][t_base + t] = val;

          if (valid_type == L2::ValidType::ALL) {
            cache.sparse[a].push(val, t_base + t);
          } else {
            size_t valid_idx = (t * F + valid_offset) * A + a;
            float valid_flag = static_cast<float>(base[valid_idx]);
            if (valid_flag > 0.5f) {
              cache.sparse[a].push(val, t_base + t);
            }
          }
        }
      }
    }

    t_base += T_day;
  }

  cache.n_assets = A;
  cache.n_samples = t_base;
  cache.set_key(level, feature_idx, block_idx);
  tf.blocks[block_idx].n_samples = t_base;
}

// ============================================================================
// Static Callback for Worker Pool
// ============================================================================

void TransformService::compute_asset_static(SharedData &data, size_t asset_idx, uint64_t gen, bool after_barrier) {
  auto &tf = data.transform;

  auto is_stale = [&]() {
    return tf.compute.generation.load() != gen;
  };

  if (is_stale())
    return;

  if (asset_idx >= tf.cache.raw.size())
    return;

  auto &result = tf.results[asset_idx];
  const size_t n = tf.cache.n_samples;
  if (n == 0)
    return;

  // ========== Before barrier: stationary → ts_normed ==========
  if (!after_barrier) {
    const auto &raw = tf.cache.raw[asset_idx];

    if (result.stationary.size() != n) {
      result.reserve(n);
    }

    {
      TraceN("Stationary");
      switch (tf.params.stationary_method) {
      case Transform::StationaryMethod::NONE:
        std::copy(raw.begin(), raw.end(), result.stationary.begin());
        break;
      case Transform::StationaryMethod::MA_DETREND:
        math::stationary::ma_detrend({raw.data(), n}, {result.stationary.data(), n}, tf.params.ma_window);
        break;
      case Transform::StationaryMethod::INT_DIFF:
        math::stationary::int_diff({raw.data(), n}, {result.stationary.data(), n}, tf.params.diff_order);
        break;
      case Transform::StationaryMethod::FRAC_DIFF:
        math::stationary::frac_diff({raw.data(), n}, {result.stationary.data(), n}, tf.params.frac_d, tf.params.frac_window);
        break;
      }
    }

    if (is_stale())
      return;

    {
      TraceN("TS_Norm");
      math::normalize::Params norm_params;
      norm_params.clip.k = tf.params.clip_k;
      norm_params.winsor.pct = tf.params.winsor_pct;
      norm_params.power.alpha = tf.params.power_alpha;

      math::normalize::apply_ts({result.stationary.data(), n}, {result.ts_normed.data(), n}, tf.params.ts_norm, norm_params);
    }
    return;
  }

  // ========== After barrier: ts_normed[all] → cs_normed + 指标 ==========

  {
    TraceN("CS_Norm");
    math::normalize::Params norm_params;
    norm_params.clip.k = tf.params.clip_k;
    norm_params.winsor.pct = tf.params.winsor_pct;
    norm_params.power.alpha = tf.params.power_alpha;

    if (tf.params.cs_norm == NormMethod::NONE) {
      std::copy(result.ts_normed.begin(), result.ts_normed.end(), result.cs_normed.begin());
    } else {
      thread_local std::vector<float> cs_input;
      thread_local std::vector<float> cs_output;
      const size_t n_assets = tf.cache.n_assets;
      cs_input.resize(n_assets);
      cs_output.resize(n_assets);

      for (size_t t = 0; t < n; ++t) {
        if (is_stale())
          return;

        for (size_t a = 0; a < n_assets; ++a) {
          cs_input[a] = tf.results[a].ts_normed[t];
        }

        math::normalize::apply_cs({cs_input.data(), n_assets}, {cs_output.data(), n_assets}, tf.params.cs_norm, norm_params);

        result.cs_normed[t] = cs_output[asset_idx];
      }
    }
  }

  if (is_stale())
    return;

  {
    TraceN("ADF");
    math::stationary::ADFWorkspace adf_ws;
    auto adf_result = math::stationary::adf_test({result.cs_normed.data(), n}, 4, adf_ws);
    result.adf_stat = adf_result.statistic;
    result.adf_pval = adf_result.pvalue;
    result.adf_pass = adf_result.pvalue < 0.05f;
  }

  if (is_stale())
    return;

  {
    TraceN("KPSS");
    math::stationary::KPSSWorkspace kpss_ws;
    auto kpss_result = math::stationary::kpss_test({result.cs_normed.data(), n}, -1, kpss_ws);
    result.kpss_stat = kpss_result.statistic;
    result.kpss_pval = kpss_result.pvalue;
    result.kpss_pass = kpss_result.pvalue > 0.05f;
  }

  if (is_stale())
    return;

  {
    TraceN("PSD");
    if (asset_idx < tf.psd.asset_psd.size()) {
      auto &psd_out = tf.psd.asset_psd[asset_idx];
      psd_out.fill(0.0f);

      thread_local math::spectral::MultiResPSDWorkspace<> ws;
      if (!ws.initialized)
        ws.init();
      ws.reset();

      int level = tf.cache.level;

      thread_local std::vector<float> valid_data;
      valid_data.clear();
      valid_data.reserve(n);

      float last_valid = 0.0f;
      for (size_t t = 0; t < n; ++t) {
        float val = result.cs_normed[t];
        if (std::isfinite(val)) {
          last_valid = val;
        }
        valid_data.push_back(last_valid);
      }

      size_t target_size = (level == 0) ? 16384 : (level == 1) ? 8192
                                                               : 128;

      size_t filled = 0;
      while (filled < target_size) {
        size_t idx = filled % valid_data.size();
        float val = valid_data[idx];
        if (level == 0) {
          ws.push_L0(val);
        } else if (level == 1) {
          ws.push_L1(val);
        } else {
          ws.push_L2(val);
        }
        ++filled;
      }

      ws.compute_day(std::span<float>(psd_out.data(), psd_out.size()));
    }
  }

  if (is_stale())
    return;

  {
    TraceN("KLL");
    result.KLL.clear();
    result.KLL.addBatch(result.cs_normed);
  }

  result.valid = true;
}

void TransformService::on_all_done_static(SharedData &data) {
  auto &tf = data.transform;
  auto &psd = tf.psd;

  if (psd.tick_positions.empty()) {
    psd.init_axis();
  }

  psd.avg_psd.fill(0.0f);
  size_t valid_count = 0;

  for (size_t i = 0; i < tf.results.size(); ++i) {
    if (!tf.results[i].valid)
      continue;
    if (i >= psd.asset_psd.size())
      continue;

    const auto &src = psd.asset_psd[i];
    for (size_t k = 0; k < 128; ++k) {
      psd.avg_psd[k] += src[k];
    }
    ++valid_count;
  }

  if (valid_count > 0) {
    float inv = 1.0f / static_cast<float>(valid_count);
    for (size_t k = 0; k < 128; ++k) {
      psd.avg_psd[k] *= inv;
    }
  }

  for (size_t k = 0; k < 128; ++k) {
    float v = psd.avg_psd[k];
    psd.avg_psd_db[k] = (v > 1e-20f) ? std::log10(v) : -20.0f;
  }

  float total_energy = 0.0f;
  float sec_energy = 0.0f;
  float min_energy = 0.0f;
  float hour_energy = 0.0f;
  float dc_energy = 0.0f;

  for (size_t k = 0; k < 128; ++k) {
    float e = psd.avg_psd[k];
    total_energy += e;
    if (k < 58) {
      sec_energy += e;
    } else if (k < 117) {
      min_energy += e;
    } else if (k < 127) {
      hour_energy += e;
    } else {
      dc_energy += e;
    }
  }

  if (total_energy > 1e-20f) {
    psd.ratio_sec = sec_energy / total_energy;
    psd.ratio_min = min_energy / total_energy;
    psd.ratio_hour = hour_energy / total_energy;
    psd.ratio_dc = dc_energy / total_energy;
  } else {
    psd.ratio_sec = psd.ratio_min = psd.ratio_hour = psd.ratio_dc = 0.0f;
  }

  psd.valid = true;
  tf.display.clamp(tf.cache.n_assets);
  tf.compute.status = Transform::Compute::Status::Done;
}

} // namespace GUI::Features
