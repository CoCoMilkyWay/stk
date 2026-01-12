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
                               void (*compute_fn)(SharedData &, size_t, uint64_t),
                               void (*on_all_done)(SharedData &)) {
  data_ = data;
  compute_fn_ = compute_fn;
  on_all_done_ = on_all_done;
}

void TransformWorkerPool::trigger() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ++generation_;
    last_triggered_ = generation_;
    done_count_ = 0;
    expected_count_ = data_ ? data_->transform.cache.n_assets : 0;
  }
  cv_.notify_all();
}

void TransformWorkerPool::worker_loop(size_t worker_id) {
  TraceThread(("TransformWorker_" + std::to_string(worker_id)).c_str());
  uint64_t last_gen = 0;

  while (true) {
    uint64_t cur_gen;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [&] {
        return stop_ || generation_ > last_gen;
      });
      if (stop_)
        return;
      cur_gen = generation_;
    }
    last_gen = cur_gen;

    if (!data_ || !compute_fn_)
      continue;

    auto &tf = data_->transform;
    size_t n_assets = tf.cache.n_assets;
    size_t n_workers = workers_.size();

    if (n_assets == 0)
      continue;

    // 每个worker负责 [start, end) 范围的asset
    size_t per_worker = (n_assets + n_workers - 1) / n_workers;
    size_t start = worker_id * per_worker;
    size_t end = std::min(start + per_worker, n_assets);

    for (size_t a = start; a < end; ++a) {
      // 检查是否有新的generation (被中断)
      if (generation_ > cur_gen)
        break;

      compute_fn_(*data_, a, cur_gen);
    }

    // 完成计数
    size_t done = ++done_count_;
    if (done == n_workers && on_all_done_ && generation_ == cur_gen) {
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

  // 绑定worker pool回调
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
    // 等待计算请求
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
      load_data(data, level, feat_idx, tf.selected_block);
    }

    if (!tf.cache.valid())
      continue;

    // Invalidate所有asset，触发worker计算
    invalidate_all(data);
    tf.compute.status = Transform::Compute::Status::Computing;
    pool_->trigger();
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

  // 增加generation，标记所有asset为invalid
  ++tf.compute.generation;
  for (auto &r : tf.results) {
    r.valid = false;
  }
  tf.psd.valid = false;

  tf.compute.total = n_assets;
  tf.compute.done = 0;
}

// ============================================================================
// Data Loading
// ============================================================================

void TransformService::load_data(SharedData &data, int level, int feature_idx, int block_idx) {
  auto &tf = data.transform;
  auto &cache = tf.cache;

  // 如果缓存有效且匹配，跳过加载
  if (cache.matches(level, feature_idx, block_idx))
    return;

  cache.clear();

  const size_t n_assets = data.asset.items.size();
  if (n_assets == 0 || block_idx >= (int)tf.blocks.size())
    return;

  const auto &block = tf.blocks[block_idx];

  // 预分配 tensor
  {
    TraceN("IO_Allocate");
    day_tensor_.preallocate_level(n_assets, level);
  };
  // 加载数据
  {
    TraceN("IO_Load");
    reader_.load_day_level(block.date, level, day_tensor_);
  }

  {
    TraceN("IO_Finalize");
    size_t T = day_tensor_.T[level];
    if (T == 0)
      return;

    // 获取特征的 field offset 和 valid_type
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
      // 根据 valid_type 选择对应的 valid flag offset
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

    // 提取数据到缓存 (feature_storage_t -> float 转换)
    cache.raw.resize(n_assets);
    cache.sparse.resize(n_assets);

    const feature_storage_t *base = day_tensor_.data[level].data();
    size_t F = day_tensor_.F[level];

    for (size_t a = 0; a < n_assets; ++a) {
      cache.raw[a].resize(T);
      cache.sparse[a].clear();
      cache.sparse[a].reserve(T); // 最大预分配

      for (size_t t = 0; t < T; ++t) {
        size_t idx = (t * F + f_offset) * n_assets + a;
        float val = static_cast<float>(base[idx]);
        cache.raw[a][t] = val;

        // 检查 valid flag (ALL 类型不过滤)
        if (valid_type == L2::ValidType::ALL) {
          cache.sparse[a].push(val, t);
        } else {
          size_t valid_idx = (t * F + valid_offset) * n_assets + a;
          float valid_flag = static_cast<float>(base[valid_idx]);
          if (valid_flag > 0.5f) {
            cache.sparse[a].push(val, t);
          }
        }
      }
    }

    cache.n_assets = n_assets;
    cache.n_samples = T;
    cache.set_key(level, feature_idx, block_idx);

    // 更新 block 的样本数
    tf.blocks[block_idx].n_samples = T;
  }
}

// ============================================================================
// Static Callbacks for Worker Pool
// ============================================================================

void TransformService::compute_asset_static(SharedData &data, size_t asset_idx, uint64_t gen) {
  auto &tf = data.transform;

  // 中断检测: 如果generation已变化，放弃计算
  if (tf.compute.generation != gen)
    return;

  auto &result = tf.results[asset_idx];

  // 获取原始数据
  if (asset_idx >= tf.cache.raw.size())
    return;

  const auto &raw = tf.cache.raw[asset_idx];
  const size_t n = raw.size();
  if (n == 0)
    return;

  // 确保 stationary/normalized 大小匹配
  if (result.stationary.size() != n) {
    result.reserve(n);
  }

  // 中断检测
  if (tf.compute.generation != gen)
    return;

  // 1. 平稳化
  {
    TraceN("Algo_Stationary");
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

  // 中断检测
  if (tf.compute.generation != gen)
    return;

  // 2. 归一化
  {
    TraceN("Algo_Normalize");
    math::normalize::Params norm_params;
    norm_params.clip.k = tf.params.clip_k;
    norm_params.winsor.pct = tf.params.winsor_pct;
    norm_params.power.alpha = tf.params.power_alpha;

    math::normalize::apply_ts({result.stationary.data(), n}, {result.normalized.data(), n}, tf.params.norm_method, norm_params);
  }

  // 中断检测
  if (tf.compute.generation != gen)
    return;

  // 3. ADF 检验
  {
    TraceN("Algo_ADF");
    math::stationary::ADFWorkspace adf_ws;
    auto adf_result = math::stationary::adf_test({result.stationary.data(), n}, 4, adf_ws);
    result.adf_stat = adf_result.statistic;
    result.adf_pval = adf_result.pvalue;
    result.adf_pass = adf_result.pvalue < 0.05f;
  }

  // 中断检测
  if (tf.compute.generation != gen)
    return;

  // 4. KPSS 检验
  {
    TraceN("Algo_KPSS");
    math::stationary::KPSSWorkspace kpss_ws;
    auto kpss_result = math::stationary::kpss_test({result.stationary.data(), n}, -1, kpss_ws);
    result.kpss_stat = kpss_result.statistic;
    result.kpss_pval = kpss_result.pvalue;
    result.kpss_pass = kpss_result.pvalue > 0.05f;
  }

  // 中断检测
  if (tf.compute.generation != gen)
    return;

  // 5. PSD (128 bins 非标周期轴)
  {
    TraceN("Algo_PSD");
    if (asset_idx < tf.psd.asset_psd.size()) {
      auto &psd_out = tf.psd.asset_psd[asset_idx];
      psd_out.fill(0.0f);

      thread_local math::spectral::MultiResPSDWorkspace<> ws;
      if (!ws.initialized) ws.init();
      ws.reset();

      int level = tf.cache.level;

      // 收集 valid 数据用于循环填充
      thread_local std::vector<float> valid_data;
      valid_data.clear();
      valid_data.reserve(n);

      float last_valid = 0.0f;
      for (size_t t = 0; t < n; ++t) {
        float val = result.normalized[t];
        if (std::isfinite(val)) {
          last_valid = val;
        }
        valid_data.push_back(last_valid);
      }

      // 确定目标 FFT 大小
      size_t target_size = (level == 0) ? 16384 : (level == 1) ? 8192 : 128;

      // 循环填充直到达到 FFT 大小
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

      // 计算 PSD 到 128 bins
      ws.compute_day(std::span<float>(psd_out.data(), psd_out.size()));
    }
  }

  // 中断检测
  if (tf.compute.generation != gen)
    return;

  // 6. PDF (KLLcache 持久复用)
  {
    TraceN("Algo_KLL");
    result.KLL.clear();
    result.KLL.addBatch(result.normalized);
  }

  // 完成: 设置valid，增加done计数
  result.valid = true;
  ++tf.compute.done;
}

void TransformService::on_all_done_static(SharedData &data) {
  auto &tf = data.transform;
  auto &psd = tf.psd;

  // 初始化轴刻度（只需一次）
  if (psd.tick_positions.empty()) {
    psd.init_axis();
  }

  // 计算平均 PSD
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

  // 转换为 dB (log10)
  for (size_t k = 0; k < 128; ++k) {
    float v = psd.avg_psd[k];
    psd.avg_psd_db[k] = (v > 1e-20f) ? std::log10(v) : -20.0f;
  }

  // 计算频段能量比例
  float total_energy = 0.0f;
  float sec_energy = 0.0f;   // bins 0-57
  float min_energy = 0.0f;   // bins 58-116
  float hour_energy = 0.0f;  // bins 117-126
  float dc_energy = 0.0f;    // bin 127

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

  // Clamp display indices
  tf.display.clamp(tf.cache.n_assets);

  tf.compute.status = Transform::Compute::Status::Done;
}

} // namespace GUI::Features
