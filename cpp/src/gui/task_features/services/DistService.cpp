#include "gui/task_features/services/DistService.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "shared/Config.hpp"
#include "shared/SharedData.hpp"

#include <cassert>
#include <cstring>

namespace GUI::Features {

// ============================================================================
// DistService
// ============================================================================

DistService::DistService() = default;

DistService::~DistService() { Stop(); }

void DistService::Start(SharedData &data) {
  if (thread_.joinable())
    return;
  data_ = &data;
  stop_.store(false, std::memory_order_relaxed);
  thread_ = std::thread(&DistService::worker_loop, this);
}

void DistService::Stop() {
  if (!thread_.joinable())
    return;
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    stop_.store(true, std::memory_order_relaxed);
    cancel_.store(true, std::memory_order_relaxed);
  }
  req_cv_.notify_all();
  thread_.join();
}

void DistService::Shutdown() {
  Stop(); // join 之后 clear 无竞争
  if (data_)
    data_->dist.clear();
}

void DistService::RequestCompute(SharedData &data) {
  const auto &sel = data.feature.selection;
  if (sel.primary_feature_idx() < 0)
    return;
  // 分析只在 L1 上跑 (L0 数据量下全量分布分析无意义)
  if (sel.selected_level != 1)
    return;

  Request req;
  req.columns = {static_cast<size_t>(sel.primary_feature_idx())};

  // valid 列: 按特征元数据的 valid_type 决定是否带 _meta 列 (编码见 Meta.hpp; L1 只有 DATA 门控)
  const auto &meta_list = data.feature.metadata.features[sel.selected_level];
  assert(static_cast<size_t>(sel.primary_feature_idx()) < meta_list.size());
  const L2::ValidType valid_type = meta_list[sel.primary_feature_idx()].valid_type;
  if (valid_type != L2::ValidType::ALL) {
    bool found = false;
    for (size_t i = 0; i < meta_list.size(); ++i) {
      if (std::strcmp(meta_list[i].code, "_meta") == 0) {
        req.columns.push_back(i);
        found = true;
        break;
      }
    }
    assert(found && "valid_type 要求 _meta 门控列, 但字段表里找不到");
  }

  req.months = dist_enumerate_months(data.config.start_date, data.config.end_date);
  if (req.months.empty())
    return;
  // universe 子轴: 与特征计算同一推导 (A 轴/文件列序/目录都由它定)
  req.uni = universe_axis(data.config, data.asset.items.size());
  req.features_dir = data.config.FeatureUniverseDir();

  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_ = std::move(req);
    cancel_.store(true, std::memory_order_relaxed); // 放弃在跑
  }
  req_cv_.notify_all();
}

void DistService::RequestPrewarm(SharedData &data) {
  const size_t n_assets = universe_axis(data.config, data.asset.items.size()).size();
  const size_t n_months = dist_enumerate_months(data.config.start_date, data.config.end_date).size();
  assert(n_assets > 0 && n_months > 0 && "prewarm 只在输入就绪后触发");
  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_prewarm_ = Prewarm{n_assets, n_months};
  }
  req_cv_.notify_all();
}

// ============================================================================
// Worker
// ============================================================================

void DistService::worker_loop() {
  // FeatureRead 每请求现构造 (轻量, 只有目录 + 期望子轴);
  // 常驻平面挂在 Dist 私有缓冲里, 跨请求复用
  while (true) {
    Request req;
    {
      std::unique_lock<std::mutex> lock(req_mutex_);
      req_cv_.wait(lock, [&] { return stop_.load() || pending_.has_value() || pending_prewarm_.has_value(); });
      if (stop_.load())
        return;
      if (!pending_.has_value()) { // 只有预热: 锁外做, 回头继续等真实请求
        const Prewarm pw = *pending_prewarm_;
        pending_prewarm_.reset();
        lock.unlock();
        data_->dist.prewarm(pw.n_assets, pw.n_months);
        continue;
      }
      pending_prewarm_.reset(); // 真实请求在场, 预热多余 (reset/build 自会分配)
      req = std::move(*pending_);
      pending_.reset();
      cancel_.store(false, std::memory_order_relaxed); // 与消费同临界区, 免竞争
    }

    auto &dist = data_->dist;

    FeatureRead reader(req.features_dir, req.uni.size(), req.uni.hash);
    dist.reset_for_build(std::move(req.columns), req.months, std::move(req.uni.ids));

    if (dist.build(reader, cancel_)) {
      dist.status.store(Dist::Status::Done, std::memory_order_release);
    } else {
      dist.status.store(Dist::Status::Cancelled, std::memory_order_release);
    }
  }
}

} // namespace GUI::Features
