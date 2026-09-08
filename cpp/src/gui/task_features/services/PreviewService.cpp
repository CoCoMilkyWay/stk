#include "gui/task_features/services/PreviewService.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "shared/Config.hpp"
#include "shared/Dist.hpp" // dist_enumerate_months
#include "shared/SharedData.hpp"

#include <cassert>
#include <cstring>

namespace GUI::Features {

// ============================================================================
// PreviewService
// ============================================================================

PreviewService::PreviewService() = default;

PreviewService::~PreviewService() { Stop(); }

void PreviewService::Start(SharedData &data) {
  if (thread_.joinable())
    return;
  data_ = &data;
  stop_.store(false, std::memory_order_relaxed);
  thread_ = std::thread(&PreviewService::worker_loop, this);
}

void PreviewService::Stop() {
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

void PreviewService::Shutdown() {
  Stop(); // join 之后 clear 无竞争
  if (data_)
    data_->preview.clear();
}

void PreviewService::RequestCompute(SharedData &data) {
  // 预览只在 L1 上跑 (kPvLevel); 覆盖该层全部非 META 特征, 不看选中
  const auto &meta_list = data.feature.metadata.features[kPvLevel];
  assert(!meta_list.empty());

  Request req;
  req.n_features = meta_list.size();
  size_t meta_col = meta_list.size();
  for (size_t i = 0; i < meta_list.size(); ++i) {
    if (std::strcmp(meta_list[i].code, "_meta") == 0)
      meta_col = i;
    if (meta_list[i].data_type == FeatureDataType::META)
      continue; // 元数据列不预览 (含 _meta 自身)
    req.feat_cols.push_back(i);
    req.valid_types.push_back(meta_list[i].valid_type);
  }
  assert(meta_col < meta_list.size() && "字段表里找不到 _meta 门控列");
  assert(!req.feat_cols.empty());
  req.meta_col = meta_col;

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

// ============================================================================
// Worker
// ============================================================================

void PreviewService::worker_loop() {
  while (true) {
    Request req;
    {
      std::unique_lock<std::mutex> lock(req_mutex_);
      req_cv_.wait(lock, [&] { return stop_.load() || pending_.has_value(); });
      if (stop_.load())
        return;
      req = std::move(*pending_);
      pending_.reset();
      cancel_.store(false, std::memory_order_relaxed); // 与消费同临界区, 免竞争
    }

    auto &pv = data_->preview;

    FeatureRead reader(req.features_dir, req.uni.size(), req.uni.hash);
    pv.reset_for_build(std::move(req.feat_cols), std::move(req.valid_types), req.meta_col,
                       req.months, req.n_features, req.uni.size());

    if (pv.build(reader, cancel_)) {
      pv.status.store(FeaturePreview::Status::Done, std::memory_order_release);
    } else {
      pv.status.store(FeaturePreview::Status::Cancelled, std::memory_order_release);
    }
  }
}

} // namespace GUI::Features
