#include "gui/task_features/services/TransformService.hpp"
#include "features/Backend/FeatureRead.hpp"
#include "misc/profiler.hpp"
#include "shared/AssetAxis.hpp" // universe_asset_ids
#include "shared/Config.hpp"
#include "shared/Dist.hpp" // dist_enumerate_months
#include "shared/SharedData.hpp"

#include <cassert>
#include <cstring>

#define TF_STR_(x) #x
#define TF_STR(x) TF_STR_(x)

namespace GUI::Features {

TransformService::TransformService(const std::string &features_dir)
    : features_dir_(features_dir) {}

TransformService::~TransformService() { Stop(); }

void TransformService::Start(SharedData &data) {
  if (thread_.joinable())
    return;
  data_ = &data;
  stop_.store(false, std::memory_order_relaxed);
  thread_ = std::thread(&TransformService::worker_loop, this);
}

void TransformService::Stop() {
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

void TransformService::Shutdown() {
  Stop();
  if (data_)
    data_->transform.clear();
}

void TransformService::RequestCompute(SharedData &data, const Transform::Params &params) {
  const auto &sel = data.feature.selection;
  if (sel.primary_feature_idx() < 0)
    return;
  if (sel.selected_level != static_cast<int>(params.level))
    return;

  const auto &meta_list = data.feature.metadata.features[sel.selected_level];
  assert(static_cast<size_t>(sel.primary_feature_idx()) < meta_list.size());
  auto find_col = [&](const char *code) -> size_t {
    for (size_t i = 0; i < meta_list.size(); ++i)
      if (std::strcmp(meta_list[i].code, code) == 0)
        return i;
    assert(false && "字段表里找不到该列");
    return 0;
  };

  Request req;
  req.params = params;
  req.columns = {static_cast<size_t>(sel.primary_feature_idx())};
  if (params.cs_neutral()) {
    req.columns.push_back(find_col(TF_STR(NEUTRAL_RANK_MCAP)));
    req.columns.push_back(find_col(TF_STR(NEUTRAL_RANK_INDUSTRY)));
  }
  // valid 列: 按特征元数据的 valid_type 决定是否带 _meta 门控列 (恒为末列; L1 只有 DATA 门控)
  if (meta_list[sel.primary_feature_idx()].valid_type != L2::ValidType::ALL) {
    req.columns.push_back(find_col("_meta"));
    req.has_valid = true;
  }
  req.months = dist_enumerate_months(data.config.start_date, data.config.end_date);
  if (req.months.empty())
    return;
  // universe: 与特征计算同一名单 (Compute 只算这些列, 其余恒零)
  req.active = universe_asset_ids(data.config, data.asset.items.size());

  {
    std::lock_guard<std::mutex> lock(req_mutex_);
    pending_ = std::move(req);
    cancel_.store(true, std::memory_order_relaxed); // 放弃在跑
  }
  req_cv_.notify_all();
}

void TransformService::worker_loop() {
  TraceThread("TransformWorker");
  FeatureRead reader(features_dir_);

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

    auto &tf = data_->transform;
    tf.reset_for_build(req.params, std::move(req.columns), req.has_valid, req.months,
                       data_->asset.items.size(), std::move(req.active));
    tf.status.store(tf.build(reader, cancel_) ? Transform::Status::Done : Transform::Status::Cancelled,
                    std::memory_order_release);
  }
}

} // namespace GUI::Features
