// KlineService — 见头文件
#include "gui/task_factors/services/KlineService.hpp"

#include "features/Storage/FeatureRead.hpp"
#include "features/MetaFlag.hpp" // fmeta::data_valid
#include "misc/profiler.hpp"

#include <algorithm>
#include <cassert>
#include <limits>

namespace GUI::Factors {

struct KlineService::Impl {
  std::optional<FeatureRead> reader; // 作用域 (目录 + 库轴) 变了重建
  std::string dir;
  uint64_t hash = 0;
  size_t A = 0;
  FeatureRead::DayColumns cols;
  std::vector<size_t> idx;

  void ensure(const analysis::ReadScope &sc) {
    if (reader && dir == sc.lib.dir && hash == sc.lib.axis.hash && A == sc.lib.axis.size()) {
      reader->revive(); // 新请求 = Inspect 刚读过库 (库已落地), 判废解除
      return;
    }
    dir = sc.lib.dir, hash = sc.lib.axis.hash, A = sc.lib.axis.size();
    reader.emplace(sc.lib);
    cols.preallocate(A, analysis::kLevel, 5);
  }
};

KlineService::KlineService() { Start(); }
KlineService::~KlineService() { Stop(); }

void KlineService::Request(KlineRequest req) {
  assert(req.sub >= 0 && static_cast<size_t>(req.sub) < req.scope.lib.axis.size() && !req.dates.empty());
  Service::Request(std::move(req));
}

bool KlineService::run(KlineRequest &req) {
  reset(std::move(req));
  s_.begin(Phase::Running, cur_.dates.size());
  for (size_t d = 0; d < cur_.dates.size(); ++d) {
    if (cancel_.load(std::memory_order_relaxed))
      return false; // 一天一步: 新请求在天与天之间打断
    if (!step(d))
      return false; // 库判废: 停流 (Inspect 重算后新代重来)
    s_.publish(1);
  }
  return true;
}

void KlineService::reset(KlineRequest &&req) {
  TraceN("KlineBegin");
  // 此刻 GUI 请求代 == req.gen ≠ 发布代 → GUI 不读前缀, 数组可整体重建
  if (!impl_)
    impl_ = std::make_unique<Impl>();
  impl_->ensure(req.scope);
  cur_ = std::move(req);
  y_min_ = (std::numeric_limits<double>::max)();
  y_max_ = std::numeric_limits<double>::lowest();
  s_.pub.store(KlineStream::pack(cur_.gen, 0, 0), std::memory_order_release); // 先零计数再换代
  const size_t cap = cur_.dates.size() * level_valid_rows(analysis::kLevel);
  for (std::vector<double> *v : {&s_.x, &s_.open, &s_.high, &s_.low, &s_.close}) {
    v->clear();
    v->reserve(cap); // 满容量: 之后只 push_back, data() 恒稳定
  }
}

bool KlineService::step(size_t d) {
  TraceN("KlineDay");
  assert(d < cur_.dates.size());
  FeatureRead &reader = *impl_->reader;
  auto &idx = impl_->idx;
  idx.assign(std::begin(cur_.col), std::end(cur_.col));
  reader.load_day_columns(cur_.dates[d], idx, impl_->cols);
  if (reader.stale())
    return false;
  const size_t a = static_cast<size_t>(cur_.sub), VR = level_valid_rows(analysis::kLevel);
  const double x0 = static_cast<double>(d * VR);
  const FeatureRead::DayColumns &c = impl_->cols;
  for (size_t m = 0; m < VR; ++m) {
    if (!fmeta::data_valid(static_cast<float>(c.get(m, 4, a))))
      continue;
    const double o = static_cast<double>(c.get(m, 0, a)), h = static_cast<double>(c.get(m, 1, a));
    const double l = static_cast<double>(c.get(m, 2, a)), cl = static_cast<double>(c.get(m, 3, a));
    s_.x.push_back(x0 + static_cast<double>(m));
    s_.open.push_back(o);
    s_.high.push_back(h);
    s_.low.push_back(l);
    s_.close.push_back(cl);
    y_min_ = std::min(y_min_, l);
    y_max_ = std::max(y_max_, h);
  }
  // 发布: 先范围 (relaxed), 后 pub word (release) — GUI acquire 读 pub 即见全量
  if (y_min_ <= y_max_) {
    s_.y_min.store(y_min_, std::memory_order_relaxed);
    s_.y_max.store(y_max_, std::memory_order_relaxed);
  }
  s_.pub.store(KlineStream::pack(cur_.gen, d + 1, s_.x.size()), std::memory_order_release);
  return true;
}

} // namespace GUI::Factors
