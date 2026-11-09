#include "shared/Analysis.hpp"

namespace analysis {

void prepare_slots(std::vector<KLLcache> &slots, size_t n, size_t capacity, size_t resolution) {
  if (slots.size() == n) {
    clear_slots(slots);
    return;
  }
  slots.clear();
  slots.reserve(n);
  for (size_t i = 0; i < n; ++i)
    slots.emplace_back(capacity, resolution);
}

void clear_slots(std::vector<KLLcache> &slots) {
  for (auto &kll : slots)
    kll.clear();
}

size_t threads_for(size_t max_tasks) { return static_cast<size_t>(misc::exec::fit(max_tasks)); }

} // namespace analysis
