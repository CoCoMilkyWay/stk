#pragma once

#include <memory>

namespace GUI {
struct TaskHandle;
}
namespace GUI::Compute {
struct ComputeHub;
}

namespace GUI::Tasks {

TaskHandle CreateFeaturesTask(std::shared_ptr<Compute::ComputeHub> compute);

} // namespace GUI::Tasks
