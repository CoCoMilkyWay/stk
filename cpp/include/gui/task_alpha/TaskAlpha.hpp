#pragma once

#include <memory>

namespace GUI {
struct TaskHandle;
}
namespace GUI::Compute {
struct ComputeHub;
}
namespace GUI::Alpha {
struct AlphaHub;
}

namespace GUI::Tasks {

// Alpha / Market 共用 (定义见 gui/task_alpha/AlphaHub.hpp)
std::shared_ptr<Alpha::AlphaHub> CreateAlphaHub();
TaskHandle CreateAlphaTask(std::shared_ptr<Compute::ComputeHub> compute, std::shared_ptr<Alpha::AlphaHub> alpha);

} // namespace GUI::Tasks
