#pragma once

#include <memory>

namespace GUI {
struct TaskHandle;
}
namespace GUI::Compute {
struct ComputeHub;
}
namespace GUI::Engine {
struct EngineHub;
}

namespace GUI::Tasks {

TaskHandle CreateAlphaTask(std::shared_ptr<Compute::ComputeHub> compute, std::shared_ptr<Engine::EngineHub> engine);

} // namespace GUI::Tasks
