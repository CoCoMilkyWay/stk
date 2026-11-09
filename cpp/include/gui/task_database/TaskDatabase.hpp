#pragma once

#include <memory>

namespace GUI {
struct TaskHandle;
}
namespace GUI::Market {
struct MarketHub;
}

namespace GUI::Tasks {

TaskHandle CreateDatabaseTask(std::shared_ptr<const Market::MarketHub> market);

} // namespace GUI::Tasks
