#pragma once

#include <memory>

namespace GUI {
struct TaskHandle;
}
namespace GUI::Database {
struct Market;
}

namespace GUI::Tasks {

TaskHandle CreateDatabaseTask(std::shared_ptr<Database::Market> market);

} // namespace GUI::Tasks
