#pragma once

#include <memory>

namespace GUI {
struct TaskHandle;
}
namespace GUI::Quote {
struct QuoteHub;
}

namespace GUI::Tasks {

TaskHandle CreateDatabaseTask(std::shared_ptr<const Quote::QuoteHub> quote);

} // namespace GUI::Tasks
