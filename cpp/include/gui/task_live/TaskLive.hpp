#pragma once

#include <memory>

namespace GUI {
struct TaskHandle;
}
namespace GUI::Quote {
struct QuoteHub;
}

namespace GUI::Tasks {

TaskHandle CreateLiveTask(std::shared_ptr<Quote::QuoteHub> quote);

} // namespace GUI::Tasks
