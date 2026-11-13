#pragma once

#include <memory>

namespace GUI {
struct TaskHandle;
}
namespace GUI::Quote {
struct QuoteHub;
}
namespace GUI::Account {
class AccountHub;
}

namespace GUI::Tasks {

TaskHandle CreateLiveTask(std::shared_ptr<Quote::QuoteHub> quote, std::shared_ptr<Account::AccountHub> account);

} // namespace GUI::Tasks
