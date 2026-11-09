#pragma once

#include <memory>

namespace GUI {
struct TaskHandle;
}
namespace GUI::Market {
struct MarketHub;
}
namespace GUI::Alpha {
struct AlphaHub;
}

namespace GUI::Tasks {

TaskHandle CreateMarketTask(std::shared_ptr<Market::MarketHub> market, std::shared_ptr<Alpha::AlphaHub> alpha);

} // namespace GUI::Tasks
