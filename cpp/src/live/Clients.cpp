#include "live/Clients.hpp"

#include "live/tdx/Tdx.hpp"

namespace live {

namespace {

constexpr Client kClients[] = {
    {"通达信", "Wine 下的 TdxW.exe (先登录并停在持仓页): 读其内存里的持仓 / 成交 / 挂单 / 信用账户, 下单与重查经注入的 shim 驱动其界面", &tdx::make},
};

} // namespace

std::span<const Client> clients() { return kClients; }

} // namespace live
