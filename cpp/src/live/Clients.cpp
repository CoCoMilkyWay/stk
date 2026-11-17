#include "live/Clients.hpp"

#include "live/tdx/Tdx.hpp"

namespace live {

namespace {

constexpr Client kClients[] = {
    {"通达信", "Wine 下的 TdxW.exe (先登录): 持仓 / 成交 / 挂单 / 信用账户经注入的 shim 在客户端里直发查询 (不动界面), 下单经 shim 驱动闪电下单窗", &tdx::make},
};

} // namespace

std::span<const Client> clients() { return kClients; }

} // namespace live
