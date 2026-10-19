#include "gui/task_icon_bar/NetworkService.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <optional>
#include <thread>

namespace GUI::TaskIconBar {
namespace {

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

constexpr milliseconds kTimeout{2000}; // 整轮上限: 到点未连上的目标记 -1
constexpr milliseconds kTick{50};      // 轮间等待的取消检查粒度

// 一个目标的探测链: resolve → connect 首个端点; 连上写耗时 (ms), 否则保持 -1. 回调捕获 this: 只能就地构造
struct Probe {
  tcp::resolver resolver;
  tcp::socket socket;
  steady_clock::time_point t0 = steady_clock::now();
  int ms = -1;

  Probe(asio::io_context &io, const char *host) : resolver(io), socket(io) {
    resolver.async_resolve(host, "80", [this](const boost::system::error_code &ec, const tcp::resolver::results_type &eps) {
      if (ec)
        return;
      socket.async_connect(eps.begin()->endpoint(), [this](const boost::system::error_code &ec) {
        if (!ec)
          ms = static_cast<int>(std::chrono::duration_cast<milliseconds>(steady_clock::now() - t0).count());
      });
    });
  }
};

} // namespace

NetworkService::NetworkService(milliseconds interval) {
  for (std::atomic<int> &p : ping)
    p.store(-1, std::memory_order_relaxed);
  Start();
  Request({interval});
}

bool NetworkService::run(NetworkRequest &req) {
  while (true) {
    {
      asio::io_context io;
      std::array<std::optional<Probe>, kNetTargets.size()> probes; // 先于 io 析构: 撤掉未完成的链
      for (size_t i = 0; i < kNetTargets.size(); ++i)
        probes[i].emplace(io, kNetTargets[i].host);
      io.run_for(kTimeout);
      for (size_t i = 0; i < kNetTargets.size(); ++i)
        ping[i].store(probes[i]->ms, std::memory_order_relaxed);
    }
    publish(1);
    for (milliseconds w{0}; w < req.interval; w += kTick) {
      if (cancel_.load(std::memory_order_relaxed))
        return false;
      std::this_thread::sleep_for(kTick);
    }
  }
}

int NetworkService::best() const {
  int b = -1;
  for (const std::atomic<int> &p : ping) {
    const int v = p.load(std::memory_order_relaxed);
    if (v >= 0 && (b < 0 || v < b))
      b = v;
  }
  return b;
}

NetworkService::Status NetworkService::status() const {
  if (done.load(std::memory_order_acquire) == 0)
    return Status::Unknown;
  const int b = best();
  return b < 0 ? Status::Offline : b < kGoodMs ? Status::Good
                               : b < kMediumMs ? Status::Medium
                                               : Status::Bad;
}

} // namespace GUI::TaskIconBar
