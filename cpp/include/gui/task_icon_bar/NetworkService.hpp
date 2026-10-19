// NetworkService — 图标栏网络探测: 每轮对全部目标并发 TCP 连 :80 计时 (一个 io_context, 整轮共用一个超时), 轮末 publish.
// 线程模型 (gui/util/Service.hpp, 随对象): 构造即发唯一请求, run 常驻轮询, 只在 Stop 取消时返回.
// GUI 免锁读 ping[i] (-1 = 超时 / 不通); done = 已完成轮数 (0 = 还没测完一轮 → Unknown). 档位由 ping 现算, 不另存.
#pragma once

#include "gui/util/Service.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>

namespace GUI::TaskIconBar {

struct NetTarget {
  const char *host;
  const char *name;
};
inline constexpr std::array<NetTarget, 2> kNetTargets{{{"1.1.1.1", "Cloudflare"}, {"www.baidu.com", "Baidu"}}};

struct NetworkRequest {
  std::chrono::milliseconds interval;
};

class NetworkService : public Service<NetworkService, NetworkRequest>, public Progress {
  friend Service<NetworkService, NetworkRequest>;

public:
  enum class Status : uint8_t { Unknown,
                                Good,
                                Medium,
                                Bad,
                                Offline };
  static constexpr int kGoodMs = 50, kMediumMs = 100;
  static constexpr const char *kName = "NetworkWorker";

  explicit NetworkService(std::chrono::milliseconds interval);
  ~NetworkService() { Stop(); }

  std::array<std::atomic<int>, kNetTargets.size()> ping;

  int best() const; // 通的目标里最小 ping; 全不通 -1
  Status status() const;

private:
  static constexpr Phase kStart = Phase::Running;
  Progress &board() { return *this; }
  bool run(NetworkRequest &req);
};

} // namespace GUI::TaskIconBar
