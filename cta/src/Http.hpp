#pragma once

// 最小 HTTP/1.1 服务: 单线程, 只收 GET, 一问一答即断; 只听本机
//   handler(路径, 查询参数) → Reply; 发送用 MSG_NOSIGNAL (浏览器中途断开不致 SIGPIPE)
#include <arpa/inet.h>
#include <cassert>
#include <cstdio>
#include <functional>
#include <map>
#include <netinet/in.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>

struct Reply {
  std::string type, body;
  int code = 200;
};

using Query = std::map<std::string, std::string>;
using Handler = std::function<Reply(const std::string &, const Query &)>;

inline std::string url_decode(std::string_view s) {
  std::string o;
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '+')
      o += ' ';
    else if (s[i] == '%' && i + 2 < s.size())
      o += static_cast<char>(std::stoi(std::string(s.substr(i + 1, 2)), nullptr, 16)), i += 2;
    else
      o += s[i];
  }
  return o;
}

inline Query query_of(std::string_view qs) {
  Query q;
  while (!qs.empty()) {
    const size_t a = qs.find('&');
    const std::string_view kv = qs.substr(0, a);
    const size_t e = kv.find('=');
    q[url_decode(kv.substr(0, e))] = e == std::string_view::npos ? "" : url_decode(kv.substr(e + 1));
    qs = a == std::string_view::npos ? std::string_view{} : qs.substr(a + 1);
  }
  return q;
}

// 绑定 127.0.0.1:port 后调 ready, 然后一直服务
inline void serve(int port, const Handler &h, const std::function<void()> &ready) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  assert(fd >= 0 && "socket");
  const int on = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  sockaddr_in a{};
  a.sin_family = AF_INET, a.sin_port = htons(static_cast<uint16_t>(port)), a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  [[maybe_unused]] const int b = ::bind(fd, reinterpret_cast<const sockaddr *>(&a), sizeof(a));
  assert(b == 0 && "端口被占");
  [[maybe_unused]] const int l = ::listen(fd, 16);
  assert(l == 0 && "listen");
  ready();
  for (;;) {
    const int c = ::accept(fd, nullptr, nullptr);
    assert(c >= 0 && "accept");
    std::string req;
    char buf[4096];
    while (req.find("\r\n\r\n") == std::string::npos) {
      const ssize_t n = ::read(c, buf, sizeof(buf));
      if (n <= 0)
        break;
      req.append(buf, static_cast<size_t>(n));
    }
    // 首行: GET /路径?查询 HTTP/1.1; 连接半途断开的 (req 不全) 直接关
    const size_t sp1 = req.find(' '), sp2 = req.find(' ', sp1 + 1);
    if (req.find("\r\n\r\n") != std::string::npos && sp1 != std::string::npos && sp2 != std::string::npos) {
      Reply r = req.compare(0, sp1, "GET") == 0 ? Reply{} : Reply{"text/plain", "只收 GET", 405};
      if (r.code == 200) {
        const std::string_view target = std::string_view(req).substr(sp1 + 1, sp2 - sp1 - 1);
        const size_t qm = target.find('?');
        r = h(url_decode(target.substr(0, qm)), qm == std::string_view::npos ? Query{} : query_of(target.substr(qm + 1)));
      }
      const std::string head = "HTTP/1.1 " + std::to_string(r.code) + (r.code == 200 ? " OK" : " ERR") + "\r\nContent-Type: " + r.type +
                               "\r\nContent-Length: " + std::to_string(r.body.size()) + "\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n";
      const std::string out = head + r.body;
      for (size_t k = 0; k < out.size();) {
        const ssize_t n = ::send(c, out.data() + k, out.size() - k, MSG_NOSIGNAL);
        if (n <= 0)
          break;
        k += static_cast<size_t>(n);
      }
    }
    ::close(c);
  }
}
