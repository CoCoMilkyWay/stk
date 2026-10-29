#include "agent/Llm.hpp"
#include "agent/Config.hpp"
#include "nlohmann/json.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <dlfcn.h>
#include <openssl/ssl.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <string_view>
#include <thread>

namespace agent::llm {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = boost::asio::ip::tcp;
using json = nlohmann::json;
using Clock = std::chrono::steady_clock;

namespace {

using Headers = std::vector<std::pair<std::string, std::string>>;
using Chunk = std::function<bool(std::string_view)>; // body 增量; false = 停读

// 接口失败 (非 2xx / 代理拒绝 / 流异常收尾): 抛到 send / models 顶层, 变成给 UI 的原因
struct Fail {
  std::string why;
};

// 请求发出前 (连接 / 代理隧道 / TLS 握手) 的网络错: 服务端没收到请求, 非幂等的 POST 也能重发
struct Unsent : boost::system::system_error {
  using boost::system::system_error::system_error;
};

// 一次请求一条 HTTPS 连接 (不复用). 同步外观、内部异步: tcp_stream 的超时只管异步操作, 且等待中要能看取消旗
struct Conn {
  net::io_context ioc;
  ssl::context ctx;
  ssl::stream<beast::tcp_stream> s;
  const std::atomic<bool> *cancel; // 可空
  int timeout;                     // 每步限时 (秒)
  bool cancelled = false;
  Conn(const char *host, const std::atomic<bool> *cancel, int timeout) : ctx(make_ctx()), s(ioc, ctx), cancel(cancel), timeout(timeout) {
    [[maybe_unused]] const long sni = SSL_set_tlsext_host_name(s.native_handle(), const_cast<char *>(host));
    assert(sni == 1);
    s.set_verify_callback(ssl::host_name_verification(host));
  }
  ~Conn() {
    beast::error_code ec;
    [[maybe_unused]] auto shutdown_ec = beast::get_lowest_layer(s).socket().shutdown(tcp::socket::shutdown_both, ec);
  }
  static ssl::context make_ctx() {
    // 全局符号查找须先落到系统 libcrypto (CMake 链接序): 别的库打包导出的 OpenSSL 符号若排在前面, asio 会调到它
    [[maybe_unused]] static const bool own_crypto = [] {
      Dl_info i{};
      return dladdr(dlsym(RTLD_DEFAULT, "X509_STORE_CTX_get_ex_data"), &i) != 0 && std::string_view(i.dli_fname).find("libcrypto") != std::string_view::npos;
    }();
    assert(own_crypto && "OpenSSL 符号被别的库抢占: app_main 须先链 OpenSSL 再链 arrow");
    ssl::context c(ssl::context::tls_client);
    c.set_default_verify_paths();
    c.set_verify_mode(ssl::verify_peer);
    return c;
  }
  // 跑一个异步操作到完成 (每步限时 timeout); 每 200ms 看一次取消旗, 置位 → 关流等它以 operation_aborted 收尾.
  // false = 取消; 其余错误抛 system_error (调用方按网络瞬抖重试)
  template <class Start>
  bool run(Start &&start) {
    beast::error_code ec;
    bool done = false;
    beast::get_lowest_layer(s).expires_after(std::chrono::seconds(timeout));
    start([&](beast::error_code e, auto &&...) { ec = e, done = true; });
    ioc.restart();
    while (!done) {
      ioc.run_for(std::chrono::milliseconds(200));
      if (!done && !cancelled && cancel && cancel->load(std::memory_order_relaxed)) {
        cancelled = true;
        beast::get_lowest_layer(s).cancel();
      }
    }
    if (cancelled)
      return false;
    if (ec)
      throw boost::system::system_error(ec);
    return true;
  }
  // 已连上 HTTP 代理: CONNECT 打到 host:443 的隧道, 之后 TLS 直接跑在隧道上. 代理拒绝 → Fail; false = 取消
  bool tunnel(const char *host) {
    beast::tcp_stream &raw = beast::get_lowest_layer(s);
    const std::string target = std::string(host) + ":443";
    http::request<http::empty_body> req{http::verb::connect, target, 11};
    req.set(http::field::host, target);
    beast::flat_buffer buf;
    http::response_parser<http::empty_body> p;
    p.skip(true); // CONNECT 的 2xx 没有 body, 头后即隧道
    if (!run([&](auto h) { http::async_write(raw, req, h); }) || !run([&](auto h) { http::async_read_header(raw, buf, p, h); }))
      return false;
    if (p.get().result_int() != 200)
      throw Fail{"代理拒绝 CONNECT " + target + ": HTTP " + std::to_string(p.get().result_int())};
    if (buf.size() != 0)
      throw Fail{"代理在 TLS 握手前多发了字节"};
    return true;
  }
};

struct Resp {
  unsigned status = 0;
  std::string body;       // 流式读的 2xx 为空 (已交给 on_chunk)
  bool cancelled = false; // 取消旗打断
};

// on_chunk 非空且 2xx → 逐段交 body 增量; 否则读全
Resp request(const Net &net, const char *host, http::verb verb, const std::string &target, const char *key, const std::string &body,
             const Headers &hdrs, const Chunk &on_chunk, const std::atomic<bool> *cancel, int timeout = kReadTimeoutSeconds) {
  Conn c(host, cancel, timeout);
  Resp r;
  beast::tcp_stream &raw = beast::get_lowest_layer(c.s);
  tcp::resolver resolver(c.ioc);
  try {
    const tcp::resolver::results_type eps = net.proxy ? resolver.resolve(net.host, std::to_string(net.port)) : resolver.resolve(host, "443");
    if (!c.run([&](auto h) { raw.async_connect(eps, h); }) || (net.proxy && !c.tunnel(host)) ||
        !c.run([&](auto h) { c.s.async_handshake(ssl::stream_base::client, h); })) {
      r.cancelled = true;
      return r;
    }
  } catch (const boost::system::system_error &e) {
    throw Unsent(e.code(), "发出前");
  }
  http::request<http::string_body> req{verb, target, 11};
  req.set(http::field::host, host);
  req.set(http::field::user_agent, "stk-agent/1.0");
  req.set(http::field::authorization, std::string("Bearer ") + key);
  if (!body.empty()) {
    req.set(http::field::content_type, "application/json");
    req.body() = body;
  }
  for (const auto &[k, v] : hdrs)
    req.set(k, v);
  req.prepare_payload();
  beast::flat_buffer buf;
  http::response_parser<http::string_body> p;
  p.body_limit(boost::none);
  if (!c.run([&](auto h) { http::async_write(c.s, req, h); }) || !c.run([&](auto h) { http::async_read_header(c.s, buf, p, h); })) {
    r.cancelled = true;
    return r;
  }
  r.status = p.get().result_int();
  if (on_chunk && r.status / 100 == 2) {
    while (!p.is_done()) {
      if (!c.run([&](auto h) { http::async_read_some(c.s, buf, p, h); })) {
        r.cancelled = true;
        return r;
      }
      std::string &b = p.get().body();
      if (b.empty())
        continue;
      const bool more = on_chunk(b);
      b.clear();
      if (!more)
        break;
    }
    return r;
  }
  if (!c.run([&](auto h) { http::async_read(c.s, buf, p, h); })) {
    r.cancelled = true;
    return r;
  }
  r.body = std::move(p.get().body());
  return r;
}

// SSE 切帧: 空行收一帧 (event / data 多行以 \n 连 / id); 注释行 (':' 开头) 丢
struct Sse {
  std::string buf, event, data, id;
  // f(event, data, id) → false = 停; 返回 false 若被叫停
  template <class F>
  bool feed(std::string_view s, F &&f) {
    buf.append(s);
    size_t nl;
    while ((nl = buf.find('\n')) != std::string::npos) {
      std::string line = buf.substr(0, nl);
      buf.erase(0, nl + 1);
      if (!line.empty() && line.back() == '\r')
        line.pop_back();
      if (line.empty()) {
        const bool any = !event.empty() || !data.empty();
        const bool go = !any || f(event, data, id);
        event.clear(), data.clear(), id.clear();
        if (!go)
          return false;
        continue;
      }
      if (line[0] == ':')
        continue;
      const size_t c = line.find(':');
      const std::string field = line.substr(0, c);
      std::string value = c == std::string::npos ? std::string() : line.substr(c + 1);
      if (!value.empty() && value[0] == ' ')
        value.erase(0, 1);
      if (field == "event")
        event = std::move(value);
      else if (field == "data")
        data += (data.empty() ? "" : "\n") + value;
      else if (field == "id")
        id = std::move(value);
    }
    return true;
  }
};

// 重试前等一会 (逐秒看取消旗; 不限次数, 直到成功或取消); false = 等待中被取消
bool wait_retry(const char *what, int attempt, const std::string &why, const std::atomic<bool> &cancel) {
  std::fprintf(stderr, "[agent] %s: %s (第 %d 次失败, %ds 后重试)\n", what, why.c_str(), attempt + 1, kRetryIntervalSeconds);
  for (int i = 0; i < kRetryIntervalSeconds; ++i) {
    if (cancel.load(std::memory_order_relaxed))
      return false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return !cancel.load(std::memory_order_relaxed);
}

// 响应体 / 流事件必是合法 JSON (服务端契约), 否则断言
json parse_json(std::string_view s) {
  json j = json::parse(s, nullptr, false);
  assert(!j.is_discarded() && "响应不是合法 JSON");
  return j;
}

// 非 2xx → Fail: 两家错误体都是 {"error": {"code", "message"}}, 取出来; 否则原样截断
[[noreturn]] void fail_status(const char *what, const Resp &r) {
  std::fprintf(stderr, "[agent] %s: HTTP %u\n%s\n", what, r.status, r.body.substr(0, 2048).c_str());
  std::string why = std::string(what) + ": HTTP " + std::to_string(r.status);
  const json j = json::parse(r.body, nullptr, false);
  if (!j.is_discarded() && j.is_object() && j.contains("error") && j["error"].is_object()) {
    const json &e = j["error"];
    if (e.contains("code"))
      why += " " + (e["code"].is_string() ? e["code"].get<std::string>() : e["code"].dump());
    if (e.contains("message") && e["message"].is_string())
      why += ": " + e["message"].get<std::string>();
  } else if (!r.body.empty()) {
    why += ": " + r.body.substr(0, 300);
  }
  throw Fail{why};
}

// 智谱限流 / 过载: 429 + 业务码 1302 (并发超限) / 1305 (过载)
bool zhipu_busy(const Resp &r) {
  if (r.status != 429)
    return false;
  const json j = json::parse(r.body, nullptr, false);
  if (j.is_discarded() || !j.contains("error"))
    return false;
  const std::string code = j["error"].value("code", "");
  return code == "1302" || code == "1305";
}

std::string str_of(const json &j, const char *k) { return j.contains(k) && j[k].is_string() ? j[k].get<std::string>() : std::string(); }

// Cursor JSON 接口 (非流式). POST 非幂等: 只重试发出前的网络错 (Unsent), 发出后的照抛; null = 被取消
json cursor_call(const Net &net, http::verb verb, const std::string &target, const std::string &body, const std::atomic<bool> &cancel) {
  for (int attempt = 0;; ++attempt) {
    Resp r;
    try {
      r = request(net, kCursorHost, verb, target, key(Backend::Cursor).c_str(), body, {}, nullptr, &cancel);
    } catch (const Unsent &e) {
      if (!wait_retry(target.c_str(), attempt, e.what(), cancel))
        return nullptr;
      continue;
    }
    if (r.cancelled)
      return nullptr;
    if (r.status / 100 != 2)
      fail_status(target.c_str(), r);
    return r.body.empty() ? json::object() : parse_json(r.body);
  }
}

} // namespace

const std::string &key(Backend b) {
  static const std::array<std::string, kBackendCount> keys = [] {
    std::array<std::string, kBackendCount> k;
    std::ifstream f(kKeyFile);
    if (!f.is_open())
      return k;
    const json j = json::parse(f, nullptr, false);
    assert(!j.is_discarded() && j.is_object() && "kKeyFile 须是 JSON 对象");
    k[static_cast<int>(Backend::Zhipu)] = str_of(j, "zhipu");
    k[static_cast<int>(Backend::Cursor)] = str_of(j, "cursor");
    return k;
  }();
  return keys[static_cast<int>(b)];
}

Models models(Backend b, const Net &net, const std::atomic<bool> &cancel) {
  assert(!key(b).empty());
  assert(!net.proxy || (!net.host.empty() && net.port > 0 && net.port < 65536));
  const bool zhipu = b == Backend::Zhipu;
  const char *host = zhipu ? kZhipuHost : kCursorHost;
  const char *target = zhipu ? kZhipuModelsTarget : "/v1/models";
  Models out;
  try {
    for (int attempt = 0;; ++attempt) {
      Resp r;
      try {
        r = request(net, host, http::verb::get, target, key(b).c_str(), "", {}, nullptr, &cancel);
      } catch (const boost::system::system_error &e) {
        if (!wait_retry("models", attempt, e.what(), cancel))
          return out;
        continue;
      }
      if (r.cancelled)
        return out;
      if (r.status / 100 != 2)
        fail_status(target, r);
      const json j = parse_json(r.body);
      if (zhipu) {
        assert(j.contains("data"));
        std::vector<std::pair<int64_t, std::string>> v;
        for (const json &m : j["data"])
          v.emplace_back(m.value("created", int64_t{0}), str_of(m, "id"));
        std::stable_sort(v.begin(), v.end(), [](const auto &x, const auto &y) { return x.first > y.first; });
        for (auto &[t, id] : v)
          out.list.push_back({id, id});
      } else {
        assert(j.contains("items"));
        for (const json &m : j["items"]) {
          const std::string id = str_of(m, "id");
          out.list.push_back({id, m.value("displayName", id)});
        }
      }
      return out;
    }
  } catch (const Fail &f) {
    out.error = f.why;
    return out;
  }
}

Session::Session(Options o) : o_(std::move(o)) {
  assert(!o_.net.proxy || (!o_.net.host.empty() && o_.net.port > 0 && o_.net.port < 65536));
  if (o_.backend == Backend::Zhipu) {
    assert(!key(Backend::Zhipu).empty() && "kKeyFile 里 zhipu 未填");
    assert(!o_.model.empty());
  } else {
    assert(!key(Backend::Cursor).empty() && "kKeyFile 里 cursor 未填");
  }
}

Reply Session::send(const std::string &user, const Delta &on_delta, const std::atomic<bool> &cancel) {
  const Clock::time_point t0 = Clock::now();
  Reply r;
  try {
    r = o_.backend == Backend::Zhipu ? send_zhipu(user, on_delta, cancel) : send_cursor(user, on_delta, cancel);
  } catch (const Fail &f) {
    r = Reply{.finish = "error", .error = f.why};
  } catch (const boost::system::system_error &e) { // Cursor 的 POST 发出后遇网络错 (不重发)
    r = Reply{.finish = "error", .error = std::string("网络: ") + e.what()};
  }
  r.seconds = std::chrono::duration<double>(Clock::now() - t0).count();
  rounds_ += r.ok();
  return r;
}

Reply Session::send_zhipu(const std::string &user, const Delta &on_delta, const std::atomic<bool> &cancel) {
  json msgs = json::array();
  if (!o_.system.empty())
    msgs.push_back({{"role", "system"}, {"content", o_.system}});
  for (const auto &[role, content] : msgs_)
    msgs.push_back({{"role", role}, {"content", content}});
  msgs.push_back({{"role", "user"}, {"content", user}});
  json body;
  body["model"] = o_.model;
  body["messages"] = std::move(msgs);
  body["stream"] = true;
  body["response_format"] = {{"type", "json_object"}};
  body["thinking"] = {{"type", "enabled"}};
  body["reasoning_effort"] = kZhipuEffort;
  const std::string payload = body.dump();

  Reply r;
  for (int attempt = 0;; ++attempt) {
    if (attempt > 0)
      on_delta(Part::Restart, {});
    r = Reply{};
    Sse sse;
    const auto on_event = [&](const std::string &, const std::string &data, const std::string &) {
      if (data == "[DONE]")
        return false;
      const json j = parse_json(data);
      if (const std::string id = str_of(j, "id"); !id.empty())
        r.id = id;
      if (j.contains("usage") && j["usage"].is_object()) {
        const json &u = j["usage"];
        r.usage.prompt = u.value("prompt_tokens", 0);
        r.usage.completion = u.value("completion_tokens", 0);
        if (u.contains("prompt_tokens_details") && u["prompt_tokens_details"].is_object())
          r.usage.cached = u["prompt_tokens_details"].value("cached_tokens", 0);
      }
      if (!j.contains("choices") || j["choices"].empty())
        return true;
      const json &c0 = j["choices"][0];
      if (c0.contains("delta") && c0["delta"].is_object()) {
        const json &d = c0["delta"];
        if (const std::string t = str_of(d, "reasoning_content"); !t.empty()) {
          r.thinking += t;
          on_delta(Part::Thinking, t);
        }
        if (const std::string t = str_of(d, "content"); !t.empty()) {
          r.text += t;
          on_delta(Part::Text, t);
        }
      }
      if (const std::string f = str_of(c0, "finish_reason"); !f.empty())
        r.finish = f;
      return true;
    };
    Resp resp;
    bool again = false; // 网络瞬抖 / 限流: 等一会整轮重发
    try {
      resp = request(o_.net, kZhipuHost, http::verb::post, kZhipuTarget, key(Backend::Zhipu).c_str(), payload, {{"Accept", "text/event-stream"}}, [&](std::string_view chunk) { return sse.feed(chunk, on_event); }, &cancel);
    } catch (const boost::system::system_error &e) {
      again = true;
      resp.cancelled = !wait_retry("zhipu", attempt, e.what(), cancel);
    }
    if (!again && !resp.cancelled && zhipu_busy(resp)) {
      again = true;
      resp.cancelled = !wait_retry("zhipu", attempt, "限流 " + resp.body, cancel);
    }
    if (resp.cancelled) {
      r.finish = "cancelled";
      return r;
    }
    if (again)
      continue;
    if (resp.status / 100 != 2)
      fail_status("zhipu", resp);
    break;
  }
  if (r.finish.empty())
    throw Fail{"智谱流结束却没有 finish_reason"};
  if (r.ok()) {
    msgs_.emplace_back("user", user);
    msgs_.emplace_back("assistant", r.text);
  }
  return r;
}

Reply Session::send_cursor(const std::string &user, const Delta &on_delta, const std::atomic<bool> &cancel) {
  json body;
  if (agent_.empty()) {
    body["prompt"]["text"] = o_.system.empty() ? user : o_.system + "\n\n---\n\n" + user;
    if (!o_.model.empty() && o_.model != "default")
      body["model"]["id"] = o_.model;
  } else {
    body["prompt"]["text"] = user;
  }
  const json j = cursor_call(o_.net, http::verb::post, agent_.empty() ? std::string("/v1/agents") : "/v1/agents/" + agent_ + "/runs", body.dump(), cancel);
  if (j.is_null())
    return Reply{.finish = "cancelled"};
  assert(j.contains("run") && (!agent_.empty() || j.contains("agent")));
  if (agent_.empty())
    agent_ = str_of(j["agent"], "id");
  Reply r;
  r.id = str_of(j["run"], "id");
  assert(!agent_.empty() && !r.id.empty());
  const std::string run_path = "/v1/agents/" + agent_ + "/runs/" + r.id;

  std::string last_id; // 断线续流 (Last-Event-ID)
  bool cancelled = false, done = false;
  const auto on_event = [&](const std::string &ev, const std::string &data, const std::string &id) {
    if (!id.empty())
      last_id = id;
    if (ev == "assistant" || ev == "thinking") {
      const std::string t = str_of(parse_json(data), "text");
      (ev == "assistant" ? r.text : r.thinking) += t;
      on_delta(ev == "assistant" ? Part::Text : Part::Thinking, t);
    } else if (ev == "result") {
      const json x = parse_json(data);
      r.finish = str_of(x, "status");
      if (const std::string t = str_of(x, "text"); !t.empty())
        r.text = t; // 终稿为准
    } else if (ev == "error") {
      std::fprintf(stderr, "[agent] cursor stream error: %s\n", data.c_str());
      const json x = json::parse(data, nullptr, false);
      r.finish = "ERROR";
      r.error = x.is_object() ? str_of(x, "code") + ": " + str_of(x, "message") : data;
    } else if (ev == "done") {
      done = true;
      return false;
    }
    return true;
  };
  for (int attempt = 0; !done;) {
    Headers h{{"Accept", "text/event-stream"}};
    if (!last_id.empty())
      h.emplace_back("Last-Event-ID", last_id);
    Sse sse;
    Resp resp;
    try {
      resp = request(o_.net, kCursorHost, http::verb::get, run_path + "/stream", key(Backend::Cursor).c_str(), "", h, [&](std::string_view chunk) { return sse.feed(chunk, on_event); }, &cancel);
    } catch (const boost::system::system_error &e) {
      cancelled = !wait_retry("cursor stream", attempt++, e.what(), cancel);
      if (cancelled)
        break;
      continue;
    }
    if ((cancelled = resp.cancelled))
      break;
    if (resp.status / 100 != 2)
      fail_status("cursor stream", resp);
    if (!done && !r.finish.empty()) // 收到终态后服务端先断了
      break;
    if (!done && (cancelled = !wait_retry("cursor stream", attempt++, "流提前断开", cancel)))
      break;
  }
  if (cancelled) {
    const Resp c = request(o_.net, kCursorHost, http::verb::post, run_path + "/cancel", key(Backend::Cursor).c_str(), "", {}, nullptr, nullptr, kCancelTimeoutSeconds);
    if (c.status / 100 != 2 && c.status != 409) // 409 = 已是终态
      fail_status("cursor cancel", c);
    r.finish = "cancelled";
  }
  if (r.finish.empty())
    throw Fail{"Cursor 流结束却没有 result"};
  return r;
}

} // namespace agent::llm
