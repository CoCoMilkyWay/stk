#include "agent/Llm.hpp"
#include "agent/Config.hpp"
#include "nlohmann/json.hpp"

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <openssl/ssl.h>

#include <array>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <fstream>
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

// 一次请求一条 HTTPS 连接 (不复用). 同步外观、内部异步: tcp_stream 的超时只管异步操作, 且等待中要能看取消旗
struct Conn {
  net::io_context ioc;
  ssl::context ctx;
  ssl::stream<beast::tcp_stream> s;
  const std::atomic<bool> *cancel; // 可空
  bool cancelled = false;
  Conn(const char *host, const std::atomic<bool> *cancel) : ctx(make_ctx()), s(ioc, ctx), cancel(cancel) {
    [[maybe_unused]] const long sni = SSL_set_tlsext_host_name(s.native_handle(), const_cast<char *>(host));
    assert(sni == 1);
    s.set_verify_callback(ssl::host_name_verification(host));
  }
  ~Conn() {
    beast::error_code ec;
    [[maybe_unused]] auto shutdown_ec = beast::get_lowest_layer(s).socket().shutdown(tcp::socket::shutdown_both, ec);
  }
  static ssl::context make_ctx() {
    ssl::context c(ssl::context::tls_client);
    c.set_default_verify_paths();
    c.set_verify_mode(ssl::verify_peer);
    return c;
  }
  // 跑一个异步操作到完成 (每步限时 kReadTimeoutSeconds); 每 200ms 看一次取消旗, 置位 → 关流等它以 operation_aborted 收尾.
  // false = 取消; 其余错误抛 system_error (调用方按网络瞬抖重试)
  template <class Start>
  bool run(Start &&start) {
    beast::error_code ec;
    bool done = false;
    beast::get_lowest_layer(s).expires_after(std::chrono::seconds(kReadTimeoutSeconds));
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
};

struct Resp {
  unsigned status = 0;
  std::string body;       // 流式读的 2xx 为空 (已交给 on_chunk)
  bool cancelled = false; // 取消旗打断
};

// on_chunk 非空且 2xx → 逐段交 body 增量; 否则读全
Resp request(const char *host, http::verb verb, const std::string &target, const char *key, const std::string &body, const Headers &hdrs,
             const Chunk &on_chunk, const std::atomic<bool> *cancel) {
  Conn c(host, cancel);
  Resp r;
  beast::tcp_stream &raw = beast::get_lowest_layer(c.s);
  tcp::resolver resolver(c.ioc);
  const tcp::resolver::results_type eps = resolver.resolve(host, "443");
  if (!c.run([&](auto h) { raw.async_connect(eps, h); }) || !c.run([&](auto h) { c.s.async_handshake(ssl::stream_base::client, h); })) {
    r.cancelled = true;
    return r;
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

// 重试前等一会 (逐秒看取消旗); false = 等待中被取消
bool wait_retry(const char *what, int attempt, const std::string &why, const std::atomic<bool> &cancel) {
  std::fprintf(stderr, "[agent] %s: %s (第 %d/%d 次)\n", what, why.c_str(), attempt + 1, kRetryMax + 1);
  assert(attempt < kRetryMax);
  for (int i = 0; i < kRetryIntervalSeconds; ++i) {
    if (cancel.load(std::memory_order_relaxed))
      return false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return !cancel.load(std::memory_order_relaxed);
}

void fail_status(const char *what, const Resp &r) {
  std::fprintf(stderr, "[agent] %s: HTTP %u\n%s\n", what, r.status, r.body.substr(0, 2048).c_str());
  assert(false && "LLM 接口返回非 2xx");
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

// Cursor JSON 接口 (非流式, 不重试: POST 非幂等)
json cursor_call(http::verb verb, const std::string &target, const std::string &body) {
  const Resp r = request(kCursorHost, verb, target, key(Backend::Cursor).c_str(), body, {}, nullptr, nullptr);
  if (r.status / 100 != 2)
    fail_status(target.c_str(), r);
  return r.body.empty() ? json::object() : json::parse(r.body);
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

Session::Session(Options o) : o_(std::move(o)) {
  if (o_.backend == Backend::Zhipu) {
    assert(!key(Backend::Zhipu).empty() && "kKeyFile 里 zhipu 未填");
    assert(!o_.model.empty());
  } else {
    assert(!key(Backend::Cursor).empty() && "kKeyFile 里 cursor 未填");
  }
}

Reply Session::send(const std::string &user, const Delta &on_delta, const std::atomic<bool> &cancel) {
  const Clock::time_point t0 = Clock::now();
  Reply r = o_.backend == Backend::Zhipu ? send_zhipu(user, on_delta, cancel) : send_cursor(user, on_delta, cancel);
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
      const json j = json::parse(data);
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
      resp = request(kZhipuHost, http::verb::post, kZhipuTarget, key(Backend::Zhipu).c_str(), payload, {{"Accept", "text/event-stream"}}, [&](std::string_view chunk) { return sse.feed(chunk, on_event); }, &cancel);
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
  assert(!r.finish.empty() && "智谱流结束却没有 finish_reason");
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
  const json j = cursor_call(http::verb::post, agent_.empty() ? std::string("/v1/agents") : "/v1/agents/" + agent_ + "/runs", body.dump());
  if (agent_.empty())
    agent_ = j["agent"]["id"].get<std::string>();
  Reply r;
  r.id = j["run"]["id"].get<std::string>();
  const std::string run_path = "/v1/agents/" + agent_ + "/runs/" + r.id;

  std::string last_id; // 断线续流 (Last-Event-ID)
  bool cancelled = false, done = false;
  const auto on_event = [&](const std::string &ev, const std::string &data, const std::string &id) {
    if (!id.empty())
      last_id = id;
    if (ev == "assistant" || ev == "thinking") {
      const std::string t = str_of(json::parse(data), "text");
      (ev == "assistant" ? r.text : r.thinking) += t;
      on_delta(ev == "assistant" ? Part::Text : Part::Thinking, t);
    } else if (ev == "result") {
      const json x = json::parse(data);
      r.finish = str_of(x, "status");
      if (const std::string t = str_of(x, "text"); !t.empty())
        r.text = t; // 终稿为准
    } else if (ev == "error") {
      std::fprintf(stderr, "[agent] cursor stream error: %s\n", data.c_str());
      r.finish = "ERROR";
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
      resp = request(kCursorHost, http::verb::get, run_path + "/stream", key(Backend::Cursor).c_str(), "", h, [&](std::string_view chunk) { return sse.feed(chunk, on_event); }, &cancel);
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
    const Resp c = request(kCursorHost, http::verb::post, run_path + "/cancel", key(Backend::Cursor).c_str(), "", {}, nullptr, nullptr);
    if (c.status / 100 != 2 && c.status != 409) // 409 = 已是终态
      fail_status("cursor cancel", c);
    r.finish = "cancelled";
  }
  assert(!r.finish.empty() && "Cursor 流结束却没有 result");
  return r;
}

} // namespace agent::llm
