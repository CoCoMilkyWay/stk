#include "gui/task_agent/Llm.hpp"
#include "disk/Disk.hpp"
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
#include <string_view>
#include <thread>
#include <utility>

namespace GUI::Agent::llm {

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

bool valid(const Net &n) { return !n.proxy || (!n.host.empty() && n.port > 0 && n.port < 65536); }

// 全部连接共用的 TLS 上下文 (CA 证书只读一次; 建好后多线程只读)
ssl::context &tls() {
  static ssl::context c = [] {
    Dl_info i{};
    [[maybe_unused]] const bool own_crypto =
        dladdr(dlsym(RTLD_DEFAULT, "X509_STORE_CTX_get_ex_data"), &i) != 0 && std::string_view(i.dli_fname).find("libcrypto") != std::string_view::npos;
    assert(own_crypto && "OpenSSL 符号被别的库抢占: app_main 须先链 OpenSSL 再链 arrow");
    ssl::context x(ssl::context::tls_client);
    x.set_default_verify_paths();
    x.set_verify_mode(ssl::verify_peer);
    return x;
  }();
  return c;
}

enum class Err : uint8_t { None,
                           Unsent, // 发出前 (DNS / 连接 / 代理隧道 / TLS 握手) 网络错: 非幂等 POST 也可重发
                           Net,    // 请求发出后网络错
                           Fail,   // 协议或接口错误 (HTTP 非 2xx / 代理拒绝 / 流残缺), 不重试
                           Cancel };

struct Resp {
  Err err = Err::None;
  unsigned status = 0;
  std::string body, why;
  bool ok() const { return err == Err::None; }
};

// 一次请求一条 HTTPS 连接 (同步外观、内部异步: 每 200ms 看一次取消旗)
struct Conn {
  net::io_context ioc;
  ssl::stream<beast::tcp_stream> s;
  const std::atomic<bool> *cancel;
  int timeout;
  bool cancelled = false;

  Conn(const char *host, const std::atomic<bool> *cancel, int timeout) : s(ioc, tls()), cancel(cancel), timeout(timeout) {
    [[maybe_unused]] const long sni = SSL_set_tlsext_host_name(s.native_handle(), const_cast<char *>(host));
    assert(sni == 1);
    s.set_verify_callback(ssl::host_name_verification(host));
  }
  ~Conn() {
    beast::error_code ec;
    [[maybe_unused]] auto shutdown_ec = beast::get_lowest_layer(s).socket().shutdown(tcp::socket::shutdown_both, ec);
  }

  template <class Start>
  Err run(Start &&start, Err net_kind, std::string &why) {
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
      return Err::Cancel;
    if (ec) {
      why = ec.message();
      return net_kind;
    }
    return Err::None;
  }

  Err tunnel(const char *host, std::string &why) {
    beast::tcp_stream &raw = beast::get_lowest_layer(s);
    const std::string target = std::string(host) + ":443";
    http::request<http::empty_body> req{http::verb::connect, target, 11};
    req.set(http::field::host, target);
    beast::flat_buffer buf;
    http::response_parser<http::empty_body> p;
    p.skip(true);
    if (const Err e = run([&](auto h) { http::async_write(raw, req, h); }, Err::Unsent, why); e != Err::None)
      return e;
    if (const Err e = run([&](auto h) { http::async_read_header(raw, buf, p, h); }, Err::Unsent, why); e != Err::None)
      return e;
    if (p.get().result_int() != 200) {
      why = "代理拒绝 CONNECT " + target + ": HTTP " + std::to_string(p.get().result_int());
      return Err::Fail;
    }
    if (buf.size() != 0) {
      why = "代理在 TLS 握手前多发了字节";
      return Err::Fail;
    }
    return Err::None;
  }
};

Resp request(Backend b, const Net &net, http::verb verb, const std::string &target, const std::string &body, const Headers &hdrs, const Chunk &on_chunk,
             const std::atomic<bool> *cancel, int timeout = kReadTimeoutSeconds) {
  const char *host = def(b).host;
  Conn c(host, cancel, timeout);
  Resp r;
  beast::tcp_stream &raw = beast::get_lowest_layer(c.s);
  tcp::resolver resolver(c.ioc);
  beast::error_code ec;
  const tcp::resolver::results_type eps = net.proxy ? resolver.resolve(net.host, std::to_string(net.port), ec) : resolver.resolve(host, "443", ec);
  if (ec)
    return {.err = Err::Unsent, .why = ec.message()};
  if (const Err e = c.run([&](auto h) { raw.async_connect(eps, h); }, Err::Unsent, r.why); e != Err::None)
    return {.err = e, .why = std::move(r.why)};
  if (net.proxy)
    if (const Err e = c.tunnel(host, r.why); e != Err::None)
      return {.err = e, .why = std::move(r.why)};
  if (const Err e = c.run([&](auto h) { c.s.async_handshake(ssl::stream_base::client, h); }, Err::Unsent, r.why); e != Err::None)
    return {.err = e, .why = std::move(r.why)};

  http::request<http::string_body> req{verb, target, 11};
  req.set(http::field::host, host);
  req.set(http::field::user_agent, "stk-agent/1.0");
  req.set(http::field::authorization, "Bearer " + key(b));
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
  if (const Err e = c.run([&](auto h) { http::async_write(c.s, req, h); }, Err::Net, r.why); e != Err::None)
    return {.err = e, .why = std::move(r.why)};
  if (const Err e = c.run([&](auto h) { http::async_read_header(c.s, buf, p, h); }, Err::Net, r.why); e != Err::None)
    return {.err = e, .why = std::move(r.why)};
  r.status = p.get().result_int();
  if (on_chunk && r.status / 100 == 2) {
    while (!p.is_done()) {
      if (const Err e = c.run([&](auto h) { http::async_read_some(c.s, buf, p, h); }, Err::Net, r.why); e != Err::None)
        return {.err = e, .why = std::move(r.why)};
      std::string &chunk = p.get().body();
      if (chunk.empty())
        continue;
      const bool more = on_chunk(chunk);
      chunk.clear();
      if (!more)
        break;
    }
    return r;
  }
  if (const Err e = c.run([&](auto h) { http::async_read(c.s, buf, p, h); }, Err::Net, r.why); e != Err::None)
    return {.err = e, .why = std::move(r.why)};
  r.body = std::move(p.get().body());
  return r;
}

// SSE 切帧: 空行收一帧 (event / data 多行以 \n 连 / id); 注释行 (':' 开头) 丢
struct Sse {
  std::string buf, event, data, id;
  template <class F>
  bool feed(std::string_view s, F &&f) {
    buf.append(s);
    size_t at = 0, nl;
    bool go = true;
    while (go && (nl = buf.find('\n', at)) != std::string::npos) {
      std::string_view line(buf.data() + at, nl - at);
      at = nl + 1;
      if (!line.empty() && line.back() == '\r')
        line.remove_suffix(1);
      if (line.empty())
        go = flush(f);
      else
        take(line);
    }
    buf.erase(0, at);
    return go;
  }

private:
  template <class F>
  bool flush(F &f) {
    const bool go = (event.empty() && data.empty()) || f(event, data, id);
    event.clear(), data.clear(), id.clear();
    return go;
  }
  void take(std::string_view line) {
    if (line.front() == ':')
      return;
    const size_t c = line.find(':');
    const std::string_view field = line.substr(0, c);
    std::string_view value = c == std::string_view::npos ? std::string_view() : line.substr(c + 1);
    if (!value.empty() && value.front() == ' ')
      value.remove_prefix(1);
    if (field == "event")
      event = value;
    else if (field == "data")
      data += (data.empty() ? "" : "\n") + std::string(value);
    else if (field == "id")
      id = value;
  }
};

bool wait_retry(const char *what, int attempt, const std::string &why, const std::atomic<bool> &cancel) {
  std::fprintf(stderr, "[agent] %s: %s (第 %d 次失败, %ds 后重试)\n", what, why.c_str(), attempt + 1, kRetryIntervalSeconds);
  for (int i = 0; i < kRetryIntervalSeconds; ++i) {
    if (cancel.load(std::memory_order_relaxed))
      return false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return !cancel.load(std::memory_order_relaxed);
}

enum class Resend : uint8_t { Any,
                              Unsent };
std::string never(const Resp &) { return {}; }

template <class Send, class Again>
Resp retried(const char *what, Resend rs, const std::atomic<bool> &cancel, Send &&send, Again &&again) {
  for (int attempt = 0;; ++attempt) {
    Resp r = send(attempt);
    if (r.err == Err::Cancel || r.err == Err::Fail || (r.err == Err::Net && rs == Resend::Unsent))
      return r;
    const std::string why = r.ok() ? again(r) : r.why;
    if (r.ok() && why.empty())
      return r;
    if (!wait_retry(what, attempt, why, cancel))
      return {.err = Err::Cancel};
  }
}

void put(Reply &r, const Delta &on_delta, Part part, std::string_view chunk) {
  if (chunk.empty())
    return;
  (part == Part::Text ? r.text : r.thinking).append(chunk);
  on_delta(part, chunk);
}

json parse_json(std::string_view s) {
  json j = json::parse(s, nullptr, false);
  assert(!j.is_discarded() && "响应不是合法 JSON");
  return j;
}

std::string str_of(const json &j, const char *k) { return j.contains(k) && j[k].is_string() ? j[k].get<std::string>() : std::string(); }

// 检查响应状态码; 非 2xx 或传输错 → 格式化原因写进 why, 返回 false
bool check_2xx(const char *what, const Resp &r, std::string &why) {
  if (!r.ok()) {
    why = r.err == Err::Net ? "网络: " + r.why : r.why;
    return false;
  }
  if (r.status / 100 == 2)
    return true;
  std::fprintf(stderr, "[agent] %s: HTTP %u\n%s\n", what, r.status, r.body.substr(0, 2048).c_str());
  why = std::string(what) + ": HTTP " + std::to_string(r.status);
  const json j = json::parse(r.body, nullptr, false);
  if (!j.is_discarded() && j.is_object() && j.contains("error") && j["error"].is_object()) {
    const json &e = j["error"];
    if (e.contains("code"))
      why += " " + (e["code"].is_string() ? e["code"].get<std::string>() : e["code"].dump());
    if (const std::string m = str_of(e, "message"); !m.empty())
      why += ": " + m;
  } else if (!r.body.empty()) {
    why += ": " + r.body.substr(0, 300);
  }
  return false;
}

Reply fail_reply(const Resp &r, const std::string &why) {
  return r.err == Err::Cancel ? Reply{.end = End::Cancelled} : Reply{.end = End::Error, .error = why};
}

// ---- Zhipu 驱动 ----
std::string zhipu_busy(const Resp &r) {
  if (r.status != 429)
    return {};
  const json j = json::parse(r.body, nullptr, false);
  if (j.is_discarded() || !j.contains("error"))
    return {};
  const std::string code = j["error"].value("code", "");
  return code == "1302" || code == "1305" ? "限流 " + r.body : std::string();
}

std::vector<Model> zhipu_models(const json &j) {
  assert(j.contains("data"));
  std::vector<std::pair<int64_t, std::string>> v;
  for (const json &m : j["data"])
    v.emplace_back(m.value("created", int64_t{0}), str_of(m, "id"));
  std::stable_sort(v.begin(), v.end(), [](const auto &x, const auto &y) { return x.first > y.first; });
  std::vector<Model> out;
  for (auto &[t, id] : v)
    out.push_back({id, id});
  return out;
}

Reply zhipu_send(const Options &o, std::string_view system, const Turns &t, const Delta &on_delta, const std::atomic<bool> &cancel) {
  json msgs = json::array();
  if (!system.empty())
    msgs.push_back({{"role", "system"}, {"content", system}});
  for (const Turn &x : t)
    msgs.push_back({{"role", x.user ? "user" : "assistant"}, {"content", x.text}});
  json body;
  body["model"] = o.model;
  body["messages"] = std::move(msgs);
  body["stream"] = true;
  body["response_format"] = {{"type", "json_object"}};
  body["thinking"] = {{"type", "enabled"}};
  body["reasoning_effort"] = kZhipuEffort;
  const std::string payload = body.dump();

  Reply r;
  std::string finish;
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
      put(r, on_delta, Part::Thinking, str_of(c0["delta"], "reasoning_content"));
      put(r, on_delta, Part::Text, str_of(c0["delta"], "content"));
    }
    if (const std::string f = str_of(c0, "finish_reason"); !f.empty())
      finish = f;
    return true;
  };
  const Resp resp = retried(
      "zhipu", Resend::Any, cancel,
      [&](int attempt) {
        if (attempt > 0)
          on_delta(Part::Restart, {});
        r = Reply{}, finish.clear();
        Sse sse;
        return request(Backend::Zhipu, o.net, http::verb::post, kZhipuChat, payload, {{"Accept", "text/event-stream"}}, [&](std::string_view chunk) { return sse.feed(chunk, on_event); }, &cancel);
      },
      zhipu_busy);
  std::string why;
  if (!check_2xx("zhipu", resp, why))
    return fail_reply(resp, why);
  if (finish.empty())
    return {.end = End::Error, .error = "智谱流结束却没有 finish_reason"};
  r.end = finish == "stop" ? End::Ok : End::Cut;
  if (!r.ok())
    r.error = finish;
  return r;
}

// ---- Cursor 驱动 ----
std::vector<Model> cursor_models(const json &j) {
  assert(j.contains("items"));
  std::vector<Model> out;
  for (const json &m : j["items"]) {
    const std::string id = str_of(m, "id");
    out.push_back({id, m.value("displayName", id)});
  }
  return out;
}

Reply cursor_send(const Options &o, std::string_view system, const Turns &t, const Delta &on_delta, const std::atomic<bool> &cancel) {
  json body;
  body["prompt"]["text"] = (system.empty() ? std::string() : std::string(system) + "\n\n---\n\n") + Flat(t);
  if (o.model != "default")
    body["model"]["id"] = o.model;
  const std::string payload = body.dump();
  const Resp made = retried(
      "cursor", Resend::Unsent, cancel, [&](int) { return request(Backend::Cursor, o.net, http::verb::post, "/v1/agents", payload, {}, nullptr, &cancel); }, never);
  std::string why;
  if (!check_2xx("cursor", made, why))
    return fail_reply(made, why);
  const json j = parse_json(made.body);
  assert(j.contains("run") && j.contains("agent"));
  const std::string agent = str_of(j["agent"], "id");
  Reply r;
  r.id = str_of(j["run"], "id");
  assert(!agent.empty() && !r.id.empty());
  const std::string run = "/v1/agents/" + agent + "/runs/" + r.id;

  std::string last_id, status, error;
  bool done = false;
  const auto on_event = [&](const std::string &ev, const std::string &data, const std::string &id) {
    if (!id.empty())
      last_id = id;
    if (ev == "assistant" || ev == "thinking") {
      put(r, on_delta, ev == "assistant" ? Part::Text : Part::Thinking, str_of(parse_json(data), "text"));
    } else if (ev == "result") {
      const json x = parse_json(data);
      status = str_of(x, "status");
      if (const std::string txt = str_of(x, "text"); !txt.empty())
        r.text = txt;
    } else if (ev == "error") {
      std::fprintf(stderr, "[agent] cursor stream error: %s\n", data.c_str());
      const json x = json::parse(data, nullptr, false);
      error = x.is_object() ? str_of(x, "code") + ": " + str_of(x, "message") : data;
    } else if (ev == "done") {
      done = true;
      return false;
    }
    return true;
  };
  const Resp streamed = retried(
      "cursor stream", Resend::Any, cancel,
      [&](int) {
        Headers h{{"Accept", "text/event-stream"}};
        if (!last_id.empty())
          h.emplace_back("Last-Event-ID", last_id);
        Sse sse;
        return request(Backend::Cursor, o.net, http::verb::get, run + "/stream", "", h, [&](std::string_view chunk) { return sse.feed(chunk, on_event); }, &cancel);
      },
      [&](const Resp &x) { return x.status / 100 == 2 && !done && status.empty() && error.empty() ? std::string("流提前断开") : std::string(); });
  if (streamed.err == Err::Cancel) {
    const Resp x = request(Backend::Cursor, o.net, http::verb::post, run + "/cancel", "", {}, nullptr, nullptr, kCancelTimeoutSeconds);
    if (x.status != 409) {
      std::string ignore;
      check_2xx("cursor cancel", x, ignore);
    }
    r.end = End::Cancelled;
    return r;
  }
  if (!check_2xx("cursor stream", streamed, why))
    return fail_reply(streamed, why);
  if (!error.empty()) {
    r.end = End::Error, r.error = error;
    return r;
  }
  if (status.empty())
    return {.end = End::Error, .error = "Cursor 流结束却没有 result"};
  r.end = status == "FINISHED" ? End::Ok : End::Cut;
  if (!r.ok())
    r.error = status;
  return r;
}

// ---- 后端驱动表 (下标 = Backend, 与 kBackends 一一对应) ----
struct Driver {
  std::vector<Model> (*models)(const json &);
  Reply (*send)(const Options &, std::string_view, const Turns &, const Delta &, const std::atomic<bool> &);
};
constexpr Driver kDrivers[] = {{zhipu_models, zhipu_send}, {cursor_models, cursor_send}};
static_assert(std::size(kDrivers) == kNum<Backend>);

} // namespace

const std::string &key(Backend b) {
  static const std::array<std::string, kNum<Backend>> keys = [] {
    if (!disk::exists(disk::AgentKeyFile)) {
      json j;
      for (const BackendDef &d : kBackends)
        j[d.id] = d.fake;
      disk::write(disk::AgentKeyFile, {}, j.dump(2) + "\n");
    }
    const json j = json::parse(disk::read(disk::AgentKeyFile), nullptr, false);
    assert(!j.is_discarded() && j.is_object() && "output/config/agent/keys.json 须是 JSON 对象");
    std::array<std::string, kNum<Backend>> k;
    for (size_t i = 0; i < k.size(); ++i)
      if (std::string s = str_of(j, kBackends[i].id); s != kBackends[i].fake)
        k[i] = std::move(s);
    return k;
  }();
  return keys[static_cast<size_t>(b)];
}

Models models(Backend b, const Net &net, const std::atomic<bool> &cancel) {
  assert(!key(b).empty() && valid(net));
  const char *target = def(b).models;
  const std::string what = std::string(def(b).id) + " models";
  const Resp r = retried(what.c_str(), Resend::Any, cancel, [&](int) { return request(b, net, http::verb::get, target, "", {}, nullptr, &cancel); }, never);
  if (r.err == Err::Cancel)
    return {};
  Models out;
  if (check_2xx(target, r, out.error))
    out.list = kDrivers[static_cast<size_t>(b)].models(parse_json(r.body));
  return out;
}

std::string Flat(const Turns &t) {
  assert(!t.empty() && t.front().user && t.back().user);
  if (t.size() == 1)
    return t.front().text;
  std::string s;
  for (size_t i = 0; i + 1 < t.size(); ++i)
    s += std::string(t[i].user ? "# 消息" : "# 你的回复") + "\n\n" + t[i].text + "\n\n";
  return s + "# 本条消息\n\n" + t.back().text;
}

Reply send(const Options &o, std::string_view system, const Turns &t, const Delta &on_delta, const std::atomic<bool> &cancel) {
  assert(valid(o.net) && !key(o.backend).empty() && !o.model.empty());
  assert(!t.empty() && t.front().user && t.back().user);
  for (size_t i = 1; i < t.size(); ++i)
    assert(t[i].user != t[i - 1].user && "user / assistant 交替");
  const Clock::time_point t0 = Clock::now();
  Reply r = kDrivers[static_cast<size_t>(o.backend)].send(o, system, t, on_delta, cancel);
  r.seconds = std::chrono::duration<double>(Clock::now() - t0).count();
  return r;
}

} // namespace GUI::Agent::llm

namespace GUI::Agent {

Chat::Chat()
    : Async([](ChatReq &req, const Pipe &pipe) {
        assert(req.system);
        const auto on_delta = [&](llm::Part part, std::string_view chunk) {
          if (part == llm::Part::Restart) {
            pipe.edit([](ChatLive &lv, uint64_t &gen) { lv.reply = {}, ++gen; });
            pipe.step(ChatStep::Restart);
            return;
          }
          bool first = false;
          pipe.edit([&](ChatLive &lv, uint64_t &) {
            std::string &to = part == llm::Part::Text ? lv.reply.text : lv.reply.thinking;
            first = to.empty() && lv.reply.text.empty();
            to.append(chunk);
          });
          if (first)
            pipe.step(part == llm::Part::Text ? ChatStep::Write : ChatStep::Think);
        };
        return llm::send(req.opt, *req.system, req.turns, on_delta, pipe.cancel);
      }) {}

} // namespace GUI::Agent
