#include "agent/Llm.hpp"
#include "misc/fs.hpp"
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
#include <filesystem>
#include <fstream>
#include <optional>
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

bool valid(const Net &n) { return !n.proxy || (!n.host.empty() && n.port > 0 && n.port < 65536); }

// 全部连接共用的 TLS 上下文 (CA 证书只读一次; 建好后多线程只读)
ssl::context &tls() {
  static ssl::context c = [] {
    // 全局符号查找须先落到系统 libcrypto (CMake 链接序): 别的库打包导出的 OpenSSL 符号若排在前面, asio 会调到它
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

// 一次请求一条 HTTPS 连接 (不复用). 同步外观、内部异步: tcp_stream 的超时只管异步操作, 且等待中要能看取消旗
struct Conn {
  net::io_context ioc;
  ssl::stream<beast::tcp_stream> s;
  const std::atomic<bool> *cancel; // 可空
  int timeout;                     // 每步限时 (秒)
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

// 对该后端主机发一个请求; on_chunk 非空且 2xx → 逐段交 body 增量; 否则读全
Resp request(Backend b, const Net &net, http::verb verb, const std::string &target, const std::string &body, const Headers &hdrs, const Chunk &on_chunk,
             const std::atomic<bool> *cancel, int timeout = kReadTimeoutSeconds) {
  const char *host = def(b).host;
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

// SSE 切帧: 空行收一帧 (event / data 多行以 \n 连 / id); 注释行 (':' 开头) 丢. 一段增量扫一遍, 末尾半行留到下段
struct Sse {
  std::string buf, event, data, id;
  // f(event, data, id) → false = 停; 返回 false 若被叫停
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
    if (field == "event") {
      event = value;
    } else if (field == "data") {
      if (!data.empty())
        data += '\n';
      data += value;
    } else if (field == "id") {
      id = value;
    }
  }
};

// 重试前等一会 (逐秒看取消旗); false = 等待中被取消
bool wait_retry(const char *what, int attempt, const std::string &why, const std::atomic<bool> &cancel) {
  std::fprintf(stderr, "[agent] %s: %s (第 %d 次失败, %ds 后重试)\n", what, why.c_str(), attempt + 1, kRetryIntervalSeconds);
  for (int i = 0; i < kRetryIntervalSeconds; ++i) {
    if (cancel.load(std::memory_order_relaxed))
      return false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return !cancel.load(std::memory_order_relaxed);
}

// 网络错的重发范围: 幂等请求任何网络错都重发; 非幂等 POST 只重发未发出的 (Unsent)
enum class Resend : uint8_t { Any,
                              Unsent };

// 响应一到即收
std::string never(const Resp &) { return {}; }

// 一个请求发到有响应为止 (不限次数, 每次等 kRetryIntervalSeconds): 范围内的网络错 / again(响应) 非空 (= 重发原因) → 重发.
// send(attempt) 发一次; nullopt = 取消 (请求途中或等待中)
template <class Send, class Again>
std::optional<Resp> retried(const char *what, Resend rs, const std::atomic<bool> &cancel, Send &&send, Again &&again) {
  for (int attempt = 0;; ++attempt) {
    std::string why;
    try {
      Resp r = send(attempt);
      if (r.cancelled)
        return std::nullopt;
      why = again(r);
      if (why.empty())
        return r;
    } catch (const Unsent &e) {
      why = e.what();
    } catch (const boost::system::system_error &e) {
      if (rs == Resend::Unsent)
        throw;
      why = e.what();
    }
    if (!wait_retry(what, attempt, why, cancel))
      return std::nullopt;
  }
}

// 一段增量: 记进本轮 + 交回调
void put(Reply &r, const Delta &on_delta, Part part, std::string_view chunk) {
  if (chunk.empty())
    return;
  (part == Part::Text ? r.text : r.thinking).append(chunk);
  on_delta(part, chunk);
}

// 响应体 / 流事件必是合法 JSON (服务端契约), 否则断言
json parse_json(std::string_view s) {
  json j = json::parse(s, nullptr, false);
  assert(!j.is_discarded() && "响应不是合法 JSON");
  return j;
}

std::string str_of(const json &j, const char *k) { return j.contains(k) && j[k].is_string() ? j[k].get<std::string>() : std::string(); }

// 非 2xx → Fail: 两家错误体都是 {"error": {"code", "message"}}, 取出来; 否则原样截断
void expect_2xx(const char *what, const Resp &r) {
  if (r.status / 100 == 2)
    return;
  std::fprintf(stderr, "[agent] %s: HTTP %u\n%s\n", what, r.status, r.body.substr(0, 2048).c_str());
  std::string why = std::string(what) + ": HTTP " + std::to_string(r.status);
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
  throw Fail{why};
}

// 智谱限流 / 过载: 429 + 业务码 1302 (并发超限) / 1305 (过载) → 重发原因
std::string zhipu_busy(const Resp &r) {
  if (r.status != 429)
    return {};
  const json j = json::parse(r.body, nullptr, false);
  if (j.is_discarded() || !j.contains("error"))
    return {};
  const std::string code = j["error"].value("code", "");
  return code == "1302" || code == "1305" ? "限流 " + r.body : std::string();
}

// 模型表响应 → 模型 (下标 = Backend)
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
std::vector<Model> cursor_models(const json &j) {
  assert(j.contains("items"));
  std::vector<Model> out;
  for (const json &m : j["items"]) {
    const std::string id = str_of(m, "id");
    out.push_back({id, m.value("displayName", id)});
  }
  return out;
}
constexpr std::vector<Model> (*kModelsOf[])(const json &) = {zhipu_models, cursor_models};
static_assert(static_cast<int>(std::size(kModelsOf)) == kBackendCount);

} // namespace

const std::string &key(Backend b) {
  static const std::array<std::string, kBackendCount> keys = [] {
    if (!std::filesystem::exists(kKeyFile)) {
      json j;
      for (const BackendDef &d : kBackends)
        j[d.field] = d.fake;
      const std::string text = j.dump(2) + "\n";
      misc::atomic_write(kKeyFile, text.data(), text.size());
    }
    std::ifstream f(kKeyFile);
    assert(f.is_open());
    const json j = json::parse(f, nullptr, false);
    assert(!j.is_discarded() && j.is_object() && "kKeyFile 须是 JSON 对象");
    std::array<std::string, kBackendCount> k;
    for (int i = 0; i < kBackendCount; ++i)
      if (std::string s = str_of(j, kBackends[i].field); s != kBackends[i].fake)
        k[i] = std::move(s);
    return k;
  }();
  return keys[static_cast<int>(b)];
}

Models models(Backend b, const Net &net, const std::atomic<bool> &cancel) {
  assert(!key(b).empty() && valid(net));
  const char *target = def(b).models;
  Models out;
  try {
    const std::optional<Resp> r = retried("models", Resend::Any, cancel, [&](int) { return request(b, net, http::verb::get, target, "", {}, nullptr, &cancel); }, never);
    if (!r)
      return out;
    expect_2xx(target, *r);
    out.list = kModelsOf[static_cast<int>(b)](parse_json(r->body));
  } catch (const Fail &f) {
    out.error = f.why;
  }
  return out;
}

Session::Session(Options o) : o_(std::move(o)), conv_(o_.backend == Backend::Zhipu ? Conv(Zhipu{}) : Conv(Cursor{})) {
  assert(valid(o_.net));
  assert(!key(o_.backend).empty() && "kKeyFile 里该后端未填");
  assert(!o_.model.empty());
}

Reply Session::send(const std::string &user, const Delta &on_delta, const std::atomic<bool> &cancel) {
  const Clock::time_point t0 = Clock::now();
  Reply r;
  try {
    r = std::visit([&](auto &c) { return send(c, user, on_delta, cancel); }, conv_);
  } catch (const Fail &f) {
    r = Reply{.end = End::Error, .why = f.why};
  } catch (const boost::system::system_error &e) { // 非幂等 POST 发出后遇网络错 (不重发)
    r = Reply{.end = End::Error, .why = std::string("网络: ") + e.what()};
  }
  r.seconds = std::chrono::duration<double>(Clock::now() - t0).count();
  return r;
}

void Session::fold(size_t turn, std::string text) {
  Zhipu &c = std::get<Zhipu>(conv_);
  assert(2 * turn + 1 < c.msgs.size() && c.msgs[2 * turn].first == "user");
  c.msgs[2 * turn].second = std::move(text);
}

Reply Session::send(Zhipu &c, const std::string &user, const Delta &on_delta, const std::atomic<bool> &cancel) {
  json msgs = json::array();
  if (!o_.system.empty())
    msgs.push_back({{"role", "system"}, {"content", o_.system}});
  for (const auto &[role, content] : c.msgs)
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
  std::string finish; // finish_reason
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
  // 整轮可重发 (平台无对话态): 网络瞬抖 / 限流都从头再来
  const std::optional<Resp> resp = retried(
      "zhipu", Resend::Any, cancel,
      [&](int attempt) {
        if (attempt > 0)
          on_delta(Part::Restart, {});
        r = Reply{}, finish.clear();
        Sse sse;
        return request(Backend::Zhipu, o_.net, http::verb::post, kZhipuChat, payload, {{"Accept", "text/event-stream"}}, [&](std::string_view chunk) { return sse.feed(chunk, on_event); }, &cancel);
      },
      zhipu_busy);
  if (!resp) {
    r.end = End::Cancelled;
    return r;
  }
  expect_2xx("zhipu", *resp);
  if (finish.empty())
    throw Fail{"智谱流结束却没有 finish_reason"};
  r.end = finish == "stop" ? End::Ok : End::Cut;
  if (!r.ok()) {
    r.why = finish;
    return r;
  }
  c.msgs.emplace_back("user", user);
  c.msgs.emplace_back("assistant", r.text);
  return r;
}

Reply Session::send(Cursor &c, const std::string &user, const Delta &on_delta, const std::atomic<bool> &cancel) {
  json body;
  body["prompt"]["text"] = c.agent.empty() && !o_.system.empty() ? o_.system + "\n\n---\n\n" + user : user;
  if (c.agent.empty() && o_.model != "default")
    body["model"]["id"] = o_.model;
  const std::string target = c.agent.empty() ? std::string("/v1/agents") : "/v1/agents/" + c.agent + "/runs";
  const std::string payload = body.dump();
  const std::optional<Resp> made = retried(
      "cursor", Resend::Unsent, cancel, [&](int) { return request(Backend::Cursor, o_.net, http::verb::post, target, payload, {}, nullptr, &cancel); }, never);
  if (!made)
    return Reply{.end = End::Cancelled};
  expect_2xx("cursor", *made);
  const json j = parse_json(made->body);
  assert(j.contains("run") && (!c.agent.empty() || j.contains("agent")));
  if (c.agent.empty())
    c.agent = str_of(j["agent"], "id");
  Reply r;
  r.id = str_of(j["run"], "id");
  assert(!c.agent.empty() && !r.id.empty());
  const std::string run = "/v1/agents/" + c.agent + "/runs/" + r.id;

  std::string last_id, status, error; // 断线续流游标 (Last-Event-ID) / result 终态 / 流里的错误
  bool done = false;
  const auto on_event = [&](const std::string &ev, const std::string &data, const std::string &id) {
    if (!id.empty())
      last_id = id;
    if (ev == "assistant" || ev == "thinking") {
      put(r, on_delta, ev == "assistant" ? Part::Text : Part::Thinking, str_of(parse_json(data), "text"));
    } else if (ev == "result") {
      const json x = parse_json(data);
      status = str_of(x, "status");
      if (const std::string t = str_of(x, "text"); !t.empty())
        r.text = t; // 终稿为准
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
  // 流 GET 幂等: 断线按 Last-Event-ID 接着读; 收到终态后服务端先断了也算收齐
  const std::optional<Resp> streamed = retried(
      "cursor stream", Resend::Any, cancel,
      [&](int) {
        Headers h{{"Accept", "text/event-stream"}};
        if (!last_id.empty())
          h.emplace_back("Last-Event-ID", last_id);
        Sse sse;
        return request(Backend::Cursor, o_.net, http::verb::get, run + "/stream", "", h, [&](std::string_view chunk) { return sse.feed(chunk, on_event); }, &cancel);
      },
      [&](const Resp &x) { return x.status / 100 == 2 && !done && status.empty() && error.empty() ? std::string("流提前断开") : std::string(); });
  if (!streamed) {
    const Resp x = request(Backend::Cursor, o_.net, http::verb::post, run + "/cancel", "", {}, nullptr, nullptr, kCancelTimeoutSeconds);
    if (x.status != 409) // 409 = 已是终态
      expect_2xx("cursor cancel", x);
    r.end = End::Cancelled;
    return r;
  }
  expect_2xx("cursor stream", *streamed);
  if (!error.empty()) {
    r.end = End::Error, r.why = error;
    return r;
  }
  if (status.empty())
    throw Fail{"Cursor 流结束却没有 result"};
  r.end = status == "FINISHED" ? End::Ok : End::Cut;
  if (!r.ok())
    r.why = status;
  return r;
}

} // namespace agent::llm
