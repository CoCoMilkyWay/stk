#include "api/tushare/client.hpp"
#include "misc/exec.hpp"
#include "yyjson/yyjson.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

namespace tushare {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

namespace {

std::string build_body(const std::string &token, std::string_view api_name, const Params &params) {
  yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
  yyjson_mut_val *root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);

  yyjson_mut_obj_add_strncpy(doc, root, "api_name", api_name.data(), api_name.size());
  yyjson_mut_obj_add_strncpy(doc, root, "token", token.data(), token.size());
  yyjson_mut_obj_add_str(doc, root, "fields", "");

  yyjson_mut_val *params_obj = yyjson_mut_obj(doc);
  yyjson_mut_obj_add_val(doc, root, "params", params_obj);
  for (const auto &[k, v] : params)
    yyjson_mut_obj_add_strncpy(doc, params_obj, k.c_str(), v.data(), v.size());

  std::size_t len = 0;
  char *raw = yyjson_mut_write(doc, 0, &len);
  assert(raw);
  std::string out(raw, len);
  std::free(raw);
  yyjson_mut_doc_free(doc);
  return out;
}

// 按接口排发送时刻: 相邻两次至少隔 60s / per_min; 进程内全部 Client 共用 (限额按 token × 接口计)
void pace(std::string_view api, int per_min) {
  assert(per_min > 0);
  using clock = std::chrono::steady_clock;
  static std::mutex mu;
  static std::map<std::string, clock::time_point, std::less<>> next;
  const auto gap = std::chrono::microseconds(60'000'000 / per_min);
  clock::time_point at;
  {
    std::lock_guard lk(mu);
    auto it = next.find(api);
    if (it == next.end())
      it = next.emplace(std::string(api), clock::time_point{}).first;
    at = std::max(it->second, clock::now());
    it->second = at + gap;
  }
  std::this_thread::sleep_until(at);
}

tcp::resolver::results_type resolve_endpoints() {
  for (int attempt = 0;; ++attempt) {
    net::io_context ioc;
    tcp::resolver resolver(ioc);
    beast::error_code ec;
    auto ep = resolver.resolve(kHost, kPort, ec);
    if (!ec)
      return ep;
    std::cerr << "\n[http] resolve error (attempt " << (attempt + 1) << "/" << (kRetryMax + 1) << "): " << ec.message()
              << std::endl;
    assert(attempt < kRetryMax);
    std::this_thread::sleep_for(std::chrono::seconds(kRetryIntervalSeconds));
  }
}

// 单线程 HTTP/1.1 keep-alive 会话: batch 内每工作线程持有一份, 跨请求复用连接
struct Session {
  net::io_context ioc;
  std::optional<beast::tcp_stream> stream;
  beast::flat_buffer buffer;

  ~Session() { close(); }

  void close() {
    if (!stream)
      return;
    beast::error_code ec;
    [[maybe_unused]] auto r = stream->socket().shutdown(tcp::socket::shutdown_both, ec);
    stream.reset();
  }

  // 返回的 yyjson_doc* 由调用方 yyjson_doc_free; HTTP 5xx → nullptr
  yyjson_doc *call(const tcp::resolver::results_type &ep, const std::string &token, const Query &q) {
    const std::string_view api_name = q.api;
    const std::string body = build_body(token, api_name, q.params);
    for (int attempt = 0;;) {
      if (q.per_min > 0)
        pace(api_name, q.per_min);
      const bool reused = stream.has_value();
      beast::error_code ec;
      if (!stream) {
        stream.emplace(ioc);
        stream->expires_after(std::chrono::seconds(kTimeoutSeconds));
        stream->connect(ep, ec);
      }
      http::response<http::string_body> res;
      if (!ec) {
        stream->expires_after(std::chrono::seconds(kTimeoutSeconds));
        http::request<http::string_body> req{http::verb::post, "/", 11};
        req.set(http::field::host, kHost);
        req.set(http::field::user_agent, "stk-tushare/1.0");
        req.set(http::field::content_type, "application/json");
        req.keep_alive(true);
        req.body() = body;
        req.prepare_payload();
        http::write(*stream, req, ec);
        if (!ec) {
          buffer.clear();
          http::read(*stream, buffer, res, ec);
        }
      }
      if (ec) {
        close();
        if (reused) // keep-alive 空闲连接被对端关闭: 立即换新连接重发, 不计入退避
          continue;
        std::cerr << "\n[http] transient error (attempt " << (attempt + 1) << "/" << (kRetryMax + 1)
                  << ") api=" << api_name << ": " << ec.message() << std::endl;
        assert(attempt < kRetryMax);
        std::this_thread::sleep_for(std::chrono::seconds(kRetryIntervalSeconds));
        ++attempt;
        continue;
      }
      if (!res.keep_alive())
        close();
      if (res.result_int() >= 500) { // 服务端故障 (502 / 503 / 504), 重发无益: 交调用方跳过
        close();
        std::cerr << "\n[http] HTTP " << res.result_int() << " api=" << api_name << ", 跳过" << std::endl;
        return nullptr;
      }

      yyjson_doc *doc = yyjson_read(res.body().data(), res.body().size(), 0);
      if (!doc) {
        std::cerr << "Tushare API: failed to parse response, body=\n"
                  << res.body().substr(0, 1024) << std::endl;
        assert(false);
      }

      yyjson_val *root_val = yyjson_doc_get_root(doc);
      yyjson_val *code_val = yyjson_obj_get(root_val, "code");
      assert(yyjson_is_int(code_val) || yyjson_is_uint(code_val));
      const std::int64_t code = yyjson_get_int(code_val);
      if (code == 0)
        return doc;

      yyjson_val *msg_val = yyjson_obj_get(root_val, "msg");
      const char *msg = (msg_val && yyjson_is_str(msg_val)) ? yyjson_get_str(msg_val) : "";
      if (code == 40203) { // 频率超限, 等窗口刷新恢复
        std::cerr << "\n[http] rate limit (attempt " << (attempt + 1) << "/" << (kRetryMax + 1) << ") api=" << api_name
                  << ": " << msg << std::endl;
        yyjson_doc_free(doc);
        assert(attempt < kRetryMax);
        std::this_thread::sleep_for(std::chrono::seconds(kRetryIntervalSeconds));
        ++attempt;
        continue;
      }
      std::cerr << "Tushare API error: code=" << code << " msg=" << msg << " api=" << api_name << std::endl;
      assert(false);
    }
  }
};

// 两种列型逐项对仗: Arrow 类型 / builder / JSON 判型 / 取值
struct Num {
  using Builder = arrow::DoubleBuilder;
  static std::shared_ptr<arrow::DataType> type() { return arrow::float64(); }
  static bool is(yyjson_val *v) { return yyjson_is_num(v); }
  static arrow::Status put(Builder &b, yyjson_val *v) { return b.Append(yyjson_get_num(v)); }
};
struct Str {
  using Builder = arrow::StringBuilder;
  static std::shared_ptr<arrow::DataType> type() { return arrow::utf8(); }
  static bool is(yyjson_val *v) { return yyjson_is_str(v); }
  static arrow::Status put(Builder &b, yyjson_val *v) {
    return b.Append(yyjson_get_str(v), static_cast<int32_t>(yyjson_get_len(v)));
  }
};

// items 第 c 列 → Array. builder 调用不包在 assert 里 (NDEBUG 下 assert 整句被编译掉).
template <class K>
std::shared_ptr<arrow::Array> column(yyjson_val *items, std::size_t c) {
  typename K::Builder b;
  size_t i, n;
  yyjson_val *item;
  yyjson_arr_foreach(items, i, n, item) {
    yyjson_val *v = yyjson_arr_get(item, c);
    const bool null = !v || yyjson_is_null(v);
    assert((null || K::is(v)) && "tushare 列类型行间不一致");
    const arrow::Status st = null ? b.AppendNull() : K::put(b, v);
    assert(st.ok());
  }
  std::shared_ptr<arrow::Array> arr;
  const arrow::Status st = b.Finish(&arr);
  assert(st.ok());
  return arr;
}

// envelope root.data.{fields, items} → Table; 结构不符 → assert
std::shared_ptr<arrow::Table> to_table(yyjson_doc *doc, const std::vector<std::string> &num) {
  yyjson_val *data = yyjson_obj_get(yyjson_doc_get_root(doc), "data");
  assert(data && "tushare response missing 'data'");
  yyjson_val *fields = yyjson_obj_get(data, "fields");
  yyjson_val *items = yyjson_obj_get(data, "items");
  assert(fields && items && yyjson_is_arr(fields) && yyjson_is_arr(items));

  std::vector<std::string> names;
  names.reserve(yyjson_arr_size(fields));
  size_t i, n;
  yyjson_val *v;
  yyjson_arr_foreach(fields, i, n, v) {
    assert(yyjson_is_str(v));
    names.emplace_back(yyjson_get_str(v));
  }
  for (const std::string &f : num)
    assert(std::find(names.begin(), names.end(), f) != names.end() &&
           "tushare num 声明的列不在响应里");

  std::vector<std::shared_ptr<arrow::Field>> out_fields;
  std::vector<std::shared_ptr<arrow::Array>> out_arrays;
  for (std::size_t c = 0; c < names.size(); ++c) {
    const bool is_num = std::find(num.begin(), num.end(), names[c]) != num.end();
    out_fields.push_back(arrow::field(names[c], is_num ? Num::type() : Str::type()));
    out_arrays.push_back(is_num ? column<Num>(items, c) : column<Str>(items, c));
  }
  return arrow::Table::Make(arrow::schema(out_fields), out_arrays);
}

} // namespace

Client::Client(std::string token) : token_(std::move(token)) {
  assert(!token_.empty());
}

Client::Client() : Client(kToken) {}

std::shared_ptr<arrow::Table> Client::query(const Query &q) {
  Session s;
  yyjson_doc *doc = s.call(resolve_endpoints(), token_, q);
  if (!doc)
    return nullptr;
  auto t = to_table(doc, q.num);
  yyjson_doc_free(doc);
  return t;
}

std::vector<std::shared_ptr<arrow::Table>> Client::batch(const std::vector<Query> &qs, const std::function<void(std::size_t)> &tick) {
  std::vector<std::shared_ptr<arrow::Table>> out(qs.size());
  if (qs.empty())
    return out;
  const tcp::resolver::results_type ep = resolve_endpoints();
  std::atomic<std::size_t> done{0};
  misc::exec::ForkJoin ex(std::min(kWidth, misc::exec::fit(qs.size())));
  std::vector<std::unique_ptr<Session>> sessions(static_cast<std::size_t>(ex.threads()));
  ex.tasks(static_cast<int>(qs.size()), [&](int i, int tid) {
    std::unique_ptr<Session> &s = sessions[static_cast<std::size_t>(tid)];
    if (!s)
      s = std::make_unique<Session>();
    const Query &q = qs[static_cast<std::size_t>(i)];
    if (yyjson_doc *doc = s->call(ep, token_, q)) {
      out[static_cast<std::size_t>(i)] = to_table(doc, q.num);
      yyjson_doc_free(doc);
    }
    if (tick)
      tick(done.fetch_add(1, std::memory_order_relaxed) + 1);
  });
  return out;
}

} // namespace tushare
