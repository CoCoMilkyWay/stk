#include "api/tushare/client.hpp"
#include "shared/Config.hpp"
#include "yyjson/yyjson.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>

namespace tushare {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

namespace {

// 返回的 yyjson_doc* 由 caller 用 yyjson_doc_free 释放
yyjson_doc *call(const std::string &token, std::string_view api_name, const Params &params) {
  // ---- Build request body ----
  yyjson_mut_doc *body_doc = yyjson_mut_doc_new(nullptr);
  yyjson_mut_val *root = yyjson_mut_obj(body_doc);
  yyjson_mut_doc_set_root(body_doc, root);

  std::string api_str(api_name);
  yyjson_mut_obj_add_strncpy(body_doc, root, "api_name", api_str.data(),
                             api_str.size());
  yyjson_mut_obj_add_strncpy(body_doc, root, "token", token.data(),
                             token.size());
  yyjson_mut_obj_add_str(body_doc, root, "fields", "");

  yyjson_mut_val *params_obj = yyjson_mut_obj(body_doc);
  yyjson_mut_obj_add_val(body_doc, root, "params", params_obj);
  for (auto &[k, v] : params) {
    yyjson_mut_obj_add_strncpy(body_doc, params_obj, k.c_str(), v.data(),
                               v.size());
  }

  size_t body_len = 0;
  char *body_str = yyjson_mut_write(body_doc, 0, &body_len);
  assert(body_str);
  std::string body(body_str, body_len);
  std::free(body_str);
  yyjson_mut_doc_free(body_doc);

  // ---- POST via boost.beast (retry on transient network errors + rate limit) ----
  // 网络瞬抖 (boost system_error) 与 tushare 频率超限 (code=40203) 共用同一 retry
  // 预算；其它业务错误 (code != 0) 与解析失败仍由 assert 兜底
  std::string res_body;
  yyjson_doc *doc = nullptr;
  for (int attempt = 0;; ++attempt) {
    try {
      net::io_context ioc;
      tcp::resolver resolver(ioc);
      beast::tcp_stream stream(ioc);
      stream.expires_after(
          std::chrono::seconds(::config::TUSHARE_HTTP_TIMEOUT_SECONDS));

      auto results = resolver.resolve(::config::TUSHARE_HTTP_HOST,
                                      ::config::TUSHARE_HTTP_PORT);
      stream.connect(results);

      http::request<http::string_body> req{http::verb::post, "/", 11};
      req.set(http::field::host, ::config::TUSHARE_HTTP_HOST);
      req.set(http::field::user_agent, "qmt-tushare/1.0");
      req.set(http::field::content_type, "application/json");
      req.body() = body;
      req.prepare_payload();

      http::write(stream, req);

      beast::flat_buffer buffer;
      http::response<http::string_body> res;
      http::read(stream, buffer, res);

      beast::error_code ec;
      [[maybe_unused]] auto shutdown_ec =
          stream.socket().shutdown(tcp::socket::shutdown_both, ec);

      res_body = std::move(res.body());
    } catch (const boost::system::system_error &e) {
      std::cerr << "\n[http] transient error (attempt " << (attempt + 1) << "/"
                << (::config::TUSHARE_HTTP_RETRY_MAX + 1) << ") api=" << api_name
                << ": " << e.what() << std::endl;
      assert(attempt < ::config::TUSHARE_HTTP_RETRY_MAX);
      std::this_thread::sleep_for(
          std::chrono::seconds(::config::TUSHARE_HTTP_RETRY_INTERVAL_SECONDS));
      continue;
    }

    // ---- Parse response ----
    doc = yyjson_read(res_body.data(), res_body.size(), 0);
    if (!doc) {
      std::cerr << "Tushare API: failed to parse response, body=\n"
                << res_body.substr(0, 1024) << std::endl;
      assert(false);
    }

    yyjson_val *root_val = yyjson_doc_get_root(doc);
    yyjson_val *code_val = yyjson_obj_get(root_val, "code");
    assert(yyjson_is_int(code_val) || yyjson_is_uint(code_val));
    int64_t code = yyjson_get_int(code_val);
    if (code == 0) {
      break;
    }
    yyjson_val *msg_val = yyjson_obj_get(root_val, "msg");
    const char *msg =
        (msg_val && yyjson_is_str(msg_val)) ? yyjson_get_str(msg_val) : "";
    // 40203 = 频率超限 (e.g. "访问接口(income_vip)频率超限(400次/分钟)")，等 1 分钟窗口刷新即可恢复
    if (code == 40203) {
      std::cerr << "\n[http] rate limit (attempt " << (attempt + 1) << "/"
                << (::config::TUSHARE_HTTP_RETRY_MAX + 1) << ") api=" << api_name
                << ": " << msg << std::endl;
      yyjson_doc_free(doc);
      doc = nullptr;
      assert(attempt < ::config::TUSHARE_HTTP_RETRY_MAX);
      std::this_thread::sleep_for(
          std::chrono::seconds(::config::TUSHARE_HTTP_RETRY_INTERVAL_SECONDS));
      continue;
    }
    std::cerr << "Tushare API error: code=" << code << " msg=" << msg
              << " api=" << api_name << std::endl;
    assert(false);
  }

  return doc;
}

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

Client::Client() : Client(::config::TUSHARE_TOKEN) {}

std::shared_ptr<arrow::Table> Client::query(const Query &q) {
  yyjson_doc *doc = call(token_, q.api, q.params);
  auto t = to_table(doc, q.num);
  yyjson_doc_free(doc);
  return t;
}

} // namespace tushare
