#include "api/tushare/http.hpp"
#include "shared/Config.hpp"

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include <cassert>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

namespace tushare {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

Http::Http(std::string token) : token_(std::move(token)) {
  assert(!token_.empty());
}

yyjson_doc *
Http::call(std::string_view api_name,
           const std::vector<std::pair<std::string, std::string>> &params) {
  // ---- Build request body ----
  yyjson_mut_doc *body_doc = yyjson_mut_doc_new(nullptr);
  yyjson_mut_val *root = yyjson_mut_obj(body_doc);
  yyjson_mut_doc_set_root(body_doc, root);

  std::string api_str(api_name);
  yyjson_mut_obj_add_strncpy(body_doc, root, "api_name", api_str.data(),
                             api_str.size());
  yyjson_mut_obj_add_strncpy(body_doc, root, "token", token_.data(),
                             token_.size());
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

} // namespace tushare
