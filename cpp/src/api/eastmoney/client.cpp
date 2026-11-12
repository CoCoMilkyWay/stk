#include "api/eastmoney/client.hpp"
#include "yyjson/yyjson.h"

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>

#include <algorithm>
#include <cassert>
#include <iostream>
#include <set>
#include <string>
#include <string_view>
#include <thread>

namespace eastmoney {

namespace beast = boost::beast;
namespace http = boost::beast::http;
using tcp = boost::asio::ip::tcp;

namespace {

constexpr auto kIdle = std::chrono::seconds(10);
constexpr int kPage = 100; // 服务端每页上限

constexpr std::string_view kAllA = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048";

// 列表一条 (f12 代码 / f13 市场 0 深北 1 沪 / 文本字段: 板块取 f14 名称, 个股取 f103 所属概念)
struct Item {
  std::string code, text;
  int64_t market = 0;
};

std::string_view json_str(yyjson_val *o, const char *k) {
  yyjson_val *v = yyjson_obj_get(o, k);
  assert(v && yyjson_is_str(v) && "eastmoney: 字段缺失");
  return {yyjson_get_str(v), yyjson_get_len(v)};
}

// 一页回包 → 本页条目追加到 out, 返回列表总数 (data = null ⇔ 空列表)
size_t page(const std::string &body, const char *text, std::vector<Item> &out) {
  yyjson_doc *doc = yyjson_read(body.data(), body.size(), 0);
  assert(doc && "eastmoney: 回包不是 JSON");
  yyjson_val *root = yyjson_doc_get_root(doc), *rc = yyjson_obj_get(root, "rc"), *data = yyjson_obj_get(root, "data");
  assert(rc && yyjson_get_int(rc) == 0 && "eastmoney: rc ≠ 0");
  size_t total = 0;
  if (data && !yyjson_is_null(data)) {
    yyjson_val *n = yyjson_obj_get(data, "total"), *diff = yyjson_obj_get(data, "diff");
    assert(n && yyjson_is_uint(n) && diff && yyjson_is_arr(diff) && "eastmoney: data 无 total / diff");
    total = yyjson_get_uint(n);
    size_t i, m;
    yyjson_val *o;
    yyjson_arr_foreach(diff, i, m, o) {
      yyjson_val *mk = yyjson_obj_get(o, "f13");
      assert(mk && yyjson_is_int(mk) && "eastmoney: f13 缺失");
      out.push_back({std::string(json_str(o, "f12")), std::string(json_str(o, text)), yyjson_get_int(mk)});
    }
  }
  yyjson_doc_free(doc);
  return total;
}

// fs 列表全部条目: 按代码升序逐页拉到 total 条; 任一页网络不通 → nullopt
template <class Get>
std::optional<std::vector<Item>> list(Get &&get, std::string_view fs, const char *text) {
  std::vector<Item> out;
  for (int pn = 1;; ++pn) {
    const size_t had = out.size();
    const std::optional<std::string> body = get("/api/qt/clist/get?pn=" + std::to_string(pn) + "&pz=" + std::to_string(kPage) +
                                                "&po=0&np=1&fltt=2&invt=2&fid=f12&fs=" + std::string(fs) + "&fields=f12,f13," + text);
    if (!body)
      return std::nullopt;
    const size_t total = page(*body, text, out);
    if (out.size() >= total)
      break;
    assert(out.size() > had && "eastmoney: 翻页没有新条目");
  }
  std::set<std::string_view> seen;
  for (const Item &x : out)
    assert(seen.insert(x.code).second && "eastmoney: 列表代码重复");
  return out;
}

} // namespace

Client::Client() = default;

std::optional<std::string> Client::get(const std::string &target) {
  for (int attempt = 0;; ++attempt) {
    try {
      const auto now = std::chrono::steady_clock::now();
      if (!stream_ || now - used_ > kIdle) {
        stream_.emplace(ioc_);
        stream_->expires_after(std::chrono::seconds(kTimeoutSeconds));
        stream_->connect(tcp::resolver(ioc_).resolve(kHost, "80"));
      }
      http::request<http::empty_body> req{http::verb::get, target, 11};
      req.set(http::field::host, kHost);
      req.set(http::field::user_agent, "Mozilla/5.0");
      req.keep_alive(true);
      stream_->expires_after(std::chrono::seconds(kTimeoutSeconds));
      http::write(*stream_, req);
      beast::flat_buffer buf;
      http::response<http::string_body> res;
      http::read(*stream_, buf, res);
      used_ = std::chrono::steady_clock::now();
      if (!res.keep_alive())
        stream_.reset();
      if (res.result() != http::status::ok) {
        std::cerr << "[eastmoney] HTTP " << res.result_int() << " " << target.substr(0, 120) << std::endl;
        assert(false && "eastmoney: HTTP 非 200");
      }
      assert(res[http::field::content_encoding].empty() && "eastmoney: 未要压缩却回了 Content-Encoding");
      return std::move(res.body());
    } catch (const boost::system::system_error &e) {
      stream_.reset();
      std::cerr << "[eastmoney] transient error (attempt " << (attempt + 1) << "/" << (kRetryMax + 1)
                << "): " << e.what() << std::endl;
      if (attempt == kRetryMax)
        return std::nullopt;
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

std::optional<std::vector<Board>> Client::concepts() {
  std::optional<std::vector<Item>> items = list([this](const std::string &t) { return get(t); }, "m:90+t:3+f:!50", "f14");
  if (!items)
    return std::nullopt;
  std::vector<Board> out;
  for (Item &x : *items) {
    assert(x.code.starts_with("BK") && "eastmoney: 板块代码不是 BKxxxx");
    out.push_back({std::move(x.code), std::move(x.text)});
  }
  return out;
}

std::optional<std::vector<Stock>> Client::stocks() {
  const std::optional<std::vector<Item>> items = list([this](const std::string &t) { return get(t); }, kAllA, "f103");
  if (!items)
    return std::nullopt;
  std::vector<Stock> out;
  for (const Item &x : *items) {
    assert(x.code.size() == 6 && (x.market == 0 || x.market == 1) && "eastmoney: 个股代码 / 市场不符");
    const char *ex = x.market == 1 ? ".SH" : (x.code[0] == '4' || x.code[0] == '8' || x.code[0] == '9') ? ".BJ"
                                                                                                        : ".SZ";
    Stock s{x.code + ex, {}};
    if (x.text != "-")
      for (size_t i = 0, j; i <= x.text.size(); i = j + 1) {
        j = std::min(x.text.find(',', i), x.text.size());
        assert(j > i && "eastmoney: f103 空概念名");
        s.concepts.emplace_back(x.text, i, j - i);
      }
    out.push_back(std::move(s));
  }
  return out;
}

} // namespace eastmoney
