#include "api/sina/client.hpp"
#include "misc/netlog.hpp"
#include "yyjson/yyjson.h"

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>
#include <zlib.h>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <chrono>
#include <string>
#include <thread>

namespace sina {

namespace beast = boost::beast;
namespace http = boost::beast::http;
using tcp = boost::asio::ip::tcp;

namespace {

constexpr auto kIdle = std::chrono::seconds(10);
constexpr int kDayScale = 240;        // getKLineData 的日 K 档
constexpr int kCloseMinute = 15 * 60; // 日 K 的标号 = 收盘分钟

// 解不开 (回包截断 / 坏) → 空
std::optional<std::string> gunzip(const std::string &in) {
  z_stream zs{};
  [[maybe_unused]] const int init = inflateInit2(&zs, 16 + MAX_WBITS);
  assert(init == Z_OK);
  zs.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(in.data()));
  zs.avail_in = static_cast<uInt>(in.size());
  std::string out;
  int rc = Z_OK;
  while (rc != Z_STREAM_END) {
    const size_t at = out.size();
    out.resize(at + std::max<size_t>(in.size() * 4, 1 << 16));
    zs.next_out = reinterpret_cast<Bytef *>(out.data() + at);
    zs.avail_out = static_cast<uInt>(out.size() - at);
    rc = inflate(&zs, Z_NO_FLUSH);
    out.resize(out.size() - zs.avail_out);
    if (rc != Z_OK && rc != Z_STREAM_END) {
      inflateEnd(&zs);
      return std::nullopt;
    }
  }
  inflateEnd(&zs);
  return out;
}

// 不是数 → 空
std::optional<double> num_of(std::string_view s) {
  double v = 0;
  const auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
  if (ec != std::errc() || p != s.data() + s.size())
    return std::nullopt;
  return v;
}
double num(std::string_view s) {
  const std::optional<double> v = num_of(s);
  assert(v && "sina: 数值字段不符");
  return *v;
}

// "HH:MM:SS" → 当日秒; 不是这形 → 空
std::optional<int> clock_of(std::string_view s) {
  if (s.size() != 8 || s[2] != ':' || s[5] != ':')
    return std::nullopt;
  const std::optional<double> h = num_of(s.substr(0, 2)), m = num_of(s.substr(3, 2)), c = num_of(s.substr(6, 2));
  if (!h || !m || !c)
    return std::nullopt;
  return static_cast<int>(*h) * 3600 + static_cast<int>(*m) * 60 + static_cast<int>(*c);
}
int clock_sec(std::string_view s) {
  const std::optional<int> v = clock_of(s);
  assert(v && "sina: 时刻字段不是 HH:MM:SS");
  return *v;
}

std::string_view json_str(yyjson_val *o, const char *k) {
  yyjson_val *v = yyjson_obj_get(o, k);
  assert(v && yyjson_is_str(v) && "sina: K 线字段缺失");
  return {yyjson_get_str(v), yyjson_get_len(v)};
}

} // namespace

Client::Client(std::string host) : host_(std::move(host)) {}

std::string Client::get(const std::string &target) {
  std::optional<std::string> body = fetch(target, -1);
  assert(body && "sina: 回包不符 (非 200 / 未知编码 / gzip 解不开)");
  return std::move(*body);
}

std::optional<std::string> Client::fetch(const std::string &target, int tries) {
  for (int fails = 0;;) {
    const bool reused = stream_ && std::chrono::steady_clock::now() - used_ <= kIdle;
    try {
      if (!reused) {
        stream_.emplace(ioc_);
        stream_->expires_after(std::chrono::seconds(kTimeoutSeconds));
        stream_->connect(tcp::resolver(ioc_).resolve(host_, "80"));
      }
      http::request<http::empty_body> req{http::verb::get, target, 11};
      req.set(http::field::host, host_);
      req.set(http::field::referer, kReferer);
      req.set(http::field::user_agent, "Mozilla/5.0");
      req.set(http::field::accept_encoding, "gzip");
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
        netlog::fail("sina", host_, "HTTP " + std::to_string(res.result_int()) + " " + netlog::clip(target));
        return std::nullopt;
      }
      const auto enc = res[http::field::content_encoding];
      if (enc.empty())
        return std::move(res.body());
      if (enc != "gzip") {
        netlog::fail("sina", host_, "未知的 Content-Encoding " + std::string(enc.data(), enc.size()));
        return std::nullopt;
      }
      std::optional<std::string> out = gunzip(res.body());
      if (!out)
        netlog::fail("sina", host_, "gzip 回包解不开 " + netlog::clip(target));
      return out;
    } catch (const boost::system::system_error &e) {
      stream_.reset();
      if (reused) // keep-alive 连接被对端关了: 立即换新连接重发
        continue;
      netlog::retry("sina", host_, e.code().message(), fails++, 1);
      if (tries >= 0 && fails >= tries)
        return std::nullopt;
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

// 一行: var hq_str_<sym>="名称,今开,昨收,现价,最高,最低,买一,卖一,量,额,买1量,买1价,…,买5价,卖1量,卖1价,…,卖5价,日期,时间,…";
// 回包 → 各只; 有一行不认识 → 空
std::optional<std::vector<Quote>> Client::parse_quotes(std::string_view body, size_t n) {
  std::vector<Quote> out;
  out.reserve(n);
  std::string_view rest = body;
  std::vector<std::string_view> f;
  while (!rest.empty()) {
    const size_t nl = rest.find('\n');
    const std::string_view line = rest.substr(0, nl);
    rest = nl == std::string_view::npos ? std::string_view{} : rest.substr(nl + 1);
    if (line.empty())
      continue;
    const size_t k = line.find("hq_str_"), eq = line.find("=\""), end = line.rfind('"');
    if (k == std::string_view::npos || eq == std::string_view::npos || end <= eq)
      return std::nullopt;
    const std::string_view v = line.substr(eq + 2, end - eq - 2);
    if (v.empty())
      continue; // 查无此码
    f.clear();
    for (size_t a = 0;;) {
      const size_t b = v.find(',', a);
      f.push_back(v.substr(a, b - a));
      if (b == std::string_view::npos)
        break;
      a = b + 1;
    }
    if (f.size() < 32)
      return std::nullopt;
    bool ok = true;
    const auto at = [&](size_t i) {
      const std::optional<double> x = num_of(f[i]);
      ok = ok && x.has_value();
      return x.value_or(0);
    };
    Quote q;
    q.sym = line.substr(k + 7, eq - k - 7);
    q.open = static_cast<float>(at(1));
    q.last_close = static_cast<float>(at(2));
    q.price = static_cast<float>(at(3));
    q.high = static_cast<float>(at(4));
    q.low = static_cast<float>(at(5));
    q.volume = at(8);
    q.amount = at(9);
    for (int i = 0; i < kLevels; ++i) {
      q.bid_vol[i] = at(static_cast<size_t>(10 + 2 * i));
      q.bid[i] = static_cast<float>(at(static_cast<size_t>(11 + 2 * i)));
      q.ask_vol[i] = at(static_cast<size_t>(20 + 2 * i));
      q.ask[i] = static_cast<float>(at(static_cast<size_t>(21 + 2 * i)));
    }
    if (q.bid[0] > 0 && q.bid_vol[0] > 0 && q.ask[0] > 0 && q.ask_vol[0] > 0)
      q.price = static_cast<float>((q.ask[0] * q.bid_vol[0] + q.bid[0] * q.ask_vol[0]) / (q.ask_vol[0] + q.bid_vol[0]));
    q.date = misc::parse_ymd(f[30]);
    const std::optional<int> sec = clock_of(f[31]);
    if (!ok || q.date <= 0 || !sec)
      return std::nullopt;
    q.sec = *sec;
    out.push_back(std::move(q));
  }
  return out;
}

std::string Client::list_target(const std::vector<std::string> &syms) {
  assert(!syms.empty() && syms.size() <= static_cast<size_t>(kQuoteMax));
  std::string target = "/list=";
  for (size_t i = 0; i < syms.size(); ++i)
    (target += i ? "," : "") += syms[i];
  return target;
}

std::vector<Quote> Client::quotes(const std::vector<std::string> &syms) {
  std::optional<std::vector<Quote>> out = parse_quotes(get(list_target(syms)), syms.size());
  assert(out && "sina: 快照行不认识");
  return std::move(*out);
}

std::optional<std::vector<Quote>> Client::try_quotes(const std::vector<std::string> &syms, int tries) {
  assert(tries > 0);
  const std::optional<std::string> body = fetch(list_target(syms), tries);
  if (!body)
    return std::nullopt;
  std::optional<std::vector<Quote>> out = parse_quotes(*body, syms.size());
  if (!out)
    netlog::fail("sina", host_, "快照回包不认识");
  return out;
}

std::vector<Bar> Client::kline(std::string_view sym, int n) { return bars(sym, 1, n); }

std::vector<Bar> Client::daily(std::string_view sym, int n) { return bars(sym, kDayScale, n); }

// JSON 列表 [{day, open, high, low, close, volume, amount}] (值皆字符串; day 分钟 K "YYYY-MM-DD HH:MM:SS" / 日 K "YYYY-MM-DD", 日 K 无 amount); 查无 → null
std::vector<Bar> Client::bars(std::string_view sym, int scale, int n) {
  assert(n > 0);
  const std::string body = get("/cn/api/json_v2.php/CN_MarketDataService.getKLineData?symbol=" + std::string(sym) +
                               "&scale=" + std::to_string(scale) + "&ma=no&datalen=" + std::to_string(n));
  yyjson_doc *doc = yyjson_read(body.data(), body.size(), 0);
  assert(doc && "sina: K 线回包不是 JSON");
  yyjson_val *root = yyjson_doc_get_root(doc);
  std::vector<Bar> out;
  if (yyjson_is_arr(root)) {
    out.reserve(yyjson_arr_size(root));
    size_t i, m;
    yyjson_val *o;
    yyjson_arr_foreach(root, i, m, o) {
      const std::string_view day = json_str(o, "day");
      assert(day.size() == (scale == kDayScale ? 10u : 19u) && "sina: K 线标号不是 YYYY-MM-DD[ HH:MM:SS]");
      Bar b;
      b.date = misc::parse_ymd(day.substr(0, 10));
      assert(b.date > 0);
      b.minute = scale == kDayScale ? kCloseMinute : clock_sec(day.substr(11)) / 60;
      b.open = static_cast<float>(num(json_str(o, "open")));
      b.high = static_cast<float>(num(json_str(o, "high")));
      b.low = static_cast<float>(num(json_str(o, "low")));
      b.close = static_cast<float>(num(json_str(o, "close")));
      b.volume = num(json_str(o, "volume"));
      if (scale != kDayScale)
        b.amount = num(json_str(o, "amount"));
      out.push_back(b);
    }
  } else {
    assert(yyjson_is_null(root) && "sina: K 线回包既非列表也非 null");
  }
  yyjson_doc_free(doc);
  return out;
}

} // namespace sina
