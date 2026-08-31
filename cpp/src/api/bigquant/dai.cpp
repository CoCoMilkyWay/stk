#include "api/bigquant/dai.hpp"

#include "shared/Config.hpp"
#include "yyjson/yyjson.h"

#include <arrow/buffer.h>
#include <arrow/flight/api.h>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace bigquant {

namespace flight = arrow::flight;

namespace {

// 构造 DAI Flight ticket JSON. yyjson 比手拼字符串安全 (escape 自动处理).
std::string build_ticket_json(std::string_view sql, const DaiFilters &filters) {
  yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
  yyjson_mut_val *root = yyjson_mut_obj(doc);
  yyjson_mut_doc_set_root(doc, root);

  yyjson_mut_obj_add_strncpy(doc, root, "sql", sql.data(), sql.size());
  yyjson_mut_obj_add_bool(doc, root, "full_db_scan", false);

  yyjson_mut_val *fil_obj = yyjson_mut_obj(doc);
  for (auto &[k, vs] : filters) {
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (auto &v : vs) {
      yyjson_mut_arr_add_strncpy(doc, arr, v.data(), v.size());
    }
    yyjson_mut_obj_add_val(doc, fil_obj, k.c_str(), arr);
  }
  yyjson_mut_obj_add_val(doc, root, "filters", fil_obj);
  yyjson_mut_obj_add_null(doc, root, "params");

  size_t len = 0;
  char *out = yyjson_mut_write(doc, 0, &len);
  assert(out);
  std::string s(out, len);
  std::free(out);
  yyjson_mut_doc_free(doc);
  return s;
}

// 任何 arrow::Status / arrow::Result 失败一律 assert; 错误信息写 stderr.
template <typename T>
T unwrap(arrow::Result<T> &&r, const char *what) {
  if (!r.ok()) {
    std::cerr << "[bigquant.dai] arrow " << what << " failed: " << r.status().ToString() << std::endl;
    assert(false);
  }
  return std::move(r).ValueUnsafe();
}

} // namespace

DaiClient::DaiClient(std::string ak, std::string sk)
    : ak_(std::move(ak)), sk_(std::move(sk)) {
  assert(!ak_.empty() && !sk_.empty());
}

DaiClient::DaiClient() : DaiClient(::config::BIGQUANT_AK, ::config::BIGQUANT_SK) {}

void DaiClient::ensure_flight() {
  if (flight_)
    return;

  arrow::Result<flight::Location> loc_r =
      flight::Location::Parse(::config::BIGQUANT_FLIGHT_URI);
  flight::Location loc = unwrap(std::move(loc_r), "Location::Parse");

  flight::FlightClientOptions opts = flight::FlightClientOptions::Defaults();
  // grpc.max_metadata_size: 与 SDK 一致 (默认 8KB 会被 JWT + custom headers 撑爆).
  opts.generic_options.emplace_back(
      std::string("grpc.max_metadata_size"),
      std::variant<int, std::string>(static_cast<int>(
          ::config::BIGQUANT_FLIGHT_GRPC_MAX_METADATA_SIZE)));

  auto client_r = flight::FlightClient::Connect(loc, opts);
  flight_ = unwrap(std::move(client_r), "FlightClient::Connect");

  // ak 作 username, sk 作 password; server 返回 ("authorization", "Bearer <JWT>") 头对.
  flight::FlightCallOptions empty;
  auto auth_r = flight_->AuthenticateBasicToken(empty, ak_, sk_);
  flight_auth_ = unwrap(std::move(auth_r), "AuthenticateBasicToken");
  assert(!flight_auth_.first.empty() && !flight_auth_.second.empty());
}

std::shared_ptr<arrow::Table>
DaiClient::query(std::string_view sql, const DaiFilters &filters) {
  ensure_flight();

  std::string ticket_json = build_ticket_json(sql, filters);

  flight::FlightCallOptions opts;
  opts.headers.emplace_back(flight_auth_.first, flight_auth_.second);

  flight::Ticket ticket{ticket_json};
  auto reader_r = flight_->DoGet(opts, ticket);
  std::unique_ptr<flight::FlightStreamReader> reader =
      unwrap(std::move(reader_r), "DoGet");

  auto table_r = reader->ToTable();
  std::shared_ptr<arrow::Table> table = unwrap(std::move(table_r), "FlightStreamReader::ToTable");
  assert(table);
  return table;
}

Quota DaiClient::quota() {
  ensure_flight();

  flight::FlightCallOptions opts;
  opts.headers.emplace_back(flight_auth_.first, flight_auth_.second);

  flight::Action action{"quota", arrow::Buffer::FromString("")};
  auto stream_r = flight_->DoAction(opts, action);
  std::unique_ptr<flight::ResultStream> stream =
      unwrap(std::move(stream_r), "DoAction(quota)");

  auto res_r = stream->Next();
  std::unique_ptr<flight::Result> res =
      unwrap(std::move(res_r), "ResultStream::Next");
  assert(res && res->body && "DAI quota: 空响应");

  yyjson_doc *doc =
      yyjson_read(reinterpret_cast<const char *>(res->body->data()),
                  static_cast<std::size_t>(res->body->size()), 0);
  assert(doc && "DAI quota: 响应 JSON 解析失败");
  yyjson_val *root = yyjson_doc_get_root(doc);
  yyjson_val *w = yyjson_obj_get(root, "weekly_quota");
  yyjson_val *u = yyjson_obj_get(root, "used_quota");
  assert(w && u && "DAI quota: 响应缺 weekly_quota / used_quota");
  Quota q{yyjson_get_sint(w), yyjson_get_sint(u)};
  yyjson_doc_free(doc);

  assert(q.weekly > 0 && "DAI quota: weekly_quota 非正");
  return q;
}

} // namespace bigquant
