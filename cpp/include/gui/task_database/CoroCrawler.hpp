#pragma once
#include "gui/task_database/AssetInfo.hpp"
#include <boost/asio/awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;

namespace GUI::Database {

class CoroCrawler {
public:
  CoroCrawler(DatabaseState &state, asio::io_context &io_ctx);
  
  // Refresh asset metadata from exchanges
  asio::awaitable<void> refresh_asset_metadata();
  
private:
  DatabaseState &state_;
  asio::io_context &io_ctx_;
  
  // SH API functions
  asio::awaitable<void> fetch_sh_stock(AssetInfo &asset);
  
  // SZ bulk download
  asio::awaitable<void> fetch_sz_bulk();
  
  // JSON cache operations
  void load_cache(const std::string &path);
  void save_cache(const std::string &path);
};

} // namespace GUI::Database

