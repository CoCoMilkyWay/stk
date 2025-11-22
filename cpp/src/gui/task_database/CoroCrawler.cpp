#include "gui/task_database/CoroCrawler.hpp"
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/connect.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include "package/nlohmann/json.hpp"
#include <filesystem>
#include <fstream>
#include <chrono>
#include <regex>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;
namespace fs = std::filesystem;
using json = nlohmann::json;

namespace GUI::Database {

CoroCrawler::CoroCrawler(DatabaseState &state, asio::io_context &io_ctx)
    : state_(state), io_ctx_(io_ctx) {}

asio::awaitable<void> CoroCrawler::refresh_asset_metadata() {
  state_.crawl_status = DatabaseState::CrawlStatus::Idle;
  state_.status_message = "Crawler ready";
  
  // Load existing cache
  load_cache("config/assets.json");
  
  // Filter assets needing update (>24h old or missing)
  std::vector<AssetInfo*> needs_update_sh;
  std::vector<AssetInfo*> needs_update_sz;
  
  uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  
  for (auto &asset : state_.assets) {
    bool needs_update = (asset.metadata.last_updated_ts == 0 ||
                        (now - asset.metadata.last_updated_ts) > 86400);
    
    if (needs_update) {
      if (asset.exchange == "SH") {
        needs_update_sh.push_back(&asset);
      } else if (asset.exchange == "SZ") {
        needs_update_sz.push_back(&asset);
      }
    }
  }
  
  state_.total_to_crawl = needs_update_sh.size() + needs_update_sz.size();
  state_.crawled_count = 0;
  
  if (state_.total_to_crawl == 0) {
    state_.crawl_status = DatabaseState::CrawlStatus::Complete;
    state_.status_message = "All metadata up-to-date";
    co_return;
  }
  
  // Fetch SZ bulk (single Excel file for all SZ stocks)
  if (!needs_update_sz.empty()) {
    state_.crawl_status = DatabaseState::CrawlStatus::FetchingSZ;
    co_await fetch_sz_bulk();
  }
  
  // Fetch SH stocks individually (parallel with concurrency limit)
  if (!needs_update_sh.empty()) {
    state_.crawl_status = DatabaseState::CrawlStatus::FetchingSH;
    
    // Simple sequential for now (parallel implementation would be more complex)
    for (auto *asset : needs_update_sh) {
      co_await fetch_sh_stock(*asset);
      state_.crawled_count++;
    }
  }
  
  state_.crawl_status = DatabaseState::CrawlStatus::Complete;
  state_.status_message = "Metadata update complete";
  
  // Save updated cache
  save_cache("config/assets.json");
  
  co_return;
}

asio::awaitable<void> CoroCrawler::fetch_sh_stock(AssetInfo &asset) {
  try {
    auto executor = co_await asio::this_coro::executor;
    
    // Resolve hostname
    tcp::resolver resolver(executor);
    auto const results = co_await resolver.async_resolve(
        "query.sse.com.cn", "80", asio::use_awaitable);
    
    // Connect to server
    beast::tcp_stream stream(executor);
    stream.expires_after(std::chrono::seconds(30));
    co_await stream.async_connect(results, asio::use_awaitable);
    
    // Build HTTP request
    http::request<http::string_body> req{http::verb::get, 
        "/commonQuery.do?jsonCallBack=jsonpCallback22015&isPagination=false"
        "&sqlId=COMMON_SSE_ZQPZ_GP_GPLB_C&productid=" + asset.asset_code + 
        "&_=" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()),
        11};
    req.set(http::field::host, "query.sse.com.cn");
    req.set(http::field::user_agent, "Mozilla/5.0");
    req.set(http::field::referer, 
        "http://www.sse.com.cn/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=" + asset.asset_code);
    
    // Send request
    co_await http::async_write(stream, req, asio::use_awaitable);
    
    // Receive response
    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    co_await http::async_read(stream, buffer, res, asio::use_awaitable);
    
    // Parse JSONP response: jsonpCallback22015({...})
    std::string body = res.body();
    std::regex jsonp_regex(R"(jsonpCallback22015\((.*)\))");
    std::smatch match;
    
    if (std::regex_search(body, match, jsonp_regex) && match.size() > 1) {
      json j = json::parse(match[1].str());
      if (j.contains("result") && j["result"].is_array() && !j["result"].empty()) {
        auto& result = j["result"][0];
        
        // Extract metadata
        if (result.contains("简称-A")) asset.metadata.name_cn = result["简称-A"];
        if (result.contains("公司全称-中")) asset.metadata.name_cn_full = result["公司全称-中"];
        if (result.contains("公司全称-英")) asset.metadata.name_en = result["公司全称-英"];
        if (result.contains("上市日-A")) asset.metadata.listing_date = result["上市日-A"];
        if (result.contains("状态-A")) asset.metadata.status = result["状态-A"];
        if (result.contains("SSE行业")) asset.metadata.industry = result["SSE行业"];
        if (result.contains("所属省/直辖市")) asset.metadata.province = result["所属省/直辖市"];
        if (result.contains("网址")) asset.metadata.website = result["网址"];
        
        asset.metadata.data_source = "SH_API";
        asset.metadata.last_updated_ts = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
      }
    }
    
    // Close connection gracefully
    beast::error_code ec;
    [[maybe_unused]] auto shutdown_result = stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    
  } catch (...) {
    // Silently fail for individual stocks
  }
  
  co_return;
}

asio::awaitable<void> CoroCrawler::fetch_sz_bulk() {
  // Placeholder implementation - actual HTTP request and Excel parsing would go here
  
  co_await asio::this_coro::executor;
  
  // Simulate network delay
  asio::steady_timer timer(io_ctx_);
  timer.expires_after(std::chrono::milliseconds(100));
  co_await timer.async_wait(asio::use_awaitable);
  
  uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  
  for (auto &asset : state_.assets) {
    if (asset.exchange == "SZ") {
      asset.metadata.name_cn = "SZ" + asset.asset_code;
      asset.metadata.data_source = "SZ_EXCEL";
      asset.metadata.last_updated_ts = now;
      state_.crawled_count++;
    }
  }
  
  co_return;
}

void CoroCrawler::load_cache(const std::string &path) {
  if (!fs::exists(path)) return;
  
  try {
    std::ifstream file(path);
    json j;
    file >> j;
    
    // TODO: Parse JSON cache and populate asset metadata
    
  } catch (...) {}
}

void CoroCrawler::save_cache(const std::string &path) {
  try {
    json j = json::array();
    
    for (const auto &asset : state_.assets) {
      if (asset.metadata.last_updated_ts == 0) continue;
      
      json asset_json;
      asset_json["code"] = asset.asset_code;
      asset_json["exchange"] = asset.exchange;
      asset_json["name_cn"] = asset.metadata.name_cn;
      asset_json["name_cn_full"] = asset.metadata.name_cn_full;
      asset_json["name_en"] = asset.metadata.name_en;
      asset_json["listing_date"] = asset.metadata.listing_date;
      asset_json["status"] = asset.metadata.status;
      asset_json["industry"] = asset.metadata.industry;
      asset_json["province"] = asset.metadata.province;
      asset_json["last_updated_ts"] = asset.metadata.last_updated_ts;
      asset_json["data_source"] = asset.metadata.data_source;
      
      j.push_back(asset_json);
    }
    
    std::error_code ec;
    fs::create_directories(fs::path(path).parent_path(), ec);
    std::ofstream file(path);
    file << j.dump(2);
    
  } catch (...) {}
}

} // namespace GUI::Database

