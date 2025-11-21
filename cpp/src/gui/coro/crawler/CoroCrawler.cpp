#include "gui/coro/CoroCrawler.hpp"
#include "gui/GuiState.hpp"
#include "gui/coro/CoroManager.hpp"
#include "shared/SharedData.hpp"
#include "imgui.h"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <format>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;

// ============================================================================
// CoroCrawler Implementation
// ============================================================================

CoroCrawler::CoroCrawler() {
  // Example URLs to crawl
  target_urls_ = {
    "https://example.com",
    "https://example.org",
    "https://example.net",
  };
}

const char* CoroCrawler::GetName() const {
  return "Crawler";
}

const char* CoroCrawler::GetStatus() const {
  int pending = pending_count_.load();
  return pending > 0 ? "crawling" : "idle";
}

StatusColor CoroCrawler::GetStatusColor() const {
  int pending = pending_count_.load();
  return pending > 0 ? StatusColor::Purple() : StatusColor::Green();
}

void CoroCrawler::DrawPanel([[maybe_unused]] SharedData& data, GuiState& gui_state) {
  if (!gui_state_) {
    gui_state_ = &gui_state;
  }
  
  ImGui::Text("Web Crawler");
  ImGui::Separator();
  
  ImGui::Text("Pending: %d", pending_count_.load());
  ImGui::Text("Success: %d", success_count_.load());
  ImGui::Text("Failed: %d", failed_count_.load());
  
  ImGui::Separator();
  
  if (ImGui::Button("Start Crawling")) {
    StartCrawling();
  }
  
  ImGui::SameLine();
  
  if (ImGui::Button("Stop All")) {
    CancelAllCoroutines();
    pending_count_.store(0);
  }
}

void CoroCrawler::StartCrawling() {
  // Start one coroutine per URL (concurrent crawling)
  for (const auto& url : target_urls_) {
    pending_count_.fetch_add(1);
    
    // Each URL gets its own coroutine
    if (gui_state_) {
      auto handle = gui_state_->Coro().Spawn(RunCrawl(url));
      AddCoroutine(std::move(handle));
    }
  }
  
  if (gui_state_) {
    gui_state_->terminal.AddLine(
      std::format("Started crawling {} URLs", target_urls_.size()),
      Color::Blue()
    );
  }
}

asio::awaitable<void> CoroCrawler::RunCrawl(std::string url) {
  // Execute async HTTP request (non-blocking)
  CrawlResult result = co_await DoHttpGetAsync(url);
  
  // Update counters
  pending_count_.fetch_sub(1);
  if (result.success) {
    success_count_.fetch_add(1);
  } else {
    failed_count_.fetch_add(1);
  }
  
  // Log result
  if (gui_state_) {
    if (result.success) {
      gui_state_->terminal.AddLine(
        std::format("✓ {} - {} bytes (HTTP {})", url, result.content_size, result.status_code),
        Color::Green()
      );
    } else {
      gui_state_->terminal.AddLine(
        std::format("✗ {} - HTTP {}", url, result.status_code),
        Color::Red()
      );
    }
  }
}

asio::awaitable<CoroCrawler::CrawlResult> CoroCrawler::DoHttpGetAsync(const std::string& url) {
  using tcp = net::ip::tcp;
  
  // Parse URL (simple parser for http://host/path format)
  std::string host, target;
  size_t scheme_end = url.find("://");
  if (scheme_end == std::string::npos) {
    co_return CrawlResult{url, false, 0, 0};
  }
  
  size_t host_start = scheme_end + 3;
  size_t path_start = url.find('/', host_start);
  
  if (path_start == std::string::npos) {
    host = url.substr(host_start);
    target = "/";
  } else {
    host = url.substr(host_start, path_start - host_start);
    target = url.substr(path_start);
  }
  
  auto executor = co_await asio::this_coro::executor;
  
  try {
    // Resolve host
    tcp::resolver resolver(executor);
    auto results = co_await resolver.async_resolve(host, "80", net::use_awaitable);
    
    // Connect to server
    beast::tcp_stream stream(executor);
    stream.expires_after(std::chrono::seconds(10));
    co_await stream.async_connect(results, net::use_awaitable);
    
    // Build HTTP GET request
    http::request<http::string_body> req{http::verb::get, target, 11};
    req.set(http::field::host, host);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    
    // Send request
    co_await http::async_write(stream, req, net::use_awaitable);
    
    // Receive response
    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    co_await http::async_read(stream, buffer, res, net::use_awaitable);
    
    // Graceful shutdown
    try { stream.socket().shutdown(tcp::socket::shutdown_both); } catch (...) {}
    
    // Return result
    co_return CrawlResult{
      url,
      res.result() == http::status::ok,
      static_cast<int>(res.result_int()),
      res.body().size()
    };
    
  } catch (const std::exception& e) {
    // Connection or HTTP error
    co_return CrawlResult{url, false, 0, 0};
  }
}

// ============================================================================
// Factory Function
// ============================================================================

IGuiTask* CreateCoroCrawler() {
  return new CoroCrawler();
}

