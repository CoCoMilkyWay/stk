#include "gui/task_crawler/CoroCrawler.hpp"
#include "gui/Tasks.hpp"
#include "gui/coro/CoroManager.hpp"
#include "gui/task_terminal/TaskTerminal.hpp"
#include "imgui.h"
#include "shared/GuiState.hpp"
#include "shared/SharedData.hpp"
#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <format>
#include <memory>
#include <string>
#include <vector>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace asio = boost::asio;

namespace GUI::Tasks {

class CoroCrawler {
public:
  CoroCrawler();
  ~CoroCrawler();

  const char *GetName() const;
  const char *GetStatus() const;
  void OnExpand();
  void OnCollapse();
  void DrawPanel(SharedData &data, GuiState &gui_state);

private:
  struct CrawlResult {
    std::string url;
    bool success;
    int status_code;
    size_t content_size;
  };

  void EnsureGuiState(GuiState &gui_state);
  void StartCrawling();
  void LogStart(size_t total) const;
  void UpdateCounts(const CrawlResult &result);
  void LogResult(const CrawlResult &result);

  asio::awaitable<void> RunCrawl(std::string url);
  asio::awaitable<CrawlResult> DoHttpGetAsync(const std::string &url);

  void AddCoroutine(std::unique_ptr<CoroutineHandle> handle);
  void CancelAllCoroutines();

  std::vector<std::unique_ptr<CoroutineHandle>> coroutines_;
  std::atomic<int> pending_count_{0};
  std::atomic<int> success_count_{0};
  std::atomic<int> failed_count_{0};
  std::vector<std::string> target_urls_;
  GuiState *gui_state_{nullptr};
};

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

CoroCrawler::~CoroCrawler() {
  CancelAllCoroutines();
}

void CoroCrawler::AddCoroutine(std::unique_ptr<CoroutineHandle> handle) {
  coroutines_.push_back(std::move(handle));
}

void CoroCrawler::CancelAllCoroutines() {
  for (auto &handle : coroutines_) {
    if (handle) {
      handle->Cancel();
    }
  }
  coroutines_.clear();
}

const char *CoroCrawler::GetName() const {
  return "Crawler";
}

const char *CoroCrawler::GetStatus() const {
  int pending = pending_count_.load();
  return pending > 0 ? "crawling" : "idle";
}

void CoroCrawler::EnsureGuiState(GuiState &gui_state) {
  if (!gui_state_) {
    gui_state_ = &gui_state;
  }
}

void CoroCrawler::OnExpand() {
  // No-op: crawler is user-driven
}

void CoroCrawler::OnCollapse() {
  // No-op: crawler keeps state regardless of panel visibility
}

void CoroCrawler::DrawPanel([[maybe_unused]] SharedData &data, GuiState &gui_state) {
  EnsureGuiState(gui_state);

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
  if (!gui_state_) {
    return;
  }

  const size_t total = target_urls_.size();
  for (const auto &url : target_urls_) {
    pending_count_.fetch_add(1);
    auto handle = gui_state_->Coro().Spawn(RunCrawl(url));
    AddCoroutine(std::move(handle));
  }

  LogStart(total);
}

asio::awaitable<void> CoroCrawler::RunCrawl(std::string url) {
  // Execute async HTTP request (non-blocking)
  CrawlResult result = co_await DoHttpGetAsync(url);

  UpdateCounts(result);
  LogResult(result);
}

void CoroCrawler::LogStart(size_t total) const {
  if (!gui_state_ || !gui_state_->terminal) {
    return;
  }
  gui_state_->terminal->AddLine(
      std::format("Started crawling {} URLs", total),
      Color::Blue());
}

void CoroCrawler::UpdateCounts(const CrawlResult &result) {
  pending_count_.fetch_sub(1);
  if (result.success) {
    success_count_.fetch_add(1);
  } else {
    failed_count_.fetch_add(1);
  }
}

void CoroCrawler::LogResult(const CrawlResult &result) {
  if (!gui_state_ || !gui_state_->terminal) {
    return;
  }

  if (result.success) {
    gui_state_->terminal->AddLine(
        std::format("✓ {} - {} bytes (HTTP {})", result.url, result.content_size, result.status_code),
        Color::Green());
  } else {
    gui_state_->terminal->AddLine(
        std::format("✗ {} - HTTP {}", result.url, result.status_code),
        Color::Red());
  }
}

asio::awaitable<CoroCrawler::CrawlResult> CoroCrawler::DoHttpGetAsync(const std::string &url) {
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
    try {
      stream.socket().shutdown(tcp::socket::shutdown_both);
    } catch (...) {
    }

    // Return result
    co_return CrawlResult{
        url,
        res.result() == http::status::ok,
        static_cast<int>(res.result_int()),
        res.body().size()};

  } catch (const std::exception &e) {
    // Connection or HTTP error
    co_return CrawlResult{url, false, 0, 0};
  }
}

// ============================================================================
// Factory Function
// ============================================================================

TaskHandle CreateCrawlerTask() {
  auto instance = std::make_shared<CoroCrawler>();

  TaskHandle handle;
  handle.name = instance->GetName();
  handle.task_instance = instance.get();
  handle.storage = instance;
  handle.GetStatus = [instance]() { return instance->GetStatus(); };
  handle.OnExpand = [instance]() { instance->OnExpand(); };
  handle.OnCollapse = [instance]() { instance->OnCollapse(); };
  handle.DrawPanel = [instance](SharedData &data, GuiState &gui) { instance->DrawPanel(data, gui); };
  handle.Destroy = [instance]() mutable { instance.reset(); };

  return handle;
}

} // namespace GUI::Tasks
