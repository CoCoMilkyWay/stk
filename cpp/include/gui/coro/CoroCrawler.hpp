#pragma once
#include "gui/GuiTask.hpp"
#include <atomic>
#include <vector>
#include <string>
#include <boost/asio/awaitable.hpp>

struct SharedData;
struct GuiState;

namespace asio = boost::asio;

// Web crawler using ASIO/Beast async HTTP client
// Spawns one coroutine per URL, all run concurrently
class CoroCrawler : public TaskWithCoroutines {
public:
  CoroCrawler();
  ~CoroCrawler() override = default;
  
  // IGuiTask interface
  const char* GetName() const override;
  const char* GetStatus() const override;
  StatusColor GetStatusColor() const override;
  void DrawPanel(SharedData& data, GuiState& gui_state) override;
  
private:
  // Result structure
  struct CrawlResult {
    std::string url;
    bool success;
    int status_code;
    size_t content_size;
  };
  
  // Start crawling all URLs
  void StartCrawling();
  
  // Main coroutine (one per URL)
  asio::awaitable<void> RunCrawl(std::string url);
  
  // Async HTTP GET using Beast
  asio::awaitable<CrawlResult> DoHttpGetAsync(const std::string& url);
  
  // State
  std::atomic<int> pending_count_{0};
  std::atomic<int> success_count_{0};
  std::atomic<int> failed_count_{0};
  std::vector<std::string> target_urls_;
  GuiState* gui_state_{nullptr};
};

// Factory function
IGuiTask* CreateCoroCrawler();

