// SearchService — 因子自由参数搜索:
//   纯计算核: SearchFactor (低差异采样 → GP 曲面拟合 → 活跃峰自适应细化 → 非冗余显著集落盘 WriteFactorSearch, GUI 与 Agent 共用)
//   薄线程壳: SearchService (FactorWorker<SearchService, SearchRequest>)
#pragma once

#include "factor/Search/Contract.hpp"
#include "factor/Stat/Contract.hpp"
#include "gui/engine/File.hpp"
#include "gui/engine/Worker.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Engine {

struct SearchSpec {
  std::string file, tmpl;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  int hold = 0;
  bool fresh = false;
};
using SearchRequest = Request<SearchSpec>;
bool MakeSearchRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, int years, const FactorRow &row, int hold,
                       bool fresh, bool gpu, SearchRequest &req);

struct SearchCand {
  std::string expr;
  std::vector<double> u;
  int pass = 0;
  factor::search::Score sc;
};
struct SearchPick {
  std::string expr;
  factor::search::Score sc;
  double corr = 0.0;
};
struct SearchChunk {
  int days = 0;
  double mb = 0.0;
};
struct SearchResult {
  std::string file, tmpl;
  std::vector<std::string> slots;
  int hold = 0;
  int n_peak = 0;
  std::vector<SearchCand> cands;
  std::vector<SearchPick> picks;
  int n_pass = 0;
  int n_chunk = 0, chunk_days = 0, batch = 0;
  int n_assets = 0;
  std::vector<SearchChunk> chunk_plan;
  std::string backend;
  double seconds = 0.0;
  double n_full = 0.0;
  int n_weights = 0;
  bool wrote = false;
};

enum class SearchStage : uint8_t { Prepare,
                                   Design,
                                   Refine };

struct SearchLive {
  std::atomic<SearchStage> stage{SearchStage::Prepare};
  std::atomic<int> alive{0}, pass{0}, chunk{0}, chunks{0};
  void reset() {
    stage.store(SearchStage::Prepare, std::memory_order_relaxed);
    alive.store(0, std::memory_order_relaxed);
    pass.store(0, std::memory_order_relaxed);
    chunk.store(0, std::memory_order_relaxed);
    chunks.store(0, std::memory_order_relaxed);
  }
};

// 参数搜索纯计算核 (内部按趟获取 Lease, 趟间让位给交互服务):
// Ok = 搜索完成并落盘 (out.wrote = true, why = 完成说明); Empty / Broken = 正常输入放弃 (why = 原因, 文件不动); Cancel / Stale = 中断
Rc SearchFactor(const SearchRequest &req, FactorFlow &F, SearchResult &out, std::string &why, SearchLive *live = nullptr,
                const std::function<void(const SearchResult &)> &on_pass = {});

class SearchService : public FactorWorker<SearchService, SearchRequest> {
  friend Service<SearchService, SearchRequest>;

public:
  static constexpr const char *kName = "SearchWorker";
  SearchService() : FactorWorker(Prio::Batch) { Start(); }
  ~SearchService() { Stop(); }

  SearchStage stage() const { return live_.stage.load(std::memory_order_relaxed); }
  int alive() const { return live_.alive.load(std::memory_order_relaxed); }
  int pass() const { return live_.pass.load(std::memory_order_relaxed); }
  int chunk() const { return live_.chunk.load(std::memory_order_relaxed); }
  int chunks() const { return live_.chunks.load(std::memory_order_relaxed); }

  SearchResult result;

private:
  static constexpr Phase kStart = Phase::Loading;
  bool run(SearchRequest &req);

  SearchLive live_;
};

} // namespace GUI::Engine
