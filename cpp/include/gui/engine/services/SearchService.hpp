// SearchService — 因子自由参数搜索:
//   纯计算核: SearchFactor (低差异采样 → 一趟分块评完全部点 → 逐点中位窗 mono (Stat/Roll.hpp) 定向打分 → 最高的一个落盘 WriteFactorSearch, GUI 与 Agent 共用)
//   薄线程壳: SearchService (FactorWorker<SearchService, SearchRequest>)
#pragma once

#include "factor/Stat/Contract.hpp"
#include "factor/Stat/Roll.hpp"
#include "gui/engine/File.hpp"
#include "gui/engine/Worker.hpp"
#include "misc/date.hpp"

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
  factor::stat::Ret ret = factor::stat::Ret::Excess; // 因子文件 type
  HoldId hold = kHoldT1;
  bool fresh = false;
};
using SearchRequest = Request<SearchSpec>;
bool MakeSearchRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, int years, const FactorRow &row, HoldId hold,
                       bool fresh, bool gpu, SearchRequest &req);

// 一个评过的参数点 (已按所评方向; Search 契约【目标】【方向】)
struct SearchCand {
  std::string expr;                       // 具体规范串 (不含 sign)
  std::vector<double> v;                  // 各槽参数值 (SearchResult::slots 同序; 整数槽取整, k / w 为规范串往返值)
  int sign = 1;                           // +1 正向 / −1 取反
  factor::stat::Roll roll;                // 滚动窗统计 (score = 中位窗 mono)
  std::vector<float> line;                // [日] 窗 mono 线 (SearchResult::dates 同长; 不计入的窗 NaN)
  double mono = 0.0, ls = 0.0, ric = 0.0; // 全期: 分层单调性 / 每期真实毛多空 / rIC 均值 (只展示)
};
struct SearchChunk {
  int days = 0;
  double mb = 0.0;
};
struct SearchResult {
  std::string file, tmpl;
  std::vector<std::string> slots;
  HoldId hold = kHoldT1;
  std::vector<misc::Ymd> dates;  // 评估期交易日
  std::vector<SearchCand> cands; // 评过的点, score 降序 (无窗的在末)
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
                                   Eval };

struct SearchLive {
  std::atomic<SearchStage> stage{SearchStage::Prepare};
  std::atomic<int> chunk{0}, chunks{0};
  void reset() {
    stage.store(SearchStage::Prepare, std::memory_order_relaxed);
    chunk.store(0, std::memory_order_relaxed);
    chunks.store(0, std::memory_order_relaxed);
  }
};

// 参数搜索纯计算核 (内部获取 Lease, 让位给交互服务):
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
  int chunk() const { return live_.chunk.load(std::memory_order_relaxed); }
  int chunks() const { return live_.chunks.load(std::memory_order_relaxed); }

  SearchResult result;

private:
  static constexpr Phase kStart = Phase::Loading;
  bool run(SearchRequest &req);

  SearchLive live_;
};

} // namespace GUI::Engine
