// RunService — 因子表扫描与分层回测:
//   纯计算核: ScanFactors (扫目录 + 跨行查重) / EvalFactorFile (单文件共享森林评估 + 回写 WriteFactorStats, GUI 与 Agent 共用同一份)
//   薄线程壳: RunService (FactorWorker<RunService, RunRequest>)
#pragma once

#include "gui/engine/File.hpp"
#include "gui/engine/Ledger.hpp"
#include "gui/engine/Table.hpp"
#include "gui/engine/Worker.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Engine {

struct RunSpec {
  bool evaluate = false; // false = 只扫描解析 (不读库)
  std::string only_file; // 非空 = 只评这个因子文件的权重; 空 = Run all
};
using RunRequest = Request<RunSpec>;
bool MakeRunRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, int years, bool evaluate, bool gpu, RunRequest &req);

// 扫因子目录: 每权重一行 (文件级问题 widx = −1) + 跨行查重 dup_of
void ScanFactors(const FactorScope &sc, std::vector<FactorRow> &out);

// 一个求值行在一个持有期上的逐日序列 (已按权重方向): 逐日 rIC + 超额分层 / LS 日净值线 + 各 LS 线年换手
struct DayLine {
  std::vector<float> ric;  // [日] 当日 ok 行 rIC 均值 (无 ok 行 = NaN)
  LedgerLines nav;         // [kNavExcess][日] 日收益 (线序见 Table.hpp kNavExcess)
  float turn[kNavLs] = {}; // LS 线年换手 (倍; 整组 / 尾部各该级两端)
};
struct DayLines {
  HoldId hold = kHoldT1;        // 调用方给: 持有期
  std::vector<misc::Ymd> dates; // 评估期交易日
  std::vector<DayLine> rows;    // 与 idx 同序 (算不出的行留空)
};

// 单文件评估 (须持 Lease): rows[idx] 同属一文件, 合成共享森林评全部持有期与成本段 → 填 EvalStat 或 error (status = Done) → Ok 时回写文件.
// on_plan / on_row 可选: 值域核完开跑前 / 每行收尾时回调; day 非空 = 另出 day->hold 上的逐日序列 (agent eval_factor)
Rc EvalFactorFile(const FactorScope &sc, FactorFlow &F, std::vector<FactorRow> &rows, std::span<const size_t> idx,
                  const std::function<void()> &on_plan = {}, const std::function<void(size_t)> &on_row = {}, DayLines *day = nullptr);

class RunService : public FactorWorker<RunService, RunRequest> {
  friend Service<RunService, RunRequest>;

public:
  static constexpr const char *kName = "RunWorker";
  RunService() : FactorWorker(Prio::Batch) { Start(); }
  ~RunService() { Stop(); }

  int broken() const { return broken_.load(std::memory_order_relaxed); }

  std::vector<FactorRow> rows;
  RunRequest current;

private:
  static constexpr Phase kStart = Phase::Scanning;
  bool run(RunRequest &req);

  std::atomic<int> broken_{0};
};

} // namespace GUI::Engine
