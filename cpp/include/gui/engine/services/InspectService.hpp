// InspectService — 单因子深度展示与导出:
//   纯计算核: InspectFactor (整期一块: EvalStat + 一级 rIC/秩平面 + 分层/尾部/LS 日净值线 + 池等权线 + 可选前 N 只进出/快照 CSV)
//   薄线程壳: InspectService (FactorWorker<InspectService, InspectRequest>)
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/engine/Book.hpp"
#include "gui/engine/File.hpp"
#include "gui/engine/Worker.hpp"
#include "math/distribution/PdfSnap.hpp"
#include "shared/ReadRequest.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Engine {

enum class InspectExport : uint8_t { None,
                                     Log,
                                     Snap };
struct InspectSpec {
  FactorRef f;
  std::string row;
  InspectExport ex = InspectExport::None;
  int top_n = 0;
  std::string csv;
  std::vector<std::string> codes, names;
};
using InspectRequest = Request<InspectSpec>;

inline std::string inspect_key(FactorLevelId lv, const std::string &pool, int years, const std::string &row, const std::string &expr, factor::stat::Dir dir) {
  return std::string(factor_level(lv).name) + "|" + row + "|" + expr + (dir == factor::stat::kRev ? "|-" : "|+") + "|" + pool + "|" + std::to_string(years) + "y";
}
bool MakeInspectRequest(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, int years, const FactorRow &row, bool reload,
                        bool gpu, InspectExport ex, int top_n, InspectRequest &req);

inline constexpr int kNavLayers = factor::stat::kGroups + factor::stat::kEnds * kTails;
inline constexpr int kNavLs = 1 + kTails;
inline constexpr int kNavLines = 2 * (kNavLayers + kNavLs);
inline constexpr int layer_tail(int e, int j) { return factor::stat::kGroups + e * kTails + j; }

struct InspectPlanes {
  std::vector<float> ric;
  std::vector<uint16_t> rx;
  std::vector<uint16_t> rx_all;
  void clear() { ric.clear(), rx.clear(), rx_all.clear(); }
};

struct InspectResult : EvalStat {
  std::string key;
  std::string row, expr;
  FactorLevelId lv = 0;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  std::vector<misc::Ymd> dates;
  factor::stat::Holds hd;
  reader::ReadScope read_scope;
  BookLines pool_index;
  float pool_ann[factor::stat::kMaxHold] = {};
  InspectPlanes pl;
  BookLines nav;
  factor::stat::CostAcc ls_turn[kTails][factor::stat::kMaxHold] = {};
  AggPdf ic_agg[factor::stat::kMaxHold];
  AlphaDist alpha_dist;
  std::string error;

  const float *ric_at(int hi) const { return pl.ric.data() + static_cast<size_t>(hi) * scope.T; }
  const float *nav_at(int hi, int line) const { return nav.at(hi * kNavLines + line); }
  const factor::stat::CostAcc &ls_turn_of(int line, int hi) const { return line == 0 ? costs.front().acc[hi] : ls_turn[line - 1][hi]; }
};

struct InspectCache {
  InspectPlanes pl;
  BookLines pool;
  uint64_t pool_gen = 0;
};

// 单因子评估纯计算核 (须持 Lease): 结果填进 out (一级平面在 cache.pl, 调用方按需与结果交换以复用容量);
// on_ready 在结果就绪后、CSV 导出读价前回调 (GUI 可先发布再写 CSV)
Rc InspectFactor(const InspectRequest &req, FactorFlow &F, InspectCache &cache, InspectResult &out, std::string &export_msg,
                 const std::function<void(InspectResult &)> &on_ready = {});

class InspectService : public FactorWorker<InspectService, InspectRequest> {
  friend Service<InspectService, InspectRequest>;

public:
  static constexpr const char *kName = "InspectWorker";
  InspectService() : FactorWorker(Prio::Interactive) { Start(); }
  ~InspectService() { Stop(); }

  InspectResult result;
  uint64_t result_gen = 0;

private:
  static constexpr Phase kStart = Phase::Loading;
  bool run(InspectRequest &req);

  InspectCache cache_;
};

} // namespace GUI::Engine
