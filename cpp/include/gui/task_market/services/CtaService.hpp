// CtaService — Market→CTA 页的单 worker: 选中的一个标的 (篮子线 / 个股) 的 1 分钟后复权价 → 单资产 中线 + 分层 模型的滚动调参全期轨迹 (cta/Trace.hpp)
//   价格源 = MinuteLib (与 MinuteService 同一段开场 open, 同一种沿用 Kept); 有效根序列按标的留最近 kKeep 个 (只改设定不重读)
//   请求 = 标的 + CtaSpec (无标的 = 只备价格库); 新请求覆盖旧的 (换标的 / 改设定即重算). 进度: Loading = 合成天, Running = 选参日数
// 线程模型 (gui/util/Service.hpp, 随对象): UI 持 mutex 读 result (latest)
#pragma once

#include "cta/Trace.hpp"
#include "gui/quote/Quote.hpp" // Side
#include "gui/task_market/services/MinuteLib.hpp"
#include "gui/util/Service.hpp"
#include "misc/exec.hpp"
#include "shared/Progress.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Market {

using namespace Quote;

// 模型与回测设定 (UI 编辑, 进请求)
struct CtaSpec {
  size_t goal = cta::kCapture;                // 优化目标 (cta::kGoals 下标)
  size_t kind = cta::kEma;                    // 中线模型 (cta::kCenters 下标)
  int dir = cta::kLong;                       // 方向 (cta::kDirs 下标)
  int roll = static_cast<int>(cta::kMinRoll); // 滚动窗 (日, ≥ kMinRoll)
  int grid = 9;                               // 网格配置数上限 (各多值轴轮流分格点)
  float commission_bp = 1;                    // 佣金率 [bp] (买卖各一次)
  float stamp_bp = 5;                         // 印花税率 [bp] (卖出单边)
  float impact_bp = 5;                        // 冲击率 [bp] (买卖各一次)
  float vol = 0;                              // 目标年化波动 (0 = 不调)
  float buffer = 0;                           // 不交易带宽
  bool operator==(const CtaSpec &) const = default;
};

struct CtaRequest {
  uint32_t gen = 0;
  Quote::Ref ref; // 标的; 无 = 只备价格库
  CtaSpec spec;
  bool operator==(const CtaRequest &) const = default;
};

struct CtaResult {
  uint32_t gen = 0; // = 请求的
  Quote::Ref ref;
  CtaSpec spec;
  std::string lack;                      // 算不了的原因 (空 = 有轨迹)
  std::vector<misc::Ymd> dates;          // 日轴: 有效根的日下标 = t / kSlots
  std::shared_ptr<const cta::Series> se; // 有效根
  cta::Track tr;                         // [有效根] 轨迹 (策略 / 持有两本账)
};

class CtaService : public Service<CtaService, CtaRequest>, public Published {
  friend Service<CtaService, CtaRequest>;

public:
  static constexpr const char *kName = "CtaWorker";
  explicit CtaService(MinuteLib &lib) : lib_(lib) { Start(); }
  ~CtaService() { Stop(); }

  std::shared_ptr<const CtaResult> result; // UI 持 mutex 读; 空 = 未算

private:
  static constexpr Phase kStart = Phase::Loading;
  static constexpr size_t kKeep = 4; // 留在内存的标的序列数
  static constexpr int kThreads = 8; // 合成 / 网格线程 (合成: 每线程一日的分钟读 + 篮子累加 + 个股原价, 数十 MB; 网格: 每线程一个配置的几条根列)
  Progress &board() { return *this; }
  bool run(CtaRequest &req);
  bool publish(const CtaRequest &req, std::string lack, std::shared_ptr<CtaResult> out = nullptr);

  MinuteLib &lib_;
  misc::exec::ForkJoin pool_{misc::exec::fit(kThreads)};
  Kept<cta::Series> kept_{kKeep};
};

} // namespace GUI::Market