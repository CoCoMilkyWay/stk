// CtaService — Market→CTA 页的单 worker: 选中的一个标的 (篮子线 / 个股) 的 1 分钟后复权价 → 单资产 中线 + 分层 模型的滚动调参全期轨迹 (cta/Trace.hpp)
//   价格源 = 价格库发布的 PriceView (QuoteHub 的): 请求带该份的日脊 + 标的的分钟价 (不可变, 只拷指针) + 戳;
//     有效根序列按 (标的, 戳) 留最近 kKeep 个 (只改设定不重算)
//   请求 = 标的 + cta::Spec + 价; 新请求覆盖旧的 (换标的 / 改设定 / 库变即重算). 进度: Running = 选参日数
// 线程模型 (gui/util/Service.hpp, 随对象): UI 持 mutex 读 result (latest)
#pragma once

#include "cta/Trace.hpp"
#include "gui/quote/PriceLib.hpp"
#include "gui/quote/Quote.hpp" // Side
#include "gui/util/Service.hpp"
#include "misc/exec.hpp"
#include "shared/Progress.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace GUI::Market {

using namespace Quote;

struct CtaRequest {
  uint32_t gen = 0;
  Quote::Ref ref;                                      // 标的
  cta::Spec spec;                                      // 模型与回测设定 (UI 编辑)
  cta::Rates rates;                                    // 成本率 = 系统配置 (Config::commission / stamp / fixed_impact)
  Quote::PriceStamp stamp;                             // 价格库发布的戳
  std::shared_ptr<const std::vector<misc::Ymd>> dates; // 该戳的日脊
  std::shared_ptr<const Quote::Series> series;         // 标的在该戳的全史 (连分钟)
  bool operator==(const CtaRequest &) const = default;
};

struct CtaResult {
  uint32_t gen = 0; // = 请求的
  Quote::Ref ref;
  cta::Spec spec;
  std::string lack;                      // 算不了的原因 (空 = 有轨迹)
  std::vector<misc::Ymd> dates;          // 日轴: 有效根的日下标 = t / kSlots
  std::shared_ptr<const cta::Series> se; // 有效根
  cta::Track tr;                         // [有效根] 轨迹 (策略 / 持有两本账)
};

class CtaService : public Service<CtaService, CtaRequest>, public Published {
  friend Service<CtaService, CtaRequest>;

public:
  static constexpr const char *kName = "CtaWorker";
  CtaService() { Start(); }
  ~CtaService() { Stop(); }

  std::shared_ptr<const CtaResult> result; // UI 持 mutex 读; 空 = 未算

private:
  static constexpr Phase kStart = Phase::Running;
  static constexpr size_t kKeep = 4; // 留在内存的标的序列数
  static constexpr int kThreads = 8; // 网格线程 (一个配置一个任务)
  Progress &board() { return *this; }
  bool run(CtaRequest &req);
  bool publish(const CtaRequest &req, std::string lack, std::shared_ptr<CtaResult> out = nullptr);

  misc::exec::ForkJoin pool_{misc::exec::fit(kThreads)};
  Kept<cta::Series> kept_{kKeep};
};

} // namespace GUI::Market