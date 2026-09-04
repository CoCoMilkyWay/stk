#pragma once

// =============================================================================
// ToxicCr - 毒订单流撤单率: 短窗口撤单 / 长窗口撤单, 检测高频撤单
// =============================================================================
//   toxic_cr = Σ|O^C|_{t-5s}^{t} / Σ|O^C|_{t-60s}^{t}   (60 秒环形缓冲, 按秒推进)
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "features/DataDefine.hpp"

class ToxicCr {
  static constexpr size_t SHORT_WINDOW = 5; // 秒
  static constexpr size_t LONG_WINDOW = 60; // 秒

public:
  enum Out : size_t { value,
                      kCount };
  float y[kCount] = {};

  explicit ToxicCr(TickData &td) : td_(td) {}

  inline void compute() {
    const uint32_t cur_sec = td_.l0_index;

    // 按秒推进: 当前槽入账 → 写指针前移 → 移出长/短窗口尾部 → 清新槽
    while (last_sec_ < cur_sec) {
      short_sum_ += cancel_buffer_[write_idx_];
      long_sum_ += cancel_buffer_[write_idx_];
      write_idx_ = (write_idx_ + 1) % LONG_WINDOW;
      long_sum_ -= cancel_buffer_[write_idx_];
      short_sum_ -= cancel_buffer_[(write_idx_ + LONG_WINDOW - SHORT_WINDOW) % LONG_WINDOW];
      cancel_buffer_[write_idx_] = 0.0f;
      ++last_sec_;
    }

    if (td_.lob.order_type == L2::OrderType::CANCEL)
      cancel_buffer_[write_idx_] += static_cast<float>(td_.lob.volume);
  }

  // 分钟末结算 (含当前秒尚未入账的量)
  inline void flush() {
    float cur_short = short_sum_ + cancel_buffer_[write_idx_];
    float cur_long = long_sum_ + cancel_buffer_[write_idx_];
    y[value] = cur_long > 1e-6f ? cur_short / cur_long : 0.0f;
  }

  inline void reset() {
    for (size_t i = 0; i < LONG_WINDOW; ++i)
      cancel_buffer_[i] = 0.0f;
    write_idx_ = 0;
    last_sec_ = 0;
    short_sum_ = long_sum_ = 0.0f;
  }

private:
  TickData &td_;
  float cancel_buffer_[LONG_WINDOW] = {};
  size_t write_idx_ = 0;
  uint32_t last_sec_ = 0;
  float short_sum_ = 0.0f;
  float long_sum_ = 0.0f;
};

// ---- 节点实例 + 落盘列 (CMake 扫描汇总到 NodesGenerated.hpp, 格式见 FeaturesDefine.hpp) ----
#define NODE_ToxicCr(N) N(ToxicCr, (ToxicCr), (tick_data), onTick, onMinute)

#define FIELDS_L1_ToxicCr(X) \
  X(toxic_cr, ORDER_FLOW, RATIO, NONE, "Toxic Cancel Ratio", "毒订单流撤单率", "短窗口撤单占长窗口撤单比例,检测高频撤单行为(降频)", R"(\frac{\sum_{\tau=t-5\mathrm{s}}^{t}|O_{\tau}^{C}|}{\sum_{\tau=t-60\mathrm{s}}^{t}|O_{\tau}^{C}|}, \quad |O_{\tau}^{C}|=|O_{\tau}^{C,B}|+|O_{\tau}^{C,A}|)", OP(ToxicCr))
