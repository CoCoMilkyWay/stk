#pragma once

#include <cstdint>

//========================================================================================
// TIME-BASED OHLCV BAR RESAMPLER
//========================================================================================
// Accumulates tick-by-tick data into OHLCV bars for hierarchical time resampling
// Key features:
// - Minimal interface: reset() + update() only
// - Direct state access via public members (zero-overhead)
// - Cache-friendly layout (64 bytes, single cache line)
// - Hot-path optimized for tick->minute->hour aggregation
//========================================================================================

template <typename PriceType = double, typename VolumeType = uint64_t>
class ResampleTimeBar {
public:
  // State: OHLCV accumulator (public for zero-overhead access)
  PriceType open{0};
  PriceType high{0};
  PriceType low{0};
  VolumeType volume{0};
  double sum_price_volume{0};

  // Reset with initial price (called at bar boundary)
  [[gnu::hot, gnu::always_inline]] inline void reset(PriceType price) noexcept {
    open = high = low = price;
    volume = 0;
    sum_price_volume = 0;
  }

  // Update with new tick (called for each trade)
  [[gnu::hot, gnu::always_inline]] inline void update(PriceType price, VolumeType vol) noexcept {
    // First tick after reset sets open
    if (volume == 0) [[unlikely]]
      open = price;

    // Update high/low
    if (price > high)
      high = price;
    if (price < low)
      low = price;

    // Accumulate volume-weighted price
    sum_price_volume += static_cast<double>(price) * static_cast<double>(vol);
    volume += vol;
  }

  // VWAP (volume-weighted average price)
  [[nodiscard, gnu::hot, gnu::always_inline]] inline PriceType vwap() const noexcept {
    return volume > 0 ? static_cast<PriceType>(sum_price_volume / volume) : open;
  }
};

