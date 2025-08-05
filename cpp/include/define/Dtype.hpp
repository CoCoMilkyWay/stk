#pragma once

#include <cstdint>

namespace Table {
#pragma pack(push, 1)
struct Snapshot_3s_Record {
  uint32_t index_1m;          // 1 byte - index to corresponding 1m bar table (20years of data)
  uint8_t seconds;            // 1 byte - seconds in minute
  int16_t latest_price_tick;  // 2 bytes - price * 100
  uint8_t trade_count;        // 1 byte
  uint32_t turnover;          // 4 bytes - RMB * 100
  uint16_t volume;            // 2 bytes - units of 100 shares
  int16_t bid_price_ticks[5]; // 10 bytes - prices * 100
  uint16_t bid_volumes[5];    // 10 bytes - units of 100 shares
  int16_t ask_price_ticks[5]; // 10 bytes - prices * 100
  uint16_t ask_volumes[5];    // 10 bytes - units of 100 shares
  uint8_t direction;          // 1 byte
                              // Total: 54 bytes
};
#pragma pack(pop)

// low-freq data should be aligned to 32b boundary for better cache performance
#pragma pack(push, 1)
struct Bar_1m_Record {
  uint16_t year;              // 2 bytes
  uint8_t month;              // 1 byte
  uint8_t day;                // 1 byte
  uint8_t hour;               // 1 byte
  uint8_t minute;             // 1 byte
  float open;                 // 4 bytes
  float high;                 // 4 bytes
  float low;                  // 4 bytes
  float close;                // 4 bytes
  float volume;               // 4 bytes
  float turnover;             // 4 bytes
                              // Total: 24 bytes
};
#pragma pack(pop)
} // namespace Table

// Data Struct
inline constexpr int BLen = 100;
inline constexpr int snapshot_interval = 3;
inline constexpr int trade_hrs_in_a_day = 5;
