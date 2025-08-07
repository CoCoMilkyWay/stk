#pragma once

#include <cstdint>

namespace Table {

// because no need to dump whole snapshot table, we dont pack it tight in memory
struct Snapshot_Record {   // discrete or 3s fixed interval
  // timestamp ============================================
  uint16_t year;           // 2 bytes
  uint8_t month;           // 1 byte
  uint8_t day;             // 1 byte
  uint8_t hour;            // 1 byte
  uint8_t minute;          // 1 byte
  uint8_t second;          // 1 byte
  uint32_t seconds_in_day; // 4 bytes - no guarantee that every day start exactly at market open
  // LOB ==================================================
  float latest_price_tick;  // 4 bytes - price in RMB
  uint8_t trade_count;      // 1 byte
  uint16_t volume;          // 2 bytes - units of 100 shares
  uint32_t turnover;        // 4 bytes - RMB * 100
  float bid_price_ticks[5]; // 20 bytes - prices in RMB
  uint16_t bid_volumes[5];  // 10 bytes - units of 100 shares
  float ask_price_ticks[5]; // 20 bytes - prices in RMB
  uint16_t ask_volumes[5];  // 10 bytes - units of 100 shares
  uint8_t direction;        // 1 byte - 0: buy, 1: sell

  

                            // Total:  bytes
};

// low-freq data should be aligned to 32b boundary for better cache performance
#pragma pack(push, 1)
struct Bar_1m_Record {
  uint16_t year;  // 2 bytes
  uint8_t month;  // 1 byte
  uint8_t day;    // 1 byte
  uint8_t hour;   // 1 byte
  uint8_t minute; // 1 byte
  float open;     // 4 bytes
  float high;     // 4 bytes
  float low;      // 4 bytes
  float close;    // 4 bytes
  float volume;   // 4 bytes
  float turnover; // 4 bytes
                  // Total: 24 bytes
};
#pragma pack(pop)
} // namespace Table

// Data Struct
inline constexpr int BLen = 100;
inline constexpr int snapshot_interval = 3;
inline constexpr int trade_hrs_in_a_day = 5;
