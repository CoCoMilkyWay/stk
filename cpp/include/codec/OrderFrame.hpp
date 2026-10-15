#pragma once

#include "L2_DataType.hpp"
#include <cstdint>
#include <vector>

// zstd 只在 OrderFrame.cpp 里展开, 头文件只认上下文的不完整类型
struct ZSTD_CCtx_s;
struct ZSTD_DCtx_s;

namespace L2 {

// ============================================================================
// 帧 — 一个 (资产, 当天) 的逐笔, 两种形态互逆
// ============================================================================
//
//   FrameView  解开的: Order × count, 已按 (时刻, 优先级, 源序) 排好, 直接喂 LOB
//   Frame      压好的: 一个 zstd 帧, 解开就是 Order × count
//
//   FrameEncoder::encode(FrameView) → Frame        FrameDecoder::decode(Frame) → FrameView
//
// 两侧都只碰内存, 不碰 fd —— 帧进出日文件归 DayWriter / DayReader (见
// shared/OrdersDay.hpp). FrameHead 随帧走: 编码侧来自准入校验, 解码侧来自日文件
// 目录, 交给 LOB 把绝对价折成档位下标 (见 L2_DataType.hpp 的 kPriceIndexRange).

// 帧的两项元数据, FrameView / Frame / FrameRef 共用同一份.
struct FrameHead {
  uint32_t count = 0;      // 逐笔条数
  uint32_t price_base = 0; // LOB 档位窗口基准 (分)
};

struct FrameView {
  const Order *orders = nullptr;
  FrameHead head;
};

struct Frame {
  const char *data = nullptr;
  uint32_t size = 0; // 压缩字节数
  FrameHead head;
};

// Zstandard 压缩级别 (离线编码): 6 在体积与编码速度之间取平衡
inline constexpr int ZSTD_COMPRESSION_LEVEL = 6;

// 编码侧. 帧内容校验 (xxh64) 压缩时顺手写入 (成本可忽略), 离线核查位腐烂靠它.
class FrameEncoder {
public:
  FrameEncoder();
  ~FrameEncoder();

  FrameEncoder(const FrameEncoder &) = delete;
  FrameEncoder &operator=(const FrameEncoder &) = delete;

  // 返回的字节在内部缓冲里, 下一次 encode 前有效. 压缩失败直接断言.
  Frame encode(const FrameView &view);

private:
  // 只增不缩: 一个 worker 顺序编成千上万个 (资产, 日期), 容量涨到峰值后不再 malloc
  std::vector<char> frame_; // 压好的帧
  ZSTD_CCtx_s *ctx_ = nullptr;
};

// 解码侧. 跳过帧内容校验: 日文件原子落盘 (tmp + rename), 目录自洽 + 解压尺寸与
// 目录条数对账已能挡住结构性损坏; 特征 / 因子计算高频解码, 每次再算 xxh64 是白花的.
class FrameDecoder {
public:
  FrameDecoder();
  ~FrameDecoder();

  FrameDecoder(const FrameDecoder &) = delete;
  FrameDecoder &operator=(const FrameDecoder &) = delete;

  // 压好的帧在内存里的落脚处: 日文件读端把 size 字节读进来, 再交给 decode. 与
  // FrameEncoder::encode 产出的帧字节对仗 —— 两侧的压缩字节都归编解码器所有, 跟着
  // 线程走, 不跟着日文件走. 下一次 frame_buffer 前有效.
  char *frame_buffer(size_t size);

  // 返回的逐笔在内部缓冲里, 下一次 decode 前有效. 帧与目录不符直接断言.
  FrameView decode(const Frame &frame);

private:
  // 只增不缩, 同 FrameEncoder
  std::vector<char> frame_;   // 压好的帧 (读端读进来)
  std::vector<Order> orders_; // 解开的逐笔
  ZSTD_DCtx_s *ctx_ = nullptr;
};

} // namespace L2
