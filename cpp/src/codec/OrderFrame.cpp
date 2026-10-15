// 暴露 ZSTD_d_forceIgnoreChecksum (实验参数, 需静态链接宏) — 必须在任何
// zstd.h 展开之前定义, 所以放在本文件最顶部的 include 之前.
#define ZSTD_STATIC_LINKING_ONLY

#include "codec/OrderFrame.hpp"
#include "misc/profiler.hpp"
#include <cassert>
#include <cstring>

#include <zstd.h>

namespace L2 {

// ============================================================================
// FrameEncoder
// ============================================================================

FrameEncoder::FrameEncoder() {
  ctx_ = ZSTD_createCCtx();
  assert(ctx_ && "Failed to create ZSTD CCtx");
  ZSTD_CCtx_setParameter(ctx_, ZSTD_c_compressionLevel, ZSTD_COMPRESSION_LEVEL);
  ZSTD_CCtx_setParameter(ctx_, ZSTD_c_checksumFlag, 1);
}

FrameEncoder::~FrameEncoder() {
  ZSTD_freeCCtx(ctx_);
}

Frame FrameEncoder::encode(const FrameView &view) {
  assert(view.count > 0 && "encode: 空资产走墓碑, 没有帧");
  const uint64_t count = view.count;
  const size_t raw_size = frame_raw_size(view.count);

  if (raw_.size() < raw_size)
    raw_.resize(raw_size);
  std::memcpy(raw_.data(), &count, sizeof(count));
  std::memcpy(raw_.data() + sizeof(count), view.orders, view.count * sizeof(Order));

  const size_t bound = ZSTD_compressBound(raw_size);
  if (frame_.size() < bound)
    frame_.resize(bound);

  size_t size;
  {
    TraceN("ZstdCompress");
    size = ZSTD_compress2(ctx_, frame_.data(), bound, raw_.data(), raw_size);
  }
  assert(!ZSTD_isError(size) && "encode: 帧压缩失败");
  assert(size <= UINT32_MAX && "encode: 帧长溢出 32 位");

  return {frame_.data(), static_cast<uint32_t>(size), view.count, view.price_base};
}

// ============================================================================
// FrameDecoder
// ============================================================================

FrameDecoder::FrameDecoder() {
  ctx_ = ZSTD_createDCtx();
  assert(ctx_ && "Failed to create ZSTD DCtx");
  ZSTD_DCtx_setParameter(ctx_, ZSTD_d_forceIgnoreChecksum, ZSTD_d_ignoreChecksum);
}

FrameDecoder::~FrameDecoder() {
  ZSTD_freeDCtx(ctx_);
}

FrameView FrameDecoder::decode(const Frame &frame) {
  assert(frame.size > 0 && frame.order_count > 0 && "decode: 墓碑没有帧");
  const size_t raw_size = frame_raw_size(frame.order_count);

  if (raw_.size() < raw_size)
    raw_.resize(raw_size);

  size_t size;
  {
    TraceN("ZstdDecompress");
    size = ZSTD_decompressDCtx(ctx_, raw_.data(), raw_size, frame.data, frame.size);
  }
  assert(!ZSTD_isError(size) && size == raw_size && "decode: 帧解压失败或尺寸与目录不符");
  (void)size;

  uint64_t count;
  std::memcpy(&count, raw_.data(), sizeof(count));
  assert(count == frame.order_count && "decode: 帧内条数与目录不符");
  (void)count;

  return {reinterpret_cast<const Order *>(raw_.data() + sizeof(uint64_t)), frame.order_count, frame.price_base};
}

} // namespace L2
