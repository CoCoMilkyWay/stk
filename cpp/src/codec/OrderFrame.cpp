// 暴露 ZSTD_d_forceIgnoreChecksum (实验参数, 需静态链接宏) — 必须在任何
// zstd.h 展开之前定义, 所以放在本文件最顶部的 include 之前.
#define ZSTD_STATIC_LINKING_ONLY

#include "codec/OrderFrame.hpp"
#include "misc/profiler.hpp"
#include <cassert>

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
  assert(view.head.count > 0 && "encode: 空资产走墓碑, 没有帧");
  const size_t raw_size = size_t(view.head.count) * sizeof(Order);

  const size_t bound = ZSTD_compressBound(raw_size);
  if (frame_.size() < bound)
    frame_.resize(bound);

  size_t size;
  {
    TraceN("ZstdCompress");
    size = ZSTD_compress2(ctx_, frame_.data(), bound, view.orders, raw_size);
  }
  assert(!ZSTD_isError(size) && "encode: 帧压缩失败");
  assert(size <= UINT32_MAX && "encode: 帧长溢出 32 位");

  return {frame_.data(), static_cast<uint32_t>(size), view.head};
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
  assert(frame.size > 0 && frame.head.count > 0 && "decode: 墓碑没有帧");
  const size_t raw_size = size_t(frame.head.count) * sizeof(Order);

  if (orders_.size() < frame.head.count)
    orders_.resize(frame.head.count);

  size_t size;
  {
    TraceN("ZstdDecompress");
    size = ZSTD_decompressDCtx(ctx_, orders_.data(), raw_size, frame.data, frame.size);
  }
  assert(!ZSTD_isError(size) && size == raw_size && "decode: 帧解压失败或条数与目录不符");
  (void)size;

  return {orders_.data(), frame.head};
}

} // namespace L2
