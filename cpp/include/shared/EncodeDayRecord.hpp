#pragma once

#include "codec/L2_Validator.hpp"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// ============================================================================
// 一天全市场的逐笔 — orders/YYYY/MM/YYYYMMDD.bin
// ============================================================================
//
// 一天一个文件: 当天每个 (资产, 当天) 一个独立的 zstd 帧, 加上当天的账目与目录.
//
//   [DayFileHeader][帧][帧]...[EncodeDayIndexEntry × entry_count]
//
//   头: 定宽, 全 uint32 无填充 —— magic / 头长 / 目录条数 / 账目 (complete +
//       处置分类 + 判据命中数, 见 EncodeDayRecord). 头长即 sizeof, 判据列数
//       (kCheckBitCount) 一改头就变长, 对不上当场拒读.
//   帧: 解开是 [u64 count][Order × count], count 与目录项的 order_count 对账.
//       帧内容校验 (xxh64) 压缩时写入, 热读路径跳过 (见 BinaryDecoder_L2).
//   目录: 在尾部, 起点 = 文件长 − 条数 × 24, 按 asset_id 升序. 编码时各 worker
//       并发往数据区追加帧, 帧长要到收工才全知道, 所以目录与头最后写.
//
// 存在即完整: 写 .tmp, 收工 fdatasync 后 rename 到最终路径, 最终路径上不可能
// 是半截文件. 进程被杀只留下 .tmp, 下次编码同一天时截断重写.
//
// 读侧: 扫描每天 open 一次读头 + 目录, 把每个资产的 (偏移, 帧长, 条数,
// price_base) 灌进 Asset::items[].date_info; 解码凭这四个数 pread 一帧, 不再读
// 任何头. 一天一个 fd, 多线程各持一个并发 pread, 无锁.
//
// 目录键用 A 轴下标而非 "000001.SZ": 轴 append-only 且自校验 (见 AssetAxis),
// 下标恒定.

// 一个 (资产, 当天) 的结局与帧位置.
//
// compressed_size 为 0 就是墓碑: 那天这个资产的源数据不足以编码 (停牌 / 文件
// 只有表头), 没有帧, 其余字段皆 0. 编出来的帧至少有一条逐笔 (空的走墓碑, 见
// BinaryEncoder_L2::finish_asset), 所以这个判据不会误伤.
struct EncodeDayIndexEntry {
  uint64_t offset;          // 帧在日文件中的字节偏移
  uint32_t asset_id;        // A 轴下标 (== Asset::items 下标)
  uint32_t order_count;     // 帧内逐笔条数
  uint32_t compressed_size; // 帧字节数
  uint32_t price_base;      // LOB 档位窗口基准 (分), 见 L2_DataType.hpp 的 kPriceIndexRange

  bool is_tombstone() const {
    return compressed_size == 0;
  }
};
static_assert(sizeof(EncodeDayIndexEntry) == 24, "日文件目录项必须紧凑无填充");

// size_t 进、定宽出, 收窄的边界检查只写这一份. 32 位的余量很宽: 最活跃的标的
// 单日逐笔是百万量级, 一帧是几 MB.
inline EncodeDayIndexEntry make_day_entry(size_t asset_id, uint64_t offset, size_t order_count,
                                          size_t compressed_size, uint32_t price_base) {
  assert(asset_id <= UINT32_MAX && "日文件目录: A 轴下标溢出 32 位");
  assert(order_count > 0 && order_count <= UINT32_MAX && "日文件目录: 单日逐笔条数为 0 或溢出 32 位");
  assert(compressed_size > 0 && compressed_size <= UINT32_MAX && "日文件目录: 帧长为 0 或溢出 32 位");
  return {offset, static_cast<uint32_t>(asset_id), static_cast<uint32_t>(order_count),
          static_cast<uint32_t>(compressed_size), price_base};
}

// 墓碑: 有结局没数据.
inline EncodeDayIndexEntry make_day_tombstone(size_t asset_id) {
  assert(asset_id <= UINT32_MAX && "日文件目录: A 轴下标溢出 32 位");
  return {0, static_cast<uint32_t>(asset_id), 0, 0, 0};
}

// 在按 asset_id 升序的目录里查; 没有返回 nullptr
const EncodeDayIndexEntry *find_day_entry(const std::vector<EncodeDayIndexEntry> &index, size_t asset_id);

struct EncodeDayRecord {
  // 齐备 = 当天每个 (资产, 日期) 都有了结局: 落了帧或记了墓碑
  bool complete = false;

  // 分母: 当天归档里落在 A 轴上、且有逐笔委托文件的资产数
  size_t assets_total = 0;

  // 处置分类, 互斥, 加起来 ≤ assets_total. ok / skipped 与目录里的帧 / 墓碑
  // 条数严格相等 (读写两端都断言)
  size_t assets_ok = 0;      // 落了帧
  size_t assets_skipped = 0; // 记了墓碑 (停牌 / 只有表头)
  size_t assets_corrupt = 0; // 源 CSV 坏行或归档流断
  size_t assets_invalid = 0; // 准入校验未过
  size_t assets_failed = 0;  // 环境错误 (压缩失败; 写盘失败直接 assert)

  // 按 L2::Check 的位记"命中这一条判据的标的数". 一个标的可能同时命中多条,
  // 所以这几列的和会大于 assets_invalid.
  size_t checks[L2::kCheckBitCount] = {};

  size_t assets_error() const {
    return assets_corrupt + assets_invalid + assets_failed;
  }
};

// 读日文件的账目 + 目录 (一次 open, 两次 pread). 文件不存在返回 false (那天
// 从没编过); 存在但不自洽直接 assert —— 日文件只经 rename 整体落地, 不自洽
// 只可能是盘坏或被外部改写.
bool read_day_file(const std::string &path, EncodeDayRecord &rec, std::vector<EncodeDayIndexEntry> &index);

// 一天的日文件写端.
//
// 构造即建 <path>.tmp (目录不在就建), 数据区从头之后开始. 各 worker 拿到压好
// 的帧就 append —— fetch_add 占一段偏移再 pwrite, 全程无锁. 收工 commit 一次:
// 目录 + 头 + fdatasync + rename. 没 commit 就析构 (取消 / 放弃) 则删掉 .tmp,
// 最终路径上的旧文件原样不动.
class DayFileWriter {
public:
  explicit DayFileWriter(std::string path);
  ~DayFileWriter();

  DayFileWriter(const DayFileWriter &) = delete;
  DayFileWriter &operator=(const DayFileWriter &) = delete;

  // 线程安全. 返回这段字节在日文件中的偏移.
  uint64_t append(const void *data, size_t size);

  // 把 src_path (同一天的旧日文件) 里 entries 的帧原样搬进来 —— 免解压免重编,
  // 并就地改写各项的 offset. 墓碑跳过. 增量补编时 producer 在推批之前调.
  void copy_frames(const std::string &src_path, std::vector<EncodeDayIndexEntry> &entries);

  // 收工. index 是当天全部结局 (帧 + 墓碑), 在这里按 asset_id 排序.
  void commit(const EncodeDayRecord &rec, std::vector<EncodeDayIndexEntry> index);

private:
  std::string path_;
  std::string tmp_path_;
  int fd_ = -1;
  std::atomic<uint64_t> end_;
};
