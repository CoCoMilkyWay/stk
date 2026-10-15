#pragma once

#include "codec/OrderEncoder.hpp" // L2::Outcome
#include "codec/OrderFrame.hpp"
#include "misc/date.hpp"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

// ============================================================================
// 一天全市场的逐笔 — orders/YYYY/MM/YYYYMMDD.bin
// ============================================================================
//
// 一天一个文件: 当天每个 (资产, 当天) 一个独立的 zstd 帧, 加上当天的账目与目录.
//
//   [DayFileHeader][帧][帧]...[FrameRef × entry_count]
//
//   头: 定宽, 全 uint32 无填充 —— magic / 头长 / 目录条数 / 账目 (complete +
//       分母 + 各结局计数 + 判据命中数, 见 DayRecord). 头长即 sizeof, 判据列数
//       (kCheckBitCount) 一改头就变长, 对不上当场拒读.
//   帧: 见 codec/OrderFrame.hpp. 帧内 count 与目录项的 order_count 对账.
//   目录: 在尾部, 起点 = 文件长 − 条数 × 24, 按 asset_id 升序. 编码时各 worker
//       并发往数据区追加帧, 帧长要到收工才全知道, 所以目录与头最后写.
//
// 存在即完整: 写 .tmp, 收工 fdatasync 后 rename 到最终路径, 最终路径上不可能
// 是半截文件. 进程被杀只留下 .tmp, 下次编码同一天时截断重写.
//
// 两端互逆:
//
//   DayWriter::append(asset_id, Frame) → FrameRef     DayReader::read(FrameRef) → Frame
//   DayWriter::commit(DayIndex)                       DayReader::index() → DayIndex
//
// 目录键用 A 轴下标而非 "000001.SZ": 轴 append-only 且自校验 (见 AssetAxis),
// 下标恒定.

// 一个 (资产, 当天) 的帧在日文件里的位置 —— 日文件的目录项, 也是 OrdersCatalog
// 的格子.
//
// compressed_size 为 0 就是空: 在目录里是墓碑 (那天这个资产的源数据为空, 有结局
// 没数据, 其余字段皆 0), 在 OrdersCatalog 里还包括"没编过". 编出来的帧至少有一条
// 逐笔 (空的走墓碑, 见 OrderEncoder::finish), 所以这个判据不会误伤.
struct FrameRef {
  uint64_t offset = 0;          // 帧在日文件中的字节偏移
  uint32_t asset_id = 0;        // A 轴下标 (== Asset::items 下标)
  uint32_t order_count = 0;     // 帧内逐笔条数
  uint32_t compressed_size = 0; // 帧字节数
  uint32_t price_base = 0;      // LOB 档位窗口基准 (分), 见 L2_DataType.hpp 的 kPriceIndexRange

  bool empty() const {
    return compressed_size == 0;
  }
};
static_assert(sizeof(FrameRef) == 24, "日文件目录项必须紧凑无填充");

// 墓碑: 有结局没数据.
inline FrameRef make_tombstone(size_t asset_id) {
  assert(asset_id <= UINT32_MAX && "日文件目录: A 轴下标溢出 32 位");
  return {0, static_cast<uint32_t>(asset_id), 0, 0, 0};
}

// 一天的账目.
struct DayRecord {
  // 齐备 = 当天每个 (资产, 日期) 都有了结局: 落了帧或记了墓碑
  bool complete = false;

  // 分母: 当天归档里落在 A 轴上、且有逐笔委托文件的资产数
  size_t total = 0;

  // 按 L2::Outcome 分桶, 互斥, 加起来 ≤ total. Ok / Tombstone 与目录里的帧 / 墓碑
  // 条数严格相等 (读写两端都断言)
  size_t outcome[L2::kOutcomeCount] = {};

  // 按 L2::Check 的位记"命中这一条判据的标的数". 一个标的可能同时命中多条,
  // 所以这几列的和会大于 count(Invalid).
  size_t checks[L2::kCheckBitCount] = {};

  size_t count(L2::Outcome o) const {
    return outcome[static_cast<size_t>(o)];
  }

  // 记一笔结局. check_flags 只在 Invalid 时有意义 (L2::Check 的位).
  void tally(L2::Outcome o, uint32_t check_flags) {
    ++outcome[static_cast<size_t>(o)];
    if (o != L2::Outcome::Invalid)
      return;
    for (size_t bit = 0; bit < L2::kCheckBitCount; ++bit)
      if (check_flags & (1u << bit))
        ++checks[bit];
  }

  // 没留下结局的对数; 非零则当天不齐备
  size_t errors() const {
    size_t n = 0;
    for (size_t o = 0; o < L2::kOutcomeCount; ++o)
      if (!L2::settled(static_cast<L2::Outcome>(o)))
        n += outcome[o];
    return n;
  }
};

// 一天的账目 + 目录 —— 日文件除帧以外的全部内容.
struct DayIndex {
  DayRecord rec;
  std::vector<FrameRef> refs; // 帧 + 墓碑; 读出来按 asset_id 升序, 写端 commit 时排

  // 在升序目录里查; 没有返回 nullptr
  const FrameRef *find(size_t asset_id) const;
};

// 一天的日文件读端.
//
// 帧的位置/条数/档位基准都在 FrameRef 里, read 凭它 pread 一帧, 不再读任何头;
// index 只在要账目或目录时才读 (头 + 尾部目录, 一次 open 两次 pread).
// 一个 reader 一个 fd, 多线程各持一个并发 pread, 无锁.
class DayReader {
public:
  DayReader() = default;
  ~DayReader();

  DayReader(const DayReader &) = delete;
  DayReader &operator=(const DayReader &) = delete;

  // 文件不存在返回 false (那天从没编过). 与当前已开的是同一路径则什么都不做.
  bool open(const std::string &path);
  void close();

  // 存在但不自洽直接 assert —— 日文件只经 rename 整体落地, 不自洽只可能是盘坏或
  // 被外部改写.
  const DayIndex &index();

  // 返回的字节在内部缓冲里, 下一次 read / prefetch 前有效. ref 不能是空的.
  L2::Frame read(const FrameRef &ref);

  // 只为把 refs 的帧拖进 page cache, 内容即弃. 空项跳过, 首尾相接的帧并成一段
  // 顺序读. 返回读入字节数.
  size_t prefetch(std::span<const FrameRef> refs);

private:
  friend class DayWriter; // adopt 借用 fd 与缓冲整段搬帧

  std::string path_;
  int fd_ = -1;
  bool indexed_ = false;
  DayIndex index_;
  std::vector<char> buf_;
  std::vector<uint32_t> order_; // 按偏移排序的 refs 下标, 并段用
};

// 一天的日文件写端.
//
// 构造即建 <path>.tmp (目录不在就建), 数据区从头之后开始. 各 worker 拿到压好
// 的帧就 append —— fetch_add 占一段偏移再 pwrite, 全程无锁. 收工 commit 一次:
// 目录 + 头 + fdatasync + rename. 没 commit 就析构 (取消 / 放弃) 则删掉 .tmp,
// 最终路径上的旧文件原样不动.
class DayWriter {
public:
  explicit DayWriter(std::string path);
  ~DayWriter();

  DayWriter(const DayWriter &) = delete;
  DayWriter &operator=(const DayWriter &) = delete;

  // 线程安全.
  FrameRef append(size_t asset_id, const L2::Frame &frame);

  // 把 src (同一天的旧日文件) 里 refs 的帧原样搬进来 —— 免解压免重编, 并就地
  // 改写各项的 offset. 空项跳过. 增量补编时 producer 在推批之前调.
  void adopt(DayReader &src, std::vector<FrameRef> &refs);

  // 收工. index.refs 是当天全部结局 (帧 + 墓碑), 在这里按 asset_id 排序.
  void commit(DayIndex index);

private:
  uint64_t append_bytes(const void *data, size_t size);

  std::string path_;
  std::string tmp_path_;
  int fd_ = -1;
  std::atomic<uint64_t> end_;
};

// ============================================================================
// 全库帧目录 — [日][资产] 一格一个 FrameRef
// ============================================================================
//
// 扫描每天读一次日文件的账目 + 目录, 灌进当天那一行; 解码凭格子直接 read 一帧.
// 日主序: 下游 (特征计算 / 预读 / 覆盖统计) 全是按天推进、天内扫资产.
//
// 密集 = 日数 × 资产数 × 24B ≈ 400MB, 定址 O(1). 每次扫描整体重建, 中间不做增量
// 修补 —— 一次重建是几百次读日文件头 + 目录, 百毫秒量级. 扫描之后新上市的资产
// (AssetLoader 追加) 下标 ≥ width, at 对越界返回空格子.
struct OrdersCatalog {
  std::vector<misc::Ymd> days;    // 有日文件的天, 升序
  std::vector<DayRecord> records; // 每天的账目, 与 days 对齐
  size_t width = 0;               // 每行格数 = 扫描时的资产数
  std::vector<FrameRef> frames;   // days.size() × width

  static constexpr size_t kNoDay = SIZE_MAX;

  // 不存在返回 kNoDay (row / at 对它自然给空)
  size_t day_idx(misc::Ymd date) const;

  std::span<const FrameRef> row(size_t didx) const {
    if (didx >= days.size())
      return {};
    return {frames.data() + didx * width, width};
  }

  const FrameRef &at(size_t didx, size_t asset_id) const {
    static constexpr FrameRef kEmpty{};
    return didx < days.size() && asset_id < width ? frames[didx * width + asset_id] : kEmpty;
  }
};
