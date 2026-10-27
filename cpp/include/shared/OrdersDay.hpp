#pragma once

#include "codec/L2_Validator.hpp" // L2::Outcome / L2::kCheckBitCount
#include "codec/OrderFrame.hpp"
#include "misc/date.hpp"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
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
//   头: 定宽, 全 uint32 无填充 —— magic / 头长 / 目录条数 / 账目 (DayRecord 整块
//       原样落盘). 头长即 sizeof, 结局数 (kOutcomeCount) 或判据列数
//       (kCheckBitCount) 一改头就变长, 对不上当场拒读.
//   帧: 见 codec/OrderFrame.hpp. 解开的字节数与目录项的 head.count 对账.
//   目录: 在尾部, 起点 = 文件长 − 条数 × 24, 按 asset_id 升序. 编码时各 worker
//       并发往数据区追加帧, 帧长要到收工才全知道, 所以目录与头最后写.
//
// 存在即完整: 写 .tmp, 收工 fdatasync 后 rename 到最终路径, 最终路径上不可能
// 是半截文件. 进程被杀只留下 .tmp, 下次编码同一天时截断重写.
//
// 两端互逆:
//
//   DayWriter(dir, date)                                   DayReader(dir, date)
//   DayWriter::put(asset_id, FrameEncoder, View) → Ref     DayReader::get(Ref, FrameDecoder) → View     一帧
//   DayWriter::adopt(DayReader, refs)                      DayReader::read_runs(refs, fn)               整段
//   DayWriter::commit(DayIndex)                            DayReader::index() → DayIndex                账目 + 目录
//   DayIndex::settle (写端记账)                             DayIndex::check (读写两端对账)
//
// DayReader::prefetch 不在表里: 只把帧交给内核预读, 字节不进用户态, 没有写端对端.
//
// 两端都是构造即定天、一天一个对象; 帧字节的缓冲归 FrameEncoder / FrameDecoder
// (跟线程走), 日文件对象本身不留跨天状态.
//
// 目录键用 A 轴下标而非 "000001.SZ": 轴 append-only 且自校验 (见 AssetAxis),
// 下标恒定.

// 一天的源与产物, 路径成对:
//   archive/YYYY/YYYYMM/YYYYMMDD.rar   一天的源 (编码读)
//   orders/YYYY/MM/YYYYMMDD.bin        一天的产物 (编码写, 特征读)
// orders_days 是 orders_day_path 的逆: 目录下全部日文件 (升序), 编码中的 .tmp 与
// 放错位置的文件不认.
std::string archive_day_path(const std::string &dir, misc::Ymd date);
std::string orders_day_path(const std::string &dir, misc::Ymd date);
std::vector<misc::Ymd> orders_days(const std::string &dir);

// 一个 (资产, 当天) 的帧在日文件里的位置 —— 日文件的目录项, 也是 OrdersCatalog
// 的格子. 与 L2::Frame 对仗: 内存指针换成文件偏移, 再带上目录键.
//
// 三态, 由 (size, offset) 判:
//   帧    size > 0
//   墓碑  size = 0, offset = 0         有结局没数据 (那天这个资产的源数据为空, 其余字段皆 0)
//   缺席  size = 0, offset = kAbsent   没有结局 (没编过 / Corrupt / Invalid / 不在包里)
// 目录里只有帧与墓碑; 缺席只出现在 OrdersCatalog 里 (默认构造即缺席). 编出来的帧
// 至少有一条逐笔 (空的走墓碑, 见 OrderEncoder::finish), 所以 size 判据不会误伤.
struct FrameRef {
  static constexpr uint64_t kAbsent = UINT64_MAX;

  uint64_t offset = kAbsent; // 帧在日文件中的字节偏移
  uint32_t asset_id = 0;     // A 轴下标 (== Asset::items 下标)
  uint32_t size = 0;         // 帧字节数
  L2::FrameHead head;        // 条数 + 价位基准, 同 L2::Frame::head

  // 没有帧 (墓碑或缺席)
  bool empty() const {
    return size == 0;
  }
  // 有结局没数据
  bool tombstone() const {
    return size == 0 && offset == 0;
  }
};
static_assert(sizeof(FrameRef) == 24, "日文件目录项必须紧凑无填充");

// 墓碑: 有结局没数据.
inline FrameRef make_tombstone(size_t asset_id) {
  assert(asset_id <= UINT32_MAX && "日文件目录: A 轴下标溢出 32 位");
  return {0, static_cast<uint32_t>(asset_id), 0, {}};
}

// 一天的账目 —— 全 uint32, 就是日文件头里的那一块, 读写整块拷贝.
struct DayRecord {
  // 齐备 (0 / 1) = 当天每个 (资产, 日期) 都有了结局: 落了帧或记了墓碑
  uint32_t complete = 0;

  // 分母: 当天归档里落在 A 轴上、且有逐笔委托文件的资产数
  uint32_t total = 0;

  // 按 L2::Outcome 分桶, 互斥, 加起来 ≤ total. Ok / Tombstone 与目录里的帧 / 墓碑
  // 条数严格相等 (读写两端都断言)
  uint32_t outcome[L2::kOutcomeCount] = {};

  // 按 L2::Check 的位记"命中这一条判据的资产数". 一个资产可能同时命中多条,
  // 所以这几列的和会大于 count(Invalid).
  uint32_t checks[L2::kCheckBitCount] = {};

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
static_assert(sizeof(DayRecord) == 4 * (2 + L2::kOutcomeCount + L2::kCheckBitCount), "日文件账目必须定宽无填充");

// 一天的账目 + 目录 —— 日文件除帧以外的全部内容.
struct DayIndex {
  DayRecord rec;
  std::vector<FrameRef> refs; // 帧 + 墓碑; 读出来按 asset_id 升序, 写端 commit 时排

  // 记一笔结局: 账目计数, 有结局的 (帧 / 墓碑) 同时进目录. 只有 Ok 带帧;
  // check_flags 只在 Invalid 时有意义 (L2::Check 的位).
  void settle(L2::Outcome o, uint32_t check_flags, const FrameRef &ref);

  // 目录结构自洽 + 与账目对账, data_end = 数据区末尾 (目录起点). 写端落盘前过
  // 一遍, 读端采信前再过一遍.
  void check(uint64_t data_end) const;

  // 在升序目录里查; 没有返回 nullptr
  const FrameRef *find(size_t asset_id) const;
};

// 一天的日文件读端.
//
// 构造即 open (同 DayWriter 构造即建 .tmp), 析构 close. 日文件必须存在 —— 调用方
// 凭帧目录 / 增量新鲜度判定确认过才来读, 打不开直接 assert.
//
// 帧的位置/条数/价位基准都在 FrameRef 里, get 凭它 pread 一帧, 不读任何头;
// index 只在要账目或目录时才读 (头 + 尾部目录, 两次 pread).
// 一个 reader 一个 fd, 多线程各持一个并发 pread, 无锁.
class DayReader {
public:
  DayReader(const std::string &dir, misc::Ymd date);
  ~DayReader();

  DayReader(const DayReader &) = delete;
  DayReader &operator=(const DayReader &) = delete;

  misc::Ymd date() const {
    return date_;
  }

  // 存在但不自洽直接 assert —— 日文件只经 rename 整体落地, 不自洽只可能是盘坏或
  // 被外部改写.
  const DayIndex &index();

  // 一帧 pread 进 dec 的帧缓冲 + 解码. 返回的逐笔在 dec 的缓冲里, 下一次
  // dec.decode 前有效 (与本 reader 的寿命无关). ref 必须有帧.
  L2::FrameView get(const FrameRef &ref, L2::FrameDecoder &dec);

  // 整段读: refs 的帧按偏移排好, 首尾相接的并成一段 (≤ 16MB) 一次 pread, 逐段
  // 回调 fn(段字节, 段长, 段起点偏移, 段内各项在 refs 里的下标). 空项跳过.
  // 字节在内部缓冲里, 只在回调期间有效. DayWriter::adopt 的对端.
  using RunFn = std::function<void(const char *bytes, size_t len, uint64_t begin, std::span<const uint32_t> run)>;
  void read_runs(std::span<const FrameRef> refs, const RunFn &fn);

  // 只为把 refs 的帧拖进 page cache: 按偏移升序交给内核预读, 不经用户态, 空项跳过.
  // 异步, 返回时 IO 未必读完. 返回下发预读的字节数.
  size_t prefetch(std::span<const FrameRef> refs);

private:
  // refs 里有帧的项的下标, 按偏移升序; 结果在 order_ 里, 下一次调用前有效
  std::span<const uint32_t> by_offset(std::span<const FrameRef> refs);

  misc::Ymd date_;
  int fd_;
  bool indexed_ = false;
  DayIndex index_;
  std::vector<char> buf_;       // 整段读的缓冲 (read_runs)
  std::vector<uint32_t> order_; // 按偏移排序的 refs 下标 (by_offset)
};

// 一天的日文件写端.
//
// 构造即建 <path>.tmp (目录不在就建), 数据区从头之后开始. 各 worker 编好帧就
// put —— fetch_add 占一段偏移再 pwrite, 全程无锁. 收工 commit 一次:
// 目录 + 头 + fdatasync + rename. 没 commit 就析构 (取消 / 放弃) 则删掉 .tmp,
// 最终路径上的旧文件原样不动.
class DayWriter {
public:
  DayWriter(const std::string &dir, misc::Ymd date);
  ~DayWriter();

  DayWriter(const DayWriter &) = delete;
  DayWriter &operator=(const DayWriter &) = delete;

  // 一帧编码 + 追加. enc 是调用方线程自己的; 追加线程安全.
  FrameRef put(size_t asset_id, L2::FrameEncoder &enc, const L2::FrameView &view);

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
// 扫描每天读一次日文件的账目 + 目录, 灌进当天那一行; 解码凭格子直接 get 一帧.
// 日主序: 下游 (特征计算 / 预读 / 覆盖统计 / 订单流重放) 全是按天推进、天内扫资产.
//
// 密集 = 日数 × 资产数 × 24B ≈ 400MB, 定址 O(1). 每次扫描整体重建成一份新的,
// 建好后只读, 以快照发布 (见 Asset::orders) —— 读者持有期间内容不变, 与重扫描
// 并发安全. 一次重建是几百次读日文件头 + 目录, 百毫秒量级. 扫描之后新上市的
// 资产 (AssetLoader 追加) 下标 ≥ width, at 对越界返回缺席格子.
//
// 格子三态 (见 FrameRef): 帧 / 墓碑 / 缺席 —— 与编码器的结局同源, 覆盖统计直接读.
struct OrdersCatalog {
  std::vector<misc::Ymd> days;    // 有日文件的天, 升序
  std::vector<DayRecord> records; // 每天的账目, 与 days 对齐
  size_t width = 0;               // 每行格数 = 扫描时的资产数
  std::vector<FrameRef> frames;   // days.size() × width

  static constexpr size_t kNoDay = SIZE_MAX;

  OrdersCatalog() = default;

  // 定形: 行 = days, 每行 width 格; 格子全缺席, 账目全零
  OrdersCatalog(std::vector<misc::Ymd> days, size_t width);

  // 读第 didx 天的日文件账目 + 目录, 灌进那一行. 行与行不相交, 不同 didx 可并发.
  void fill_day(size_t didx, const std::string &dir);

  // 不存在返回 kNoDay (row / at 对它自然给空)
  size_t day_idx(misc::Ymd date) const;

  // 切片: 行 = dates (升序; 本目录没有的天给缺席行), 列 = ids (A 轴下标, 越界给缺席格子).
  // 特征计算按 回测日 × 库轴 开算, worker 凭 (日期轴下标, 库轴下标) 直接定址.
  OrdersCatalog slice(std::span<const misc::Ymd> dates, std::span<const uint32_t> ids) const;

  std::span<const FrameRef> row(size_t didx) const {
    if (didx >= days.size())
      return {};
    return {frames.data() + didx * width, width};
  }

  const FrameRef &at(size_t didx, size_t asset_id) const {
    static constexpr FrameRef kAbsentCell{};
    return didx < days.size() && asset_id < width ? frames[didx * width + asset_id] : kAbsentCell;
  }
};
