#include "shared/OrdersDay.hpp"
#include "misc/cross_platform.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>

namespace {

// 全 uint32 排列, 没有填充, 结构体整块读写. 字段顺序即盘上顺序 (merge_orders.py
// 按同一布局写); outcome 的下标即 L2::Outcome.
struct DayFileHeader {
  static constexpr uint32_t kMagic = 0x444F324C; // 'L','2','O','D' 小端

  uint32_t magic;
  uint32_t header_size; // = sizeof(DayFileHeader)
  uint32_t entry_count;
  uint32_t complete; // 0 / 1
  uint32_t total;
  uint32_t outcome[L2::kOutcomeCount];
  uint32_t checks[L2::kCheckBitCount];
};
static_assert(sizeof(DayFileHeader) == 4 * (5 + L2::kOutcomeCount + L2::kCheckBitCount), "日文件头必须定宽无填充");

// 偏移首尾相接的一串帧并成一段整块读写, 一段最多这么大
constexpr size_t kRunCap = size_t(16) << 20;

uint32_t narrow(size_t v) {
  assert(v <= UINT32_MAX && "日文件账目: 计数溢出 32 位");
  return static_cast<uint32_t>(v);
}

// refs 里的帧按偏移排好, 首尾相接的并成一段 (≤ kRunCap), 逐段回调
// fn(段起点, 段长, 段内各项在 refs 里的下标). 空项跳过. order 是调用方复用的缓冲.
template <typename Fn>
void for_each_run(std::span<const FrameRef> refs, std::vector<uint32_t> &order, Fn fn) {
  order.clear();
  for (size_t i = 0; i < refs.size(); ++i)
    if (!refs[i].empty())
      order.push_back(static_cast<uint32_t>(i));
  std::sort(order.begin(), order.end(),
            [&refs](uint32_t a, uint32_t b) { return refs[a].offset < refs[b].offset; });

  for (size_t i = 0; i < order.size();) {
    const uint64_t run_begin = refs[order[i]].offset;
    uint64_t run_end = run_begin + refs[order[i]].compressed_size;
    size_t j = i + 1;
    while (j < order.size() && refs[order[j]].offset == run_end &&
           run_end - run_begin + refs[order[j]].compressed_size <= kRunCap)
      run_end += refs[order[j++]].compressed_size;
    fn(run_begin, static_cast<size_t>(run_end - run_begin), std::span<const uint32_t>(order.data() + i, j - i));
    i = j;
  }
}

// 目录的结构自洽 + 与账目对账. 读写两端共用: 写端在落盘前过一遍, 读端在
// 采信前再过一遍.
void check_index(const DayIndex &index, uint64_t data_end) {
  const std::vector<FrameRef> &refs = index.refs;
  const DayRecord &rec = index.rec;
  size_t frames = 0;
  for (size_t i = 0; i < refs.size(); ++i) {
    const FrameRef &e = refs[i];
    assert((i == 0 || refs[i - 1].asset_id < e.asset_id) && "日文件目录: 未按 asset_id 严格升序 (同一资产两条?)");
    if (e.empty()) {
      assert(e.offset == 0 && e.order_count == 0 && e.price_base == 0 && "日文件目录: 墓碑带了数据");
      continue;
    }
    assert(e.order_count > 0 && "日文件目录: 帧的条数为 0");
    assert(e.offset >= sizeof(DayFileHeader) && e.offset + e.compressed_size <= data_end &&
           "日文件目录: 帧越出数据区");
    ++frames;
  }
  assert(rec.count(L2::Outcome::Ok) == frames && rec.count(L2::Outcome::Tombstone) == refs.size() - frames &&
         "日文件账目: Ok / Tombstone 与目录里的帧 / 墓碑条数不符");
  size_t tallied = 0;
  for (size_t o = 0; o < L2::kOutcomeCount; ++o)
    tallied += rec.outcome[o];
  assert(tallied <= rec.total && "日文件账目: 各结局之和超过分母");
  (void)frames;
  (void)tallied;
  (void)data_end;
}

} // namespace

// ============================================================================
// DayIndex
// ============================================================================

const FrameRef *DayIndex::find(size_t asset_id) const {
  const auto it = std::lower_bound(refs.begin(), refs.end(), asset_id,
                                   [](const FrameRef &e, size_t id) { return e.asset_id < id; });
  return it != refs.end() && it->asset_id == asset_id ? &*it : nullptr;
}

// ============================================================================
// DayReader
// ============================================================================

DayReader::~DayReader() {
  close();
}

bool DayReader::open(const std::string &path) {
  if (fd_ >= 0 && path == path_)
    return true;
  close();
  fd_ = file_open_read(path.c_str());
  if (fd_ < 0)
    return false;
  path_ = path;
  return true;
}

void DayReader::close() {
  if (fd_ >= 0)
    file_close(fd_);
  fd_ = -1;
  path_.clear();
  indexed_ = false;
}

const DayIndex &DayReader::index() {
  assert(fd_ >= 0 && "DayReader::index: 未 open");
  if (indexed_)
    return index_;

  const uint64_t size = file_size(fd_);
  DayFileHeader head{};
  assert(size >= sizeof(head) && "日文件短于头 (损坏)");
  const size_t head_got = file_pread(fd_, &head, sizeof(head), 0);
  assert(head_got == sizeof(head) && "日文件头读不全");
  (void)head_got;
  assert(head.magic == DayFileHeader::kMagic && "不是 orders 日文件 (magic 不符)");
  assert(head.header_size == sizeof(DayFileHeader) && "日文件头长不符 (判据列数 kCheckBitCount 变了?)");
  assert(head.complete <= 1 && "日文件头: complete 非 0/1");

  const uint64_t refs_bytes = uint64_t(head.entry_count) * sizeof(FrameRef);
  assert(size >= sizeof(head) + refs_bytes && "日文件目录越界 (截断?)");
  const uint64_t refs_offset = size - refs_bytes;

  index_.refs.resize(head.entry_count);
  const size_t refs_got = file_pread(fd_, index_.refs.data(), refs_bytes, refs_offset);
  assert(refs_got == refs_bytes && "日文件目录读不全");
  (void)refs_got;

  DayRecord &rec = index_.rec;
  rec = DayRecord{};
  rec.complete = head.complete != 0;
  rec.total = head.total;
  for (size_t o = 0; o < L2::kOutcomeCount; ++o)
    rec.outcome[o] = head.outcome[o];
  for (size_t bit = 0; bit < L2::kCheckBitCount; ++bit)
    rec.checks[bit] = head.checks[bit];

  check_index(index_, refs_offset);
  indexed_ = true;
  return index_;
}

L2::Frame DayReader::read(const FrameRef &ref) {
  assert(fd_ >= 0 && "DayReader::read: 未 open");
  assert(!ref.empty() && "DayReader::read: 空格子没有帧");
  if (buf_.size() < ref.compressed_size)
    buf_.resize(ref.compressed_size);
  const size_t got = file_pread(fd_, buf_.data(), ref.compressed_size, ref.offset);
  assert(got == ref.compressed_size && "DayReader::read: 帧越过日文件末尾 (目录与文件不符)");
  (void)got;
  return {buf_.data(), ref.compressed_size, ref.order_count, ref.price_base};
}

size_t DayReader::prefetch(std::span<const FrameRef> refs) {
  assert(fd_ >= 0 && "DayReader::prefetch: 未 open");
  size_t bytes = 0;
  for_each_run(refs, order_, [&](uint64_t begin, size_t len, std::span<const uint32_t>) {
    if (buf_.size() < len)
      buf_.resize(len);
    const size_t got = file_pread(fd_, buf_.data(), len, begin);
    assert(got == len && "DayReader::prefetch: 帧越过日文件末尾 (目录与文件不符)");
    bytes += got;
  });
  return bytes;
}

// ============================================================================
// DayWriter
// ============================================================================

DayWriter::DayWriter(std::string path)
    : path_(std::move(path)), tmp_path_(path_ + ".tmp"), end_(sizeof(DayFileHeader)) {
  std::filesystem::create_directories(std::filesystem::path(path_).parent_path());
  fd_ = file_open_trunc(tmp_path_.c_str());
  assert(fd_ >= 0 && "日文件 .tmp 建不出来");
}

DayWriter::~DayWriter() {
  if (fd_ < 0)
    return; // 已 commit
  file_close(fd_);
  std::error_code ec;
  std::filesystem::remove(tmp_path_, ec);
}

uint64_t DayWriter::append_bytes(const void *data, size_t size) {
  assert(fd_ >= 0 && "日文件已 commit, 不能再追加");
  const uint64_t off = end_.fetch_add(size, std::memory_order_relaxed);
  const size_t written = file_pwrite(fd_, data, size, off);
  assert(written == size && "日文件写入失败 (磁盘满?)");
  (void)written;
  return off;
}

// 32 位的余量很宽: 最活跃的标的单日逐笔是百万量级, 一帧是几 MB.
FrameRef DayWriter::append(size_t asset_id, const L2::Frame &frame) {
  assert(asset_id <= UINT32_MAX && "日文件目录: A 轴下标溢出 32 位");
  assert(frame.size > 0 && frame.order_count > 0 && "DayWriter::append: 空帧走墓碑");
  const uint64_t offset = append_bytes(frame.data, frame.size);
  return {offset, static_cast<uint32_t>(asset_id), frame.order_count, frame.size, frame.price_base};
}

void DayWriter::adopt(DayReader &src, std::vector<FrameRef> &refs) {
  for_each_run(refs, src.order_, [&](uint64_t begin, size_t len, std::span<const uint32_t> run) {
    assert(src.fd_ >= 0 && "DayWriter::adopt: 旧日文件未 open");
    if (src.buf_.size() < len)
      src.buf_.resize(len);
    const size_t got = file_pread(src.fd_, src.buf_.data(), len, begin);
    assert(got == len && "增量补编: 旧日文件的帧读不全");
    (void)got;
    const uint64_t base = append_bytes(src.buf_.data(), len);
    for (const uint32_t k : run)
      refs[k].offset = base + (refs[k].offset - begin);
  });
}

void DayWriter::commit(DayIndex index) {
  assert(fd_ >= 0 && "日文件 commit 两次");
  std::vector<FrameRef> &refs = index.refs;
  const DayRecord &rec = index.rec;
  std::sort(refs.begin(), refs.end(),
            [](const FrameRef &a, const FrameRef &b) { return a.asset_id < b.asset_id; });

  const uint64_t refs_offset = end_.load(std::memory_order_relaxed);
  check_index(index, refs_offset);

  const size_t refs_bytes = refs.size() * sizeof(FrameRef);
  const size_t refs_written = file_pwrite(fd_, refs.data(), refs_bytes, refs_offset);
  assert(refs_written == refs_bytes && "日文件目录写入失败 (磁盘满?)");
  (void)refs_written;

  DayFileHeader head{};
  head.magic = DayFileHeader::kMagic;
  head.header_size = sizeof(DayFileHeader);
  head.entry_count = narrow(refs.size());
  head.complete = rec.complete ? 1 : 0;
  head.total = narrow(rec.total);
  for (size_t o = 0; o < L2::kOutcomeCount; ++o)
    head.outcome[o] = narrow(rec.outcome[o]);
  for (size_t bit = 0; bit < L2::kCheckBitCount; ++bit)
    head.checks[bit] = narrow(rec.checks[bit]);
  const size_t head_written = file_pwrite(fd_, &head, sizeof(head), 0);
  assert(head_written == sizeof(head) && "日文件头写入失败");
  (void)head_written;

  file_sync(fd_);
  file_close(fd_);
  fd_ = -1;

  std::error_code ec;
  std::filesystem::rename(tmp_path_, path_, ec);
  assert(!ec && "日文件 rename 到最终路径失败");
}

// ============================================================================
// OrdersCatalog
// ============================================================================

size_t OrdersCatalog::day_idx(misc::Ymd date) const {
  const auto it = std::lower_bound(days.begin(), days.end(), date);
  return it != days.end() && *it == date ? static_cast<size_t>(it - days.begin()) : kNoDay;
}
