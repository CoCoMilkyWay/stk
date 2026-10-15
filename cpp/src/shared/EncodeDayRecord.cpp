#include "shared/EncodeDayRecord.hpp"
#include "misc/cross_platform.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>

namespace {

// 全 uint32 排列, 没有填充, 结构体整块读写. 字段顺序即盘上顺序 (merge_orders.py
// 按同一布局写).
struct DayFileHeader {
  static constexpr uint32_t kMagic = 0x444F324C; // 'L','2','O','D' 小端

  uint32_t magic;
  uint32_t header_size; // = sizeof(DayFileHeader)
  uint32_t entry_count;
  uint32_t complete; // 0 / 1
  uint32_t assets_total;
  uint32_t assets_ok;
  uint32_t assets_skipped;
  uint32_t assets_corrupt;
  uint32_t assets_invalid;
  uint32_t assets_failed;
  uint32_t checks[L2::kCheckBitCount];
};
static_assert(sizeof(DayFileHeader) == 4 * (10 + L2::kCheckBitCount), "日文件头必须定宽无填充");

// 增量补编搬旧帧时, 偏移首尾相接的一串帧并成一段整块读写, 一段最多这么大
constexpr size_t kCopyChunk = size_t(16) << 20;

uint32_t narrow(size_t v) {
  assert(v <= UINT32_MAX && "日文件账目: 计数溢出 32 位");
  return static_cast<uint32_t>(v);
}

// 目录的结构自洽 + 与账目对账. 读写两端共用: 写端在落盘前过一遍, 读端在
// 采信前再过一遍.
void check_index(const std::vector<EncodeDayIndexEntry> &index, uint64_t data_end,
                 const EncodeDayRecord &rec) {
  size_t frames = 0;
  for (size_t i = 0; i < index.size(); ++i) {
    const EncodeDayIndexEntry &e = index[i];
    assert((i == 0 || index[i - 1].asset_id < e.asset_id) && "日文件目录: 未按 asset_id 严格升序 (同一资产两条?)");
    if (e.is_tombstone()) {
      assert(e.offset == 0 && e.order_count == 0 && e.price_base == 0 && "日文件目录: 墓碑带了数据");
      continue;
    }
    assert(e.order_count > 0 && "日文件目录: 帧的条数为 0");
    assert(e.offset >= sizeof(DayFileHeader) && e.offset + e.compressed_size <= data_end &&
           "日文件目录: 帧越出数据区");
    ++frames;
  }
  assert(rec.assets_ok == frames && rec.assets_skipped == index.size() - frames &&
         "日文件账目: ok / skipped 与目录里的帧 / 墓碑条数不符");
  assert(rec.assets_ok + rec.assets_skipped + rec.assets_error() <= rec.assets_total &&
         "日文件账目: 处置分类之和超过分母");
  (void)frames;
  (void)data_end;
  (void)rec;
}

} // namespace

const EncodeDayIndexEntry *find_day_entry(const std::vector<EncodeDayIndexEntry> &index, size_t asset_id) {
  const auto it = std::lower_bound(index.begin(), index.end(), asset_id,
                                   [](const EncodeDayIndexEntry &e, size_t id) { return e.asset_id < id; });
  return it != index.end() && it->asset_id == asset_id ? &*it : nullptr;
}

bool read_day_file(const std::string &path, EncodeDayRecord &rec, std::vector<EncodeDayIndexEntry> &index) {
  rec = EncodeDayRecord{};
  index.clear();

  const int fd = file_open_read(path.c_str());
  if (fd < 0)
    return false;

  const uint64_t size = file_size(fd);
  DayFileHeader head{};
  assert(size >= sizeof(head) && "日文件短于头 (损坏)");
  const size_t head_got = file_pread(fd, &head, sizeof(head), 0);
  assert(head_got == sizeof(head) && "日文件头读不全");
  (void)head_got;
  assert(head.magic == DayFileHeader::kMagic && "不是 orders 日文件 (magic 不符)");
  assert(head.header_size == sizeof(DayFileHeader) && "日文件头长不符 (判据列数 kCheckBitCount 变了?)");
  assert(head.complete <= 1 && "日文件头: complete 非 0/1");

  const uint64_t index_bytes = uint64_t(head.entry_count) * sizeof(EncodeDayIndexEntry);
  assert(size >= sizeof(head) + index_bytes && "日文件目录越界 (截断?)");
  const uint64_t index_offset = size - index_bytes;

  index.resize(head.entry_count);
  const size_t index_got = file_pread(fd, index.data(), index_bytes, index_offset);
  assert(index_got == index_bytes && "日文件目录读不全");
  (void)index_got;
  file_close(fd);

  rec.complete = head.complete != 0;
  rec.assets_total = head.assets_total;
  rec.assets_ok = head.assets_ok;
  rec.assets_skipped = head.assets_skipped;
  rec.assets_corrupt = head.assets_corrupt;
  rec.assets_invalid = head.assets_invalid;
  rec.assets_failed = head.assets_failed;
  for (size_t bit = 0; bit < L2::kCheckBitCount; ++bit)
    rec.checks[bit] = head.checks[bit];

  check_index(index, index_offset, rec);
  return true;
}

DayFileWriter::DayFileWriter(std::string path)
    : path_(std::move(path)), tmp_path_(path_ + ".tmp"), end_(sizeof(DayFileHeader)) {
  std::filesystem::create_directories(std::filesystem::path(path_).parent_path());
  fd_ = file_open_trunc(tmp_path_.c_str());
  assert(fd_ >= 0 && "日文件 .tmp 建不出来");
}

DayFileWriter::~DayFileWriter() {
  if (fd_ < 0)
    return; // 已 commit
  file_close(fd_);
  std::error_code ec;
  std::filesystem::remove(tmp_path_, ec);
}

uint64_t DayFileWriter::append(const void *data, size_t size) {
  assert(fd_ >= 0 && "日文件已 commit, 不能再追加");
  const uint64_t off = end_.fetch_add(size, std::memory_order_relaxed);
  const size_t written = file_pwrite(fd_, data, size, off);
  assert(written == size && "日文件写入失败 (磁盘满?)");
  (void)written;
  return off;
}

void DayFileWriter::copy_frames(const std::string &src_path, std::vector<EncodeDayIndexEntry> &entries) {
  std::vector<size_t> order;
  order.reserve(entries.size());
  for (size_t i = 0; i < entries.size(); ++i)
    if (!entries[i].is_tombstone())
      order.push_back(i);
  if (order.empty())
    return;
  std::sort(order.begin(), order.end(),
            [&entries](size_t a, size_t b) { return entries[a].offset < entries[b].offset; });

  const int src = file_open_read(src_path.c_str());
  assert(src >= 0 && "增量补编: 旧日文件打不开");

  std::vector<char> buf;
  for (size_t i = 0; i < order.size();) {
    const uint64_t run_begin = entries[order[i]].offset;
    uint64_t run_end = run_begin + entries[order[i]].compressed_size;
    size_t j = i + 1;
    while (j < order.size() && entries[order[j]].offset == run_end &&
           run_end - run_begin + entries[order[j]].compressed_size <= kCopyChunk)
      run_end += entries[order[j++]].compressed_size;

    const size_t len = static_cast<size_t>(run_end - run_begin);
    if (buf.size() < len)
      buf.resize(len);
    const size_t got = file_pread(src, buf.data(), len, run_begin);
    assert(got == len && "增量补编: 旧日文件的帧读不全");
    (void)got;
    const uint64_t base = append(buf.data(), len);
    for (size_t k = i; k < j; ++k)
      entries[order[k]].offset = base + (entries[order[k]].offset - run_begin);
    i = j;
  }
  file_close(src);
}

void DayFileWriter::commit(const EncodeDayRecord &rec, std::vector<EncodeDayIndexEntry> index) {
  assert(fd_ >= 0 && "日文件 commit 两次");
  std::sort(index.begin(), index.end(),
            [](const EncodeDayIndexEntry &a, const EncodeDayIndexEntry &b) { return a.asset_id < b.asset_id; });

  const uint64_t index_offset = end_.load(std::memory_order_relaxed);
  check_index(index, index_offset, rec);

  const size_t index_bytes = index.size() * sizeof(EncodeDayIndexEntry);
  const size_t index_written = file_pwrite(fd_, index.data(), index_bytes, index_offset);
  assert(index_written == index_bytes && "日文件目录写入失败 (磁盘满?)");
  (void)index_written;

  DayFileHeader head{};
  head.magic = DayFileHeader::kMagic;
  head.header_size = sizeof(DayFileHeader);
  head.entry_count = narrow(index.size());
  head.complete = rec.complete ? 1 : 0;
  head.assets_total = narrow(rec.assets_total);
  head.assets_ok = narrow(rec.assets_ok);
  head.assets_skipped = narrow(rec.assets_skipped);
  head.assets_corrupt = narrow(rec.assets_corrupt);
  head.assets_invalid = narrow(rec.assets_invalid);
  head.assets_failed = narrow(rec.assets_failed);
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
