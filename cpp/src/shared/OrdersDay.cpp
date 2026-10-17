#include "shared/OrdersDay.hpp"
#include "misc/cross_platform.hpp"
#include "shared/Config.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>

namespace {

// 全 uint32 排列, 没有填充, 结构体整块读写. 字段顺序即盘上顺序; 账目即
// DayRecord 原样.
struct DayFileHeader {
  static constexpr uint32_t kMagic = 0x544F324C; // 'L','2','O','T' 小端

  uint32_t magic;
  uint32_t header_size; // = sizeof(DayFileHeader)
  uint32_t entry_count;
  DayRecord rec;
};
static_assert(sizeof(DayFileHeader) == 4 * 3 + sizeof(DayRecord), "日文件头必须定宽无填充");

// 偏移首尾相接的一串帧并成一段整块读写, 一段最多这么大
constexpr size_t kRunCap = size_t(16) << 20;

} // namespace

// ============================================================================
// 路径
// ============================================================================

std::string archive_day_path(const std::string &dir, misc::Ymd date) {
  const std::string d = misc::dense(date);
  return dir + "/" + d.substr(0, 4) + "/" + d.substr(0, 6) + "/" + d + config::ARCHIVE_EXTENSION;
}

std::string orders_day_path(const std::string &dir, misc::Ymd date) {
  const std::string d = misc::dense(date);
  return dir + "/" + d.substr(0, 4) + "/" + d.substr(4, 2) + "/" + d + config::BINARY_EXTENSION;
}

std::vector<misc::Ymd> orders_days(const std::string &dir) {
  namespace fs = std::filesystem;
  std::vector<misc::Ymd> days;
  for (const auto &year_entry : fs::directory_iterator(dir)) {
    if (!year_entry.is_directory())
      continue;
    for (const auto &month_entry : fs::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory())
        continue;
      for (const auto &day_entry : fs::directory_iterator(month_entry.path())) {
        if (!day_entry.is_regular_file() || day_entry.path().extension().string() != config::BINARY_EXTENSION)
          continue;
        const std::string stem = day_entry.path().stem().string();
        const misc::Ymd date = stem.size() == 8 ? misc::parse_ymd(stem) : 0;
        // 只认 orders_day_path 给得出的位置: YYYY/MM 必须是这一天的年月
        if (!date || year_entry.path().filename().string() != stem.substr(0, 4) ||
            month_entry.path().filename().string() != stem.substr(4, 2))
          continue;
        days.push_back(date);
      }
    }
  }
  std::sort(days.begin(), days.end());
  return days;
}

// ============================================================================
// DayIndex
// ============================================================================

void DayIndex::settle(L2::Outcome o, uint32_t check_flags, const FrameRef &ref) {
  assert((o == L2::Outcome::Ok) == !ref.empty() && "DayIndex::settle: 只有 Ok 带帧");
  rec.tally(o, check_flags);
  if (L2::settled(o))
    refs.push_back(ref);
}

void DayIndex::check(uint64_t data_end) const {
  size_t frames = 0;
  for (size_t i = 0; i < refs.size(); ++i) {
    const FrameRef &e = refs[i];
    assert((i == 0 || refs[i - 1].asset_id < e.asset_id) && "日文件目录: 未按 asset_id 严格升序 (同一资产两条?)");
    if (e.empty()) {
      assert(e.offset == 0 && e.head.count == 0 && e.head.price_base == 0 && "日文件目录: 墓碑带了数据");
      continue;
    }
    assert(e.head.count > 0 && "日文件目录: 帧的条数为 0");
    assert(e.offset >= sizeof(DayFileHeader) && e.offset + e.size <= data_end &&
           "日文件目录: 帧越出数据区");
    ++frames;
  }
  assert(rec.complete <= 1 && "日文件账目: complete 非 0/1");
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

const FrameRef *DayIndex::find(size_t asset_id) const {
  const auto it = std::lower_bound(refs.begin(), refs.end(), asset_id,
                                   [](const FrameRef &e, size_t id) { return e.asset_id < id; });
  return it != refs.end() && it->asset_id == asset_id ? &*it : nullptr;
}

// ============================================================================
// DayReader
// ============================================================================

DayReader::DayReader(const std::string &dir, misc::Ymd date)
    : date_(date), fd_(file_open_read(orders_day_path(dir, date).c_str())) {
  assert(fd_ >= 0 && "日文件打不开 (登记过却不存在: 扫描 / 增量判定之后有人在动 orders?)");
}

DayReader::~DayReader() {
  file_close(fd_);
}

const DayIndex &DayReader::index() {
  if (indexed_)
    return index_;

  const uint64_t size = file_size(fd_);
  DayFileHeader head{};
  assert(size >= sizeof(head) && "日文件短于头 (损坏)");
  const size_t head_got = file_pread(fd_, &head, sizeof(head), 0);
  assert(head_got == sizeof(head) && "日文件头读不全");
  (void)head_got;
  assert(head.magic == DayFileHeader::kMagic && "不是 orders 日文件 (magic 不符)");
  assert(head.header_size == sizeof(DayFileHeader) && "日文件头长不符 (结局数 / 判据列数变了?)");

  const uint64_t refs_bytes = uint64_t(head.entry_count) * sizeof(FrameRef);
  assert(size >= sizeof(head) + refs_bytes && "日文件目录越界 (截断?)");
  const uint64_t refs_offset = size - refs_bytes;

  index_.refs.resize(head.entry_count);
  const size_t refs_got = file_pread(fd_, index_.refs.data(), refs_bytes, refs_offset);
  assert(refs_got == refs_bytes && "日文件目录读不全");
  (void)refs_got;

  index_.rec = head.rec;
  index_.check(refs_offset);
  indexed_ = true;
  return index_;
}

L2::FrameView DayReader::get(const FrameRef &ref, L2::FrameDecoder &dec) {
  assert(!ref.empty() && "DayReader::get: 空格子没有帧");
  char *const bytes = dec.frame_buffer(ref.size);
  const size_t got = file_pread(fd_, bytes, ref.size, ref.offset);
  assert(got == ref.size && "DayReader::get: 帧越过日文件末尾 (目录与文件不符)");
  (void)got;
  return dec.decode({bytes, ref.size, ref.head});
}

std::span<const uint32_t> DayReader::by_offset(std::span<const FrameRef> refs) {
  order_.clear();
  for (size_t i = 0; i < refs.size(); ++i)
    if (!refs[i].empty())
      order_.push_back(static_cast<uint32_t>(i));
  std::sort(order_.begin(), order_.end(),
            [&refs](uint32_t a, uint32_t b) { return refs[a].offset < refs[b].offset; });
  return order_;
}

// refs 在回调里可以被改 (adopt 就地改写偏移): 段的划分只读尚未回调过的项.
void DayReader::read_runs(std::span<const FrameRef> refs, const RunFn &fn) {
  const std::span<const uint32_t> order = by_offset(refs);
  for (size_t i = 0; i < order.size();) {
    const uint64_t begin = refs[order[i]].offset;
    uint64_t end = begin + refs[order[i]].size;
    size_t j = i + 1;
    while (j < order.size() && refs[order[j]].offset == end && end - begin + refs[order[j]].size <= kRunCap)
      end += refs[order[j++]].size;

    const size_t len = static_cast<size_t>(end - begin);
    if (buf_.size() < len)
      buf_.resize(len);
    const size_t got = file_pread(fd_, buf_.data(), len, begin);
    assert(got == len && "DayReader::read_runs: 帧越过日文件末尾 (目录与文件不符)");
    (void)got;
    fn(buf_.data(), len, begin, order.subspan(i, j - i));
    i = j;
  }
}

size_t DayReader::prefetch(std::span<const FrameRef> refs) {
  size_t bytes = 0;
  for (const uint32_t k : by_offset(refs)) {
    file_readahead(fd_, refs[k].offset, refs[k].size);
    bytes += refs[k].size;
  }
  return bytes;
}

// ============================================================================
// DayWriter
// ============================================================================

DayWriter::DayWriter(const std::string &dir, misc::Ymd date)
    : path_(orders_day_path(dir, date)), tmp_path_(path_ + ".tmp"), end_(sizeof(DayFileHeader)) {
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

// 32 位的余量很宽: 最活跃的资产单日逐笔是百万量级, 一帧是几 MB.
FrameRef DayWriter::put(size_t asset_id, L2::FrameEncoder &enc, const L2::FrameView &view) {
  assert(asset_id <= UINT32_MAX && "日文件目录: A 轴下标溢出 32 位");
  const L2::Frame frame = enc.encode(view);
  assert(frame.size > 0 && frame.head.count > 0 && "DayWriter::put: 空帧走墓碑");
  const uint64_t offset = append_bytes(frame.data, frame.size);
  return {offset, static_cast<uint32_t>(asset_id), frame.size, frame.head};
}

void DayWriter::adopt(DayReader &src, std::vector<FrameRef> &refs) {
  src.read_runs(refs, [&](const char *bytes, size_t len, uint64_t begin, std::span<const uint32_t> run) {
    const uint64_t base = append_bytes(bytes, len);
    for (const uint32_t k : run)
      refs[k].offset = base + (refs[k].offset - begin);
  });
}

void DayWriter::commit(DayIndex index) {
  assert(fd_ >= 0 && "日文件 commit 两次");
  std::vector<FrameRef> &refs = index.refs;
  std::sort(refs.begin(), refs.end(),
            [](const FrameRef &a, const FrameRef &b) { return a.asset_id < b.asset_id; });

  const uint64_t refs_offset = end_.load(std::memory_order_relaxed);
  index.check(refs_offset);

  const size_t refs_bytes = refs.size() * sizeof(FrameRef);
  const size_t refs_written = file_pwrite(fd_, refs.data(), refs_bytes, refs_offset);
  assert(refs_written == refs_bytes && "日文件目录写入失败 (磁盘满?)");
  (void)refs_written;

  assert(refs.size() <= UINT32_MAX && "日文件目录: 条数溢出 32 位");
  DayFileHeader head{};
  head.magic = DayFileHeader::kMagic;
  head.header_size = sizeof(DayFileHeader);
  head.entry_count = static_cast<uint32_t>(refs.size());
  head.rec = index.rec;
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

OrdersCatalog::OrdersCatalog(std::vector<misc::Ymd> days_, size_t width_)
    : days(std::move(days_)), records(days.size()), width(width_), frames(days.size() * width_) {}

void OrdersCatalog::fill_day(size_t didx, const std::string &dir) {
  assert(didx < days.size() && "OrdersCatalog::fill_day: 行越界");
  DayReader reader(dir, days[didx]);
  const DayIndex &index = reader.index();
  records[didx] = index.rec;
  FrameRef *row = frames.data() + didx * width;
  for (const FrameRef &ref : index.refs) { // 帧与墓碑都进格子; 目录里没有的保持缺席
    assert(ref.asset_id < width && "日文件目录里的 A 轴下标超出帧目录宽度");
    row[ref.asset_id] = ref;
  }
}

size_t OrdersCatalog::day_idx(misc::Ymd date) const {
  const auto it = std::lower_bound(days.begin(), days.end(), date);
  return it != days.end() && *it == date ? static_cast<size_t>(it - days.begin()) : kNoDay;
}

OrdersCatalog OrdersCatalog::slice(std::span<const misc::Ymd> dates_, std::span<const uint32_t> ids) const {
  assert(std::is_sorted(dates_.begin(), dates_.end()) && "OrdersCatalog::slice: 日期须升序");
  OrdersCatalog out(std::vector<misc::Ymd>(dates_.begin(), dates_.end()), ids.size());
  for (size_t d = 0; d < out.days.size(); ++d) {
    const size_t src = day_idx(out.days[d]);
    if (src == kNoDay)
      continue;
    out.records[d] = records[src];
    FrameRef *row = out.frames.data() + d * out.width;
    for (size_t c = 0; c < ids.size(); ++c)
      row[c] = at(src, ids[c]);
  }
  return out;
}
