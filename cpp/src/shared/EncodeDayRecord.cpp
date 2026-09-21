#include "shared/EncodeDayRecord.hpp"
#include "shared/AssetAxis.hpp"
#include "shared/Config.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <ios>
#include <unordered_set>

namespace {

// 盘上布局: [StatHeader][EncodeDayIndexEntry × entry_count]
//
// 全 uint32 排列 (dir_mtime 拆成低/高两半, 免得 int64 引入对齐洞), 没有填充,
// 于是结构体可以整块读写. 判据位的列数 (kCheckBitCount) 一改就是格式变更 ——
// 长度自洽性检查会让旧文件读不出来, 那天重新列举一遍即可, 所以不需要单独
// 递增 version.
struct StatHeader {
  static constexpr uint32_t kMagic = 0x54415453; // 'S','T','A','T' 小端
  static constexpr uint32_t kVersion = 2;        // v2: + dir_mtime

  uint32_t magic;
  uint32_t version;
  uint32_t entry_count;
  uint8_t accounted;
  uint8_t complete;
  uint16_t reserved; // 置 0
  uint32_t dir_mtime_lo;
  uint32_t dir_mtime_hi;
  uint32_t assets_total;
  uint32_t assets_ok;
  uint32_t assets_skipped;
  uint32_t assets_corrupt;
  uint32_t assets_invalid;
  uint32_t assets_failed;
  uint32_t checks[L2::kCheckBitCount];
};
static_assert(sizeof(StatHeader) == 16 + 8 + 6 * 4 + 4 * L2::kCheckBitCount,
              "整天统计的头必须定宽无填充");

} // namespace

int64_t day_dir_mtime(const std::string &day_dir) {
  std::error_code ec;
  const auto wt = std::filesystem::last_write_time(day_dir, ec);
  return ec ? 0 : wt.time_since_epoch().count();
}

void write_encode_day_stat(const std::string &day_dir, EncodeDayRecord rec) {
  std::sort(rec.assets.begin(), rec.assets.end(),
            [](const EncodeDayIndexEntry &a, const EncodeDayIndexEntry &b) {
              return a.asset_id < b.asset_id;
            });

  for (size_t i = 1; i < rec.assets.size(); ++i) {
    assert(rec.assets[i - 1].asset_id != rec.assets[i].asset_id &&
           "整天统计: 同一资产两条明细 (销账重复?)");
  }

  // 原地 trunc 而不是 tmp + rename: rename 会改日目录的 mtime, 而明细正是
  // 拿目录 mtime 当"仍然一致"的凭证 —— 每写一次就让当天判"动过", 那条快路径
  // 再也稳不下来. 写到一半的风险由读端的长度自洽检查兜住.
  std::ofstream out(day_dir + "/" + kEncodeStatName, std::ios::binary | std::ios::trunc);
  assert(out.is_open() && "整天统计写不出去");

  // 文件已打开 (首次创建改过目录 mtime 了), 此刻取的 mtime 才是之后能对上的
  rec.dir_mtime = day_dir_mtime(day_dir);

  StatHeader head{};
  head.magic = StatHeader::kMagic;
  head.version = StatHeader::kVersion;
  head.entry_count = static_cast<uint32_t>(rec.assets.size());
  const uint64_t mtime_bits = static_cast<uint64_t>(rec.dir_mtime);
  head.dir_mtime_lo = static_cast<uint32_t>(mtime_bits & 0xFFFFFFFFu);
  head.dir_mtime_hi = static_cast<uint32_t>(mtime_bits >> 32);
  head.accounted = rec.accounted ? 1 : 0;
  head.complete = rec.complete ? 1 : 0;
  head.assets_total = static_cast<uint32_t>(rec.assets_total);
  head.assets_ok = static_cast<uint32_t>(rec.assets_ok);
  head.assets_skipped = static_cast<uint32_t>(rec.assets_skipped);
  head.assets_corrupt = static_cast<uint32_t>(rec.assets_corrupt);
  head.assets_invalid = static_cast<uint32_t>(rec.assets_invalid);
  head.assets_failed = static_cast<uint32_t>(rec.assets_failed);
  for (size_t bit = 0; bit < L2::kCheckBitCount; ++bit)
    head.checks[bit] = static_cast<uint32_t>(rec.checks[bit]);

  out.write(reinterpret_cast<const char *>(&head), sizeof(head));
  if (!rec.assets.empty()) {
    out.write(reinterpret_cast<const char *>(rec.assets.data()),
              static_cast<std::streamsize>(rec.assets.size() * sizeof(EncodeDayIndexEntry)));
  }

  out.flush();
  assert(out.good() && "整天统计写到一半失败");
}

bool read_encode_day_stat(const std::string &day_dir, EncodeDayRecord &out) {
  out = EncodeDayRecord{};

  std::ifstream in(day_dir + "/" + kEncodeStatName, std::ios::binary | std::ios::ate);
  if (!in.is_open())
    return false;

  const std::streamoff bytes = in.tellg();
  if (bytes < static_cast<std::streamoff>(sizeof(StatHeader)))
    return false;

  in.seekg(0);
  StatHeader head{};
  in.read(reinterpret_cast<char *>(&head), sizeof(head));
  if (!in.good() || head.magic != StatHeader::kMagic || head.version != StatHeader::kVersion)
    return false;

  // 长度必须恰好装下声明的条数 —— 少了是写到一半被打断, 多了是格式对不上
  const std::streamoff expected =
      static_cast<std::streamoff>(sizeof(StatHeader)) +
      static_cast<std::streamoff>(head.entry_count) *
          static_cast<std::streamoff>(sizeof(EncodeDayIndexEntry));
  if (bytes != expected)
    return false;

  out.dir_mtime = static_cast<int64_t>((static_cast<uint64_t>(head.dir_mtime_hi) << 32) | head.dir_mtime_lo);
  out.accounted = head.accounted != 0;
  out.complete = head.complete != 0;
  out.assets_total = head.assets_total;
  out.assets_ok = head.assets_ok;
  out.assets_skipped = head.assets_skipped;
  out.assets_corrupt = head.assets_corrupt;
  out.assets_invalid = head.assets_invalid;
  out.assets_failed = head.assets_failed;
  for (size_t bit = 0; bit < L2::kCheckBitCount; ++bit)
    out.checks[bit] = head.checks[bit];

  if (head.entry_count > 0) {
    out.assets.resize(head.entry_count);
    in.read(reinterpret_cast<char *>(out.assets.data()),
            static_cast<std::streamsize>(head.entry_count * sizeof(EncodeDayIndexEntry)));
    if (!in.good()) {
      out = EncodeDayRecord{};
      return false;
    }
  }

  return true;
}

std::vector<DayBin> list_day_bins(const std::string &day_dir, const AssetAxis &axis) {
  const std::string bin_ext = config::BINARY_EXTENSION;

  std::vector<DayBin> bins;
  std::error_code ec;
  auto dir = std::filesystem::directory_iterator(day_dir, ec);
  if (ec)
    return bins; // 目录读不动 (被整个删了?)

  for (const auto &file_entry : dir) {
    const std::string filename = file_entry.path().filename().string();
    if (!filename.ends_with(bin_ext))
      continue;

    // "000023.SZ.bin" → "000023.SZ". .bin 是按当前代码落盘的 (归档里的老代码
    // 只出现在包内路径上), 直接查轴.
    const size_t asset_id = axis.find(filename.substr(0, filename.size() - bin_ext.size()));
    if (asset_id == axis.size())
      continue; // 轴外文件

    bins.push_back({asset_id, file_entry.path().string()});
  }
  return bins;
}

bool day_index_matches(const EncodeDayRecord &rec, const std::vector<DayBin> &bins) {
  std::unordered_set<uint32_t> declared;
  declared.reserve(rec.assets.size());
  for (const auto &entry : rec.assets)
    if (!entry.is_tombstone())
      declared.insert(entry.asset_id);

  if (declared.size() != bins.size())
    return false;
  for (const auto &bin : bins)
    if (declared.count(static_cast<uint32_t>(bin.asset_id)) == 0)
      return false; // 盘上多了一个明细没声明的
  return true;
}

bool day_index_current(const std::string &day_dir, const EncodeDayRecord &rec, const AssetAxis &axis) {
  const int64_t now = day_dir_mtime(day_dir);
  if (now != 0 && now == rec.dir_mtime)
    return true;
  return day_index_matches(rec, list_day_bins(day_dir, axis));
}
