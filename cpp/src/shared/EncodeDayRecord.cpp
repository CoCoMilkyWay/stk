#include "shared/EncodeDayRecord.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <fstream>
#include <ios>

namespace {

// 盘上布局: [StatHeader][EncodeDayIndexEntry × entry_count]
//
// 全 uint32 排列, 没有填充洞, 于是结构体可以整块读写. 判据位的列数
// (kCheckBitCount) 一改就是格式变更 —— 长度自洽性检查会让旧文件读不出来,
// 那天重新列举一遍即可, 所以不需要单独递增 version.
struct StatHeader {
  static constexpr uint32_t kMagic = 0x54415453; // 'S','T','A','T' 小端
  static constexpr uint32_t kVersion = 1;

  uint32_t magic;
  uint32_t version;
  uint32_t entry_count;
  uint8_t accounted;
  uint8_t complete;
  uint16_t reserved; // 置 0
  uint32_t assets_total;
  uint32_t assets_ok;
  uint32_t assets_skipped;
  uint32_t assets_corrupt;
  uint32_t assets_invalid;
  uint32_t assets_failed;
  uint32_t checks[L2::kCheckBitCount];
};
static_assert(sizeof(StatHeader) == 16 + 6 * 4 + 4 * L2::kCheckBitCount,
              "整天统计的头必须定宽无填充");

} // namespace

void write_encode_day_stat(const std::string &day_dir, EncodeDayRecord rec) {
  std::sort(rec.assets.begin(), rec.assets.end(),
            [](const EncodeDayIndexEntry &a, const EncodeDayIndexEntry &b) {
              return a.asset_id < b.asset_id;
            });

  for (size_t i = 1; i < rec.assets.size(); ++i) {
    assert(rec.assets[i - 1].asset_id != rec.assets[i].asset_id &&
           "整天统计: 同一资产两条明细 (销账重复?)");
  }

  StatHeader head{};
  head.magic = StatHeader::kMagic;
  head.version = StatHeader::kVersion;
  head.entry_count = static_cast<uint32_t>(rec.assets.size());
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

  // 原地 trunc 而不是 tmp + rename: rename 会改日目录的 mtime, 而增量扫描正是
  // 拿目录 mtime 当基线 —— 每写一次就让当天判"动过", 那条快路径再也稳不下来.
  // 写到一半的风险由读端的长度自洽检查 + 与 readdir 名单的比对兜住.
  std::ofstream out(day_dir + "/" + kEncodeStatName, std::ios::binary | std::ios::trunc);
  assert(out.is_open() && "整天统计写不出去");

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
