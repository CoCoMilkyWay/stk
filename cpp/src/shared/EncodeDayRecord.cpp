#include "shared/EncodeDayRecord.hpp"

#include <cassert>
#include <charconv>
#include <fstream>
#include <string_view>
#include <unordered_map>

namespace {

// 固定字段的键名. 判据位的键名取自 L2::check_meta, 不在这里重复.
constexpr const char *kKeyComplete = "complete";
constexpr const char *kKeyTotal = "total";
constexpr const char *kKeyOk = "ok";
constexpr const char *kKeySkipped = "skip";
constexpr const char *kKeyCorrupt = "corrupt";
constexpr const char *kKeyInvalid = "invalid";
constexpr const char *kKeyFailed = "failed";

size_t parse_count(std::string_view text) {
  size_t value = 0;
  const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
  assert(ec == std::errc{} && ptr == text.data() + text.size() &&
         "整天记录: 值不是十进制整数");
  return value;
}

} // namespace

void write_encode_day_record(const std::string &day_dir, const EncodeDayRecord &rec) {
  std::ofstream out(day_dir + "/" + kEncodeDayRecordName, std::ios::trunc);
  assert(out.is_open() && "整天记录写不出去");

  // 固定字段一律写出 (哪怕是 0) —— 读端靠它们区分"这天编过且干净"和"旧的空标记".
  out << kKeyComplete << '=' << (rec.complete ? 1 : 0) << '\n'
      << kKeyTotal << '=' << rec.assets_total << '\n'
      << kKeyOk << '=' << rec.assets_ok << '\n'
      << kKeySkipped << '=' << rec.assets_skipped << '\n'
      << kKeyCorrupt << '=' << rec.assets_corrupt << '\n'
      << kKeyInvalid << '=' << rec.assets_invalid << '\n'
      << kKeyFailed << '=' << rec.assets_failed << '\n';

  // 判据位只写命中过的 —— 界面的原因列就按"文件里出现过"来出, 没发生过的
  // 判据不占一列.
  for (size_t bit = 0; bit < L2::kCheckBitCount; ++bit) {
    if (rec.checks[bit] == 0)
      continue;
    const L2::CheckMeta &meta = L2::check_meta(bit);
    assert(meta.key && "整天记录: 空洞位不该有命中数");
    out << meta.key << '=' << rec.checks[bit] << '\n';
  }

  out.flush();
  assert(out.good() && "整天记录写到一半失败");
}

bool read_encode_day_record(const std::string &day_dir, EncodeDayRecord &out) {
  std::ifstream in(day_dir + "/" + kEncodeDayRecordName);
  if (!in.is_open())
    return false;

  // 键名 → 落到 record 的哪个字段. 判据位一并进表, 于是解析不需要分支.
  static const std::unordered_map<std::string_view, size_t EncodeDayRecord::*> kFields{
      {kKeyTotal, &EncodeDayRecord::assets_total},
      {kKeyOk, &EncodeDayRecord::assets_ok},
      {kKeySkipped, &EncodeDayRecord::assets_skipped},
      {kKeyCorrupt, &EncodeDayRecord::assets_corrupt},
      {kKeyInvalid, &EncodeDayRecord::assets_invalid},
      {kKeyFailed, &EncodeDayRecord::assets_failed},
  };

  static const std::unordered_map<std::string_view, size_t> kCheckBits = [] {
    std::unordered_map<std::string_view, size_t> map;
    for (size_t bit = 0; bit < L2::kCheckBitCount; ++bit) {
      const L2::CheckMeta &meta = L2::check_meta(bit);
      if (meta.key)
        map.emplace(meta.key, bit);
    }
    return map;
  }();

  out = EncodeDayRecord{};

  std::string line;
  while (std::getline(in, line)) {
    if (line.empty())
      continue;
    const size_t eq = line.find('=');
    assert(eq != std::string::npos && "整天记录: 行里没有 '='");

    const std::string_view key(line.data(), eq);
    const std::string_view value(line.data() + eq + 1, line.size() - eq - 1);

    if (key == kKeyComplete) {
      out.complete = parse_count(value) != 0;
      continue;
    }
    if (auto it = kFields.find(key); it != kFields.end()) {
      out.*(it->second) = parse_count(value);
      continue;
    }
    auto bit = kCheckBits.find(key);
    assert(bit != kCheckBits.end() && "整天记录: 不认识的键名");
    out.checks[bit->second] = parse_count(value);
  }

  return true;
}
