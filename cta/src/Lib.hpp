#pragma once

// 特征库 all.bar (只读): 清单 lib.json (A 轴码表 + 日期轴) + 字段表 features.json (各层的列) + 日文件
//   日文件 <库>/YYYY/MM/DD/features_<层>.bin = [头 48B][目录 {偏移 u64, 字节 u32, 差分 u32} × F][列载荷 × F]
//     头 = magic "FTDAY2", T (层行数, 含末尾哨兵行), F, A, 轴指纹 (码表按序的 FNV 链), 字段表指纹
//     列载荷 = 一列 [T][A] fp16 按位当 u16: [u64 位图 × ⌈T·A / 64⌉ (LSB 先)][非零字面 …][u16 0]; 差分列 = 字 k 存 x[k] ^ x[k − A]
//   核对: magic / A / 轴指纹 / 形状逐文件断言; 字段表指纹 = 首个落盘日的, 之后逐文件一致 (字段表变了 = 库要重算)
//   一日落盘齐 = 末层 (L_1day) 文件在
#include "Json.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>

struct Level {
  const char *name;
  size_t rows; // 落盘行数 (末行 = 哨兵)
};
inline constexpr Level kL1min{"L_1min", 256}, kL1day{"L_1day", 2};
inline constexpr size_t kAuctionRows = 15; // L_1min 行 [0, 15) = 09:15 … 09:29 竞价段

inline constexpr uint64_t kFnvOffset = 1469598103934665603ULL, kFnvPrime = 1099511628211ULL;
// 一条码并进 FNV 链 (末尾多一轮乘法作分隔)
inline uint64_t fnv(uint64_t h, std::string_view s) {
  for (const char c : s)
    h = (h ^ static_cast<uint8_t>(c)) * kFnvPrime;
  return h * kFnvPrime;
}

struct Lib {
  std::string dir;
  std::vector<std::string> codes; // A 轴
  uint64_t axis = 0;              // 码表 FNV 链 = 日文件头的轴指纹
  std::vector<int> dates;         // 已落盘日 (升序)
  Json fields;                    // features.json
  uint64_t fp_day = 0, fp_min = 0;

  size_t A() const { return codes.size(); }
  std::string file(int d, const Level &l) const {
    char s[16];
    std::snprintf(s, sizeof(s), "/%04d/%02d/%02d", d / 10000, d / 100 % 100, d % 100);
    return dir + s + "/features_" + l.name + ".bin";
  }
  // 层 l 中 code 的列下标
  size_t col(const Level &l, std::string_view code) const {
    for (const Json &f : fields[l.name].arr)
      if (f["code"].str == code)
        return static_cast<size_t>(f["col"].num);
    assert(false && "all.bar: 字段表里没有这一列");
    return 0;
  }
  size_t width(const Level &l) const { return fields[l.name].arr.size(); }
  uint64_t fp(const Level &l) const { return l.rows == kL1day.rows ? fp_day : fp_min; }
};

namespace lib {

struct Head {
  uint64_t magic, T, F, A, axis, fp;
};
struct Span {
  uint64_t offset;
  uint32_t size, delta;
};
static_assert(sizeof(Head) == 48 && sizeof(Span) == 16);
inline constexpr uint64_t kMagic = 0x0000325941445446ULL; // "FTDAY2"

inline Head head_of(const Lib &L, int d, const Level &l) {
  const int fd = ::open(L.file(d, l).c_str(), O_RDONLY);
  assert(fd >= 0 && "all.bar: 日文件打不开");
  Head h{};
  [[maybe_unused]] const ssize_t got = ::pread(fd, &h, sizeof(h), 0);
  assert(got == sizeof(h));
  ::close(fd);
  return h;
}

// 一列载荷 → [T][A] u16
inline void decode(const uint8_t *src, size_t bytes, uint16_t *d, size_t words, size_t A, bool delta) {
  const size_t nblk = (words + 63) / 64;
  const uint64_t *bm = reinterpret_cast<const uint64_t *>(src);
  const uint16_t *lit = reinterpret_cast<const uint16_t *>(bm + nblk);
  for (size_t b = 0; b < nblk; ++b) {
    const size_t base = b * 64, n = std::min<size_t>(64, words - base);
    const uint64_t m = bm[b];
    for (size_t i = 0; i < n; ++i) {
      const size_t k = base + i;
      const uint16_t v = m >> i & 1 ? *lit++ : 0;
      d[k] = delta && k >= A ? static_cast<uint16_t>(v ^ d[k - A]) : v;
    }
  }
  assert(reinterpret_cast<const uint8_t *>(lit + 1) - src == static_cast<ptrdiff_t>(bytes) && "all.bar: 载荷与位图不自洽");
}

} // namespace lib

inline Lib open_lib(const std::string &dir) {
  Lib L;
  L.dir = dir;
  const Json m = json::read(dir + "/lib.json");
  assert(m["tier"].str == "bar" && "须是 Bar 档库");
  L.axis = kFnvOffset;
  for (const Json &c : m["codes"].arr) {
    L.codes.push_back(c.str);
    L.axis = fnv(L.axis, c.str);
  }
  for (const Json &d : m["dates"].arr)
    if (std::filesystem::exists(L.file(static_cast<int>(d.num), kL1day)))
      L.dates.push_back(static_cast<int>(d.num));
  assert(L.dates.size() >= 2 && "all.bar 落盘不足 2 日");
  L.fields = json::read(dir + "/features.json");
  L.fp_day = lib::head_of(L, L.dates.front(), kL1day).fp;
  L.fp_min = lib::head_of(L, L.dates.front(), kL1min).fp;
  return L;
}

// 一日一层选列 [n][T][A] (fp16)
struct Cols {
  size_t T = 0, A = 0;
  std::vector<uint16_t> x;
  std::vector<uint8_t> buf;
  std::vector<lib::Span> dir;

  float at(size_t i, size_t t, size_t a) const {
    const float v = static_cast<float>(std::bit_cast<_Float16>(x[(i * T + t) * A + a]));
    return std::isfinite(v) ? v : 0.f;
  }

  void read(const Lib &L, int d, const Level &l, const std::vector<size_t> &cols) {
    T = l.rows, A = L.A();
    const size_t F = L.width(l), words = T * A;
    const int fd = ::open(L.file(d, l).c_str(), O_RDONLY);
    assert(fd >= 0 && "all.bar: 日文件打不开 (库在重算?)");
    lib::Head h{};
    [[maybe_unused]] ssize_t got = ::pread(fd, &h, sizeof(h), 0);
    assert(got == sizeof(h));
    assert(h.magic == lib::kMagic && "all.bar: 日文件格式不认识");
    assert(h.A == A && h.axis == L.axis && "all.bar: 日文件的轴与 lib.json 码表不符");
    assert(h.T == T && h.F == F && "all.bar: 日文件形状与字段表不符");
    assert(h.fp == L.fp(l) && "all.bar: 字段表指纹与首个落盘日不符 (库重算到一半?)");
    dir.resize(F);
    got = ::pread(fd, dir.data(), F * sizeof(lib::Span), sizeof(h));
    assert(got == static_cast<ssize_t>(F * sizeof(lib::Span)));
    x.resize(cols.size() * words);
    for (size_t i = 0; i < cols.size(); ++i) {
      const lib::Span &s = dir[cols[i]];
      buf.resize(s.size);
      got = ::pread(fd, buf.data(), s.size, static_cast<off_t>(s.offset));
      assert(got == static_cast<ssize_t>(s.size) && "all.bar: 载荷越过文件尾");
      lib::decode(buf.data(), s.size, x.data() + i * words, words, A, s.delta != 0);
    }
    ::close(fd);
  }
};
