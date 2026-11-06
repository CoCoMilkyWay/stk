#pragma once

// 张量 (cta/tensor.py 产出, 一目录一份): bars.npy [T][B][F] float32 + time.npy [T] int64 + assets.tsv (B 行, 与 B 轴同序)
//   时间 = YYYYMMDD (日线) / YYYYMMDDHHMM (分钟); 某资产某根无数据 = NaN
//   载入即转成资产主序 [B][T][F]: 单资产时序连续
#include <cassert>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

enum Field : int { kOpen,
                   kHigh,
                   kLow,
                   kClose,
                   kAmount,
                   kRef, // 前一根收盘
                   kFields };

struct Asset {
  std::string code, name;
  std::string set;    // 测试集: idx / ind / concept / - (不进)
  std::string weight; // eq / cap
};

struct Panel {
  size_t T = 0, B = 0;
  std::vector<int64_t> time; // [T]
  std::vector<Asset> assets; // [B]
  std::vector<float> x;      // [B][T][F]
  double bars_per_year = 0;
  const float *at(size_t b, size_t t) const { return x.data() + (b * T + t) * kFields; }
};

// 时间 → YYYYMMDD
inline int ymd_of(int64_t t) { return static_cast<int>(t >= 100000000 ? t / 10000 : t); }

namespace npy {

struct Array {
  std::string descr;
  std::vector<size_t> shape;
  std::string data;
};

inline Array read(const std::string &path) {
  std::ifstream f(path, std::ios::binary);
  assert(f && "npy: 打不开");
  const std::string blob((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
  assert(blob.size() >= 12 && blob.compare(0, 6, "\x93NUMPY") == 0 && "npy: 不是 .npy");
  const auto u8 = [&](size_t i) { return static_cast<size_t>(static_cast<uint8_t>(blob[i])); };
  const bool v1 = blob[6] == 1;
  const size_t off = v1 ? 10 : 12;
  const size_t hl = v1 ? u8(8) | u8(9) << 8 : u8(8) | u8(9) << 8 | u8(10) << 16 | u8(11) << 24;
  const std::string_view head(blob.data() + off, hl);
  const auto after = [&](std::string_view key) {
    const size_t p = head.find(key);
    assert(p != std::string_view::npos && "npy: 头缺字段");
    return p + key.size();
  };
  assert(head.find("'fortran_order': False") != std::string_view::npos && "npy: 须 C 序");
  Array a;
  const size_t d = after("'descr': '");
  a.descr = head.substr(d, head.find('\'', d) - d);
  for (size_t p = after("'shape': ("); head[p] != ')';) {
    if (head[p] < '0' || head[p] > '9') {
      ++p;
      continue;
    }
    size_t v = 0;
    while (head[p] >= '0' && head[p] <= '9')
      v = v * 10 + static_cast<size_t>(head[p++] - '0');
    a.shape.push_back(v);
  }
  a.data = blob.substr(off + hl);
  return a;
}

} // namespace npy

inline Panel load_panel(const std::string &dir) {
  const npy::Array bars = npy::read(dir + "/bars.npy"), time = npy::read(dir + "/time.npy");
  assert(bars.descr == "<f4" && bars.shape.size() == 3 && bars.shape[2] == kFields && "bars.npy: 须 float32 [T][B][F]");
  assert(time.descr == "<i8" && time.shape.size() == 1 && time.shape[0] == bars.shape[0] && "time.npy: 须 int64 [T]");
  Panel p;
  p.T = bars.shape[0], p.B = bars.shape[1];
  assert(bars.data.size() == p.T * p.B * kFields * sizeof(float) && time.data.size() == p.T * sizeof(int64_t));
  p.time.resize(p.T);
  std::memcpy(p.time.data(), time.data.data(), time.data.size());
  std::vector<float> src(p.T * p.B * kFields);
  std::memcpy(src.data(), bars.data.data(), bars.data.size());
  p.x.resize(src.size());
  for (size_t t = 0; t < p.T; ++t)
    for (size_t b = 0; b < p.B; ++b)
      std::memcpy(p.x.data() + (b * p.T + t) * kFields, src.data() + (t * p.B + b) * kFields, kFields * sizeof(float));

  std::ifstream f(dir + "/assets.tsv");
  assert(f && "assets.tsv: 打不开");
  for (std::string line; std::getline(f, line);) {
    std::vector<std::string> col;
    for (size_t a = 0;;) {
      const size_t b = line.find('\t', a);
      col.push_back(line.substr(a, b - a));
      if (b == std::string::npos)
        break;
      a = b + 1;
    }
    assert(col.size() == 4 && "assets.tsv: 行须 code \\t name \\t set \\t weight");
    p.assets.push_back({col[0], col[1], col[2], col[3]});
  }
  assert(p.assets.size() == p.B && "assets.tsv: 行数与 B 轴不符");

  using namespace std::chrono;
  const auto day = [](int d) { return sys_days(year_month_day(year(d / 10000), month(static_cast<unsigned>(d / 100 % 100)), std::chrono::day(static_cast<unsigned>(d % 100)))); };
  const double years = static_cast<double>((day(ymd_of(p.time.back())) - day(ymd_of(p.time.front()))).count() + 1) / 365.25;
  p.bars_per_year = static_cast<double>(p.T) / years;
  return p;
}
