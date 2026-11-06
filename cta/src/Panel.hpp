#pragma once

// 面板 = CTA 库 (Db.hpp) 一个频率的只读映射: 资产 B × 时间 T 的根 [kFields] (开 高 低 收 额; 无根 = NaN)
//   1d: T = D, 1d.bin [D][B][kFields]; 1m: T = D · kSlots, 1m.bin [D][B][kSlots][kFields]
//   time[t] = YYYYMMDD (日) / YYYYMMDDHHMM (分钟, 标号 = 根结束分钟)
#include "Db.hpp"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <sys/mman.h>
#include <vector>

struct Panel {
  size_t D = 0, S = 0, B = 0, T = 0; // 日数 / 每日根数 / 资产 / 根数
  std::vector<int64_t> time;         // [T]
  std::vector<Asset> assets;         // [B]
  double bars_per_year = 0;
  std::shared_ptr<const float> x; // 映射
  const float *at(size_t b, size_t t) const { return x.get() + ((t / S * B + b) * S + t % S) * kFields; }
};

// 时间 → YYYYMMDD
inline int ymd_of(int64_t t) { return static_cast<int>(t >= 100000000 ? t / 10000 : t); }

// freq = 1d / 1m; 库须已 sync 到 t
inline Panel open_panel(const std::string &dir, const Table &t, const std::string &freq) {
  assert((freq == "1d" || freq == "1m") && "频率: 1d | 1m");
  db::State s;
  [[maybe_unused]] const bool ok = db::load(dir + "/head.bin", s);
  assert(ok && s.head.B == t.B() && s.head.D > 0 && "CTA 库缺或与篮子表不符");
  Panel p;
  p.D = s.head.D, p.S = freq == "1m" ? kSlots : 1, p.B = t.B(), p.T = p.D * p.S, p.assets = t.assets;
  const size_t bytes = p.T * p.B * kFields * sizeof(float);
  const std::string path = dir + "/" + freq + ".bin";
  const int fd = ::open(path.c_str(), O_RDONLY);
  assert(fd >= 0 && std::filesystem::file_size(path) == bytes && "CTA 库: 数据文件长度与头不符");
  void *m = ::mmap(nullptr, bytes, PROT_READ, MAP_SHARED, fd, 0);
  ::close(fd);
  assert(m != MAP_FAILED && "CTA 库: 映射失败");
  p.x = std::shared_ptr<const float>(static_cast<const float *>(m), [bytes](const float *q) { ::munmap(const_cast<float *>(q), bytes); });

  for (const int d : s.dates)
    for (size_t k = 0; k < p.S; ++k)
      p.time.push_back(p.S == 1 ? d : int64_t(d) * 10000 + slot_hhmm(k));
  using namespace std::chrono;
  const auto day = [](int d) { return sys_days(year_month_day(year(d / 10000), month(static_cast<unsigned>(d / 100 % 100)), std::chrono::day(static_cast<unsigned>(d % 100)))); };
  const double years = static_cast<double>((day(s.dates.back()) - day(s.dates.front())).count() + 1) / 365.25;
  p.bars_per_year = static_cast<double>(p.T) / years;
  return p;
}
