#pragma once

// CTA 库 (cta/data): 篮子 (指数 / 行业 / 概念) 的日线 + 1 分钟线, 由 all.bar 特征库 + 最近一份概念快照合成
//   篮子表 (线序): 全A 等权 / 全A 加权, 申万一级 sw01 … sw31 (加权), 子池 (等权; 字段表 L_1day 的 Pool 类 pool_* 列, 列序), 概念 (加权; 快照行序)
//     测试集 set: idx = 全A 两权 + 子池 / ind = 行业 / concept = 概念
//   交易日 D 的成员 / 权重 / 基准取前一落盘日 (D−1) 的 L_1day 收盘行:
//     全A = cs_valid ∧ 收 > 0; 行业 = 全A ∧ ind_l1 = k; 概念 = 全A ∧ 在快照该概念成分里; 子池 = pool_<池> > 0.5 ∧ 收 > 0
//     等权 = 1, 加权 = 收 × total_shares (缺股本不进); 基准 = pre_close (该行值 = 次日除权后前收)
//   点位 = 前日点位 × Σ w · 价 / 基准 / Σ w, 开 / 高 / 低 / 收各自如此 (高 / 低 = 成员高低的加权, 真实路径的外包); 额 = 成员之和
//     日根 = L_1day open_day … close_day / amount_day (当日无成交的成员比 1)
//     分钟根 = L_1min 行 (ts_valid ∧ 收 > 0) 并进 240 槽: 槽 s 标号 09:31 … 11:30, 13:01 … 15:00, 竞价段 + 09:30 并进首槽;
//       成员缺根沿用其上根的比 (首根前 = 1); 当日全库末个有根的槽之后无根
//     点位自库首日 kBase 起逐日接续; 无成员 / 无成交的日该篮子无根, 点位不动
// 落盘 (日主序, 续算只追加):
//   head.bin = Head + dates [D] i32 + level [B] f64 (末日点位); 1d.bin = f32 [D][B][kFields]; 1m.bin = f32 [D][B][kSlots][kFields]; 无根 = NaN
//   Head 的键 (库轴 / 概念 / 子池 / 两层字段表指纹 / 库首日) 对得上且已合成日是库落盘日的前缀 → 只合成其后的新日, 否则从头合成
#include "Lib.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <string>
#include <thread>
#include <vector>

enum Field : int { kOpen,
                   kHigh,
                   kLow,
                   kClose,
                   kAmount,
                   kFields };

inline constexpr size_t kSlots = 240;
inline constexpr double kBase = 1000;

// 槽 → 标号 hhmm
inline int slot_hhmm(size_t s) {
  const int m = s < 120 ? 9 * 60 + 31 + static_cast<int>(s) : 13 * 60 + 1 + static_cast<int>(s - 120);
  return m / 60 * 100 + m % 60;
}

struct Asset {
  std::string code, name;
  std::string set; // 测试集: idx / ind / concept
};

// features/Method/Fundamental.hpp SW2021_L1_NAMES
inline constexpr const char *kSw[] = {"未知", "交通运输", "传媒", "公用事业", "农林牧渔", "医药生物", "商贸零售", "国防军工", "基础化工", "家用电器", "建筑材料",
                                      "建筑装饰", "房地产", "有色金属", "机械设备", "汽车", "煤炭", "环保", "电力设备", "电子", "石油石化", "社会服务",
                                      "纺织服饰", "综合", "美容护理", "计算机", "轻工制造", "通信", "钢铁", "银行", "非银金融", "食品饮料"};
inline constexpr size_t kInds = std::size(kSw); // ind_l1 ∈ [1, kInds) 进行业, 0 = 未知只进全A

struct Table {
  std::vector<Asset> assets;                      // [B]
  size_t pools = 0;                               // 子池数
  std::vector<size_t> pool_col;                   // [子池] L_1day 列
  std::vector<std::vector<uint32_t>> concepts_of; // [A] 所属概念 (快照行序)
  uint64_t concepts_fp = kFnvOffset, pools_fp = kFnvOffset;
  int snapshot = 0; // 概念快照日

  static constexpr size_t kAllEq = 0, kAllCap = 1, kInd0 = 2; // 行业 k 的线 = kInd0 + k − 1
  size_t pool_line(size_t p) const { return kInd0 + kInds - 1 + p; }
  size_t concept_line(size_t k) const { return pool_line(pools) + k; }
  size_t B() const { return assets.size(); }
};

inline Table make_table(const Lib &L, const std::string &concept_dir) {
  Table t;
  t.assets.push_back({"all.eq", "全A.等", "idx"});
  t.assets.push_back({"all.cap", "全A.加", "idx"});
  for (size_t k = 1; k < kInds; ++k) {
    char c[16];
    std::snprintf(c, sizeof(c), "sw%02zu.cap", k);
    t.assets.push_back({c, std::string(kSw[k]) + ".加", "ind"});
  }
  std::vector<const Json *> ps;
  for (const Json &f : L.fields[kL1day.name].arr)
    if (f["type"].str == "CS" && f["cat_l1"].str == "Pool" && f["code"].str.starts_with("pool_"))
      ps.push_back(&f);
  std::sort(ps.begin(), ps.end(), [](const Json *a, const Json *b) { return (*a)["col"].num < (*b)["col"].num; });
  for (const Json *f : ps) {
    const std::string name = (*f)["code"].str.substr(5);
    t.assets.push_back({name + ".eq", (*f)["name_cn"].str + ".等", "idx"});
    t.pool_col.push_back(static_cast<size_t>((*f)["col"].num));
    t.pools_fp = fnv(t.pools_fp, name);
  }
  t.pools = ps.size();

  std::string last;
  for (const auto &e : std::filesystem::directory_iterator(concept_dir))
    if (e.path().extension() == ".tsv")
      last = std::max(last, e.path().filename().string());
  assert(!last.empty() && "概念快照目录里没有 .tsv");
  t.snapshot = std::stoi(last);
  std::map<std::string, uint32_t> at;
  for (size_t a = 0; a < L.A(); ++a)
    at[L.codes[a]] = static_cast<uint32_t>(a);
  t.concepts_of.resize(L.A());
  std::ifstream f(concept_dir + "/" + last);
  for (std::string line; std::getline(f, line);) {
    const size_t p = line.find('\t'), q = line.find('\t', p + 1);
    assert(q != std::string::npos && "概念快照: 行须 代码 \\t 名称 \\t 成分");
    const std::string code = line.substr(0, p);
    const uint32_t k = static_cast<uint32_t>(t.B() - t.concept_line(0));
    t.assets.push_back({code + ".cap", line.substr(p + 1, q - p - 1) + ".加", "concept"});
    t.concepts_fp = fnv(t.concepts_fp, code);
    for (size_t a = q + 1; a < line.size();) {
      const size_t b = std::min(line.find(',', a), line.size());
      if (const auto it = at.find(line.substr(a, b - a)); it != at.end()) {
        t.concepts_of[it->second].push_back(k);
        t.concepts_fp = fnv(t.concepts_fp, it->first);
      }
      a = b + 1;
    }
  }
  return t;
}

namespace db {

struct Head {
  uint64_t magic = 0x3141544443415443ULL; // "CTADATA1"
  uint64_t axis = 0, concepts = 0, pools = 0, fp_day = 0, fp_min = 0;
  uint32_t A = 0, B = 0;
  int32_t base = 0; // 库首日 (点位基日, 无根)
  uint32_t D = 0;   // 已合成日数
  bool same_key(const Head &o) const {
    return magic == o.magic && axis == o.axis && concepts == o.concepts && pools == o.pools && fp_day == o.fp_day && fp_min == o.fp_min && A == o.A &&
           B == o.B && base == o.base;
  }
};

struct State {
  Head head;
  std::vector<int> dates;    // [D]
  std::vector<double> level; // [B]
};

inline bool load(const std::string &path, State &s) {
  std::ifstream f(path, std::ios::binary);
  if (!f)
    return false;
  f.read(reinterpret_cast<char *>(&s.head), sizeof(Head));
  s.dates.resize(s.head.D), s.level.resize(s.head.B);
  f.read(reinterpret_cast<char *>(s.dates.data()), static_cast<std::streamsize>(s.dates.size() * sizeof(int)));
  f.read(reinterpret_cast<char *>(s.level.data()), static_cast<std::streamsize>(s.level.size() * sizeof(double)));
  assert(f && f.peek() == EOF && "head.bin: 长度与头不符");
  return true;
}

inline void save(const std::string &path, const State &s) {
  const std::string tmp = path + ".tmp";
  {
    std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
    f.write(reinterpret_cast<const char *>(&s.head), sizeof(Head));
    f.write(reinterpret_cast<const char *>(s.dates.data()), static_cast<std::streamsize>(s.dates.size() * sizeof(int)));
    f.write(reinterpret_cast<const char *>(s.level.data()), static_cast<std::streamsize>(s.level.size() * sizeof(double)));
    assert(f && "head.bin: 写不了");
  }
  std::filesystem::rename(tmp, path);
}

// L_1day 读的列 (下标同 DayCol), 子池列接在 kPools 之后
enum DayCol : size_t { kCs,
                       kTs,
                       kO,
                       kH,
                       kL,
                       kC,
                       kAmt,
                       kPre,
                       kShares,
                       kInd,
                       kPools };
enum MinCol : size_t { kMTs,
                       kMO,
                       kMH,
                       kML,
                       kMC,
                       kMAmt };

// 一日的合成累加: 格 0 = 日根, 格 1 + s = 槽 s; 一格 = Σ w · 比 (开 高 低 收) + Σ 额
inline constexpr size_t kCells = 1 + kSlots, kQ = 5;
struct Acc {
  std::vector<double> sw;  // [B] Σ w
  std::vector<double> acc; // [B][kCells][kQ]
  int end = -1;            // 有根的末槽 (全部篮子; -1 = 当日无分钟)
};

struct Worker {
  struct Bar {
    float o, h, l, c;
    double amt;
  };
  Cols prev, cur, min;
  std::vector<std::pair<uint32_t, float>> ws;
  std::array<double, kCells * kQ> r{};
  std::array<Bar, kSlots> bar{};
  std::array<bool, kSlots> has{};

  void day(const Lib &L, const Table &t, const std::vector<size_t> &dcols, const std::vector<size_t> &mcols, int d0, int d, Acc &out) {
    prev.read(L, d0, kL1day, dcols);
    cur.read(L, d, kL1day, dcols);
    min.read(L, d, kL1min, mcols);
    const size_t B = t.B();
    out.sw.assign(B, 0), out.acc.assign(B * kCells * kQ, 0), out.end = -1;
    for (size_t a = 0; a < L.A(); ++a) {
      const float c = prev.at(kC, 0, a), cap = c * prev.at(kShares, 0, a), base = prev.at(kPre, 0, a);
      ws.clear();
      if (c > 0 && prev.at(kCs, 0, a) > 0.5f) {
        ws.emplace_back(Table::kAllEq, 1.f);
        if (cap > 0) {
          ws.emplace_back(Table::kAllCap, cap);
          const float k = prev.at(kInd, 0, a);
          if (k >= 1.f && k < static_cast<float>(kInds))
            ws.emplace_back(Table::kInd0 + static_cast<size_t>(k + 0.5f) - 1, cap);
          for (const uint32_t j : t.concepts_of[a])
            ws.emplace_back(t.concept_line(j), cap);
        }
      }
      for (size_t p = 0; c > 0 && p < t.pools; ++p)
        if (prev.at(kPools + p, 0, a) > 0.5f)
          ws.emplace_back(t.pool_line(p), 1.f);
      if (ws.empty() || !(base > 0))
        continue;
      const double inv = 1.0 / base;

      const float x = cur.at(kC, 0, a);
      if (cur.at(kTs, 0, a) > 0.5f && x > 0) {
        const auto px = [&](size_t i) { const float v = cur.at(i, 0, a); return v > 0 ? v : x; };
        r[0] = px(kO) * inv, r[1] = px(kH) * inv, r[2] = px(kL) * inv, r[3] = x * inv, r[4] = std::expm1(cur.at(kAmt, 0, a));
      } else
        r[0] = r[1] = r[2] = r[3] = 1, r[4] = 0;

      has.fill(false);
      for (size_t row = 0; row + 1 < kL1min.rows; ++row) {
        const float mc = min.at(kMC, row, a);
        if (!(min.at(kMTs, row, a) > 0.5f && mc > 0))
          continue;
        const size_t s = row <= kAuctionRows ? 0 : row - kAuctionRows;
        const auto px = [&](size_t i) { const float v = min.at(i, row, a); return v > 0 ? v : mc; };
        const Bar x{px(kMO), px(kMH), px(kML), mc, std::expm1(min.at(kMAmt, row, a))};
        Bar &q = bar[s];
        if (!has[s])
          q = x, has[s] = true;
        else
          q.h = std::max(q.h, x.h), q.l = std::min(q.l, x.l), q.c = x.c, q.amt += x.amt;
      }
      double last = 1;
      for (size_t s = 0; s < kSlots; ++s) {
        double *q = r.data() + (1 + s) * kQ;
        if (has[s]) {
          const Bar &b = bar[s];
          q[0] = b.o * inv, q[1] = b.h * inv, q[2] = b.l * inv, q[3] = b.c * inv, q[4] = b.amt;
          last = q[3], out.end = std::max(out.end, static_cast<int>(s));
        } else
          q[0] = q[1] = q[2] = q[3] = last, q[4] = 0;
      }

      for (const auto &[line, w] : ws) {
        out.sw[line] += w;
        double *o = out.acc.data() + line * kCells * kQ;
        for (size_t j = 0; j < kCells * kQ; j += kQ) {
          o[j] += w * r[j], o[j + 1] += w * r[j + 1], o[j + 2] += w * r[j + 2], o[j + 3] += w * r[j + 3];
          o[j + 4] += r[j + 4];
        }
      }
    }
  }
};

inline void pwrite_all(int fd, const void *p, size_t n, size_t off) {
  [[maybe_unused]] const ssize_t got = ::pwrite(fd, p, n, static_cast<off_t>(off));
  assert(got == static_cast<ssize_t>(n) && "CTA 库: 写不了 (磁盘满?)");
}

} // namespace db

// 合成到库末个落盘日 (缺 / 键不符 → 从头; 否则续算新日)
inline void sync(const Lib &L, const Table &t, const std::string &dir) {
  std::filesystem::create_directories(dir);
  const std::string head_path = dir + "/head.bin";
  db::State s;
  db::Head key;
  key.axis = L.axis, key.concepts = t.concepts_fp, key.pools = t.pools_fp, key.fp_day = L.fp_day, key.fp_min = L.fp_min;
  key.A = static_cast<uint32_t>(L.A()), key.B = static_cast<uint32_t>(t.B()), key.base = L.dates.front();
  const bool resume = db::load(head_path, s) && s.head.same_key(key) && s.dates.size() < L.dates.size() &&
                      std::equal(s.dates.begin(), s.dates.end(), L.dates.begin() + 1);
  if (!resume)
    s = {key, {}, std::vector<double>(t.B(), kBase)};
  const size_t i0 = 1 + s.dates.size(), N = L.dates.size(), B = t.B();
  if (i0 == N)
    return;

  std::vector<size_t> dcols = {L.col(kL1day, "cs_valid"), L.col(kL1day, "ts_valid"), L.col(kL1day, "open_day"), L.col(kL1day, "high_day"),
                               L.col(kL1day, "low_day"), L.col(kL1day, "close_day"), L.col(kL1day, "amount_day"), L.col(kL1day, "pre_close"),
                               L.col(kL1day, "total_shares"), L.col(kL1day, "ind_l1")};
  dcols.insert(dcols.end(), t.pool_col.begin(), t.pool_col.end());
  const std::vector<size_t> mcols = {L.col(kL1min, "ts_valid"), L.col(kL1min, "open"), L.col(kL1min, "high"),
                                     L.col(kL1min, "low"), L.col(kL1min, "close"), L.col(kL1min, "amount")};

  const int fd_day = ::open((dir + "/1d.bin").c_str(), O_WRONLY | O_CREAT, 0644), fd_min = ::open((dir + "/1m.bin").c_str(), O_WRONLY | O_CREAT, 0644);
  assert(fd_day >= 0 && fd_min >= 0 && "CTA 库: 数据文件建不出来");
  const size_t row_day = B * kFields, row_min = B * kSlots * kFields;
  [[maybe_unused]] const int tr = ::ftruncate(fd_day, static_cast<off_t>((i0 - 1) * row_day * sizeof(float))) |
                                  ::ftruncate(fd_min, static_cast<off_t>((i0 - 1) * row_min * sizeof(float)));
  assert(tr == 0);
  std::printf("[db] %s %s → %d … %d 共 %zu 日 | %zu 篮子 (概念快照 %d) | A = %zu\n", resume ? "续算" : "从头合成", dir.c_str(), L.dates[i0], L.dates[N - 1],
              N - i0, B, t.snapshot, L.A());

  const size_t T = std::max(1u, std::thread::hardware_concurrency()), batch = 2 * T;
  std::vector<db::Worker> wk(T);
  std::vector<db::Acc> acc(batch);
  std::vector<float> out_day(row_day), out_min(row_min);
  constexpr float kNaN = std::numeric_limits<float>::quiet_NaN();
  size_t short_days = 0;
  for (size_t b0 = i0; b0 < N; b0 += batch) {
    const size_t n = std::min(batch, N - b0);
    {
      std::atomic<size_t> next{0};
      std::vector<std::jthread> th;
      for (size_t w = 0; w < T; ++w)
        th.emplace_back([&, w] {
          for (size_t k; (k = next++) < n;)
            wk[w].day(L, t, dcols, mcols, L.dates[b0 + k - 1], L.dates[b0 + k], acc[k]);
        });
    }
    for (size_t k = 0; k < n; ++k) {
      const db::Acc &a = acc[k];
      short_days += a.end + 1 < static_cast<int>(kSlots);
      std::fill(out_day.begin(), out_day.end(), kNaN), std::fill(out_min.begin(), out_min.end(), kNaN);
      for (size_t b = 0; b < B; ++b) {
        const double *q = a.acc.data() + b * db::kCells * db::kQ;
        if (!(a.sw[b] > 0 && q[4] > 0 && s.level[b] > 0))
          continue;
        const double f = s.level[b] / a.sw[b];
        const auto put = [&](float *o, const double *c) { o[0] = static_cast<float>(c[0] * f), o[1] = static_cast<float>(c[1] * f), o[2] = static_cast<float>(c[2] * f), o[3] = static_cast<float>(c[3] * f), o[4] = static_cast<float>(c[4]); };
        put(out_day.data() + b * kFields, q);
        for (int sl = 0; sl <= a.end; ++sl)
          put(out_min.data() + (b * kSlots + static_cast<size_t>(sl)) * kFields, q + (1 + static_cast<size_t>(sl)) * db::kQ);
        s.level[b] = out_day[b * kFields + kClose];
      }
      const size_t i = b0 + k - 1; // 落盘下标
      db::pwrite_all(fd_day, out_day.data(), row_day * sizeof(float), i * row_day * sizeof(float));
      db::pwrite_all(fd_min, out_min.data(), row_min * sizeof(float), i * row_min * sizeof(float));
      s.dates.push_back(L.dates[b0 + k]);
    }
    std::printf("\r[db] %d  %zu / %zu", L.dates[b0 + n - 1], b0 + n - i0, N - i0);
    std::fflush(stdout);
  }
  ::close(fd_day), ::close(fd_min);
  s.head.D = static_cast<uint32_t>(s.dates.size());
  db::save(head_path, s);
  std::printf("\n[db] 合成完 %u 日 (%zu 日分钟不足 %zu 槽)\n", s.head.D, short_days, kSlots);
}
