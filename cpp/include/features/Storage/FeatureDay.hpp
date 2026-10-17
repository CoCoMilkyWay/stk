#pragma once

#include "FeatureLevels.hpp" // 稳定层: level_info / feature_storage_t (不依赖字段表)
#include "SparseCodec.hpp"
#include "ZstdCodec.hpp" // IWYU pragma: keep
#include "misc/cross_platform.hpp"
#include "misc/date.hpp"
#include "misc/profiler.hpp"
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

// ============================================================================
// 特征日文件 — <base>/YYYY/MM/DD/features_<LVL>.bin, 一天一层一个
// ============================================================================
//
//   [FeatureFileHeader][ColRef × F][列载荷 × F]
//
//   头 + 目录定宽 (F = 该层列数, 编译期固定): 写端先空出, 逐列追加载荷,
//   收工回填头 + 目录, .tmp → rename 原子发布 (半写文件不可能以正式名存在).
//   读端一次拿头 + 目录, 所请求的列按偏移升序, 首尾相接的并成一段 (≤ kRunCap)
//   一次 pread, 逐列解码直写进 dst [n][T][A] (列主序 = 落盘单位, 无中转).
//   列载荷 = 一列 [T][A] 经 FeatureCodec 编码; ColRef.delta = 该列是否沿 T 轴
//   XOR 差分 (写端逐列按数据定, 见 write).
//   A / axis_hash = 库轴 (列 → 资产映射靠库轴顺序, 不在文件里; 读端从同一
//   config 推导同一库轴, O(1) 比对即确认列序一致); fingerprint = 字段表指纹.
//   magic (含格式版本) / 库轴 / 指纹 与读端期望不符 = 旧库 → Status 告知, 由库判废.
//   形状是编译期常量: 写端永远写满 (rows / field_count), 文件头 T/F 只做校验.
//   IO worker 按层序落盘, 最后一层的日文件兼任本日 commit 标记 (FeatureRead::has_date).
//
// 两端互逆, 共用一个 Scratch (稳态零分配):
//
//   FeatureDay::write(path, L = level_info(tier, lvl), layer [T][F][A], A, hash, s) → 字节数
//   FeatureDay::read (path, L, cols[n], dst [n][T][A], A, hash, s) → Status
//
// 与 orders 日文件 (shared/OrdersDay.hpp) 同形: 头 + 载荷 + 目录. 这里单写者顺序
// 追加且 F 已知, 目录放前部; orders 多 worker 并发追加, 目录只能放尾部.
// ============================================================================

struct FeatureFileHeader {
  static constexpr uint64_t kMagic = 0x0000325941445446; // 'F','T','D','A','Y','2' 小端

  uint64_t magic;
  uint64_t T;
  uint64_t F;
  uint64_t A;
  uint64_t axis_hash;
  uint64_t fingerprint;
};
static_assert(sizeof(FeatureFileHeader) == 48, "特征日文件头必须定宽无填充");

// 一列载荷在日文件里的位置 + 编码参数
struct ColRef {
  uint64_t offset;
  uint32_t size;
  uint32_t delta; // 1 = 沿 T 轴 XOR 差分
};
static_assert(sizeof(ColRef) == 16, "特征日文件目录项必须紧凑无填充");

inline std::string feature_day_dir(const std::string &base, misc::Ymd date) {
  return base + "/" + misc::slash(date);
}
inline std::string feature_file(const std::string &day_dir, size_t lvl) {
  return day_dir + "/features_" + level_name(lvl) + ".bin";
}

// ============================================================================
// 落盘编码选型: 两种实现同一 API (bound / encode / decode), 载荷不落自述信息,
// 读端按此别名解码 —— 换选型 = 换别名 + 重算特征库 (解码断言会拦住旧文件)
//   SparseCodec  位图+非零字面: 全天 ~45%, 内存带宽级, 差分还原融合进解码 (默认)
//   ZstdCodec    zstd 熵压缩:   全天 ~29%, 单核 ~185 MB/s 顶不住落盘节奏
// ============================================================================
using FeatureCodec = SparseCodec;

struct FeatureDay {
  struct Scratch {
    std::vector<ColRef> dir;               // 日文件目录 [F]
    std::vector<uint32_t> order;           // 读: 所请求列下标, 按载荷偏移升序 (并段用)
    std::vector<uint8_t> buf;              // 写: 一列载荷; 读: 一段载荷 (≤ kRunCap, 单列超限则为该列)
    std::vector<feature_storage_t> column; // 写: 抽出的相邻一组列 [nf][T][A] (编码可就地改写)
  };

  enum class Status : uint8_t {
    Ok,
    Missing,  // 文件不在 (库被外部清除)
    Format,   // magic 不符 (旧格式版本)
    AxisSize, // 库轴大小不符
    AxisHash, // 库轴指纹不符
    Fields,   // 字段表指纹不符
  };

  static const char *why(Status s) {
    switch (s) {
    case Status::Ok:
      return "";
    case Status::Missing:
      return "特征库已被清除";
    case Status::Format:
      return "日文件格式版本不符 (特征库是旧格式写的)";
    case Status::AxisSize:
      return "库轴大小不符 (特征库是旧 库清单/回测区间写的)";
    case Status::AxisHash:
      return "库轴指纹不符 (特征库与当前库清单/asset_axis.json 列序不一致)";
    case Status::Fields:
      return "字段表指纹不符 (特征文件是旧字段表写的)";
    }
    return "?";
  }

  // 一层一天: layer [T][F][A] 按相邻列组抽列 → 逐列编码 → 追加, 收工回填头 + 目录, rename 发布. 返回文件字节数.
  // 载荷与逐列抽取逐字节相同, 分组只改 layer 的读序.
  static size_t write(const std::string &path, const LevelInfo &L, const feature_storage_t *layer, size_t A,
                      std::uint64_t axis_hash, Scratch &s) {
    TraceN("FeatureDayWrite");
    const size_t T = L.rows, F = L.field_count;

    const std::string tmp_path = path + ".tmp";
    const int fd = file_open_trunc(tmp_path.c_str());
    assert(fd >= 0 && "特征日文件 .tmp 建不出来");

    const size_t words = T * A;
    const size_t group = std::clamp<size_t>(kGatherBytes / (words * sizeof(feature_storage_t)), 1, kGatherMaxCols);
    s.column.resize(group * words);
    s.buf.resize(FeatureCodec::bound(words));
    s.dir.resize(F);
    uint64_t end = sizeof(FeatureFileHeader) + F * sizeof(ColRef);
    bool delta[kGatherMaxCols];
    for (size_t f0 = 0; f0 < F; f0 += group) {
      const size_t nf = std::min(group, F - f0);
      gather_(layer, T, F, A, f0, nf, s.column.data(), delta);
      for (size_t j = 0; j < nf; ++j) {
        const size_t size = FeatureCodec::encode(s.column.data() + j * words, T, A, delta[j], s.buf.data(), s.buf.size());
        assert(size <= UINT32_MAX && "单列载荷超 4GB");
        const size_t written = file_pwrite(fd, s.buf.data(), size, end);
        assert(written == size && "特征日文件写入失败 (磁盘满?)");
        (void)written;
        s.dir[f0 + j] = {end, static_cast<uint32_t>(size), delta[j] ? 1u : 0u};
        end += size;
      }
    }

    const FeatureFileHeader head{FeatureFileHeader::kMagic, T, F, A, axis_hash, L.fingerprint};
    const size_t head_written = file_pwrite(fd, &head, sizeof(head), 0);
    const size_t dir_written = file_pwrite(fd, s.dir.data(), F * sizeof(ColRef), sizeof(head));
    assert(head_written == sizeof(head) && dir_written == F * sizeof(ColRef) && "特征日文件头 / 目录写入失败");
    (void)head_written;
    (void)dir_written;
    file_close(fd);

    std::filesystem::rename(tmp_path, path);
    return end;
  }

  // 一层一天: 读 cols[0..n) 到 dst [n][T][A] (第 i 槽 = 列 cols[i]). 非 Ok 时 dst 不动.
  static Status read(const std::string &path, const LevelInfo &L, const size_t *cols, size_t n, feature_storage_t *dst, size_t A,
                     std::uint64_t axis_hash, Scratch &s) {
    TraceN("FeatureDayRead");
    const size_t T = L.rows, F = L.field_count;

    const int fd = file_open_read(path.c_str());
    if (fd < 0)
      return Status::Missing;

    {
      TraceN("ReadHeader");
      FeatureFileHeader head{};
      const size_t got = file_pread(fd, &head, sizeof(head), 0);
      assert(got == sizeof(head) && "特征日文件短于头 (损坏)");
      (void)got;
      const Status st = head.magic != FeatureFileHeader::kMagic ? Status::Format
                        : head.A != A                           ? Status::AxisSize
                        : head.axis_hash != axis_hash           ? Status::AxisHash
                        : head.fingerprint != L.fingerprint     ? Status::Fields
                                                                : Status::Ok;
      if (st != Status::Ok) {
        file_close(fd);
        return st;
      }
      assert(head.T == T && head.F == F && "形状不符: 落盘形状恒为满 (rows / field_count)");

      s.dir.resize(F);
      const size_t dir_got = file_pread(fd, s.dir.data(), F * sizeof(ColRef), sizeof(head));
      assert(dir_got == F * sizeof(ColRef) && "特征日文件目录读不全");
      (void)dir_got;
    }

    s.order.resize(n);
    for (size_t i = 0; i < n; ++i) {
      assert(cols[i] < F && "列越界");
      s.order[i] = static_cast<uint32_t>(i);
    }
    std::sort(s.order.begin(), s.order.end(),
              [&](uint32_t a, uint32_t b) { return s.dir[cols[a]].offset < s.dir[cols[b]].offset; });

    for (size_t i = 0; i < n;) {
      const ColRef &first = s.dir[cols[s.order[i]]];
      const uint64_t begin = first.offset;
      uint64_t end = begin + first.size;
      size_t j = i + 1;
      while (j < n && s.dir[cols[s.order[j]]].offset == end && end - begin + s.dir[cols[s.order[j]]].size <= kRunCap)
        end += s.dir[cols[s.order[j++]]].size;

      {
        TraceN("ReadRun");
        const size_t len = static_cast<size_t>(end - begin);
        if (s.buf.size() < len)
          s.buf.resize(len);
        const size_t got = file_pread(fd, s.buf.data(), len, begin);
        assert(got == len && "特征日文件载荷越过文件末尾 (目录与文件不符)");
        (void)got;
      }

      TraceN("CodecDecode");
      for (; i < j; ++i) {
        const size_t slot = s.order[i];
        const ColRef &ref = s.dir[cols[slot]];
        FeatureCodec::decode(s.buf.data() + (ref.offset - begin), ref.size, dst + slot * T * A, T, A, ref.delta != 0);
      }
    }
    file_close(fd);
    return Status::Ok;
  }

private:
  // 首尾相接的列载荷并成一段读, 一段最多这么大
  static constexpr size_t kRunCap = size_t(4) << 20;

  // 抽列分组: 一组相邻列的抽列缓冲上限 (组内列数 = 它 / 单列字节, 夹到 [1, kGatherMaxCols]).
  // 每行这组列首尾相接, 一行一段顺序读 (预取器吃得到); 单列 [T][A] 跨距 F·A, 逐列抽是 T 次冷跳.
  // 缓冲要留在 L2/L3 里给紧随的编码读; 单列就超限的层 (L0) 退化为逐列.
  static constexpr size_t kGatherBytes = size_t(2) << 20;
  static constexpr size_t kGatherMaxCols = 32;

  // 抽相邻 nf 列 [f0, f0 + nf) → cols 第 j 段 [T][A] = 列 f0 + j, 同一遍数原值 / T 轴差分的非零字:
  // delta[j] = 差分零字更多才差分 (SparseCodec 载荷 ∝ 非零字数; 对 zstd 也是好代理)
  static void gather_(const feature_storage_t *layer, size_t T, size_t F, size_t A, size_t f0, size_t nf,
                      feature_storage_t *cols, bool *delta) {
    assert(nf >= 1 && nf <= kGatherMaxCols && f0 + nf <= F);
    const uint16_t *src = reinterpret_cast<const uint16_t *>(layer) + f0 * A;
    uint16_t *dst = reinterpret_cast<uint16_t *>(cols);
    const size_t stride = F * A, words = T * A;
    size_t nz_raw[kGatherMaxCols] = {}, nz_delta[kGatherMaxCols] = {};
    for (size_t j = 0; j < nf; ++j) {
      const uint16_t *row = src + j * A;
      uint16_t *out = dst + j * words;
      for (size_t a = 0; a < A; ++a) {
        const uint16_t v = row[a];
        out[a] = v;
        nz_raw[j] += v != 0;
      }
      nz_delta[j] = nz_raw[j];
    }
    for (size_t t = 1; t < T; ++t) {
      for (size_t j = 0; j < nf; ++j) {
        const uint16_t *row = src + t * stride + j * A;
        uint16_t *out = dst + j * words + t * A;
        const uint16_t *prev = out - A;
        size_t raw = 0, dif = 0;
        for (size_t a = 0; a < A; ++a) {
          const uint16_t v = row[a];
          out[a] = v;
          raw += v != 0;
          dif += (v ^ prev[a]) != 0;
        }
        nz_raw[j] += raw;
        nz_delta[j] += dif;
      }
    }
    for (size_t j = 0; j < nf; ++j)
      delta[j] = nz_delta[j] < nz_raw[j];
  }
};
