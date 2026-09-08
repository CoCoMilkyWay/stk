#pragma once

#include "FeatureStoreConfig.hpp" // 含落盘编码选型 FeatureCodec / CODEC_ENABLED
#include "misc/profiler.hpp"
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

// ============================================================================
// FEATURE READER - Hybrid Compressed Format
// ============================================================================
// Storage format (header = 5 × size_t: T, F, A, axis_hash, table_fingerprint), 每层按 LEVELS[lvl].columnar:
//   逐列 (L0/L1):     features_<LVL>_f{idx}.zst [Header: T,1,A,h][FeatureCodec column]  (Dist 按列选读)
// 载荷 = FeatureCodec (SparseCodec / ZstdCodec, 选型见 FeatureStoreConfig.hpp);
// xor_delta 层解码后再前缀 XOR 还原.
//
// 形状是编译期常量: 写端永远写满 (LEVELS[lvl].rows / width), 文件头的 T/F 只做
// 校验, 不做"实际维度" —— 消费端时间轴一律 level_valid_rows(lvl).
// A / axis_hash = universe 子轴大小 / 指纹 (UniverseAxis, 见 AssetAxis.hpp):
//   列 → 资产的映射不在文件里, 靠子轴顺序 (名单 → 全局轴下标升序). 读端构造
//   时带期望子轴 (A + hash), 逐文件精确比对 —— universe 名单/全局轴/特征库
//   任何一方漂移都立刻断言炸 (需重算特征), 不做兼容展宽.
// table_fingerprint = 写入时字段表指纹 (LEVELS[lvl].fingerprint):
//   字段表改了旧文件立刻断言失败, 不会静默错位.
//
// APIs (缓冲全部挂在张量结构里复用, 与写端 io_buf_/io_column_ 对仗, 稳态零分配):
//   1. load_day(date, DayTensor)          - GUI: 单日整层 (L0/L1 同一套; 整层文件直读零中转)
//   2. load_day_columns(date, cols, out)  - GUI overlay / Transform: 单日, 选列 (L0 逐列文件只碰 n 个)
//   3. load_month_columns()               - Dist/TimeSeries: 整月, 选列
// ============================================================================

class FeatureRead {
public:
  // 复用缓冲 (稳态零分配), 每个张量结构自带一份
  struct Scratch {
    std::vector<uint8_t> zbuf;           // 编码载荷 (仅 CODEC_ENABLED 时用)
    std::vector<feature_storage_t> tile; // 逐列交织 / 整层抽列中转
  };

private:
  // 读一个特征文件到 dst (T×F 行, 每行 A 个): 头校验 (子轴精确匹配) + 载荷落地.
  void read_file(const std::string &filepath, size_t lvl, size_t F,
                 feature_storage_t *dst, size_t A, Scratch &s) const {
    Trace;
    const size_t T = LEVELS[lvl].rows;
    constexpr size_t header_size = FEATURE_FILE_HEADER_WORDS * sizeof(size_t);

    std::ifstream file(filepath, std::ios::binary);
    assert(file.is_open() && "File not found");

    {
      TraceN("ReadHeader");
      size_t header[FEATURE_FILE_HEADER_WORDS];
      file.read(reinterpret_cast<char *>(header), header_size);
      assert(file.gcount() == static_cast<std::streamsize>(header_size));

      assert(header[0] == T && "T mismatch: 落盘形状恒为满 (LEVELS[lvl].rows)");
      assert(header[1] == F && "F mismatch: 落盘形状恒为满");
      assert(header[2] == A && header[2] == axis_A_ &&
             "A 不符: 特征文件与当前 universe 子轴大小不一致 (需重算特征)");
      assert(static_cast<std::uint64_t>(header[3]) == axis_hash_ &&
             "子轴指纹不符: 特征文件与当前 universe 名单/asset_axis.json 列序不一致 (需重算特征)");
      assert(static_cast<std::uint64_t>(header[4]) == LEVELS[lvl].fingerprint &&
             "字段表指纹不符: 特征文件是旧字段表写的 (需重算特征)");
    }

    size_t payload_size;
    {
      TraceN("GetFileSize");
      file.seekg(0, std::ios::end);
      payload_size = static_cast<size_t>(file.tellg()) - header_size;
      file.seekg(header_size, std::ios::beg);
    }

    const size_t raw_size = T * F * A * sizeof(feature_storage_t);

    if constexpr (!CODEC_ENABLED) {
      TraceN("ReadRaw"); // 无编码: 载荷直读进落点, 无中转
      assert(payload_size == raw_size && "payload size mismatch");
      file.read(reinterpret_cast<char *>(dst), payload_size);
      assert(file.gcount() == static_cast<std::streamsize>(payload_size));
    } else {
      {
        TraceN("ReadPayload");
        s.zbuf.resize(payload_size);
        file.read(reinterpret_cast<char *>(s.zbuf.data()), payload_size);
        assert(file.gcount() == static_cast<std::streamsize>(payload_size));
      }
      TraceN("CodecDecode");
      FeatureCodec::decode(s.zbuf.data(), payload_size, dst, raw_size);
    }

    // T 轴 XOR 差分还原 (写端 disk_write 对 xor_delta 层压缩前编码)
    if (LEVELS[lvl].xor_delta) {
      TraceN("XorDeltaDecode");
      xor_delta_decode(dst, T, F * A);
    }
  }

  // 装载原语: 某日某层的字段集合 → dst [T][n][A]. fields == nullptr 取全部列
  // (n == field_count); 整层文件 + 全列时直读 dst, 零中转.
  void load_fields(const std::string &date, size_t lvl, const size_t *fields, size_t n,
                   feature_storage_t *dst, size_t A, Scratch &s) const {
    const auto &L = LEVELS[lvl];
    const std::string day_dir = feature_day_dir(base_dir_, date);
    const size_t T = L.rows;
    assert(n <= L.field_count && (fields || n == L.field_count));

    if (L.columnar) {
      // 逐列文件: 列文件下标 == 字段下标 (columnar 层全部宽 1), 读一列交织一列
      assert(L.width == L.field_count && "columnar level must be all width-1 fields");
      s.tile.resize(T * A);
      for (size_t i = 0; i < n; ++i) {
        const size_t f = fields ? fields[i] : i;
        read_file(feature_column_file(day_dir, lvl, f), lvl, 1, s.tile.data(), A, s);
        TraceN("InterleaveColumn");
        for (size_t t = 0; t < T; ++t)
          std::memcpy(dst + (t * n + i) * A, s.tile.data() + t * A, A * sizeof(feature_storage_t));
      }
    } else if (!fields) {
      read_file(feature_file(day_dir, lvl), lvl, L.width, dst, A, s); // 整层全列: 直读
    } else {
      // 整层文件选列: 读整天到 tile 再抽列
      s.tile.resize(T * L.width * A);
      read_file(feature_file(day_dir, lvl), lvl, L.width, s.tile.data(), A, s);
      TraceN("ExtractFeatures");
      for (size_t t = 0; t < T; ++t)
        for (size_t i = 0; i < n; ++i) {
          assert(L.fields[fields[i]].width == 1 && "column selection is per width-1 field");
          std::memcpy(dst + (t * n + i) * A, s.tile.data() + (t * L.width + L.offsets[fields[i]]) * A, A * sizeof(feature_storage_t));
        }
    }
  }

public:
  // 选列区间张量 (Dist 任意日期集 / TimeSeries 整月), 布局 [N_days × T][F_selected][A];
  // 日步长恒为 LEVELS[level].rows (含末尾哨兵行), 时间轴消费用 level_valid_rows(level)
  struct MonthTensor {
    std::vector<std::string> dates;      // [N_days]
    std::vector<feature_storage_t> data; // [N_days*T × F_selected × A]
    size_t A = 0;
    size_t level = 0;
    size_t max_days = 0;
    size_t max_features = 0;
    std::vector<size_t> feature_indices;
    Scratch scratch;

    // 日 d 的时间轴起点 (定步长, 免存偏移表)
    size_t day_start(size_t d) const { return d * LEVELS[level].rows; }

    void preallocate(size_t A_, size_t max_days_, size_t max_features_, size_t level_) {
      A = A_;
      level = level_;
      max_days = max_days_;
      max_features = max_features_;
      data.resize(max_days * LEVELS[level].rows * max_features * A);
    }

    void reset() {
      dates.clear();
      feature_indices.clear();
      // data/scratch 不清, 只复用
    }
  };

  // base_dir = 该 universe 的特征库目录 (Config::FeatureUniverseDir);
  // axis_A / axis_hash = 期望子轴 (universe_axis(cfg).size() / .hash)
  FeatureRead(const std::string &base_dir, size_t axis_A, std::uint64_t axis_hash)
      : base_dir_(base_dir), axis_A_(axis_A), axis_hash_(axis_hash) {
    assert(axis_A_ > 0 && "期望子轴为空");
  }

  // ========================================================================
  // Single Day Loading (GUI: 单日整层, 任一层)
  // ========================================================================

  // 单日单层张量 [T][F_total][A]: 一个实例 = 一层, 形状是 LEVELS[level] 编译期常量
  struct DayTensor {
    std::string date;
    size_t level = 0;
    size_t A = 0;
    std::vector<feature_storage_t> data;
    Scratch scratch;

    // 宽字段用 sub 取档内下标; LVL 模板参数保住编译期定址
    template <size_t LVL>
    inline feature_storage_t get(size_t t, size_t field, size_t a, size_t sub = 0) const {
      static_assert(LVL < LEVEL_COUNT);
      assert(LVL == level && "DayTensor level mismatch");
      assert(t < LEVELS[LVL].rows && a < A && sub < LEVELS[LVL].fields[field].width);
      return data[(t * LEVELS[LVL].width + LEVELS[LVL].offsets[field] + sub) * A + a];
    }

    void preallocate(size_t A_, size_t level_) {
      A = A_;
      level = level_;
      data.resize(LEVELS[level].rows * LEVELS[level].width * A);
    }
  };

  // Load single day, all features of out.level (for GUI)
  void load_day(const std::string &date, DayTensor &out) const {
    Trace;
    assert(date.size() == 8);
    assert(out.A > 0 && "Must preallocate() before load_day()");
    out.date = date;
    load_fields(date, out.level, nullptr, LEVELS[out.level].field_count, out.data.data(), out.A, out.scratch);
  }

  // ========================================================================
  // Single Day Selected Columns (GUI 单列 overlay / Transform 单列拼接)
  // ========================================================================

  // 单日选列张量 [T][n][A]: load_day 的选列版, 布局与 MonthTensor 的单日切片同构.
  // 列宽必须为 1 (与 load_month_columns 同约束); L0/L1 逐列文件下只读 n 个列文件,
  // dst 内存都只有 [T][n][A].
  struct DayColumns {
    std::string date;
    size_t level = 0;
    size_t A = 0;
    size_t max_features = 0;
    std::vector<size_t> fields;          // [n] 字段下标
    std::vector<feature_storage_t> data; // [T × n × A]
    Scratch scratch;

    inline feature_storage_t get(size_t t, size_t i, size_t a) const {
      assert(t < LEVELS[level].rows && i < fields.size() && a < A);
      return data[(t * fields.size() + i) * A + a];
    }

    void preallocate(size_t A_, size_t level_, size_t max_features_) {
      A = A_;
      level = level_;
      max_features = max_features_;
      data.resize(LEVELS[level].rows * max_features * A);
    }
  };

  // Load single day, selected width-1 fields
  void load_day_columns(const std::string &date, const std::vector<size_t> &feature_indices, DayColumns &out) const {
    Trace;
    assert(date.size() == 8);
    assert(out.A > 0 && "Must preallocate() before load_day_columns()");
    assert(!feature_indices.empty() && feature_indices.size() <= out.max_features);
    out.date = date;
    out.fields = feature_indices;
    load_fields(date, out.level, out.fields.data(), out.fields.size(), out.data.data(), out.A, out.scratch);
  }

  // ========================================================================
  // Batch Monthly Loading (for Dist analysis)
  // ========================================================================

  // 单日选列载入到区间张量的第 day_idx 槽 (Dist 抽样流式: 逐日 IO, 进度/取消粒度归调用方)
  // dates/feature_indices 由调用方维护
  void load_date_columns_into(const std::string &date, const std::vector<size_t> &feature_indices,
                              MonthTensor &out, size_t day_idx) const {
    assert(out.A > 0 && "Must preallocate() before load_date_columns_into()");
    const size_t n = feature_indices.size();
    assert(n <= out.max_features && day_idx < out.max_days);
    load_fields(date, out.level, feature_indices.data(), n,
                out.data.data() + out.day_start(day_idx) * n * out.A, out.A, out.scratch);
  }

  void load_month_columns(
      const std::string &year,
      const std::string &month,
      const std::vector<size_t> &feature_indices,
      MonthTensor &out) const {
    Trace;

    assert(out.A > 0 && "Must preallocate() before load_month_columns()");
    assert(feature_indices.size() <= out.max_features && "Feature count exceeds preallocated");

    out.reset();
    out.feature_indices = feature_indices;

    {
      TraceN("ListDates");
      out.dates = list_dates(year, month);
      assert(!out.dates.empty() && "No dates found");
      assert(out.dates.size() <= out.max_days && "Day count exceeds preallocated");
    }

    for (size_t day_idx = 0; day_idx < out.dates.size(); ++day_idx) {
      TraceN("LoadDay");
      TraceTextS(out.dates[day_idx].c_str());
      load_date_columns_into(out.dates[day_idx], feature_indices, out, day_idx);
    }
  }

  // ========================================================================
  // Utility Functions
  // ========================================================================

  // 该日是否有特征文件 (以最后一层最后一列文件为准: IO worker 按层序落盘, 它在则全在)
  static bool has_date(const std::string &base_dir, const std::string &date) {
    assert(date.size() == 8);
    const auto &L = LEVELS[LEVEL_COUNT - 1];
    return std::filesystem::exists(feature_column_file(feature_day_dir(base_dir, date), LEVEL_COUNT - 1, L.width - 1));
  }
  bool has_date(const std::string &date) const { return has_date(base_dir_, date); }

  std::vector<std::string> list_dates(const std::string &year, const std::string &month) const {
    std::vector<std::string> dates;
    std::string dir = base_dir_ + "/" + year + "/" + month;

    if (!std::filesystem::exists(dir))
      return dates;

    for (const auto &entry : std::filesystem::directory_iterator(dir)) {
      if (entry.is_directory()) {
        std::string day = entry.path().filename().string();
        if (day.size() == 2) {
          std::string date = year + month + day;
          if (has_date(date)) {
            dates.push_back(date);
          }
        }
      }
    }

    std::sort(dates.begin(), dates.end());
    return dates;
  }

  std::string base_dir_;
  size_t axis_A_;
  std::uint64_t axis_hash_;
};
