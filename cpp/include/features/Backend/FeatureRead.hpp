#pragma once

#include "FeatureStoreConfig.hpp"
#include "ZstdHelper.hpp"
#include "misc/profiler.hpp"
#include "shared/AssetAxis.hpp"
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
//   逐列 (L0):        features_L0_f{idx}.zst   [Header: T,1,A,h][Zstd column]   (Dist 按列选读)
//   整层 (L1/DEPTH):  features_<LVL>.zst       [Header: T,F,A,h][Zstd merged]
//
// 形状是编译期常量: 写端永远写满 (LEVELS[lvl].rows / width), 文件头的 T/F 只做
// 校验, 不做"实际维度" —— 消费端时间轴一律 level_valid_rows(lvl).
// axis_hash = AssetAxis::hash_at(A_file): 列 → 资产的映射不在文件里, 靠 A 轴顺序.
//   轴 append-only, 前缀指纹永久有效 → 旧文件 (A_file < 当前 A) 照样可读:
//   逐行展宽, 新增资产列清零 (_data_valid = 0, 消费端天然视为无效).
// table_fingerprint = 写入时字段表指纹 (LEVELS[lvl].fingerprint):
//   字段表改了旧文件立刻断言失败, 不会静默错位.
//
// APIs (缓冲全部挂在张量结构里复用, 与写端 io_buf_/io_column_ 对仗, 稳态零分配):
//   1. load_day(date, DayTensor)   - GUI: 单日整层 (L0/L1/DEPTH 同一套; 整层文件直读零中转)
//   2. load_month_columns()        - Dist/TimeSeries: 整月, 选列
// ============================================================================

class FeatureRead {
public:
  // 复用缓冲 (稳态零分配), 每个张量结构自带一份
  struct Scratch {
    std::vector<uint8_t> zbuf;             // 压缩载荷 (仅 COMPRESSION_LEVEL > 0 时用)
    std::vector<feature_storage_t> narrow; // 旧文件 (A_file < A) 的展宽中转
    std::vector<feature_storage_t> tile;   // 逐列交织 / 整层抽列中转
  };

private:
  // 读一个特征文件到 dst (T×F 行, 每行 A 个): 头校验 + 载荷落地.
  // A 轴前缀兼容: A_file <= A; 旧文件解压到 narrow 再逐行展宽, 尾部资产清零.
  void read_file(const std::string &filepath, size_t lvl, size_t F,
                 feature_storage_t *dst, size_t A, Scratch &s) const {
    Trace;
    const size_t T = LEVELS[lvl].rows;
    constexpr size_t header_size = FEATURE_FILE_HEADER_WORDS * sizeof(size_t);

    std::ifstream file(filepath, std::ios::binary);
    assert(file.is_open() && "File not found");

    size_t A_file;
    {
      TraceN("ReadHeader");
      size_t header[FEATURE_FILE_HEADER_WORDS];
      file.read(reinterpret_cast<char *>(header), header_size);
      assert(file.gcount() == static_cast<std::streamsize>(header_size));

      assert(header[0] == T && "T mismatch: 落盘形状恒为满 (LEVELS[lvl].rows)");
      assert(header[1] == F && "F mismatch: 落盘形状恒为满");
      A_file = header[2];
      assert(A_file <= A && "A 轴回缩: 特征文件比当前 asset_axis.json 还宽 (需重算特征)");

      // A 轴列序锁定: 文件头存的是写入时 AssetAxis::hash_at(A_file) — 轴 append-only,
      // 该值只依赖前 A_file 条, 所以历史文件的指纹永久有效. 一次 O(1) 比对即可确认
      // "列 → 资产的映射没有漂移", 轴被重排/截断或文件被换掉都会立刻炸.
      assert(static_cast<std::uint64_t>(header[3]) == asset_axis().hash_at(A_file) &&
             "A 轴指纹不符: 特征文件与当前 asset_axis.json 列序不一致 (需重算特征)");
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

    const size_t rows = T * F; // 每行 A_file 个
    const size_t raw_size = rows * A_file * sizeof(feature_storage_t);
    feature_storage_t *landing = dst;
    if (A_file < A) {
      s.narrow.resize(rows * A_file);
      landing = s.narrow.data();
    }

    if constexpr (ZstdHelper::COMPRESSION_LEVEL == 0) {
      TraceN("ReadRaw"); // 无压缩: 载荷直读进落点, 无中转
      assert(payload_size == raw_size && "payload size mismatch");
      file.read(reinterpret_cast<char *>(landing), payload_size);
      assert(file.gcount() == static_cast<std::streamsize>(payload_size));
    } else {
      {
        TraceN("ReadCompressed");
        s.zbuf.resize(payload_size);
        file.read(reinterpret_cast<char *>(s.zbuf.data()), payload_size);
        assert(file.gcount() == static_cast<std::streamsize>(payload_size));
      }
      TraceN("Decompress");
      ZstdHelper::decompress(s.zbuf.data(), payload_size, landing, raw_size);
    }

    if (A_file < A) {
      TraceN("WidenAxis"); // 旧文件展宽: 新增资产列清零 (_data_valid = 0 → 无效)
      for (size_t r = 0; r < rows; ++r) {
        std::memcpy(dst + r * A, s.narrow.data() + r * A_file, A_file * sizeof(feature_storage_t));
        std::memset(dst + r * A + A_file, 0, (A - A_file) * sizeof(feature_storage_t));
      }
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
  // 整月选列张量 (Dist/TimeSeries), 布局 [N_days × T][F_selected][A]; 日步长恒为
  // LEVELS[level].rows (含末尾哨兵行), 时间轴消费用 level_valid_rows(level)
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

  explicit FeatureRead(const std::string &base_dir) : base_dir_(base_dir) {}

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

    // 宽字段 (DEPTH 的 N 档) 用 sub 取档内下标; LVL 模板参数保住编译期定址
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
  // Batch Monthly Loading (for Dist analysis)
  // ========================================================================

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

    const size_t n = feature_indices.size();
    for (size_t day_idx = 0; day_idx < out.dates.size(); ++day_idx) {
      TraceN("LoadDay");
      TraceTextS(out.dates[day_idx].c_str());
      load_fields(out.dates[day_idx], out.level, feature_indices.data(), n,
                  out.data.data() + out.day_start(day_idx) * n * out.A, out.A, out.scratch);
    }
  }

  // ========================================================================
  // Utility Functions
  // ========================================================================

  // 该日是否有特征文件 (以最后一层的整层文件为准: IO worker 按层序落盘, 它在则全在)
  static bool has_date(const std::string &base_dir, const std::string &date) {
    assert(date.size() == 8);
    return std::filesystem::exists(feature_file(feature_day_dir(base_dir, date), LEVEL_COUNT - 1));
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
};
