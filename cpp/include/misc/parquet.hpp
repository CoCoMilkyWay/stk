#pragma once

#include <arrow/table.h>

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

// ============================================================================
// parquet 读写 (纯 IO, 不含任何数据集布局; 基本面布局见 fundamental/Store.hpp).
//
// 读: read_table → arrow::Table (CombineChunks 后单 chunk, 行级随机访问).
//     columns 非空 = 只解这几列. 宽表全列解是纯浪费 —— financial_ttm_shift
//     185 列 2.1 GB 只有 6 列被用到, 全列 6.3s / 裁剪后 0.5s.
//     dict_columns 非空 = 这些 BYTE_ARRAY 列以 dictionary<string,int32> 落地,
//     用 DictCol 访问 (见下). 文件走 mmap, 省掉一次 page cache → 用户态拷贝.
// 写: write_table_atomic → zstd + tmp+rename (单文件原子, 中断不留半成品).
//     rename 不动旧 inode ⇒ 并发读者手上的 mmap 不会被写者打穿.
//
// TableView / Col: 类型化列访问 (pit build / axis / overlay / parse 共用).
//   yyyymmdd(i): timestamp[ns] 或 "YYYYMMDD" string 列 → int32; null → 0
//   minute_of_day(i): timestamp[ns] 列 → 墙钟分钟 hh·60 + mm (无时区, 按存储字段); null → -1
//   f32(i):      double/float/int 列 → float; null → NaN
//   i32(i, def): int/uint/bool 列 → int;   null → def
//   str(i):      string 列 → string_view;  null → {}
//   yyyymmdd_all(out): 整列一次转 (千万行量级的批量扫描走这条)
// 列名缺失 / 类型不符 → assert (fail fast).
// ============================================================================
namespace misc::pq {

// columns 空 = 全列; 非空 = 只读这些列 (列名缺失 → assert).
// dict_columns 里的列以 dictionary<string,int32> 落地 (须是 BYTE_ARRAY 物理类型).
std::shared_ptr<arrow::Table>
read_table(const std::filesystem::path &path, const std::vector<std::string> &columns = {},
           const std::vector<std::string> &dict_columns = {});

// 日期列 max → YYYYMMDD, 只读 footer 列统计 (零解码); 0 行 / 全 null → 0.
//   列须为 timestamp[ns] 或 "YYYYMMDD" string (与 Col::yyyymmdd 同口径); 有值却缺统计 → assert.
std::int32_t max_yyyymmdd(const std::filesystem::path &path, const std::string &column);

void write_table_atomic(const std::filesystem::path &path,
                        const std::shared_ptr<arrow::Table> &t);

// 增量 append — 旧文件 + t 拼接后原子重写 (开放月水位增量落盘).
//   旧文件不存在 → 等价 write_table_atomic (0 行也落文件建档);
//   旧文件存在 ∧ t 0 行 → 只 touch mtime (探测计入 dedup 窗口, 连跑不重发查询);
//   schema 不一致 → assert (服务端同表 schema 稳定; 变了 fail fast).
void append_table_atomic(const std::filesystem::path &path,
                         const std::shared_ptr<arrow::Table> &t);

// 写者痕迹 (只读 footer): created_by ≠ 本库 (parquet::DEFAULT_CREATED_BY) 或带键值元数据
//   (ARROW:schema / pandas …) → true. write_table_atomic 写的两者皆无; 外来写者 (远端 pyarrow /
//   换版前的本库) 至少占一条.
bool foreign_writer(const std::filesystem::path &path);

// foreign_writer → 以本库原样重写 (write_table_atomic) 并返回 true; 否则不动, 返回 false.
//   去 schema 元数据, 重读逐值比对 (NaN 视同; 不一致 → assert),
//   mtime 还原 (写盘日承载 "数据何时拉的", 换编码不算重拉).
bool renative(const std::filesystem::path &path);

// null / f32 内联直读缓存的位图与值缓冲 (逐行热路径: 千万行扫描每行每列一调)
class Col {
public:
  Col() = default;
  explicit Col(std::shared_ptr<arrow::Array> a);

  bool valid() const { return arr_ != nullptr; }
  bool null(std::int64_t i) const {
    const std::int64_t j = offset_ + i;
    return nulls_ != nullptr && !(nulls_[j >> 3] >> (j & 7) & 1);
  }
  std::int32_t yyyymmdd(std::int64_t i) const;
  int minute_of_day(std::int64_t i) const;
  float f32(std::int64_t i) const {
    if (null(i))
      return std::numeric_limits<float>::quiet_NaN();
    const std::int64_t j = offset_ + i;
    switch (type_) {
    case arrow::Type::DOUBLE:
      return static_cast<float>(value<double>(j));
    case arrow::Type::FLOAT:
      return value<float>(j);
    case arrow::Type::INT64:
      return static_cast<float>(value<std::int64_t>(j));
    case arrow::Type::INT32:
      return static_cast<float>(value<std::int32_t>(j));
    case arrow::Type::INT16:
      return static_cast<float>(value<std::int16_t>(j));
    case arrow::Type::INT8:
      return static_cast<float>(value<std::int8_t>(j));
    default:
      assert(false && "Col::f32: 列类型须为 double / float / int");
      return std::numeric_limits<float>::quiet_NaN();
    }
  }
  int i32(std::int64_t i, int def) const;
  std::string_view str(std::int64_t i) const;

  // 整列 → YYYYMMDD int32 (null → 0), 写入 out (resize 到行数).
  // timestamp 列逐行 yyyymmdd() 的 civil-from-days 在千万行量级上是热点; 这里
  // 对 "与上一行同值" 短路 —— 月度分片内 date 只有 20~31 个不同值且按日成块,
  // 命中率 ~100%, 于是整列只做 K 次真实转换.
  void yyyymmdd_all(std::vector<std::int32_t> &out) const;

private:
  template <class V>
  V value(std::int64_t j) const { return static_cast<const V *>(values_)[j]; }

  std::shared_ptr<arrow::Array> arr_;
  int type_ = -1;                       // arrow::Type::type 缓存
  std::int64_t offset_ = 0;             // 切片偏移 (位图 / 值缓冲下标 = offset_ + i)
  const std::uint8_t *nulls_ = nullptr; // validity 位图; 无 null → nullptr
  const void *values_ = nullptr;        // buffers[1] 起点 (定宽数值列 = 值数组)
};

// 字典编码 string 列 — parquet 的 BYTE_ARRAY 本就是 RLE_DICTIONARY, read_table
// 的 dict_columns 让它直接以 dictionary<string,int32> 落地, 免掉逐行物化 string.
// 用法: 先对 dict_size() 个字典项建一次映射, 逐行只取 indices()[i] 当下标 ——
// 千万行的 code → id 于是从"每行一次哈希"降到"每文件一次".
class DictCol {
public:
  DictCol() = default;
  explicit DictCol(const std::shared_ptr<arrow::Array> &a);

  bool valid() const { return idx_ != nullptr; }
  std::int32_t dict_size() const { return dict_size_; }
  std::string_view dict_value(std::int32_t k) const;
  const std::int32_t *indices() const { return idx_; } // 长度 = 行数

private:
  std::shared_ptr<arrow::Array> arr_;  // indices buffer 生命周期
  std::shared_ptr<arrow::Array> dict_; // 字典 buffer 生命周期
  const std::int32_t *idx_ = nullptr;
  const arrow::StringArray *values_ = nullptr;
  std::int32_t dict_size_ = 0;
};

class TableView {
public:
  // CombineChunks 保证单 chunk (月度文件单 row group, 通常本就单 chunk 零拷贝).
  explicit TableView(std::shared_ptr<arrow::Table> t);

  std::int64_t rows() const { return t_ ? t_->num_rows() : 0; }
  bool has(std::string_view name) const;
  Col col(std::string_view name) const;          // 缺列 assert
  DictCol dict_col(std::string_view name) const; // 须在 dict_columns 里, 否则 assert

private:
  std::shared_ptr<arrow::Table> t_;
};

} // namespace misc::pq
