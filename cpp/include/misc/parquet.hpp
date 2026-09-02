#pragma once

#include <arrow/table.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// ============================================================================
// 统一 parquet 存储层 — 数据集唯一落地形态 (bigquant / tushare / feature 共用).
//
// 布局 (git_root 相对):
//   data/YYYY-MM/<name>.parquet   月度分片 (0 行月也落 0 行文件 = "拉过")
//   data/_meta/<name>.parquet     Static / Snapshot 单文件 (整刷覆盖)
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
//   f32(i):      double/float/int 列 → float; null → NaN
//   i32(i, def): int/uint/bool 列 → int;   null → def
//   str(i):      string 列 → string_view;  null → {}
//   yyyymmdd_all(out): 整列一次转 (千万行量级的批量扫描走这条)
// 列名缺失 / 类型不符 → assert (fail fast).
// ============================================================================
namespace misc::pq {

std::filesystem::path month_path(std::string_view ym /*"YYYY-MM"*/,
                                 std::string_view name);
std::filesystem::path meta_path(std::string_view name);

// data/ 下所有 "YYYY-MM" 目录中存在 <name>.parquet 的 (ym, path), ym 升序.
std::vector<std::pair<std::string, std::filesystem::path>>
list_month_files(std::string_view name);

// columns 空 = 全列; 非空 = 只读这些列 (列名缺失 → assert).
// dict_columns 里的列以 dictionary<string,int32> 落地 (须是 BYTE_ARRAY 物理类型).
std::shared_ptr<arrow::Table>
read_table(const std::filesystem::path &path, const std::vector<std::string> &columns = {},
           const std::vector<std::string> &dict_columns = {});

void write_table_atomic(const std::filesystem::path &path,
                        const std::shared_ptr<arrow::Table> &t);

// 增量 append — 旧文件 + t 拼接后原子重写 (开放月水位增量落盘).
//   旧文件不存在 → 等价 write_table_atomic (0 行也落文件建档);
//   旧文件存在 ∧ t 0 行 → 只 touch mtime (探测计入 dedup 窗口, 连跑不重发查询);
//   schema 不一致 → assert (服务端同表 schema 稳定; 变了 fail fast).
void append_table_atomic(const std::filesystem::path &path,
                         const std::shared_ptr<arrow::Table> &t);

class Col {
public:
  Col() = default;
  explicit Col(std::shared_ptr<arrow::Array> a);

  bool valid() const { return arr_ != nullptr; }
  bool null(std::int64_t i) const;
  std::int32_t yyyymmdd(std::int64_t i) const;
  float f32(std::int64_t i) const;
  int i32(std::int64_t i, int def) const;
  std::string_view str(std::int64_t i) const;

  // 整列 → YYYYMMDD int32 (null → 0), 写入 out (resize 到行数).
  // timestamp 列逐行 yyyymmdd() 的 civil-from-days 在千万行量级上是热点; 这里
  // 对 "与上一行同值" 短路 —— 月度分片内 date 只有 20~31 个不同值且按日成块,
  // 命中率 ~100%, 于是整列只做 K 次真实转换.
  void yyyymmdd_all(std::vector<std::int32_t> &out) const;

private:
  std::shared_ptr<arrow::Array> arr_;
  int type_ = -1; // arrow::Type::type 缓存
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
