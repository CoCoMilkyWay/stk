#pragma once

#include <arrow/table.h>

#include <cstdint>
#include <filesystem>
#include <memory>
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
// 写: write_table_atomic → zstd + tmp+rename (单文件原子, 中断不留半成品).
//
// TableView / Col: 类型化列访问 (pit build / axis / overlay / parse 共用).
//   yyyymmdd(i): timestamp[ns] 或 "YYYYMMDD" string 列 → int32; null → 0
//   f32(i):      double/float/int 列 → float; null → NaN
//   i32(i, def): int/uint/bool 列 → int;   null → def
//   str(i):      string 列 → string_view;  null → {}
// 列名缺失 / 类型不符 → assert (fail fast).
// ============================================================================
namespace misc::pq {

std::filesystem::path month_path(std::string_view ym /*"YYYY-MM"*/,
                                 std::string_view name);
std::filesystem::path meta_path(std::string_view name);

// data/ 下所有 "YYYY-MM" 目录中存在 <name>.parquet 的 (ym, path), ym 升序.
std::vector<std::pair<std::string, std::filesystem::path>>
list_month_files(std::string_view name);

std::shared_ptr<arrow::Table> read_table(const std::filesystem::path &path);

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

private:
  std::shared_ptr<arrow::Array> arr_;
  int type_ = -1; // arrow::Type::type 缓存
};

class TableView {
public:
  // CombineChunks 保证单 chunk (月度文件单 row group, 通常本就单 chunk 零拷贝).
  explicit TableView(std::shared_ptr<arrow::Table> t);

  std::int64_t rows() const { return t_ ? t_->num_rows() : 0; }
  bool has(std::string_view name) const;
  Col col(std::string_view name) const; // 缺列 assert

private:
  std::shared_ptr<arrow::Table> t_;
};

} // namespace misc::pq
