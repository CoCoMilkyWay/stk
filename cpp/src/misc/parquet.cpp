#include "misc/parquet.hpp"

#include "misc/fs.hpp"

#include <arrow/array.h>
#include <arrow/io/file.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <arrow/util/compression.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/schema.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <limits>
#include <string>

namespace misc::pq {

namespace fs = std::filesystem;

// ----------------------------------------------------------------------------
// 路径
// ----------------------------------------------------------------------------
fs::path month_path(std::string_view ym, std::string_view name) {
  assert(ym.size() == 7 && ym[4] == '-');
  return git_root() / "output" / "fundamental" / ym /
         (std::string(name) + ".parquet");
}

fs::path meta_path(std::string_view name) {
  return git_root() / "output" / "fundamental" / "_meta" /
         (std::string(name) + ".parquet");
}

std::vector<std::pair<std::string, fs::path>>
list_month_files(std::string_view name) {
  std::vector<std::pair<std::string, fs::path>> out;
  fs::path data_root = git_root() / "output" / "fundamental";
  assert(fs::exists(data_root));
  std::string fname = std::string(name) + ".parquet";

  for (auto &ent : fs::directory_iterator(data_root)) {
    if (!ent.is_directory())
      continue;
    std::string ym = ent.path().filename().string();
    if (ym.size() != 7 || ym[4] != '-')
      continue;
    fs::path p = ent.path() / fname;
    if (fs::exists(p))
      out.emplace_back(std::move(ym), std::move(p));
  }
  std::sort(out.begin(), out.end());
  return out;
}

// ----------------------------------------------------------------------------
// 读写
// ----------------------------------------------------------------------------
std::shared_ptr<arrow::Table> read_table(const fs::path &path,
                                         const std::vector<std::string> &columns) {
  auto file_res = arrow::io::ReadableFile::Open(path.string());
  assert(file_res.ok() && "pq::read_table: 文件无法打开");
  auto reader_res = parquet::arrow::OpenFile(file_res.ValueOrDie(),
                                             arrow::default_memory_pool());
  assert(reader_res.ok() && "pq::read_table: 构造 reader 失败");
  std::unique_ptr<parquet::arrow::FileReader> reader =
      std::move(reader_res).ValueOrDie();

  arrow::Result<std::shared_ptr<arrow::Table>> t_res;
  if (columns.empty()) {
    t_res = reader->ReadTable();
  } else {
    // 扁平 schema: 列名即叶子路径, ColumnIndex 直接可用
    const parquet::SchemaDescriptor *schema =
        reader->parquet_reader()->metadata()->schema();
    std::vector<int> indices;
    indices.reserve(columns.size());
    for (const std::string &name : columns) {
      int i = schema->ColumnIndex(name);
      assert(i >= 0 && "pq::read_table: 请求的列不存在");
      indices.push_back(i);
    }
    t_res = reader->ReadTable(indices);
  }
  assert(t_res.ok() && "pq::read_table: ReadTable 失败");
  return std::move(t_res).ValueOrDie();
}

void write_table_atomic(const fs::path &path,
                        const std::shared_ptr<arrow::Table> &t) {
  assert(t);
  fs::create_directories(path.parent_path());
  fs::path tmp = path;
  tmp += ".tmp";

  auto out_res = arrow::io::FileOutputStream::Open(tmp.string());
  assert(out_res.ok() && "pq::write_table_atomic: tmp 无法打开");
  auto props = parquet::WriterProperties::Builder()
                   .compression(arrow::Compression::ZSTD)
                   ->compression_level(19)
                   ->build();
  // chunk_size 须 > 0 (0 行表写 1 个空 row group).
  auto st = parquet::arrow::WriteTable(*t, arrow::default_memory_pool(),
                                       out_res.ValueOrDie(),
                                       std::max<std::int64_t>(t->num_rows(), 1),
                                       props);
  assert(st.ok() && "pq::write_table_atomic: WriteTable 失败");
  fs::rename(tmp, path);
}

void append_table_atomic(const fs::path &path,
                         const std::shared_ptr<arrow::Table> &t) {
  assert(t);
  if (!fs::exists(path)) {
    write_table_atomic(path, t);
    return;
  }
  if (t->num_rows() == 0) {
    // 0 行探测: 只 touch mtime — 让探测本身进 dedup 窗口, 连跑不重复发查询.
    // 代价: 该表 pool cache key (含 mtime) 被打穿一次, 每 dedup 窗口最多一次.
    fs::last_write_time(path, fs::file_time_type::clock::now());
    return;
  }
  auto old = read_table(path);
  auto res = arrow::ConcatenateTables({old, t});
  assert(res.ok() && "pq::append_table_atomic: schema 不一致");
  write_table_atomic(path, res.ValueOrDie());
}

// ----------------------------------------------------------------------------
// Col
// ----------------------------------------------------------------------------
Col::Col(std::shared_ptr<arrow::Array> a)
    : arr_(std::move(a)), type_(static_cast<int>(arr_->type_id())) {}

bool Col::null(std::int64_t i) const { return arr_->IsNull(i); }

namespace {

// timestamp[ns] (UTC) → YYYYMMDD int32
inline std::int32_t ns_to_yyyymmdd(std::int64_t ns) {
  using namespace std::chrono;
  sys_days sd = floor<days>(sys_time<nanoseconds>(nanoseconds(ns)));
  year_month_day ymd(sd);
  return static_cast<int>(ymd.year()) * 10000 +
         static_cast<int>(static_cast<unsigned>(ymd.month())) * 100 +
         static_cast<int>(static_cast<unsigned>(ymd.day()));
}

// "YYYYMMDD" → int32; 非 8 位数字 → 0
inline std::int32_t sv_to_yyyymmdd(std::string_view s) {
  if (s.size() != 8)
    return 0;
  std::int32_t v = 0;
  for (char c : s) {
    if (c < '0' || c > '9')
      return 0;
    v = v * 10 + (c - '0');
  }
  return v;
}

} // namespace

std::int32_t Col::yyyymmdd(std::int64_t i) const {
  if (arr_->IsNull(i))
    return 0;
  switch (type_) {
  case arrow::Type::TIMESTAMP:
    return ns_to_yyyymmdd(
        static_cast<const arrow::TimestampArray &>(*arr_).Value(i));
  case arrow::Type::STRING: {
    auto v = static_cast<const arrow::StringArray &>(*arr_).GetView(i);
    return sv_to_yyyymmdd(std::string_view(v.data(), v.size()));
  }
  case arrow::Type::LARGE_STRING: {
    auto v = static_cast<const arrow::LargeStringArray &>(*arr_).GetView(i);
    return sv_to_yyyymmdd(std::string_view(v.data(), v.size()));
  }
  default:
    assert(false && "Col::yyyymmdd: 列类型须为 timestamp / string");
    return 0;
  }
}

float Col::f32(std::int64_t i) const {
  if (arr_->IsNull(i))
    return std::numeric_limits<float>::quiet_NaN();
  switch (type_) {
  case arrow::Type::DOUBLE:
    return static_cast<float>(
        static_cast<const arrow::DoubleArray &>(*arr_).Value(i));
  case arrow::Type::FLOAT:
    return static_cast<const arrow::FloatArray &>(*arr_).Value(i);
  case arrow::Type::INT64:
    return static_cast<float>(
        static_cast<const arrow::Int64Array &>(*arr_).Value(i));
  case arrow::Type::INT32:
    return static_cast<float>(
        static_cast<const arrow::Int32Array &>(*arr_).Value(i));
  case arrow::Type::INT16:
    return static_cast<float>(
        static_cast<const arrow::Int16Array &>(*arr_).Value(i));
  case arrow::Type::INT8:
    return static_cast<float>(
        static_cast<const arrow::Int8Array &>(*arr_).Value(i));
  default:
    assert(false && "Col::f32: 列类型须为 double / float / int");
    return std::numeric_limits<float>::quiet_NaN();
  }
}

int Col::i32(std::int64_t i, int def) const {
  if (arr_->IsNull(i))
    return def;
  switch (type_) {
  case arrow::Type::INT8:
    return static_cast<const arrow::Int8Array &>(*arr_).Value(i);
  case arrow::Type::INT16:
    return static_cast<const arrow::Int16Array &>(*arr_).Value(i);
  case arrow::Type::INT32:
    return static_cast<int>(
        static_cast<const arrow::Int32Array &>(*arr_).Value(i));
  case arrow::Type::INT64:
    return static_cast<int>(
        static_cast<const arrow::Int64Array &>(*arr_).Value(i));
  case arrow::Type::UINT8:
    return static_cast<const arrow::UInt8Array &>(*arr_).Value(i);
  case arrow::Type::BOOL:
    return static_cast<const arrow::BooleanArray &>(*arr_).Value(i) ? 1 : 0;
  case arrow::Type::DOUBLE: {
    double v = static_cast<const arrow::DoubleArray &>(*arr_).Value(i);
    return std::isfinite(v) ? static_cast<int>(v) : def;
  }
  default:
    assert(false && "Col::i32: 列类型须为 int / bool / double");
    return def;
  }
}

std::string_view Col::str(std::int64_t i) const {
  if (arr_->IsNull(i))
    return {};
  switch (type_) {
  case arrow::Type::STRING: {
    auto v = static_cast<const arrow::StringArray &>(*arr_).GetView(i);
    return std::string_view(v.data(), v.size());
  }
  case arrow::Type::LARGE_STRING: {
    auto v = static_cast<const arrow::LargeStringArray &>(*arr_).GetView(i);
    return std::string_view(v.data(), v.size());
  }
  default:
    assert(false && "Col::str: 列类型须为 string");
    return {};
  }
}

// ----------------------------------------------------------------------------
// TableView
// ----------------------------------------------------------------------------
TableView::TableView(std::shared_ptr<arrow::Table> t) {
  assert(t);
  auto res = t->CombineChunks(arrow::default_memory_pool());
  assert(res.ok() && "TableView: CombineChunks 失败");
  t_ = res.ValueOrDie();
}

bool TableView::has(std::string_view name) const {
  return t_->schema()->GetFieldIndex(std::string(name)) >= 0;
}

Col TableView::col(std::string_view name) const {
  auto c = t_->GetColumnByName(std::string(name));
  assert(c && "TableView::col: 列不存在");
  if (c->num_chunks() == 0 || c->chunk(0)->length() == 0) {
    // 0 行表: CombineChunks 可能留 0 chunk; 造一个空数组占位以统一访问路径.
    assert(t_->num_rows() == 0);
  }
  assert(c->num_chunks() <= 1 && "TableView: CombineChunks 后应单 chunk");
  if (c->num_chunks() == 0)
    return Col(); // 0 行: 任何行访问都是越界, 不会发生
  return Col(c->chunk(0));
}

} // namespace misc::pq
