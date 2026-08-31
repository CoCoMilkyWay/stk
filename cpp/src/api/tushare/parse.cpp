#include "api/tushare/parse.hpp"

#include "api/tushare/spec.hpp"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

namespace tushare {

namespace {

// envelope 解包: root.data.{fields, items}; 结构不符 → assert.
struct Envelope {
  yyjson_val *fields;
  yyjson_val *items;
};

Envelope unpack(yyjson_doc *doc) {
  yyjson_val *root = yyjson_doc_get_root(doc);
  yyjson_val *data = yyjson_obj_get(root, "data");
  assert(data && "tushare response missing 'data'");
  yyjson_val *fields = yyjson_obj_get(data, "fields");
  yyjson_val *items = yyjson_obj_get(data, "items");
  assert(fields && items && yyjson_is_arr(fields) && yyjson_is_arr(items));
  return {fields, items};
}

std::vector<std::string> field_names(yyjson_val *fields) {
  std::vector<std::string> out;
  out.reserve(yyjson_arr_size(fields));
  size_t i, n;
  yyjson_val *v;
  yyjson_arr_foreach(fields, i, n, v) {
    assert(yyjson_is_str(v));
    out.emplace_back(yyjson_get_str(v));
  }
  return out;
}

} // namespace

std::shared_ptr<arrow::Table>
docs_to_table(const std::vector<yyjson_doc *> &docs,
              const InterfaceSpec &spec) {
  assert(!docs.empty());

  std::vector<Envelope> envs;
  envs.reserve(docs.size());
  for (yyjson_doc *d : docs)
    envs.push_back(unpack(d));

  // fields 一致性 (per-day API 多响应必须同 schema)
  std::vector<std::string> names = field_names(envs[0].fields);
  for (size_t k = 1; k < envs.size(); ++k)
    assert(field_names(envs[k].fields) == names && "tushare fields 跨响应不一致");

  std::unordered_set<std::string> drop(spec.drop_fields.begin(),
                                       spec.drop_fields.end());

  // 列类型 = spec.num_fields 声明 (double), 其余 string. 不按数据推断:
  // 全 null 列无从推断, 会让同一列跨月漂移 → append schema 不一致.
  std::unordered_set<std::string> num(spec.num_fields.begin(),
                                      spec.num_fields.end());
  size_t n_col = names.size();
  std::vector<bool> is_num(n_col, false);
  for (size_t c = 0; c < n_col; ++c)
    is_num[c] = num.count(names[c]) > 0;

  // 声明的数值列必须都在响应 fields 里 (拼错 / API 改名 → 立刻暴露)
  for (const auto &f : spec.num_fields)
    assert(std::find(names.begin(), names.end(), f) != names.end() &&
           "tushare num_fields 声明的列不在响应里");

  // builders → arrays
  std::vector<std::shared_ptr<arrow::Field>> out_fields;
  std::vector<std::shared_ptr<arrow::Array>> out_arrays;
  for (size_t c = 0; c < n_col; ++c) {
    if (drop.count(names[c]))
      continue;

    // 注: builder 调用不包在 assert 里 (NDEBUG 下 assert 整句被编译掉).
    if (is_num[c]) {
      arrow::DoubleBuilder b;
      for (const auto &e : envs) {
        size_t i, n;
        yyjson_val *item;
        yyjson_arr_foreach(e.items, i, n, item) {
          yyjson_val *v = yyjson_arr_get(item, c);
          arrow::Status st;
          if (!v || yyjson_is_null(v)) {
            st = b.AppendNull();
          } else {
            assert(yyjson_is_num(v) && "tushare 列类型行间不一致");
            st = b.Append(yyjson_get_num(v));
          }
          assert(st.ok());
        }
      }
      std::shared_ptr<arrow::Array> arr;
      arrow::Status st = b.Finish(&arr);
      assert(st.ok());
      out_fields.push_back(arrow::field(names[c], arrow::float64()));
      out_arrays.push_back(arr);
    } else {
      arrow::StringBuilder b;
      for (const auto &e : envs) {
        size_t i, n;
        yyjson_val *item;
        yyjson_arr_foreach(e.items, i, n, item) {
          yyjson_val *v = yyjson_arr_get(item, c);
          arrow::Status st;
          if (!v || yyjson_is_null(v)) {
            st = b.AppendNull();
          } else {
            assert(yyjson_is_str(v) && "tushare 列类型行间不一致");
            st = b.Append(yyjson_get_str(v),
                          static_cast<int32_t>(yyjson_get_len(v)));
          }
          assert(st.ok());
        }
      }
      std::shared_ptr<arrow::Array> arr;
      arrow::Status st = b.Finish(&arr);
      assert(st.ok());
      out_fields.push_back(arrow::field(names[c], arrow::utf8()));
      out_arrays.push_back(arr);
    }
  }

  return arrow::Table::Make(arrow::schema(out_fields), out_arrays);
}

} // namespace tushare
