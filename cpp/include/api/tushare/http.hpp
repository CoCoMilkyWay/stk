#pragma once

#include "yyjson/yyjson.h"
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tushare {

class Http {
public:
  explicit Http(std::string token);

  // POST http://api.tushare.pro
  // body = {api_name, token, params, fields:""} — 始终拉默认显示列 (默认隐藏列无需求).
  // 返回的 yyjson_doc* 由 caller 用 yyjson_doc_free 释放;
  // code != 0 / 网络异常 直接 assert.
  yyjson_doc *
  call(std::string_view api_name,
       const std::vector<std::pair<std::string, std::string>> &params);

private:
  std::string token_;
};

} // namespace tushare
