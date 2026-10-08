#include "shared/Universe.hpp"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <fstream>

using json = nlohmann::json;

namespace universe {

namespace {

// 名单元素必须是非空字符串数组 (可为空数组)
void check_code_array(const json &arr) {
  assert(arr.is_array() && "universe json: add/del 必须是数组");
  for (const auto &c : arr) {
    assert(c.is_string() && !c.get_ref<const std::string &>().empty() && "universe json: 代码必须是非空字符串 \"CODE.EX\"");
    (void)c;
  }
}

} // namespace

Pool Pool::all() {
  Pool p;
  p.all_ = true;
  return p;
}

Pool Pool::load(const std::filesystem::path &json_path) {
  std::ifstream file(json_path);
  assert(file.is_open() && "universe 名单文件不存在: <config_dir>/universe/<name>.json (先跑 py/universe/<name>.py)");

  json j;
  file >> j;
  assert(j.is_object() && !j.empty() && "universe json 必须是非空对象 {\"YYYYMMDD\": {\"add\": [...], \"del\": [...]}}");

  Pool p;
  // nlohmann::json 对象按键排序 (std::map), "YYYYMMDD" 字典序 = 时间序
  p.dates_.reserve(j.size());
  for (const auto &[date, day] : j.items()) {
    assert(date.size() == 8 && std::all_of(date.begin(), date.end(), [](char ch) { return ch >= '0' && ch <= '9'; }) && "universe json: 日期键必须是 YYYYMMDD");
    assert(day.is_object() && day.contains("add") && day.contains("del") && "universe json: 每日必须是 {\"add\": [...], \"del\": [...]}");
    check_code_array(day["add"]);
    check_code_array(day["del"]);
    assert((p.dates_.empty() || p.dates_.back() < date) && "universe json: 日期键必须严格升序");
    p.dates_.push_back(date);
    for (const auto &c : day["add"])
      p.codes_.push_back(c.get<std::string>());
  }
  // 并集 = 所有 add 过的代码 (del 只会删 add 过的)
  std::sort(p.codes_.begin(), p.codes_.end());
  p.codes_.erase(std::unique(p.codes_.begin(), p.codes_.end()), p.codes_.end());
  assert(!p.codes_.empty() && "universe 并集为空");
  p.code_idx_.reserve(p.codes_.size());
  for (uint32_t i = 0; i < p.codes_.size(); ++i)
    p.code_idx_.emplace(p.codes_[i], i);

  // 回放: 第 i 日 = 第 i−1 日 + add − del
  const size_t n_codes = p.codes_.size();
  p.member_.assign(p.dates_.size() * n_codes, 0);
  std::vector<uint8_t> cur(n_codes, 0);
  size_t cur_n = 0;
  size_t di = 0;
  for (const auto &[date, day] : j.items()) {
    for (const auto &c : day["del"]) {
      const auto it = p.code_idx_.find(c.get_ref<const std::string &>());
      assert(it != p.code_idx_.end() && cur[it->second] && "universe json: del 的代码不在前一日池里");
      cur[it->second] = 0;
      --cur_n;
    }
    for (const auto &c : day["add"]) {
      const uint32_t idx = p.code_idx_.at(c.get_ref<const std::string &>());
      assert(!cur[idx] && "universe json: add 的代码已在前一日池里");
      cur[idx] = 1;
      ++cur_n;
    }
    assert(cur_n > 0 && "universe json: 某日池空");
    std::copy(cur.begin(), cur.end(), p.member_.begin() + static_cast<std::ptrdiff_t>(di * n_codes));
    ++di;
    (void)date;
  }
  return p;
}

std::vector<std::string> Pool::codes(std::string_view first, std::string_view last) const {
  assert(!all_ && "universe == all 无名单");
  assert(first <= last && "universe::codes: 区间颠倒");
  const auto lo = std::lower_bound(dates_.begin(), dates_.end(), first);
  const auto hi = std::upper_bound(dates_.begin(), dates_.end(), last);
  assert(lo < hi && "universe::codes: 回测区间内没有任何名单日 (重跑 py/universe/<name>.py 覆盖到 end_date)");

  const size_t n_codes = codes_.size();
  std::vector<uint8_t> seen(n_codes, 0);
  for (auto it = lo; it != hi; ++it) {
    const uint8_t *row = member_.data() + static_cast<size_t>(it - dates_.begin()) * n_codes;
    for (size_t i = 0; i < n_codes; ++i)
      seen[i] |= row[i];
  }
  std::vector<std::string> out;
  for (size_t i = 0; i < n_codes; ++i)
    if (seen[i])
      out.push_back(codes_[i]); // codes_ 已升序
  assert(!out.empty() && "universe::codes: 区间内并集为空");
  return out;
}

const std::vector<std::string> &Pool::dates() const {
  assert(!all_ && "universe == all 无名单");
  return dates_;
}

bool Pool::has_date(std::string_view yyyymmdd) const {
  if (all_)
    return true;
  return std::binary_search(dates_.begin(), dates_.end(), yyyymmdd);
}

bool Pool::in_pool(std::string_view code, std::string_view yyyymmdd) const {
  if (all_)
    return true;
  const auto dit = std::lower_bound(dates_.begin(), dates_.end(), yyyymmdd);
  assert(dit != dates_.end() && *dit == yyyymmdd && "universe: 该日没出名单 (回测区间超出名单覆盖, 重跑 py/universe)");
  assert(code.find('.') != std::string_view::npos && "universe: 键是 \"CODE.EX\", 裸 6 位码永远查不到 (cs_valid 会全 0)");
  const auto cit = code_idx_.find(std::string(code));
  if (cit == code_idx_.end())
    return false;
  const size_t di = static_cast<size_t>(dit - dates_.begin());
  return member_[di * codes_.size() + cit->second] != 0;
}

} // namespace universe
