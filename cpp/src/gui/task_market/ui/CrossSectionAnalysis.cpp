// ASSET 表 Statistics 页的截面统计 — 见头文件
#include "gui/task_market/ui/CrossSectionAnalysis.hpp"
#include "gui/util/Board.hpp" // GetBoardType / GetBoardName

#include <algorithm>
#include <cassert>
#include <cmath>
#include <map>
#include <numeric>

namespace GUI::Market {

namespace {

std::vector<float> finite(const std::vector<float> &values) {
  std::vector<float> o;
  o.reserve(values.size());
  for (const float v : values)
    if (std::isfinite(v))
      o.push_back(v);
  return o;
}

// codes[i] 的板块名; 未知板块 → 空
std::string board_of(const std::string &code) {
  const BoardType b = GetBoardType(code);
  return b == BoardType::All || b == BoardType::Unknown ? std::string{} : GetBoardName(b);
}

} // namespace

std::vector<float> RemoveOutliers(const std::vector<float> &values, float percentile) {
  std::vector<float> v = finite(values);
  std::sort(v.begin(), v.end());
  const size_t lo = static_cast<size_t>(static_cast<float>(v.size()) * percentile / 100.0f);
  const size_t hi = static_cast<size_t>(static_cast<float>(v.size()) * (100.0f - percentile) / 100.0f);
  return hi > lo && hi < v.size() ? std::vector<float>(v.begin() + static_cast<std::ptrdiff_t>(lo), v.begin() + static_cast<std::ptrdiff_t>(hi)) : v;
}

std::vector<BoardStats> GroupNumericByBoard(const std::vector<std::string> &codes, const std::vector<float> &values) {
  assert(codes.size() == values.size());
  std::map<std::string, std::vector<float>> by;
  for (size_t i = 0; i < codes.size(); ++i)
    if (std::string b = board_of(codes[i]); !b.empty() && std::isfinite(values[i]))
      by[std::move(b)].push_back(values[i]);
  std::vector<BoardStats> o;
  for (auto &[name, v] : by) {
    std::sort(v.begin(), v.end());
    const size_t n = v.size(), mid = n / 2;
    BoardStats s{name, std::accumulate(v.begin(), v.end(), 0.0f) / static_cast<float>(n), n % 2 ? v[mid] : (v[mid - 1] + v[mid]) / 2.0f, 0.0f, n};
    float sq = 0.0f;
    for (const float x : v)
      sq += (x - s.mean) * (x - s.mean);
    s.std_dev = std::sqrt(sq / static_cast<float>(n));
    o.push_back(std::move(s));
  }
  return o;
}

std::vector<std::pair<std::string, float>> GetTopN(const std::vector<std::string> &names, const std::vector<float> &values, int n, bool desc) {
  assert(names.size() == values.size() && n >= 0);
  std::vector<std::pair<std::string, float>> o;
  for (size_t i = 0; i < names.size(); ++i)
    if (std::isfinite(values[i]))
      o.emplace_back(names[i], values[i]);
  const size_t k = std::min(static_cast<size_t>(n), o.size());
  std::partial_sort(o.begin(), o.begin() + static_cast<std::ptrdiff_t>(k), o.end(),
                    [desc](const auto &a, const auto &b) { return desc ? a.second > b.second : a.second < b.second; });
  o.resize(k);
  return o;
}

std::vector<CategoryCount> CountCategories(const std::vector<std::string> &categories) {
  std::map<std::string, size_t> by;
  size_t total = 0;
  for (const std::string &c : categories)
    if (!c.empty())
      ++by[c], ++total;
  std::vector<CategoryCount> o;
  for (const auto &[label, count] : by)
    o.push_back({label, count, static_cast<float>(100.0 * static_cast<double>(count) / static_cast<double>(total))});
  std::stable_sort(o.begin(), o.end(), [](const CategoryCount &a, const CategoryCount &b) { return a.count > b.count; });
  return o;
}

std::vector<BoardCategoryBreakdown> GroupCategoricalByBoard(const std::vector<std::string> &codes, const std::vector<std::string> &categories) {
  assert(codes.size() == categories.size());
  std::map<std::string, std::vector<std::string>> by;
  for (size_t i = 0; i < codes.size(); ++i)
    if (std::string b = board_of(codes[i]); !b.empty() && !categories[i].empty())
      by[std::move(b)].push_back(categories[i]);
  std::vector<BoardCategoryBreakdown> o;
  for (const auto &[name, cs] : by)
    o.push_back({name, CountCategories(cs)});
  return o;
}

} // namespace GUI::Market
