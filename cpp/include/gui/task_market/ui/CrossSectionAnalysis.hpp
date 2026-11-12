// ASSET 表 Statistics 页的截面统计 (只算, 不画): 数值列 = 去极值 / 分板块统计 / 首尾名次, 分类列 = 计数 / 分板块计数.
//   板块按代码前缀 (GetBoardType); 非有限值 / 空类别不计
#pragma once

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

namespace GUI::Market {

// 一个板块的数值统计
struct BoardStats {
  std::string board_name;
  float mean = 0.0f;
  float median = 0.0f;
  float std_dev = 0.0f;
  size_t count = 0;
};

// 一类的计数 (percentage = 占非空类别 %)
struct CategoryCount {
  std::string label;
  size_t count = 0;
  float percentage = 0.0f;
};

// 一个板块的分类计数
struct BoardCategoryBreakdown {
  std::string board_name;
  std::vector<CategoryCount> categories;
};

// 有限值升序, 去掉首尾各 percentile %
std::vector<float> RemoveOutliers(const std::vector<float> &values, float percentile);
// codes[i] ↔ values[i] 按板块分组的统计 (板块名序)
std::vector<BoardStats> GroupNumericByBoard(const std::vector<std::string> &codes, const std::vector<float> &values);
// 有限值的前 n 名 (desc = 高在前)
std::vector<std::pair<std::string, float>> GetTopN(const std::vector<std::string> &names, const std::vector<float> &values, int n, bool desc);

// 各类计数, 多 → 少
std::vector<CategoryCount> CountCategories(const std::vector<std::string> &categories);
// codes[i] ↔ categories[i] 按板块分组的计数 (板块名序)
std::vector<BoardCategoryBreakdown> GroupCategoricalByBoard(const std::vector<std::string> &codes, const std::vector<std::string> &categories);

} // namespace GUI::Market
