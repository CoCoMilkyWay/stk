#include "shared/Concept.hpp"

#include "fundamental/AssetAxis.hpp"
#include "fundamental/Store.hpp"
#include "misc/epoch.hpp"
#include "misc/parquet.hpp"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <string_view>

namespace {

// 名称命中即非题材
constexpr std::string_view kDrop[] = {
    // 指数成分
    "_", "中证", "上证", "深证", "深成", "创业成份", "创业板综", "MSCI", "富时罗素", "标准普尔", "茅指数", "宁组合",
    // 交易制度
    "沪股通", "深股通", "融资融券", "转债标的", "B股", "AH股", "GDR", "ST股", "次新股", "摘帽", "做市",
    // 资金持股
    "重仓", "新进", "增仓", "减仓", "持股", "养老金", "举牌", "增持", "调研", "金股", "外资背景", "股权集中", "股权分散",
    "股权激励", "股权转让", "参股新三板",
    // 资质认定
    "专精特新",
    // 行情统计
    "昨日", "多板", "新高", "超跌", "破发", "破增发", "破净", "热股", "低价股", "百元股", "趋势股", "反转股", "题材股",
    // 风格
    "风格", "大盘", "中盘", "小盘", "微盘", "权重股", "龙头", "周期股", "价值股", "红利股", "微利股", "成长股", "市盈率", "市净率", "亏损",
    // 业绩预告
    "季报", "中报", "年报"};

constexpr std::uint64_t kFnvOffset = 1469598103934665603ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

std::uint64_t fnv_append(std::uint64_t h, std::string_view s) {
  for (const char c : s) {
    h ^= static_cast<std::uint8_t>(c);
    h *= kFnvPrime;
  }
  return h * kFnvPrime;
}

bool concept_theme(std::string_view name) {
  return std::none_of(std::begin(kDrop), std::end(kDrop),
                      [&](std::string_view w) { return name.find(w) != std::string_view::npos; });
}

} // namespace

const ConceptTable &concepts() {
  static const auto load = [] {
    ConceptTable o;
    AssetAxis &ax = asset_axis();
    const std::size_t G = ax.size();
    std::vector<std::vector<std::uint16_t>> by(G);
    o.hash = kFnvOffset;
    const fundamental::Catalog cat;
    if (const fundamental::Shard *sh = cat.find("concept", 0)) {
      const misc::pq::TableView v(fundamental::read(*sh));
      assert(v.rows() > 0 && "concept: _meta/concept.parquet 为空");
      o.date = v.col("trade_date").yyyymmdd(0);
      assert(o.date > 0 && "concept: trade_date 非法");
      const misc::pq::Col c_sym = v.col("ts_code"), c_code = v.col("code"), c_name = v.col("name");
      for (std::int64_t i = 0; i < v.rows();) {
        const std::string_view bcode = c_code.str(i), bname = c_name.str(i);
        std::int64_t j = i + 1;
        while (j < v.rows() && c_code.str(j) == bcode)
          ++j;
        if (concept_theme(bname) && static_cast<std::size_t>(j - i) >= kConceptMin) {
          const std::size_t k = o.size();
          assert(k < UINT16_MAX && (k == 0 || o.code.back() < bcode));
          o.code.emplace_back(bcode);
          o.name.emplace_back(bname);
          o.hash = fnv_append(o.hash, bcode);
          std::uint64_t s = fnv_append(kFnvOffset, bcode);
          for (std::int64_t r = i; r < j; ++r) {
            const std::string_view sym = c_sym.str(r);
            assert(r == i || c_sym.str(r - 1) < sym);
            if (const std::size_t g = ax.find(std::string(sym)); g < G) {
              by[g].push_back(static_cast<std::uint16_t>(k));
              o.hash = fnv_append(o.hash, std::to_string(g));
              s = fnv_append(s, std::to_string(g));
            }
          }
          o.sig.push_back(s);
        }
        i = j;
      }
    }
    o.at.assign(G + 1, 0);
    for (std::size_t g = 0; g < G; ++g) {
      o.at[g + 1] = o.at[g] + static_cast<std::uint32_t>(by[g].size());
      o.of.insert(o.of.end(), by[g].begin(), by[g].end());
    }
    return o;
  };
  struct Loaded : ConceptTable {
    Loaded() : ConceptTable(load()) {}
  };
  static misc::Epochal<Loaded> t;
  return t.get();
}
