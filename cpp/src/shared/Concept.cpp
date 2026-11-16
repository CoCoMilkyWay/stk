#include "shared/Concept.hpp"

#include "api/tushare/client.hpp"
#include "disk/Disk.hpp"
#include "fundamental/AssetAxis.hpp"
#include "misc/epoch.hpp"

#include <arrow/array.h>
#include <arrow/table.h>

#include <algorithm>
#include <cassert>
#include <iterator>
#include <sstream>

namespace {

// Tushare 东财板块接口 (dc_index 板块名单 / dc_member 板块成分, 均按交易日)
constexpr const char *kDcConcept = "概念板块"; // dc_index.idx_type 的概念一类
constexpr std::string_view kDcSuffix = ".DC";  // ts_code = 板块代码 + 后缀
constexpr int kDcLookback = 15;                // 当日无数据 (休市 / 未出) 往前找的天数上限
constexpr std::int64_t kDcRows = 8000;         // 单次返回上限 (恰满 = 截断)

// tushare 表的一列 (非 num 列 = utf8, 单块) 逐行; 缺列 / 空值 → assert
std::vector<std::string> strings(const arrow::Table &t, const char *col) {
  const auto c = t.GetColumnByName(col);
  assert(c && c->num_chunks() == 1 && c->null_count() == 0 && "concept: 列缺失或有空值");
  const auto &a = static_cast<const arrow::StringArray &>(*c->chunk(0));
  std::vector<std::string> out;
  out.reserve(static_cast<std::size_t>(a.length()));
  for (std::int64_t i = 0; i < a.length(); ++i)
    out.emplace_back(a.GetView(i));
  return out;
}

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

// s 按 sep 切开逐段 fn
template <class Fn>
void split(std::string_view s, char sep, Fn &&fn) {
  for (size_t a = 0;;) {
    const size_t b = s.find(sep, a);
    fn(s.substr(a, b - a));
    if (b == std::string_view::npos)
      return;
    a = b + 1;
  }
}

} // namespace

bool concept_theme(std::string_view name) {
  return std::none_of(std::begin(kDrop), std::end(kDrop), [&](std::string_view w) { return name.find(w) != std::string_view::npos; });
}

misc::Ymd concept_last() {
  const std::vector<disk::Key> days = disk::list(disk::ConceptDay);
  return days.empty() ? 0 : days.back().date;
}

std::vector<Concept> fetch_concepts(tushare::Client &tu, misc::Ymd day, const std::function<void(const std::string &)> &say) {
  say("板块名单");
  std::shared_ptr<arrow::Table> boards;
  std::string at;
  for (int k = 0; !boards || boards->num_rows() == 0; ++k) {
    assert(k <= kDcLookback && "concept: 近期无板块数据");
    at = misc::dense(misc::add_days(day, -k));
    boards = tu.query({"dc_index", {{"trade_date", at}, {"idx_type", kDcConcept}}, {"pct_change", "leading_pct", "total_mv", "turnover_rate", "up_num", "down_num"}});
  }
  assert(boards->num_rows() < kDcRows && "concept: dc_index 被截断");
  const std::vector<std::string> codes = strings(*boards, "ts_code"), names = strings(*boards, "name");
  for ([[maybe_unused]] const std::string &x : strings(*boards, "idx_type"))
    assert(x == kDcConcept && "concept: dc_index 混入非概念板块");
  std::vector<Concept> list;
  for (std::size_t k = 0; k < codes.size(); ++k) {
    if (!concept_theme(names[k]))
      continue;
    say("板块成分 " + std::to_string(k + 1) + "/" + std::to_string(codes.size()) + " " + names[k]);
    const auto members = tu.query({"dc_member", {{"trade_date", at}, {"ts_code", codes[k]}}, {}});
    assert(members->num_rows() < kDcRows && "concept: dc_member 被截断");
    std::vector<std::string> syms = strings(*members, "con_code");
    std::sort(syms.begin(), syms.end());
    assert(std::adjacent_find(syms.begin(), syms.end()) == syms.end() && "concept: 板块成分重复");
    if (syms.size() < kConceptMin)
      continue;
    assert(codes[k].ends_with(kDcSuffix) && "concept: ts_code 不是 BKxxxx.DC");
    list.push_back({codes[k].substr(0, codes[k].size() - kDcSuffix.size()), names[k], std::move(syms)});
  }
  std::sort(list.begin(), list.end(), [](const Concept &x, const Concept &y) { return x.code < y.code; });
  return list;
}

void save_concepts(misc::Ymd date, const std::vector<Concept> &list) {
  std::string s;
  for (size_t k = 0; k < list.size(); ++k) {
    const Concept &c = list[k];
    assert((k == 0 || list[k - 1].code < c.code) && c.syms.size() >= kConceptMin && "concept: 须按板块代码升序且成分够数");
    assert(c.code.find_first_of("\t\n") == std::string::npos && c.name.find_first_of("\t\n") == std::string::npos);
    s += c.code + '\t' + c.name + '\t';
    for (size_t i = 0; i < c.syms.size(); ++i)
      (s += i ? "," : "") += c.syms[i];
    s += '\n';
  }
  disk::write(disk::ConceptDay, {.date = date}, s);
  for (const disk::Key &k : disk::list(disk::ConceptDay))
    if (k.date != date)
      disk::remove(disk::ConceptDay, k);
}

const ConceptTable &concepts() {
  static const auto load = [] {
    ConceptTable o;
    AssetAxis &ax = asset_axis();
    const size_t G = ax.size();
    std::vector<std::vector<uint16_t>> by(G); // [全局 id] 所属概念
    o.hash = kFnvOffset;
    o.date = concept_last();
    if (o.date) {
      std::istringstream f(disk::read(disk::ConceptDay, {.date = o.date}));
      for (std::string line; std::getline(f, line);) {
        std::vector<std::string_view> col;
        split(line, '\t', [&](std::string_view x) { col.push_back(x); });
        assert(col.size() == 3 && "concept: 快照行不是 代码 \\t 名称 \\t 成分");
        const size_t k = o.size();
        assert(k < UINT16_MAX && (k == 0 || o.code.back() < col[0]));
        o.code.emplace_back(col[0]), o.name.emplace_back(col[1]);
        o.hash = fnv_append(o.hash, col[0]);
        split(col[2], ',', [&](std::string_view sym) {
          if (const size_t g = ax.find(std::string(sym)); g < G) {
            by[g].push_back(static_cast<uint16_t>(k));
            o.hash = fnv_append(o.hash, std::to_string(g));
          }
        });
      }
    }
    o.at.assign(G + 1, 0);
    for (size_t g = 0; g < G; ++g) {
      o.at[g + 1] = o.at[g] + static_cast<uint32_t>(by[g].size());
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
