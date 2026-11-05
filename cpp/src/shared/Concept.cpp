#include "shared/Concept.hpp"

#include "misc/fs.hpp"
#include "shared/AssetAxis.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iterator>

namespace {

// 名称命中即非题材
constexpr std::string_view kDrop[] = {
    // 指数成分
    "_", "中证", "上证", "深证", "深成", "创业成份", "创业板综", "MSCI", "富时罗素", "标准普尔", "茅指数", "宁组合",
    // 交易制度
    "沪股通", "深股通", "融资融券", "转债标的", "B股", "AH股", "GDR", "ST股", "次新股", "摘帽", "做市",
    // 资金持股
    "重仓", "新进", "增仓", "减仓", "持股", "养老金", "举牌", "增持", "调研", "金股", "外资背景", "股权集中", "股权分散",
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

std::filesystem::path dir() { return misc::git_root() / "output" / "concept"; }
std::filesystem::path file_of(misc::Ymd d) { return dir() / (misc::dense(d) + ".tsv"); }

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
  misc::Ymd last = 0;
  if (!std::filesystem::exists(dir()))
    return last;
  for (const std::filesystem::directory_entry &e : std::filesystem::directory_iterator(dir()))
    if (e.path().extension() == ".tsv") {
      const misc::Ymd d = misc::parse_ymd(e.path().stem().string());
      assert(d > 0 && "concept: 快照文件名不是 YYYYMMDD.tsv");
      last = std::max(last, d);
    }
  return last;
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
  misc::atomic_write(file_of(date), s.data(), s.size());
}

const ConceptTable &concepts() {
  static const ConceptTable t = [] {
    ConceptTable o;
    AssetAxis &ax = asset_axis();
    const size_t G = ax.size();
    std::vector<std::vector<uint16_t>> by(G); // [全局 id] 所属概念
    o.hash = kFnvOffset;
    o.date = concept_last();
    if (o.date) {
      std::ifstream f(file_of(o.date));
      assert(f && "concept: 快照打不开");
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
  }();
  return t;
}
