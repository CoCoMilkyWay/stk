#include "features/Panel/PoolSpec.hpp"
#include "disk/Disk.hpp"

#include "nlohmann/json.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <charconv>
#include <iterator>
#include <map>
#include <set>
#include <sstream>

namespace pool {

namespace {

using json = nlohmann::json;

// c 在取值表 defs 里的下标 (没有 → assert)
template <size_t N>
int index_of(const DimValue (&defs)[N], std::string_view c) {
  for (size_t i = 0; i < N; ++i)
    if (c == defs[i].code)
      return static_cast<int>(i);
  assert(false && "池名: 段不在该维取值表里");
  return 0;
}

} // namespace

Spec canon(Spec s) {
  if (s.n == 0)
    s.style = kStyleSize, s.end = kEndLow;
  return s;
}

std::string code(const Spec &s) {
  const StyleDef &d = kStyleDefs[s.style];
  constexpr char kHex[] = "0123456789abcdef";
  std::string c = s.n ? std::string(d.code) + "_" + d.end[s.end] + "." + std::to_string(s.n) : std::string("all.all");
  for (const char *p : {kRebalDefs[s.rebal].code, kStDefs[s.st].code, kBoardDefs[s.board].code, kMarginDefs[s.margin].code})
    c += std::string(".") + p;
  return c + ".f" + kHex[s.filter];
}

Spec parse(std::string_view c) {
  std::vector<std::string_view> seg;
  for (size_t i = 0;;) {
    const size_t j = c.find('.', i);
    seg.push_back(c.substr(i, j - i));
    if (j == std::string_view::npos)
      break;
    i = j + 1;
  }
  assert(seg.size() == kParts && "池名: 须是 7 段");
  Spec s;
  if (seg[0] == "all") {
    assert(seg[1] == "all" && "池名: 全选池的只数段须是 all");
    s.n = 0;
  } else {
    const size_t u = seg[0].rfind('_');
    assert(u != std::string_view::npos && "池名: 风格端段须是 <风格>_<端>");
    int st = 0;
    while (st < kStyles && seg[0].substr(0, u) != kStyleDefs[st].code)
      ++st;
    assert(st < kStyles && "池名: 未知风格");
    const std::string_view sd = seg[0].substr(u + 1);
    assert((sd == kStyleDefs[st].end[0] || sd == kStyleDefs[st].end[1]) && "池名: 未知端");
    s.style = static_cast<Style>(st), s.end = sd == kStyleDefs[st].end[0] ? kEndLow : kEndHigh;
    int n = 0;
    const auto [p, ec] = std::from_chars(seg[1].data(), seg[1].data() + seg[1].size(), n);
    assert(ec == std::errc() && p == seg[1].data() + seg[1].size() && n > 0 && std::ranges::find(kCounts, n) != std::end(kCounts) &&
           "池名: 只数不在 kCounts 里");
    s.n = static_cast<uint16_t>(n);
  }
  s.rebal = static_cast<Rebal>(index_of(kRebalDefs, seg[2]));
  s.st = static_cast<St>(index_of(kStDefs, seg[3]));
  s.board = static_cast<Board>(index_of(kBoardDefs, seg[4]));
  s.margin = static_cast<Margin>(index_of(kMarginDefs, seg[5]));
  assert(seg[6].size() == 2 && seg[6][0] == 'f' && "池名: 过滤段须是 f<十六进制位>");
  const char h = seg[6][1];
  assert(((h >= '0' && h <= '9') || (h >= 'a' && h <= 'f')) && "池名: 过滤段须是 f<十六进制位>");
  s.filter = static_cast<uint8_t>(h <= '9' ? h - '0' : h - 'a' + 10);
  assert(code(s) == c && "池名: 不是规范拼法");
  return s;
}

std::array<Part, kParts> parts(const Spec &s) {
  const bool keyed = s.n > 0;
  return {{
      {keyed ? kStyleDefs[s.style].end_cn[s.end] : "全", keyed},
      {keyed ? std::to_string(s.n) : "全", keyed},
      {kRebalDefs[s.rebal].name_cn, true},
      {kStDefs[s.st].name_cn, s.st != kStAll},
      {kBoardDefs[s.board].name_cn, s.board != kBoardAll},
      {kMarginDefs[s.margin].name_cn, s.margin != kMarginAll},
      {s.filter ? "剔" + std::to_string(std::popcount(s.filter)) : "不剔", s.filter != 0},
  }};
}

std::string name_cn(const Spec &s) {
  std::string c;
  for (const Part &p : parts(s))
    c += (c.empty() ? "" : ".") + p.cn;
  return c;
}

std::string filter_cn(uint8_t f) {
  std::string c;
  for (int k = 0; k < kFilters; ++k)
    if (f >> k & 1)
      c += (c.empty() ? "" : " / ") + std::string(kFilterDefs[k].name_cn);
  return f ? c : "不剔";
}

std::string short_cn(const Spec &s) {
  if (s.n)
    return kStyleDefs[s.style].end_cn[s.end] + std::to_string(s.n);
  std::string c;
  if (s.st != kStAll)
    c += kStDefs[s.st].name_cn;
  if (s.board != kBoardAll)
    c += kBoardDefs[s.board].name_cn;
  if (s.margin != kMarginAll)
    c += kMarginDefs[s.margin].name_cn;
  return c.empty() ? "全选" : c;
}

std::string detail_cn(const Spec &s) {
  std::string t = code(s) + "\n" + name_cn(s);
  if (s.n) {
    const StyleDef &d = kStyleDefs[s.style];
    t += std::string("\n• ") + d.name_cn + ": " + d.key + "\n• " + (s.end == kEndLow ? "低端" : "高端") + "前 " + std::to_string(s.n) + " 只 (" + d.end_cn[s.end] + ")";
  } else
    t += "\n• 切片内全选";
  t += std::string("\n• 调仓: ") + kRebalDefs[s.rebal].tip;
  t += std::string("\n• 切片: ST ") + kStDefs[s.st].name_cn + " · 板块 " + kBoardDefs[s.board].name_cn + " · 融资 " + kMarginDefs[s.margin].name_cn;
  return t + "\n• 剔: " + filter_cn(s.filter);
}

std::vector<std::string> aliases(const std::vector<Spec> &pools) {
  std::vector<std::string> v;
  std::map<std::string, int> total, seen;
  for (const Spec &s : pools)
    ++total[v.emplace_back(short_cn(s))];
  for (std::string &x : v)
    if (total[x] > 1) {
      std::string suf;
      for (int k = seen[x]++ + 1; k > 0; k = (k - 1) / 26)
        suf.insert(suf.begin(), static_cast<char>('A' + (k - 1) % 26));
      x += suf;
    }
  return v;
}

std::vector<Spec> default_pools() {
  constexpr uint8_t kAll = (1u << kFilters) - 1, kLive = kAll & ~(1u << kFltWarn);
  std::vector<Spec> v;
  const auto two = [&](Style st, End sd, Margin mg = kMarginAll) {
    for (const uint16_t n : {400, 200})
      v.push_back({st, sd, n, kRebalDay, kStNone, kBoardAll, mg, kAll});
  };
  two(kStyleSize, kEndLow), two(kStyleSize, kEndHigh), two(kStyleSize, kEndLow, kMarginOn);
  two(kStyleEp, kEndHigh), two(kStyleEp, kEndLow), two(kStyleBp, kEndHigh), two(kStyleBp, kEndLow), two(kStyleDy, kEndHigh);
  two(kStyleRoe, kEndHigh), two(kStyleRoe, kEndLow), two(kStyleInv, kEndLow), two(kStyleInv, kEndHigh);
  two(kStyleMom, kEndHigh), two(kStyleMom, kEndLow), two(kStyleRev, kEndLow), two(kStyleRev, kEndHigh);
  two(kStyleTurn, kEndLow), two(kStyleTurn, kEndHigh), two(kStyleIvol, kEndLow), two(kStyleIvol, kEndHigh);
  two(kStyleResvol, kEndLow), two(kStyleResvol, kEndHigh), two(kStyleBeta, kEndLow), two(kStyleBeta, kEndHigh);
  two(kStyleTrend, kEndHigh), two(kStyleTrend, kEndLow), two(kStylePrice, kEndHigh), two(kStylePrice, kEndLow);
  for (const St st : {kStSt, kStSst})
    v.push_back(canon({.n = 0, .st = st, .filter = kLive}));
  for (const Board bd : {kBoardShMain, kBoardSzMain, kBoardStar, kBoardChiNext})
    v.push_back(canon({.n = 0, .board = bd, .filter = kLive}));
  for (const Margin mg : {kMarginOn, kMarginOff})
    v.push_back(canon({.n = 0, .margin = mg, .filter = kLive}));
  return v;
}

std::vector<Spec> load_pools() {
  if (!disk::exists(disk::PoolsFile))
    return default_pools();
  const json j = json::parse(disk::read(disk::PoolsFile), nullptr, /*allow_exceptions=*/false);
  assert(!j.is_discarded() && j.is_object() && j.contains("pools") && j["pools"].is_array() && "池表文件: 顶层须是 {pools: [...]}");
  std::vector<Spec> v;
  std::set<Spec> seen;
  for (const json &o : j["pools"]) {
    assert(o.is_string() && "池表文件: 条目须是池名");
    const Spec s = parse(o.get<std::string>());
    assert(seen.insert(s).second && "池表文件: 池重复");
    v.push_back(s);
  }
  return v;
}

// 一池一行 (人能直接读 / diff)
void save_pools(const std::vector<Spec> &pools) {
  std::ostringstream f;
  {
    f << "{\n  \"pools\": [";
    for (size_t i = 0; i < pools.size(); ++i)
      f << (i ? ",\n    " : "\n    ") << json(code(pools[i])).dump();
    f << (pools.empty() ? "]\n}\n" : "\n  ]\n}\n");
  }
  disk::write(disk::PoolsFile, {}, f.str());
}

std::vector<Spec> specs_of(Style st, const Grid &g) {
  std::vector<uint8_t> flts; // 网格允许的过滤集
  for (unsigned f = 0; f < (1u << kFilters); ++f) {
    bool ok = true;
    for (int k = 0; k < kFilters; ++k)
      ok &= (g.filter[k] >> (f >> k & 1) & 1) != 0;
    if (ok)
      flts.push_back(static_cast<uint8_t>(f));
  }
  std::vector<Spec> v;
  std::set<Spec> seen;
  for (int sd = 0; sd < kEnds; ++sd)
    for (int k = 0; k < kNCounts; ++k)
      for (int t = 0; t < kSts; ++t)
        for (int bd = 0; bd < kBoards; ++bd)
          for (int mg = 0; mg < kMargins; ++mg)
            for (const uint8_t f : flts)
              for (int rb = 0; rb < kRebals; ++rb) {
                if (!(g.end >> sd & 1) || !(g.n >> k & 1) || !(g.st >> t & 1) || !(g.board >> bd & 1) || !(g.margin >> mg & 1) || !(g.rebal >> rb & 1))
                  continue;
                const Spec s = canon({st, static_cast<End>(sd), static_cast<uint16_t>(kCounts[k]), static_cast<Rebal>(rb), static_cast<St>(t),
                                      static_cast<Board>(bd), static_cast<Margin>(mg), f});
                if (seen.insert(s).second)
                  v.push_back(s);
              }
  return v;
}

} // namespace pool
