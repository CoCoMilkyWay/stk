#include "features/Panel/PoolSpec.hpp"
#include "disk/Disk.hpp"

#include "nlohmann/json.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <map>
#include <set>
#include <sstream>

namespace pool {

namespace {

using json = nlohmann::json;

// 全选池的风格 / 端 (取值归 0, 池名写 all)
bool inert(const Spec &s, int d) { return (d == kDimStyle || d == kDimEnd) && !s.keyed(); }

} // namespace

Spec canon(Spec s) {
  if (!s.keyed())
    s.v[kDimStyle] = 0, s.v[kDimEnd] = 0;
  return s;
}

bool on(const Spec &s, int d) { return !inert(s, d) && !(kDimDefs[d].all0 && s.v[d] == 0); }

std::string code(const Spec &s) {
  std::string c;
  for (int d = 0; d < kDims; ++d)
    c += std::string(d ? "." : "") + (inert(s, d) ? "all" : kDimDefs[d].values[s.v[d]].code);
  return c;
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
  assert(seg.size() == kDims && "池名: 段数须是维数");
  Spec s;
  for (int d = 0; d < kDims; ++d) {
    const std::span<const DimValue> vs = kDimDefs[d].values;
    const auto it = std::ranges::find_if(vs, [&](const DimValue &x) { return seg[d] == x.code; });
    assert((it != vs.end() || ((d == kDimStyle || d == kDimEnd) && seg[d] == "all")) && "池名: 段不在该维取值表里");
    s.v[d] = it == vs.end() ? 0 : static_cast<uint8_t>(it - vs.begin());
  }
  assert(code(s) == c && "池名: 不是规范拼法");
  return s;
}

std::array<Part, kParts> parts(const Spec &s) {
  std::array<Part, kParts> p;
  for (int d = kDimEnd; d < kDims; ++d)
    p[d - kDimEnd] = {inert(s, d)    ? "全"
                      : d == kDimEnd ? kStyleDefs[s.style()].end_cn[s.end()]
                                     : kDimDefs[d].values[s.v[d]].name_cn,
                      on(s, d)};
  return p;
}

std::string name_cn(const Spec &s) {
  std::string c;
  for (const Part &p : parts(s))
    c += (c.empty() ? "" : ".") + p.cn;
  return c;
}

std::string short_cn(const Spec &s) {
  if (s.keyed())
    return kStyleDefs[s.style()].end_cn[s.end()] + std::to_string(s.n());
  std::string c;
  for (int d = kDimSt; d < kDimNew; ++d)
    if (on(s, d))
      c += kDimDefs[d].values[s.v[d]].name_cn;
  return c.empty() ? "全选" : c;
}

std::string detail_cn(const Spec &s) {
  std::string t = code(s) + "\n" + name_cn(s);
  if (s.keyed()) {
    const StyleDef &d = kStyleDefs[s.style()];
    t += std::string("\n• ") + d.name_cn + ": " + d.key + "\n• " + kEndValues[s.end()].name_cn + "前 " + std::to_string(s.n()) + " 只 (" + d.end_cn[s.end()] + ")";
  } else
    t += "\n• 切片内全选";
  t += std::string("\n• 调仓: ") + kRebalValues[s.rebal()].tip;
  bool cut = false;
  for (int d = kDimSt; d < kDims; ++d)
    if (on(s, d)) {
      const DimValue &v = kDimDefs[d].values[s.v[d]];
      t += std::string("\n• ") + kDimDefs[d].name_cn + " " + v.name_cn + ": " + v.tip;
      cut = true;
    }
  return cut ? t : t + "\n• 切片: 全部在市";
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
  Spec live; // 剔次新 / 停牌 / 行业未知
  live.v[kDimNew] = live.v[kDimSusp] = live.v[kDimInd] = kFlagOff;
  Spec base = live; // 非ST, 再剔预警
  base.v[kDimSt] = kStNone, base.v[kDimWarn] = kFlagOff;
  std::vector<Spec> v;
  const auto two = [&](Style st, End sd, Flag mg = kFlagAll) {
    for (const Count n : {kCount400, kCount200}) {
      Spec s = base;
      s.v[kDimStyle] = st, s.v[kDimEnd] = sd, s.v[kDimN] = n, s.v[kDimMargin] = mg;
      v.push_back(s);
    }
  };
  two(kStyleSize, kEndLow), two(kStyleSize, kEndHigh), two(kStyleSize, kEndLow, kFlagOn);
  two(kStyleEp, kEndHigh), two(kStyleEp, kEndLow), two(kStyleBp, kEndHigh), two(kStyleBp, kEndLow), two(kStyleDy, kEndHigh);
  two(kStyleRoe, kEndHigh), two(kStyleRoe, kEndLow), two(kStyleInv, kEndLow), two(kStyleInv, kEndHigh);
  two(kStyleMom, kEndHigh), two(kStyleMom, kEndLow), two(kStyleRev, kEndLow), two(kStyleRev, kEndHigh);
  two(kStyleTurn, kEndLow), two(kStyleTurn, kEndHigh), two(kStyleIvol, kEndLow), two(kStyleIvol, kEndHigh);
  two(kStyleResvol, kEndLow), two(kStyleResvol, kEndHigh), two(kStyleBeta, kEndLow), two(kStyleBeta, kEndHigh);
  two(kStyleTrend, kEndHigh), two(kStyleTrend, kEndLow), two(kStylePrice, kEndHigh), two(kStylePrice, kEndLow);
  const auto cut = [&](Dim d, uint8_t x) {
    Spec s = live;
    s.v[d] = x;
    v.push_back(s);
  };
  for (const St st : {kStSt, kStSst})
    cut(kDimSt, st);
  for (const Board bd : {kBoardShMain, kBoardSzMain, kBoardStar, kBoardChiNext})
    cut(kDimBoard, bd);
  for (const Flag mg : {kFlagOn, kFlagOff})
    cut(kDimMargin, mg);
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

Style Grid::style() const {
  assert(std::popcount(bits[kDimStyle]) == 1 && "网格: 风格维须只勾一个");
  return static_cast<Style>(std::countr_zero(bits[kDimStyle]));
}

std::vector<Spec> specs_of(const Grid &g) {
  for (int d = 0; d < kDims; ++d)
    assert(g.bits[d] && g.bits[d] >> dim_size(d) == 0 && "网格: 每维至少勾一个, 不越取值表");
  std::vector<Spec> v;
  std::set<Spec> seen;
  Spec s;
  const auto first = [&](int d) { s.v[d] = static_cast<uint8_t>(std::countr_zero(g.bits[d])); };
  for (int d = 0; d < kDims; ++d)
    first(d);
  for (;;) {
    if (const Spec c = canon(s); seen.insert(c).second)
      v.push_back(c);
    int d = kDims - 1;
    for (; d >= 0; --d) {
      const unsigned rest = g.bits[d] >> (s.v[d] + 1u);
      if (rest) {
        s.v[d] = static_cast<uint8_t>(s.v[d] + 1 + std::countr_zero(rest));
        break;
      }
      first(d);
    }
    if (d < 0)
      return v;
  }
}

} // namespace pool
