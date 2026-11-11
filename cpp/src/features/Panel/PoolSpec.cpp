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

// 全选池的风格 / 只数 (取值归 0, 池名写 all)
bool inert(const Spec &s, int d) { return key_dim(d) && !s.keyed(); }

// 维 d 的段 (池名 / 中文名): 不起作用的取值维 / 集合维 0 → all / 全; 取值维 = 取值的码 / 中文名;
// 集合维 = 各位的码 / 中文名以 "+" 连 (位序), 风格池端的中文名 = 风格端名
std::string segment(const Spec &s, int d, bool cn) {
  const uint8_t m = s.v[d];
  if (inert(s, d) || (is_set(d) && !m))
    return cn ? "全" : "all";
  const std::span<const DimValue> vs = kDimDefs[d].values;
  if (!is_set(d))
    return cn ? vs[m].name_cn : vs[m].code;
  if (cn && d == kDimEnd)
    return kStyleDefs[s.style()].end_cn[s.end()];
  std::string t;
  for (int i = 0; i < dim_size(d); ++i)
    if (m >> i & 1)
      t += (t.empty() ? "" : "+") + std::string(cn ? vs[i].name_cn : vs[i].code);
  return t;
}

// 简称主体 (不带权重后缀)
std::string short_body(const Spec &s) {
  if (s.keyed())
    return segment(s, kDimEnd, true) + std::to_string(s.n());
  std::string c;
  for (int d = 0; d < kDims; ++d)
    if (kDimDefs[d].kind == kKindPart && on(s, d))
      c += segment(s, d, true);
  return c.empty() ? "全选" : c;
}

} // namespace

Spec canon(Spec s) {
  for (int d = 0; d < kDims; ++d)
    if (kDimDefs[d].kind == kKindPart && s.v[d] == full_mask(d))
      s.v[d] = 0;
  for (int d = 0; d < kDims; ++d)
    if (inert(s, d))
      s.v[d] = 0;
  return s;
}

Spec unweighted(Spec s) {
  s.v[kDimWeight] = 0;
  return s;
}

bool on(const Spec &s, int d) { return !inert(s, d) && (!is_set(d) || s.v[d] != 0); }

std::string code(const Spec &s) {
  std::string c;
  for (int d = 0; d < kDims; ++d)
    c += (d ? "." : "") + segment(s, d, false);
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
    const auto find = [&](std::string_view x) {
      const auto it = std::ranges::find_if(vs, [&](const DimValue &v) { return x == v.code; });
      assert((it != vs.end() || (seg[d] == "all" && (is_set(d) || key_dim(d)))) && "池名: 段不在该维取值表里");
      return it == vs.end() ? -1 : static_cast<int>(it - vs.begin());
    };
    if (!is_set(d)) {
      s.v[d] = static_cast<uint8_t>(std::max(find(seg[d]), 0));
      continue;
    }
    if (seg[d] == "all")
      continue;
    for (size_t i = 0;;) {
      const size_t j = seg[d].find('+', i);
      s.v[d] |= static_cast<uint8_t>(1u << find(seg[d].substr(i, j - i)));
      if (j == std::string_view::npos)
        break;
      i = j + 1;
    }
  }
  assert(code(s) == c && canon(s) == s && "池名: 不是规范拼法");
  return s;
}

std::array<Part, kParts> parts(const Spec &s) {
  std::array<Part, kParts> p;
  for (int d = kDimEnd; d < kDims; ++d)
    p[d - kDimEnd] = {segment(s, d, true), on(s, d)};
  return p;
}

std::string name_cn(const Spec &s) {
  std::string c;
  for (const Part &p : parts(s))
    c += (c.empty() ? "" : ".") + p.cn;
  return c;
}

std::string short_cn(const Spec &s) { return short_body(s) + weight_suffix(s.weight()); }

std::string detail_cn(const Spec &s) {
  std::string t = code(s) + "\n" + name_cn(s);
  if (s.keyed()) {
    const StyleDef &d = kStyleDefs[s.style()];
    t += std::string("\n• ") + d.name_cn + ": " + d.key + "\n• " + kEndValues[s.end()].name_cn + "前 " + std::to_string(s.n()) + " 只 (" + d.end_cn[s.end()] + ")";
  } else
    t += "\n• 切片内全选";
  t += std::string("\n• 调仓: ") + kRebalValues[s.rebal()].tip;
  t += std::string("\n• 权重: ") + kWeightValues[s.weight()].tip;
  bool cut = false;
  for (int d = kDimSt; d < kDims; ++d)
    if (on(s, d)) {
      t += std::string("\n• ") + kDimDefs[d].name_cn + " " + segment(s, d, true) + ":";
      for (int i = 0; i < dim_size(d); ++i)
        if (s.v[d] >> i & 1)
          t += std::string("\n    ") + kDimDefs[d].values[i].name_cn + " = " + kDimDefs[d].values[i].tip;
      cut = true;
    }
  return cut ? t : t + "\n• 切片: 全部在市";
}

std::vector<std::string> aliases(const std::vector<Spec> &pools) {
  std::vector<std::string> v;
  std::map<std::string, int> total, seen;
  for (const Spec &s : pools)
    ++total[v.emplace_back(short_cn(s))];
  for (size_t i = 0; i < v.size(); ++i)
    if (std::string &x = v[i]; total[x] > 1) {
      std::string suf;
      for (int k = seen[x]++ + 1; k > 0; k = (k - 1) / 26)
        suf.insert(suf.begin(), static_cast<char>('A' + (k - 1) % 26));
      x = short_body(pools[i]) + suf + weight_suffix(pools[i].weight());
    }
  return v;
}

std::vector<Spec> default_pools() {
  const auto bit = [](int i) { return static_cast<uint8_t>(1u << i); };
  Spec live; // 剔次新
  live.v[kDimFilter] = bit(kFltNew);
  Spec base = live; // 非ST, 再剔预警
  base.v[kDimSt] = bit(kStNone), base.v[kDimFilter] |= bit(kFltWarn);
  std::vector<Spec> v;
  const auto two = [&](Style st, End sd, uint8_t mg = 0) {
    for (const Count n : {kCount400, kCount200}) {
      Spec s = base;
      s.v[kDimStyle] = st, s.v[kDimEnd] = bit(sd), s.v[kDimN] = n, s.v[kDimMargin] = mg;
      v.push_back(s);
    }
  };
  two(kStyleSize, kEndLow), two(kStyleSize, kEndHigh), two(kStyleSize, kEndLow, bit(kMarginOn));
  two(kStyleEp, kEndHigh), two(kStyleEp, kEndLow), two(kStyleBp, kEndHigh), two(kStyleBp, kEndLow), two(kStyleDy, kEndHigh);
  two(kStyleRoe, kEndHigh), two(kStyleRoe, kEndLow), two(kStyleInv, kEndLow), two(kStyleInv, kEndHigh);
  two(kStyleMom, kEndHigh), two(kStyleMom, kEndLow), two(kStyleRev, kEndLow), two(kStyleRev, kEndHigh);
  two(kStyleTurn, kEndLow), two(kStyleTurn, kEndHigh), two(kStyleIvol, kEndLow), two(kStyleIvol, kEndHigh);
  two(kStyleResvol, kEndLow), two(kStyleResvol, kEndHigh), two(kStyleBeta, kEndLow), two(kStyleBeta, kEndHigh);
  two(kStyleTrend, kEndHigh), two(kStyleTrend, kEndLow), two(kStylePrice, kEndHigh), two(kStylePrice, kEndLow);
  const auto cut = [&](Dim d, int x) {
    Spec s = live;
    s.v[d] = bit(x);
    v.push_back(s);
  };
  for (const St st : {kStSt, kStSst})
    cut(kDimSt, st);
  for (const Board bd : {kBoardShMain, kBoardSzMain, kBoardStar, kBoardChiNext})
    cut(kDimBoard, bd);
  for (const Margin mg : {kMarginOn, kMarginOff})
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
  std::array<std::vector<uint8_t>, kDims> vals; // [维] 取值 (取值维 = 下标, 集合维 = 位集)
  for (int d = 0; d < kDims; ++d) {
    const uint16_t b = g.bits[d];
    assert(b >> dim_size(d) == 0 && (b || is_set(d)) && "网格: 取值维至少勾一个, 不越取值表");
    if (is_set(d))
      vals[d].push_back(0);
    for (int i = 0; i < dim_size(d); ++i)
      if (b >> i & 1)
        vals[d].push_back(static_cast<uint8_t>(is_set(d) ? 1u << i : i));
    if (is_set(d) && std::popcount(b) > 1)
      vals[d].push_back(static_cast<uint8_t>(b));
  }
  std::vector<Spec> v;
  std::set<Spec> seen;
  std::array<size_t, kDims> k{};
  for (;;) {
    Spec s;
    for (int d = 0; d < kDims; ++d)
      s.v[d] = vals[d][k[d]];
    if (const Spec c = canon(s); seen.insert(c).second)
      v.push_back(c);
    int d = kDims - 1;
    for (; d >= 0 && ++k[d] == vals[d].size(); --d)
      k[d] = 0;
    if (d < 0)
      return v;
  }
}

} // namespace pool
