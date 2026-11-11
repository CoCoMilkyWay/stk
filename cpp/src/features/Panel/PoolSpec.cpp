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

// 维 d 的段 (池名 / 中文名): 单选维 = 取值的码 / value_cn; 集合维 = 各位的码 / value_cn 以 "+" 连 (位序), 0 → all / none_cn
std::string segment(const Spec &s, int d, bool cn) {
  const uint8_t m = s.v[d];
  const std::span<const DimValue> vs = kDimDefs[d].values;
  const auto one = [&](int i) { return std::string(cn ? value_cn(d, i, s.style()) : vs[i].code); };
  if (!is_set(d))
    return one(m);
  if (!m)
    return cn ? none_cn(d) : "all";
  std::string t;
  for (int i = 0; i < dim_size(d); ++i)
    if (m >> i & 1)
      t += (t.empty() ? "" : "+") + one(i);
  return t;
}

// 简称主体 (不带权重后缀) = 端名 + 只数
std::string short_body(const Spec &s) { return segment(s, kDimEnd, true) + std::to_string(s.n()); }

} // namespace

Spec canon(Spec s) {
  for (int d = 0; d < kDims; ++d)
    if (kDimDefs[d].kind == kKindPart && s.v[d] == full_mask(d))
      s.v[d] = 0;
  return s;
}

Spec unweighted(Spec s) {
  s.v[kDimWeight] = 0;
  return s;
}

bool on(const Spec &s, int d) { return !is_set(d) || s.v[d] != 0; }

int slot(const Spec &s, int d) {
  const uint8_t m = s.v[d];
  if (!is_set(d))
    return m;
  return !m ? slot_none(d) : std::has_single_bit(m) ? std::countr_zero(m)
                                                    : slot_union(d);
}

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
      assert(it != vs.end() && "池名: 段不在该维取值表里");
      return static_cast<int>(it - vs.begin());
    };
    if (!is_set(d)) {
      s.v[d] = static_cast<uint8_t>(find(seg[d]));
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

std::array<DimDetail, kDims> detail(const Spec &s) {
  std::array<DimDetail, kDims> v;
  for (int d = 0; d < kDims; ++d) {
    const std::span<const DimValue> vs = kDimDefs[d].values;
    std::string n;
    if (!is_set(d))
      n = vs[s.v[d]].tip;
    else if (!s.v[d])
      n = kDimDefs[d].kind == kKindFilter ? "不剔" : "不切 (全部类)";
    else
      for (int i = 0; i < dim_size(d); ++i)
        if (s.v[d] >> i & 1)
          n += (n.empty() ? "" : "\n") + std::string(vs[i].name_cn) + " = " + vs[i].tip;
    v[d] = {segment(s, d, true), std::move(n), on(s, d)};
  }
  return v;
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
  Spec base; // 非ST, 剔次新 / 预警
  base.v[kDimSt] = bit(kStNone), base.v[kDimFilter] = bit(kFltNew) | bit(kFltWarn);
  std::vector<Spec> v;
  const auto two = [&](Style st, End sd, uint8_t mg = 0) {
    for (const Count n : {kCount400, kCount200}) {
      Spec s = base;
      s.v[kDimStyle] = st, s.v[kDimEnd] = sd, s.v[kDimN] = n, s.v[kDimMargin] = mg;
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

uint16_t Grid::slots(int d) const {
  const uint16_t b = bits[d];
  if (!is_set(d))
    return b;
  const bool many = std::popcount(b) > 1 && !(kDimDefs[d].kind == kKindPart && b == full_mask(d));
  return static_cast<uint16_t>(b | (many ? 1u << slot_union(d) : 0u) | 1u << slot_none(d));
}

std::vector<Spec> specs_of(const Grid &g) {
  std::array<std::vector<uint8_t>, kDims> vals; // [维] 各取值槽的 Spec::v (单选维 = 下标, 集合维 = 位集)
  for (int d = 0; d < kDims; ++d) {
    const uint16_t b = g.bits[d];
    assert(b >> dim_size(d) == 0 && (b || is_set(d)) && "网格: 单选维至少勾一个, 不越取值表");
    for (uint16_t k = g.slots(d); k; k &= static_cast<uint16_t>(k - 1)) {
      const int i = std::countr_zero(k);
      vals[d].push_back(static_cast<uint8_t>(!is_set(d) ? i : i < dim_size(d)  ? 1u << i
                                                          : i == slot_union(d) ? b
                                                                               : 0u));
    }
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
