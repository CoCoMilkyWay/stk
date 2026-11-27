// Table — 见头文件. include 因子契约头 (define/Label.hpp: hold_ok / close_entry), 依赖受控浮点: CMake MATH_PRECISE 源.
#include "gui/engine/Table.hpp"

#include "features/Storage/FeatureLib.hpp"
#include "features/TimeIndex.hpp" // 行末时刻 (row_clocks)
#include "misc/epoch.hpp"
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <ctime>
#include <mutex>
#include <string_view>
#include <utility>

namespace GUI::Engine {

std::string now_string() {
  const std::time_t t = std::time(nullptr);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M", std::localtime(&t));
  return buf;
}

const FeatCol *XCols::find(std::string_view code) const {
  const auto it = index.find(code);
  return it == index.end() ? nullptr : &leaves[it->second];
}

std::vector<LeafId> XCols::ids(const std::vector<std::string> &codes) const {
  std::vector<LeafId> out;
  for (const std::string &code : codes) {
    const auto it = index.find(code);
    assert(it != index.end() && "叶不在字段表里");
    out.push_back(it->second);
  }
  return out;
}

factor::expr::FeatureLookup FeatureTable::lookup() const {
  factor::expr::FeatureLookup fl;
  fl.feat = [this](std::string_view code) {
    if (const FeatCol *c = x.find(code))
      return c->allowed ? factor::expr::FeatState::OK : factor::expr::FeatState::FORBIDDEN;
    // 档.层.列 的层比网格细 (字段表只收不细于网格的层)
    const size_t p = code.find('.'), q = p == std::string_view::npos ? p : code.find('.', p + 1);
    if (q != std::string_view::npos)
      for (size_t l = 0; l < factor_level(lv).lvl; ++l)
        if (code.substr(p + 1, q - p - 1) == kLevelToken[l])
          return factor::expr::FeatState::FINER;
    return factor::expr::FeatState::MISSING;
  };
  fl.label = [this](HoldId hold) { return label_leaf(hold); };
  return fl;
}

std::string FeatureTable::label_leaf(HoldId hold) const {
  if (!y.has(hold))
    return {};
  const Col &c = y.lab[hold].lv;
  return std::string(tier_name(c.tier)) + "." + kLevelToken[c.level] + "." + feature->meta(c.tier).features[c.level][c.col].code;
}

bool FeatureTable::check_grid(const factor::expr::Expr &e, std::string &err) const {
  const FactorLevel &l = factor_level(lv);
  size_t finest = LEVEL_COUNT;
  for (const factor::expr::Node &n : e.nodes)
    if (n.op < 0 && !n.target) {
      const FeatCol *c = x.find(n.feat);
      assert(c && "parse 过的叶须在字段表里");
      finest = std::min(finest, c->level);
    }
  if (finest == LEVEL_COUNT || finest == l.lvl) // 无特征叶 (纯生成算子) 随目录层
    return true;
  assert(finest > l.lvl);
  err = std::string("最细的特征层是 ") + kLevelToken[finest] + ", 比因子层 " + l.name + " 粗: 因子层 = 所用特征的最细层, 放到对应层目录";
  return false;
}

factor::expr::Expr FeatureTable::reparse(std::string_view canon) const {
  factor::expr::Expr e;
  std::string err;
  const bool ok = factor::expr::parse(canon, lookup(), seg, e, err);
  assert(ok && "已校验的规范串必可再解析");
  (void)ok;
  return e;
}

factor::expr::Expr FeatureTable::reparse_at(std::string_view canon, HoldId hold) const {
  const factor::expr::Expr t = reparse(canon);
  return factor::expr::free_hold(t) ? reparse(factor::expr::bind_hold(t, hold)) : t;
}

FactorLevelId factor_level_of(std::string_view name) {
  for (const FactorLevel &l : kFactorLevels)
    if (l.name == name)
      return l.id;
  assert(false && "name 不是因子层");
  __builtin_unreachable();
}

HoldPoints hold_points(HoldId hold, FactorLevelId lv) {
  const FactorLevel &L = factor_level(lv);
  const bool at_close = close_entry(0, L.geo());
  const std::string px = "次分钟VWAP", close_px = "收盘撮合价", open_px = "开盘撮合价";
  const std::string at = at_close ? "C(T)" : "C(" + std::string(L.name) + ")";
  const HoldPoint open{at, at_close ? close_px : px};
  if (hold_kind(hold) == HoldKind::Min)
    return {open, {at + "+" + std::to_string(hold_n(hold)) + "m", px}};
  if (hold_kind(hold) == HoldKind::Close)
    return {open, {"C(T)", close_px}};
  const std::string n = std::to_string(hold_days(hold));
  if (hold_kind(hold) == HoldKind::Open)
    return {open, {"O(T+" + n + ")", open_px}};
  return {open, at_close ? HoldPoint{"C(T+" + n + ")", close_px} : HoldPoint{at + "+" + n + "日 (同刻)", px}};
}

std::string hold_desc(HoldId hold, FactorLevelId lv) {
  const HoldPoints p = hold_points(hold, lv);
  return "开仓: " + p.open.at + " @ " + p.open.px + ", 清仓: " + p.close.at + " @ " + p.close.px;
}

std::vector<std::string> row_clocks(FactorLevelId lv) {
  const FactorLevel &l = factor_level(lv);
  std::vector<std::string> out;
  for (int k = 0; k < l.seg; ++k) {
    ClockTime c{15, 0, 0};
    if (l.lvl == L_1min) {
      const ClockTime m = L_1min_to_Clock(static_cast<size_t>(k));
      const int total = m.hour * 60 + m.minute + 1;
      c = {static_cast<uint8_t>(total / 60), static_cast<uint8_t>(total % 60), 0};
    } else if (l.seg > 1) {
      c = bin_to_Clock(static_cast<size_t>(240 / (l.seg - 1)), static_cast<size_t>(k));
    }
    char b[16];
    format_time_hm(b, sizeof(b), c);
    out.emplace_back(b);
  }
  return out;
}

disk::Key FactorDir::of(std::string_view name) const {
  disk::Key k = key;
  [[maybe_unused]] const bool ok = disk::parse(file, name, k);
  assert(ok && "因子文件名不合布局 (<名>.json)");
  return k;
}

std::string FactorDir::dir() const { return disk::path(disk::def(file).parent, key).string(); }

std::vector<std::string> FactorDir::files() const {
  std::vector<std::string> out;
  for (const disk::Key &k : disk::list(file, key))
    out.push_back(disk::name(file, k));
  return out;
}

FactorDir factor_dir_of(const std::string &pool, FactorLevelId lv) { return {.key = {.pool = pool, .level = factor_level(lv).name}}; }

PerLib<FeatureLib> factor_libs(const std::string &pool) {
  PerLib<FeatureLib> libs;
  for (const TierId t : TIER_IDS) {
    const LibId l = lib_id(pool_id(pool), t);
    libs[l] = load_lib(l);
  }
  return libs;
}

FeatureTable BuildFeatureTable(const Feature &feature, FactorLevelId id) {
  const FactorLevel &lv = factor_level(id);
  FeatureTable t;
  t.lv = id;
  t.seg = lv.seg;
  t.feature = &feature;
  for (const TierId tier : {TierId::Lob, TierId::Bar}) {
    for (size_t l = lv.lvl; l < LEVEL_COUNT; ++l) {
      if (!has_level(tier, l))
        continue;
      const auto &list = feature.meta(tier).features[l];
      const std::string pre = std::string(tier_name(tier)) + "." + kLevelToken[l] + ".";
      for (size_t i = 0; i < list.size(); ++i) {
        const FeatureMetadata &f = list[i];
        t.x.leaves.push_back({{tier, l, static_cast<uint32_t>(i)}, pre + f.code, f.valid_type, f.data_type == FeatureDataType::TS || f.data_type == FeatureDataType::CS});
        const bool fresh = t.x.index.emplace(t.x.leaves.back().code, static_cast<LeafId>(t.x.leaves.size() - 1)).second;
        assert(fresh && "叶名重复");
        (void)fresh;
      }
    }
  }
  const Feature::Metadata &meta = feature.meta(TierId::Bar);
  const auto own = [&](uint32_t c) { return Col{TierId::Bar, lv.lvl, c}; }; // Bar 档网格层本层列
  const auto named = [&](const char *code) { return own(static_cast<uint32_t>(meta.col_of(lv.lvl, code))); };
  t.y.ts = named("ts_valid");
  t.y.cs = named("cs_valid");
  const char *ohlc[4] = {"open", "high", "low", "close"};
  for (int i = 0; i < 4; ++i) {
    char code[32];
    std::snprintf(code, sizeof(code), lv.kline, ohlc[i]);
    t.b.ohlc[i] = named(code);
  }
  const auto daily = [&](const char *code) { return Col{TierId::Bar, L_1day, static_cast<uint32_t>(meta.col_of(L_1day, code))}; }; // 日频原料 bar.1d
  t.b.adj_factor = daily("adj_factor");
  t.y.entry = named("lb_entry_ok");
  // 档 tier 层 lvl 的列 code 的下标; 无 → −1
  const auto find_at = [&](TierId tier, size_t lvl, const std::string &code) {
    const auto &list = feature.meta(tier).features[lvl];
    for (size_t i = 0; i < list.size(); ++i)
      if (code == list[i].code)
        return static_cast<int>(i);
    return -1;
  };
  const auto find = [&](TierId tier, const std::string &code) { return find_at(tier, lv.lvl, code); };
  // 标签: 本层合法的持有期 (hold_ok) 恰是落了列的 (字段表 HOLDS_FROM_<LVL> 与之一致); 名义列只偏移型
  for (int i = 0; i < kHoldCount; ++i) {
    const HoldId h = static_cast<HoldId>(i);
    const std::string tok(hold_token(h));
    const int lv_col = find(TierId::Bar, "lb_" + tok);
    assert((lv_col >= 0) == hold_ok(h, lv.geo()) && "本层的标签列与持有期合法性不符 (HOLDS_FROM_<LVL> ⇄ hold_ok)");
    if (lv_col < 0)
      continue;
    t.y.holds = t.y.holds.with(h);
    t.y.lab[h].lv = own(static_cast<uint32_t>(lv_col));
    t.y.lab[h].nv = hold_offset(h) ? named(("lb_nom_" + tok).c_str()) : own(kNoCol);
    const auto it = t.x.index.find(t.label_leaf(h));
    assert(it != t.x.index.end() && "做多标签列须是本层叶");
    t.x.leaves[it->second].target = true;
  }
  assert(t.y.holds.n > 0 && "本层无合法持有期");
  // 成本: 各层都有全部金额段; 盘口段的平面 = Lob 档 L_1min 的两侧冲击列 (分钟各层的行 = 其行末分钟那一行, 读计划 level_asof 投影);
  //   日层每行都是收盘撮合建仓 (盘口冲击 0, 与分钟层的收盘建仓尾行同约) → 无平面
  if (lv.lvl != L_1day)
    for (int k = 0; k < kLobAmts; ++k) {
      const std::string w = std::to_string(kLobWan[k]) + "w";
      const int b = find_at(TierId::Lob, L_1min, "lb_cost_buy_" + w), sl = find_at(TierId::Lob, L_1min, "lb_cost_sell_" + w);
      assert(b >= 0 && sl >= 0 && "冲击成本列不齐: 某金额段缺 buy 或 sell");
      t.y.cost[k][kBuy] = Col{TierId::Lob, L_1min, static_cast<uint32_t>(b)};
      t.y.cost[k][kSell] = Col{TierId::Lob, L_1min, static_cast<uint32_t>(sl)};
    }
  return t;
}

namespace {
struct TableShelf {
  std::mutex mu;
  PerFactorLevel<std::shared_ptr<const FeatureTable>> v;
};
} // namespace

std::shared_ptr<const FeatureTable> table_of(const Feature &feature, FactorLevelId lv) {
  static misc::Epochal<TableShelf> shelf;
  auto &[mu, v] = shelf.get();
  std::lock_guard<std::mutex> lock(mu);
  if (!v[lv])
    v[lv] = std::make_shared<FeatureTable>(BuildFeatureTable(feature, lv));
  return v[lv];
}

bool MakeFactorScope(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, int years, bool gpu, FactorScope &sc) {
  assert(ft && years >= 0);
  const FactorLevelId lv = ft->lv;
  sc = FactorScope{};
  sc.lv = lv;
  sc.pool = pool;
  sc.dir = factor_dir_of(pool, lv);
  sc.ft = std::move(ft);
  sc.years = years;
  sc.cost = data.config.cost;
  sc.gpu = gpu;
  if (data.asset.items.empty())
    return false; // 资产轴未就绪 (数据库未扫), 读不了特征库
  sc.libs = factor_libs(pool);
  return sc.libs[label_lib(pool_id(pool))].ready(); // 标签库未建 → false; 叶所在库未建由评估域报 (span_of)
}

namespace {
std::mutex g_caps_mu;
std::map<std::pair<std::string, FactorLevelId>, int> g_caps;
} // namespace

void cap_years(const std::string &pool, FactorLevelId lv, int years) {
  assert(years >= 1);
  std::lock_guard<std::mutex> lock(g_caps_mu);
  const auto [it, fresh] = g_caps.try_emplace({pool, lv}, years);
  if (!fresh)
    it->second = std::min(it->second, years);
}

int year_cap(const std::string &pool, FactorLevelId lv) {
  std::lock_guard<std::mutex> lock(g_caps_mu);
  const auto it = g_caps.find({pool, lv});
  return it == g_caps.end() ? 0 : it->second;
}

} // namespace GUI::Engine