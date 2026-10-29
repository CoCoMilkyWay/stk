// FactorsTable — 见头文件. include 因子契约头 (Stat/Contract.hpp: hold_ok / hold_from_name), 依赖受控浮点: CMake PRECISE_MATH 源.
#include "gui/task_factors/services/FactorsTable.hpp"

#include "shared/FeatureLib.hpp" // feature_pools
#include "shared/SharedData.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <set>
#include <string_view>

namespace GUI::Factors {

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

const LegCol *find_leg(const std::vector<LegCol> &cols, int key) {
  for (const LegCol &c : cols)
    if (c.key == key)
      return &c;
  return nullptr;
}

const LegCol &leg_of(const std::vector<LegCol> &cols, int key) {
  const LegCol *c = find_leg(cols, key);
  assert(c && "key 不在字段表两腿列里");
  return *c;
}

factor::expr::FeatureLookup FeatureTable::lookup() const {
  return [this](std::string_view code) {
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
}

bool FeatureTable::check_grid(const factor::expr::Expr &e, std::string &err) const {
  const FactorLevel &l = factor_level(lv);
  size_t finest = LEVEL_COUNT;
  for (const factor::expr::Node &n : e.nodes)
    if (n.op < 0) {
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

LevelId level_of(std::string_view name) {
  for (const FactorLevel &l : kFactorLevels)
    if (l.name == name)
      return l.id;
  assert(false && "name 不是因子层");
  __builtin_unreachable();
}

std::string factor_dir_of(const SharedData &data, const std::string &pool, LevelId lv) {
  const FactorLevel &l = factor_level(lv);
  assert(level_for_pool(l, pool_def(pool).tier) && "该池不能用这个因子层");
  return data.config.factor_dir + "/" + pool + "/" + l.name;
}

PerPool<FeatureLib> factor_libs(const SharedData &data, const std::string &pool, LevelId lv) {
  const FactorLevel &l = factor_level(lv);
  assert(level_for_pool(l, pool_def(pool).tier) && "该池不能用这个因子层");
  PerPool<FeatureLib> libs;
  libs[kAllPool] = load_lib(data.config, pool_def(kAllPool).name);
  if (const PoolId p = pool_id(pool); p != kAllPool)
    libs[p] = load_lib(data.config, pool);
  return libs;
}

std::vector<std::filesystem::path> list_factor_files(const std::string &dir) {
  std::vector<std::filesystem::path> files;
  if (std::filesystem::is_directory(dir))
    for (const auto &ent : std::filesystem::directory_iterator(dir))
      if (ent.is_regular_file() && ent.path().extension() == ".json")
        files.push_back(ent.path());
  std::sort(files.begin(), files.end());
  return files;
}

FeatureTable BuildFeatureTable(const Feature &feature, LevelId id) {
  const FactorLevel &lv = factor_level(id);
  FeatureTable t;
  t.lv = id;
  t.seg = lv.seg;
  t.feature = &feature;
  for (const TierId tier : {TierId::Lob, TierId::Bar}) {
    if (tier == TierId::Lob && lv.tier != TierId::Lob) // lob.* = 因子池的库, 只有 Lob 标签层的池是 Lob 池
      continue;
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
  const Feature::Metadata &meta = feature.meta(lv.tier);
  const auto &level = meta.features[lv.lvl];
  const auto own = [&](uint32_t c) { return Col{lv.tier, lv.lvl, c}; }; // 标签档网格层本层列
  const auto named = [&](const char *code) { return own(static_cast<uint32_t>(meta.col_of(lv.lvl, code))); };
  t.y.ts = named("ts_valid");
  t.y.cs = named("cs_valid");
  const char *ohlc[4] = {"open", "high", "low", "close"};
  for (int i = 0; i < 4; ++i) {
    char code[32];
    std::snprintf(code, sizeof(code), lv.kline, ohlc[i]);
    t.b.ohlc[i] = named(code);
  }
  const Feature::Metadata &bar = feature.meta(TierId::Bar);
  const auto daily = [&](const char *code) { return Col{TierId::Bar, L_1day, static_cast<uint32_t>(bar.col_of(L_1day, code))}; }; // 日频原料 bar.1d
  t.b.adj_factor = daily("adj_factor");
  t.b.total_shares = daily("total_shares");
  for (int m = 0; m < kPoolVals; ++m)
    t.b.val[m] = daily(kPoolValCols[m]);
  t.b.ind_l1 = daily("ind_l1");
  for (const PoolDef &d : feature_pools())
    t.b.pools.push_back(d.pool_col == SIZE_MAX ? Col{} : Col{TierId::Bar, L_1day, static_cast<uint32_t>(d.pool_col)});
  struct Lab {
    bool is_long;
    int hold;
    uint32_t col;
  };
  struct Cost {
    bool is_buy;
    int amt;
    uint32_t col;
  };
  std::vector<Lab> labs;
  std::vector<Cost> costs;
  std::set<int> holds, amts;
  t.y.entry = own(kNoCol);
  for (size_t i = 0; i < level.size(); ++i) {
    const FeatureMetadata &f = level[i];
    if (f.data_type != FeatureDataType::LB)
      continue;
    if (std::string_view(f.code) == "lb_entry_ok") {
      t.y.entry = own(static_cast<uint32_t>(i));
      continue;
    }
    char side[8] = {}, name[16] = {};
    int amt = 0;
    if (std::sscanf(f.code, "lb_cost_%7[a-z]_%dw", side, &amt) == 2) {
      assert((std::string_view(side) == "buy" || std::string_view(side) == "sell") && amt > 0 && "冲击成本列命名不合 lb_cost_<buy|sell>_<amt>w");
      costs.push_back({std::string_view(side) == "buy", amt, static_cast<uint32_t>(i)});
      amts.insert(amt);
      continue;
    }
    const int got = std::sscanf(f.code, "lb_%7[a-z]_%15[a-z0-9]", side, name);
    assert(got == 2 && (std::string_view(side) == "long" || std::string_view(side) == "short") && "标签列命名不合 lb_<long|short>_<name>");
    const int h = factor::stat::hold_from_name(name); // <n>m / close / t<N>o / t<N>c → 持有期键 (Stat/Contract.hpp 【持有期键】)
    labs.push_back({std::string_view(side) == "long", h, static_cast<uint32_t>(i)});
    holds.insert(h);
  }
  assert(!labs.empty() && "字段表无标签列");
  assert(t.y.entry.has() && "字段表无建仓可成交码 lb_entry_ok");
  const auto legs = [&](int key) {
    LegCol lc;
    lc.key = key;
    lc.leg[factor::stat::kLong] = lc.leg[factor::stat::kShort] = own(kNoCol);
    return lc;
  };
  const auto full = [](const LegCol &lc) { return lc.leg[factor::stat::kLong].has() && lc.leg[factor::stat::kShort].has(); };
  for (int h : holds) {
    if (!factor::stat::hold_ok(h, lv.geo()))
      continue;
    LegCol lc = legs(h);
    for (const Lab &l : labs)
      if (l.hold == h)
        lc.leg[l.is_long ? factor::stat::kLong : factor::stat::kShort].col = l.col;
    assert(full(lc) && "标签列不齐: 某 hold 缺 long 或 short");
    t.y.labels.push_back(lc);
  }
  assert(!t.y.labels.empty() && "本层无合法持有期");
  for (int a : amts) {
    LegCol lc = legs(a);
    for (const Cost &c : costs)
      if (c.amt == a)
        lc.leg[c.is_buy ? factor::stat::kLong : factor::stat::kShort].col = c.col;
    assert(full(lc) && "冲击成本列不齐: 某金额段缺 buy 或 sell");
    t.y.costs.push_back(lc);
  }
  if (t.y.costs.empty())
    t.y.costs.push_back(legs(kAmtFixed));
  return t;
}

std::shared_ptr<const FeatureTable> table_of(FeatureTables &tables, const Feature &feature, LevelId lv) {
  std::shared_ptr<const FeatureTable> &t = tables[lv];
  if (!t)
    t = std::make_shared<FeatureTable>(BuildFeatureTable(feature, lv));
  return t;
}

bool MakeFactorScope(const SharedData &data, std::shared_ptr<const FeatureTable> ft, const std::string &pool, bool gpu, FactorScope &sc) {
  assert(ft);
  const LevelId lv = ft->lv;
  sc = FactorScope{};
  sc.lv = lv;
  sc.pool = pool;
  sc.factor_dir = factor_dir_of(data, pool, lv);
  sc.ft = std::move(ft);
  sc.max_impact = data.config.max_impact;
  sc.gpu = gpu;
  if (data.asset.items.empty())
    return false; // 资产轴未就绪 (数据库未扫), 读不了特征库
  sc.libs = factor_libs(data, pool, lv);
  const PoolId p = pool_id(pool);
  return sc.libs[p].ready() && sc.libs[factor_level(lv).lab(p)].ready(); // 因子池库 / 标签库未建 → false
}

} // namespace GUI::Factors
