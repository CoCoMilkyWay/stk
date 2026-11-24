#include "live/Terms.hpp"

#include "disk/Disk.hpp"
#include "live/Pulled.hpp"
#include "live/Trace.hpp"
#include "misc/parquet.hpp"
#include "shared/AssetInfo.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <unordered_set>

namespace live::terms {

namespace {

constexpr const char *kHead = "code,name,mkt,ratio,financeable,rate,pledgeable";
constexpr const char *kMarginTable = "cn_stock_margin_trading_detail";

// 本地两融明细最新一日的代码 ("000001.SZ"): T 日盘后余额 T+1 盘前公布, 最新一日即上个交易日的交易所两融标的
std::unordered_set<std::string> margin_codes() {
  const std::vector<disk::Key> ks = disk::list(disk::FundShard, {.name = kMarginTable});
  for (auto k = ks.rbegin(); k != ks.rend(); ++k) {
    const std::filesystem::path p = disk::path(disk::FundShard, *k);
    const misc::Ymd last = misc::pq::max_yyyymmdd(p, "date");
    if (!last)
      continue;
    const misc::pq::TableView t(misc::pq::read_table(p, {"date", "instrument"}));
    const misc::pq::Col d = t.col("date"), ins = t.col("instrument");
    std::unordered_set<std::string> out;
    for (std::int64_t i = 0; i < t.rows(); ++i)
      if (d.yyyymmdd(i) == last)
        out.emplace(ins.str(i));
    return out;
  }
  fail("本地库没有两融明细, 先在 Database 同步");
}

void save(const Cache &c) {
  Pulled s{c.pulled, {}, {}};
  for (const auto &[code, t] : c.terms)
    s.rows.push_back({code, t.name, mkt_name(t.mkt), t.ratio ? strf("%.10g", *t.ratio) : "", strf("%d", t.financeable), strf("%.10g", t.rate),
                      strf("%d", t.pledgeable)});
  save_pulled(disk::LiveTermsFile, kHead, s);
}

} // namespace

Cache load() {
  const Pulled s = load_pulled(disk::LiveTermsFile, kHead);
  Cache c{s.pulled, {}};
  for (const std::vector<std::string> &f : s.rows) {
    Term &t = c.terms[f[0]];
    t.name = f[1];
    t.mkt = mkt_named(f[2]);
    if (!f[3].empty())
      t.ratio = std::stod(f[3]);
    t.financeable = f[4] == "1";
    t.rate = std::stod(f[5]);
    t.pledgeable = f[6] == "1";
  }
  return c;
}

Cache pull(Broker &b) {
  Cache c{misc::today_ymd(), b.terms()};
  int fin = 0, pl = 0;
  for (const auto &[code, t] : c.terms) {
    fin += t.financeable;
    pl += t.pledgeable;
  }
  trace::note("terms", strf("条款 %zu 只 (融资标的 %d · 担保品 %d)", c.terms.size(), fin, pl), {{"n", c.terms.size()}, {"financeable", fin}, {"pledgeable", pl}});
  save(c);
  return c;
}

Local local(const AssetInfo &info) {
  Local out;
  if (info.codes.empty())
    return out;
  const std::unordered_set<std::string> margin = margin_codes();
  for (size_t id = 0; id < info.codes.size(); ++id) {
    const StockInfo *s = info.find(id);
    if (!s)
      continue;
    const std::string &c = info.codes[id];
    assert(c.size() == 9 && c[6] == '.');
    out[c.substr(0, 6)] = {s->name, mkt_named(std::string_view(c).substr(7)), s->out_date == 0, margin.contains(c)};
  }
  return out;
}

Terms merged(const Terms &raw, const Local &local) {
  Terms out = raw;
  for (auto &[code, t] : out) {
    const auto i = local.find(code);
    if (i == local.end())
      continue;
    const Stock &s = i->second;
    t.name = s.name;
    t.mkt = s.mkt;
    t.financeable = t.financeable && s.margin;
    t.pledgeable = t.pledgeable && s.listed;
  }
  return out;
}

bool missing(const Terms &raw, const Local &local, const std::vector<std::string> &codes, const Snapshot *snap) {
  const auto miss = [&](const std::string &code) {
    if (raw.contains(code))
      return false;
    const auto i = local.find(code);
    return i == local.end() || (i->second.listed && i->second.mkt != Mkt::BJ);
  };
  return std::any_of(codes.begin(), codes.end(), miss) ||
         (snap && std::any_of(snap->pos.list.begin(), snap->pos.list.end(), [&](const Position &p) { return miss(p.code); })) ||
         (snap && std::any_of(snap->debts.list.begin(), snap->debts.list.end(), [&](const Debt &d) { return miss(d.code); }));
}

} // namespace live::terms
