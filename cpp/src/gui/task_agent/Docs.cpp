// Docs — 见头文件
#include "gui/task_agent/Docs.hpp"
#include "disk/Disk.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace GUI::Agent {

namespace {

namespace fs = std::filesystem;
using json = nlohmann::json;
using text::bwd;
using text::chars;
using text::flat;
using text::fmt;
using text::fwd;
using text::head;
using text::join;
using text::lower;

constexpr size_t kBrief = 90;
constexpr size_t kEvidence = 120;
constexpr size_t kSnip = 40;
constexpr int kSnips = 2;
constexpr size_t kTop = 12;
constexpr size_t kReadTail = 300;

constexpr std::array<std::string_view, 8> kPeriods = {"second", "intraday", "daily", "weekly", "monthly", "quarterly", "event", "na"};
constexpr std::array<const char *, 4> kStats = {"IC", "RankIC", "ICIR", "多空年化"};

std::vector<std::string> strs(const json &j) { return j.get<std::vector<std::string>>(); }
bool has(const std::vector<std::string> &v, const std::string &x) { return std::find(v.begin(), v.end(), x) != v.end(); }

std::string slurp(const fs::path &p) {
  std::ifstream f(p, std::ios::binary);
  assert(f && "研报库文件读不开");
  return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

int count_in(const std::string &hay, const std::vector<std::string> &ws) {
  int total = 0;
  for (const std::string &w : ws) {
    int k = 0;
    for (size_t p = hay.find(w); p != std::string::npos; p = hay.find(w, p + w.size()))
      ++k;
    if (k == 0)
      return 0;
    total += k;
  }
  return total;
}

// ---- 库与按篇懒加载全文 ----
struct Stage {
  std::string code, cn;
  std::vector<std::string> approaches;
};
struct Factor {
  size_t doc;
  std::string name, family, direction, horizon, formula, evidence;
  std::vector<std::string> period;
  std::array<double, 4> stats;
  std::string hay;
};
struct Doc {
  json tag;
  std::string id, dir, broker, genre, primary, value;
  int year = 0;
  std::vector<std::string> asset, stages, approaches, l1, freqs, findings, builds_on, external;
  std::vector<size_t> cited_by, factors;
  std::string hay;
};
struct Text {
  std::string body, low;
  std::vector<size_t> at;
  size_t end(size_t k) const { return k + 1 < at.size() ? at[k + 1] : body.size(); }
  size_t section(size_t p) const { return static_cast<size_t>(std::upper_bound(at.begin(), at.end(), p) - at.begin()) - 1; }
  std::string title(size_t k) const {
    return k == 0 ? std::string("(卷首)") : flat(std::string_view(body).substr(at[k], body.find('\n', at[k]) - at[k]));
  }
};

Text load_text(const Doc &d) {
  std::istringstream in(slurp(disk::path(disk::Doc) / "resources-proc" / "report" / d.dir / d.id / "report.md"));
  Text t;
  t.at.push_back(0);
  bool blank = true;
  for (std::string line; std::getline(in, line);) {
    if (!line.empty() && line.back() == '\r')
      line.pop_back();
    const size_t a = line.find_first_not_of(' ');
    const std::string_view s = a == std::string::npos ? std::string_view() : std::string_view(line).substr(a);
    if (s.starts_with("![") && s.ends_with(")"))
      continue;
    if (s.empty()) {
      if (!blank)
        t.body += '\n';
      blank = true;
      continue;
    }
    if (const size_t h = line.find_first_not_of('#'); h > 0 && h != std::string::npos && line[h] == ' ')
      t.at.push_back(t.body.size());
    t.body += line + '\n';
    blank = false;
  }
  t.low = lower(t.body);
  return t;
}

struct Lib {
  std::vector<Stage> stages;
  std::map<std::string, std::string> cn, stage_of;
  std::vector<Doc> docs;
  std::vector<Factor> factors;
  std::map<std::string, size_t> by_id;
  mutable std::mutex mu;
  mutable std::vector<std::optional<Text>> texts;

  const Text &text_at(size_t i) const {
    std::lock_guard<std::mutex> lock(mu);
    if (!texts[i])
      texts[i] = load_text(docs[i]);
    return *texts[i];
  }
  bool ensure_all_texts(const std::atomic<bool> &cancel) const {
    for (size_t i = 0; i < docs.size(); ++i) {
      if (cancel.load(std::memory_order_relaxed))
        return false;
      text_at(i);
    }
    return true;
  }
};

void names_of(Lib &L) {
  std::istringstream in(slurp(disk::path(disk::Doc) / "resources-tag" / "tag_stat.md"));
  bool on = false;
  for (std::string line; std::getline(in, line);) {
    if (line.starts_with("[按阶段]")) {
      on = true;
      continue;
    }
    if (!on || line.empty())
      continue;
    if (line.starts_with("["))
      break;
    std::istringstream w(line);
    std::string branch, code, cn;
    if (line.starts_with("L")) {
      w >> code >> cn;
      L.stages.push_back({code, cn, {}});
    } else {
      w >> branch >> code >> cn;
      assert(!L.stages.empty() && (branch == "├──" || branch == "└──") && "tag_stat.md 的 [按阶段] 段格式不对");
      L.stages.back().approaches.push_back(code);
      L.stage_of[code] = L.stages.back().code;
    }
    assert(!cn.empty() && !L.cn.count(code) && "tag_stat.md 的 [按阶段] 段格式不对");
    L.cn[code] = cn;
  }
  assert(L.stages.size() == 9 && "tag_stat.md 缺 [按阶段] 段");
}

double stat_of(const json &v) { return v.is_null() ? NAN : v.get<double>(); }

const Lib &lib() {
  static const Lib L = [] {
    Lib x;
    names_of(x);
    const fs::path root = disk::path(disk::Doc) / "resources-tag" / "report";
    assert(fs::is_directory(root) && "研报标注不在 output/data/doc");
    std::vector<fs::path> files;
    for (const auto &e : fs::recursive_directory_iterator(root, fs::directory_options::follow_directory_symlink))
      if (e.is_regular_file() && e.path().extension() == ".json")
        files.push_back(e.path());
    std::sort(files.begin(), files.end());
    for (const fs::path &f : files) {
      Doc d;
      d.tag = json::parse(slurp(f));
      const json &j = d.tag;
      d.id = j.at("id").get<std::string>();
      assert(d.id == f.stem().string() && d.id.size() > 9 && d.id[8] == '-' && "研报 id 须 = 文件名, 以 YYYYMMDD- 起头");
      d.dir = f.parent_path().lexically_relative(root).generic_string();
      d.year = std::stoi(d.id.substr(0, 4));
      const size_t b = d.id.find('-', 9);
      d.broker = d.id.substr(9, b == std::string::npos ? std::string::npos : b - 9);
      d.genre = j.at("genre").get<std::string>(), d.primary = j.at("primary").get<std::string>(), d.value = j.at("value").get<std::string>();
      d.asset = strs(j.at("asset"));
      for (const auto &[k, v] : j.at("pipe").items()) {
        assert(x.cn.count(k) && "研报阶段不在 tag_stat.md");
        d.stages.push_back(k);
        for (const std::string &a : strs(v.at("approach"))) {
          assert(x.stage_of.count(a) && "研报流派不在 tag_stat.md");
          d.approaches.push_back(a);
          if (k == "L1_factor")
            d.l1.push_back(a);
        }
        if (v.contains("setup") && v["setup"].contains("data"))
          d.freqs.push_back(v["setup"]["data"].at("freq").get<std::string>());
      }
      for (const json &it : j.at("findings"))
        d.findings.push_back(it.at("text").get<std::string>());
      d.builds_on = strs(j.at("builds_on")), d.external = strs(j.at("external_ref"));
      d.hay = d.id + "\n" + join(d.findings, "\n");
      const size_t di = x.docs.size();
      if (j.at("pipe").contains("L1_factor"))
        for (const json &it : j.at("pipe").at("L1_factor").at("factors")) {
          Factor fa{.doc = di};
          fa.name = it.at("name").get<std::string>(), fa.family = it.at("family").get<std::string>(), fa.direction = it.at("direction").get<std::string>();
          fa.horizon = it.at("horizon").get<std::string>(), fa.formula = it.at("formula").get<std::string>(), fa.evidence = it.at("evidence").get<std::string>();
          fa.period = strs(it.at("data_period"));
          const json &s = it.at("stats");
          assert(s.size() == kStats.size());
          for (size_t i = 0; i < kStats.size(); ++i)
            fa.stats[i] = stat_of(s[i]);
          fa.hay = lower(fa.name + "\n" + fa.formula + "\n" + fa.evidence);
          d.hay += "\n" + fa.name;
          d.factors.push_back(x.factors.size());
          x.factors.push_back(std::move(fa));
        }
      d.hay = lower(d.hay);
      assert(!x.by_id.count(d.id) && "研报 id 重复");
      x.by_id[d.id] = di;
      x.docs.push_back(std::move(d));
    }
    for (size_t i = 0; i < x.docs.size(); ++i)
      for (const std::string &b : x.docs[i].builds_on)
        if (const auto it = x.by_id.find(b); it != x.by_id.end())
          x.docs[it->second].cited_by.push_back(i);
    x.texts.resize(x.docs.size());
    return x;
  }();
  return L;
}

// ---- 分面与格式 ----
using Count = std::map<std::string, int>;
std::string ranked(const Count &c, size_t top) {
  std::vector<std::pair<std::string, int>> v(c.begin(), c.end());
  std::stable_sort(v.begin(), v.end(), [](const auto &a, const auto &b) { return a.second > b.second; });
  std::string s;
  for (size_t i = 0; i < v.size() && i < top; ++i)
    s += (i ? " · " : "") + v[i].first + " " + std::to_string(v[i].second);
  if (v.size() > top)
    s += " · …另 " + std::to_string(v.size() - top) + " 项";
  return s;
}
std::string named(const Lib &L, const std::string &code) { return code + " " + L.cn.at(code); }

std::string tree(const Lib &L, const std::vector<size_t> &hit) {
  Count st, ap;
  for (const size_t i : hit) {
    for (const std::string &s : L.docs[i].stages)
      ++st[s];
    for (const std::string &a : L.docs[i].approaches)
      ++ap[a];
  }
  std::string o;
  for (const Stage &s : L.stages) {
    if (!st.count(s.code))
      continue;
    Count sub;
    for (const std::string &a : s.approaches)
      if (ap.count(a))
        sub[named(L, a)] = ap[a];
    o += "- " + named(L, s.code) + " " + std::to_string(st[s.code]) + ": " + ranked(sub, SIZE_MAX) + "\n";
  }
  return o;
}

size_t period_ix(const std::string &p) {
  const auto it = std::find(kPeriods.begin(), kPeriods.end(), p);
  assert(it != kPeriods.end() && "未知周期");
  return static_cast<size_t>(it - kPeriods.begin());
}
std::string matrix(const Lib &L, const std::vector<size_t> &hit) {
  int m[kPeriods.size()][kPeriods.size()] = {};
  std::array<bool, kPeriods.size()> row{}, col{};
  for (const size_t i : hit) {
    const Factor &f = L.factors[i];
    const size_t h = period_ix(f.horizon);
    for (const std::string &p : f.period) {
      const size_t r = period_ix(p);
      ++m[r][h], row[r] = col[h] = true;
    }
  }
  std::vector<std::string> hs{"数据周期 \\ 预测期"};
  for (size_t h = 0; h < kPeriods.size(); ++h)
    if (col[h])
      hs.emplace_back(kPeriods[h]);
  text::Table tbl(hs);
  for (size_t r = 0; r < kPeriods.size(); ++r) {
    if (!row[r])
      continue;
    std::vector<std::string> cells{std::string(kPeriods[r])};
    for (size_t h = 0; h < kPeriods.size(); ++h)
      if (col[h])
        cells.push_back(m[r][h] ? std::to_string(m[r][h]) : "·");
    tbl.add(cells);
  }
  return tbl.str();
}

std::string stats_of(const std::array<double, 4> &s) {
  std::string o;
  for (size_t i = 0; i < kStats.size(); ++i)
    if (std::isfinite(s[i]))
      o += (o.empty() ? "" : " · ") + std::string(kStats[i]) + " " + (i == 3 ? fmt("%+.1f%%", s[i] * 100.0) : fmt("%.4g", s[i]));
  return o.empty() ? "原文无统计" : o;
}
std::string factor_head(const Factor &f) {
  return "**" + f.name + "** · " + f.family + " · " + f.direction + " · " + join(f.period, "+") + " → " + f.horizon + " · " + stats_of(f.stats);
}
int value_rank(const std::string &v) { return v == "high" ? 0 : v == "medium" ? 1
                                                                              : 2; }

std::string doc_row(const Doc &d, const std::string &extra) {
  std::string s = "- `" + d.id + "` · 价值 " + d.value + " · 主阶段 " + d.primary + " · 流派 " + join(d.approaches, ", ");
  if (!d.factors.empty())
    s += " · 因子 " + std::to_string(d.factors.size());
  s += extra + "\n";
  if (!d.findings.empty())
    s += "  结论: " + head(flat(d.findings[0]), kBrief) + "\n";
  return s;
}

std::string snippets(const Text &t, const std::string &w) {
  std::string o;
  size_t last = std::string::npos;
  int k = 0;
  for (size_t p = t.low.find(w); p != std::string::npos && k < kSnips; p = t.low.find(w, p + w.size())) {
    const size_t sec = t.section(p);
    if (sec == last)
      continue;
    last = sec, ++k;
    const size_t a = bwd(t.body, p, kSnip), b = fwd(t.body, p + w.size(), kSnip);
    o += "  §" + std::to_string(sec) + " " + t.title(sec) + ": …" + flat(std::string_view(t.body).substr(a, b - a)) + "…\n";
  }
  return o;
}

std::string conditions(const Call &c) {
  const std::string t = c.text();
  const size_t a = t.find('(');
  return t.size() - a > 2 ? t.substr(a) : "(全库)";
}
std::string alone(const text::Filter &f, size_t n) { return "各条件单独命中: " + f.alone(n) + "\n去掉命中少的条件或换个词再查\n"; }
std::string loosened(const text::Filter &f, size_t n) { return "没有同时含全部 query 词的, 下列含其中任一词 (各条件单独命中: " + f.alone(n) + ")\n"; }

// ---- 三个工具 (统一签名 ViewOut(Call, cancel)) ----
ViewOut search_docs(const Call &c, const std::atomic<bool> &cancel) {
  const Lib &L = lib();
  const std::string scope = c.str("scope"), stage = c.str("stage"), ap = c.str("approach"), asset = c.str("asset"), freq = c.str("freq"), genre = c.str("genre");
  const std::vector<std::string> ws = text::words(c.str("query"));
  const bool full = scope == "text";
  if (!ap.empty() && !L.stage_of.count(ap))
    return {false, "没有流派 " + ap + " (写流派 code, 见不带 approach 时分面里的 阶段 → 流派)", {}};
  if (full && ws.empty())
    return {false, "scope=text 须给 query", {}};
  if (full && !L.ensure_all_texts(cancel))
    return {};
  const auto hay = [&](size_t i) -> const std::string & { return full ? L.text_at(i).low : L.docs[i].hay; };
  text::Filter f;
  if (!stage.empty())
    f.add("stage=" + stage, [&](size_t i) { return has(L.docs[i].stages, stage); });
  if (!ap.empty())
    f.add("approach=" + ap, [&](size_t i) { return has(L.docs[i].approaches, ap); });
  if (!asset.empty())
    f.add("asset=" + asset, [&](size_t i) { return has(L.docs[i].asset, asset); });
  if (!freq.empty())
    f.add("freq=" + freq, [&](size_t i) { return has(L.docs[i].freqs, freq); });
  if (!genre.empty())
    f.add("genre=" + genre, [&](size_t i) { return L.docs[i].genre == genre; });
  f.add(ws, "query 词 ", hay);
  bool loose = false;
  std::vector<size_t> hit = f.hits(L.docs.size(), loose);
  std::vector<int> hits(L.docs.size(), 0);
  for (const size_t i : hit)
    hits[i] = ws.empty() ? 1 : count_in(hay(i), ws);
  std::string out = "命中 " + std::to_string(hit.size()) + " 篇 " + conditions(c) + "\n" + (loose ? loosened(f, L.docs.size()) : "");
  if (hit.empty())
    return {true, out + alone(f, L.docs.size()), {}};
  Count broker, year, value;
  for (const size_t i : hit)
    ++broker[L.docs[i].broker], ++year[std::to_string(L.docs[i].year)], ++value[L.docs[i].value];
  out += "\n### 阶段 → 流派 (篇数; 一篇可落多处)\n" + tree(L, hit);
  out += "\n### 机构\n" + ranked(broker, kTop) + "\n";
  std::string years;
  for (auto it = year.rbegin(); it != year.rend(); ++it)
    years += (years.empty() ? "" : " · ") + it->first + " " + std::to_string(it->second);
  out += "\n### 年份\n" + years + "\n\n### 价值 (high 可直接复用 / medium 有参考 / low 有水分)\n" + ranked(value, kTop) + "\n";

  std::stable_sort(hit.begin(), hit.end(), [&](size_t a, size_t b) {
    const Doc &x = L.docs[a], &y = L.docs[b];
    if (full && hits[a] != hits[b])
      return hits[a] > hits[b];
    if (value_rank(x.value) != value_rank(y.value))
      return value_rank(x.value) < value_rank(y.value);
    return x.id > y.id;
  });
  const size_t n = std::min(hit.size(), static_cast<size_t>(c.num("n")));
  out += "\n### 研报 (前 " + std::to_string(n) + " 篇; " + (full ? "命中处多的在前, 再按" : "按") + "价值高在前, 同价值新在前)\n";
  for (size_t r = 0; r < n; ++r) {
    const size_t i = hit[r];
    out += doc_row(L.docs[i], full ? " · 命中 " + std::to_string(hits[i]) + " 处" : "");
    if (full) {
      const Text &t = L.text_at(i);
      out += snippets(t, *std::find_if(ws.begin(), ws.end(), [&](const std::string &w) { return text::has_word(t.low, w); }));
    }
  }
  return {true, std::move(out), {}};
}

ViewOut search_factors(const Call &c, const std::atomic<bool> &) {
  const Lib &L = lib();
  const std::string family = c.str("family"), period = c.str("period"), horizon = c.str("horizon"), ap = c.str("approach"), asset = c.str("asset"), sort = c.str("sort");
  const std::vector<std::string> ws = text::words(c.str("query"));
  if (!ap.empty() && (!L.stage_of.count(ap) || L.stage_of.at(ap) != "L1_factor"))
    return {false, "没有因子构造流派 " + ap + " (因子只出自 L1_factor 的流派, code 见 search_docs 分面里 L1_factor 那行)", {}};
  const auto doc = [&](size_t i) -> const Doc & { return L.docs[L.factors[i].doc]; };
  text::Filter f;
  if (!family.empty())
    f.add("family=" + family, [&](size_t i) { return L.factors[i].family == family; });
  if (!period.empty())
    f.add("period=" + period, [&](size_t i) { return has(L.factors[i].period, period); });
  if (!horizon.empty())
    f.add("horizon=" + horizon, [&](size_t i) { return L.factors[i].horizon == horizon; });
  if (!ap.empty())
    f.add("approach=" + ap, [&](size_t i) { return has(doc(i).l1, ap); });
  if (!asset.empty())
    f.add("asset=" + asset, [&](size_t i) { return has(doc(i).asset, asset); });
  f.add(ws, "query 词 ", [&](size_t i) -> const std::string & { return L.factors[i].hay; });
  bool loose = false;
  std::vector<size_t> hit = f.hits(L.factors.size(), loose);
  Count docs, family_n, ap_n;
  for (const size_t i : hit) {
    const Factor &fa = L.factors[i];
    ++docs[L.docs[fa.doc].id], ++family_n[fa.family];
    for (const std::string &a : L.docs[fa.doc].l1)
      ++ap_n[named(L, a)];
  }
  std::string out = "命中 " + std::to_string(hit.size()) + " 个因子 (出自 " + std::to_string(docs.size()) + " 篇) " + conditions(c) + "\n" + (loose ? loosened(f, L.factors.size()) : "");
  if (hit.empty())
    return {true, out + alone(f, L.factors.size()), {}};
  out += "\n### 类别\n" + ranked(family_n, SIZE_MAX) + "\n\n### 出处流派\n" + ranked(ap_n, kTop) + "\n\n### 数据周期 × 预测期 (因子数)\n" + matrix(L, hit);

  const auto key = [&](const Factor &fa) {
    const auto &s = fa.stats;
    return sort == "ic" ? std::fabs(std::isfinite(s[1]) ? s[1] : s[0]) : sort == "icir" ? std::fabs(s[2])
                                                                     : sort == "ls"     ? s[3]
                                                                                        : NAN;
  };
  std::stable_sort(hit.begin(), hit.end(), [&](size_t a, size_t b) {
    const double ka = key(L.factors[a]), kb = key(L.factors[b]);
    if (std::isfinite(ka) != std::isfinite(kb))
      return std::isfinite(ka);
    if (std::isfinite(ka) && ka != kb)
      return ka > kb;
    return L.docs[L.factors[a].doc].id > L.docs[L.factors[b].doc].id;
  });
  const size_t n = std::min(hit.size(), static_cast<size_t>(c.num("n")));
  out += "\n### 因子 (前 " + std::to_string(n) + " 个; 统计是原文在其股票池与预测期上的数, 只看机制)\n";
  for (size_t r = 0; r < n; ++r) {
    const Factor &fa = L.factors[hit[r]];
    out += "- " + factor_head(fa) + "\n  出处 `" + L.docs[fa.doc].id + "`\n  公式: " + head(flat(fa.formula), kBrief * 2) + "\n";
  }
  return {true, std::move(out), {}};
}

std::optional<size_t> doc_index(const Lib &L, const std::string &id, std::string &error) {
  if (const auto it = L.by_id.find(id); it != L.by_id.end())
    return it->second;
  std::vector<size_t> m;
  for (size_t i = 0; i < L.docs.size(); ++i)
    if (L.docs[i].id.find(id) != std::string::npos)
      m.push_back(i);
  if (m.size() == 1)
    return m[0];
  if (m.empty()) {
    error = "没有研报 " + id + " (id 照检索结果写)";
  } else {
    error = id + " 对上 " + std::to_string(m.size()) + " 篇, 写全 id:\n";
    for (size_t k = 0; k < m.size() && k < 8; ++k)
      error += "- `" + L.docs[m[k]].id + "`\n";
  }
  return std::nullopt;
}

std::string setup_of(const json &s) {
  std::vector<std::string> p;
  if (s.contains("universe"))
    p.push_back("股票池 " + join(strs(s["universe"]), "+"));
  if (s.contains("data"))
    p.push_back("数据 " + join(strs(s["data"].at("source")), "+") + " (" + s["data"].at("freq").get<std::string>() + ")");
  if (s.contains("holding"))
    p.push_back("调仓 " + s["holding"].at("rebalance").get<std::string>() + " / 预测 " + s["holding"].at("horizon").get<std::string>());
  if (s.contains("rebalance"))
    p.push_back("调仓 " + s["rebalance"].get<std::string>());
  if (s.contains("horizon"))
    p.push_back("风险预测期 " + s["horizon"].get<std::string>());
  if (s.contains("risk_factors"))
    p.push_back("风险因子 " + join(strs(s["risk_factors"]), "+"));
  return join(p, " · ");
}

ViewOut card(const Lib &L, const Doc &d, size_t from) {
  const size_t nf = d.factors.size();
  if (from > nf)
    return {false, "from=" + std::to_string(from) + " 超出: 本篇标注卡共 " + std::to_string(nf) + " 个因子 (card 的 from = 从第几个因子列起, 0 = 整卡)", {}};
  const auto factors = [&](std::string &o, size_t i0) {
    for (size_t i = i0; i < nf; ++i) {
      const Factor &f = L.factors[d.factors[i]];
      const std::string row = std::to_string(i + 1) + ". " + factor_head(f) + "\n   公式: " + flat(f.formula) + "\n   摘录: " + head(flat(f.evidence), kEvidence) + "\n";
      if (i > i0 && chars(o) + chars(row) > static_cast<size_t>(kOutChars) - kReadTail) {
        o += "…(未完: 第 " + std::to_string(i + 1) + ".." + std::to_string(nf) + " 个因子接着读 part=card from=" + std::to_string(i + 1) + ")\n";
        return;
      }
      o += row;
    }
  };
  if (from > 0) {
    std::string out = "# " + d.id + " · 标注卡的因子, 第 " + std::to_string(from) + " 个起 (共 " + std::to_string(nf) + "; 统计是原文在其股票池与预测期上的数)\n";
    factors(out, from - 1);
    return {true, std::move(out), {}};
  }
  std::string o = "# " + d.id + "\n体裁 " + d.genre + " · 资产 " + join(d.asset, "+") + " · 主阶段 " + named(L, d.primary) + " · 价值 " + d.value + " · 所在 " +
                  d.dir + "\n\n## 涉及阶段\n";
  for (const auto &[k, v] : d.tag.at("pipe").items()) {
    std::vector<std::string> aps;
    for (const std::string &a : strs(v.at("approach")))
      aps.push_back(named(L, a));
    o += "- " + named(L, k) + ": 流派 " + join(aps, ", ");
    if (v.contains("baseline") && !v["baseline"].empty())
      o += " · 对照 " + join(strs(v["baseline"]), ", ");
    if (v.contains("setup"))
      if (const std::string s = setup_of(v["setup"]); !s.empty())
        o += " · " + s;
    o += "\n";
  }
  o += "\n## 结论 (" + std::to_string(d.findings.size()) + ")\n";
  for (size_t i = 0; i < d.findings.size(); ++i)
    o += std::to_string(i + 1) + ". " + flat(d.findings[i]) + "\n";
  std::vector<std::string> in, cited;
  for (const std::string &b : d.builds_on)
    if (L.by_id.count(b))
      in.push_back("`" + b + "`");
  for (const size_t i : d.cited_by)
    cited.push_back("`" + L.docs[i].id + "`");
  if (!in.empty() || !d.external.empty() || !cited.empty()) {
    o += "\n## 引用\n";
    if (!in.empty())
      o += "- 前作 (库内): " + join(in, ", ") + "\n";
    if (!d.external.empty())
      o += "- 前作 (库外): " + join(d.external, "; ") + "\n";
    if (!cited.empty())
      o += "- 被引 (库内): " + join(cited, ", ") + "\n";
  }
  if (nf) {
    o += "\n## 因子 (" + std::to_string(nf) + "; 统计是原文在其股票池与预测期上的数)\n";
    factors(o, 0);
  }
  return {true, std::move(o), {}};
}

std::string outline(const Doc &d, const Text &t) {
  std::string o = "# " + d.id + " · 全文 " + std::to_string(chars(t.body)) + " 字 · " + std::to_string(t.at.size()) + " 节 (节号 · 标题 · 字数; 读原文用 part=text from=节号)\n";
  for (size_t k = 0; k < t.at.size(); ++k) {
    const size_t n = chars(std::string_view(t.body).substr(t.at[k], t.end(k) - t.at[k]));
    if (k > 0 || n > 0)
      o += std::to_string(k) + " " + t.title(k) + " · " + std::to_string(n) + "\n";
  }
  return o;
}

ViewOut passage(const Doc &d, const Text &t, size_t from, size_t skip) {
  if (from >= t.at.size())
    return {false, "from=" + std::to_string(from) + " 超出: 本篇共 " + std::to_string(t.at.size()) + " 节 (0.." + std::to_string(t.at.size() - 1) + ")", {}};
  const size_t p = fwd(t.body, t.at[from], skip);
  if (skip > 0 && p >= t.end(from))
    return {false, "skip=" + std::to_string(skip) + " 超出: 第 " + std::to_string(from) + " 节共 " + std::to_string(chars(std::string_view(t.body).substr(t.at[from], t.end(from) - t.at[from]))) + " 字", {}};
  size_t q = fwd(t.body, p, static_cast<size_t>(kOutChars) - kReadTail);
  if (q < t.body.size())
    if (const size_t nl = t.body.rfind('\n', q); nl != std::string::npos && nl > p)
      q = nl + 1;
  std::string out = "# " + d.id + " · 原文, 第 " + std::to_string(from) + " 节" + (skip ? " 第 " + std::to_string(skip) + " 字" : "") + "起\n\n" + t.body.substr(p, q - p);
  if (q >= t.body.size()) {
    out += "\n(全文完)\n";
  } else {
    const size_t k = t.section(q), s = chars(std::string_view(t.body).substr(t.at[k], q - t.at[k]));
    out += "\n…(未完: 接着读 from=" + std::to_string(k) + (s ? " skip=" + std::to_string(s) : "") + ")\n";
  }
  return {true, std::move(out), {}};
}

ViewOut read_doc(const Call &c, const std::atomic<bool> &) {
  const Lib &L = lib();
  std::string error;
  const std::optional<size_t> di = doc_index(L, c.str("id"), error);
  if (!di)
    return {false, std::move(error), {}};
  const Doc &d = L.docs[*di];
  const std::string part = c.str("part");
  if (part == "card")
    return card(L, d, static_cast<size_t>(c.num("from")));
  const Text &t = L.text_at(*di);
  if (part == "outline")
    return {true, outline(d, t), {}};
  return passage(d, t, static_cast<size_t>(c.num("from")), static_cast<size_t>(c.num("skip")));
}

} // namespace

ViewOut Browse(const Call &c, const std::atomic<bool> &cancel) {
  switch (c.tool) {
  case Tool::SearchDocs:
    return search_docs(c, cancel);
  case Tool::SearchFactors:
    return search_factors(c, cancel);
  case Tool::ReadDoc:
    return read_doc(c, cancel);
  default:
    assert(false && "不是研报库工具");
    return {};
  }
}

} // namespace GUI::Agent
