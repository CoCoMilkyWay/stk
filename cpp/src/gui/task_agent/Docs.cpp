// Docs — 见头文件
#include "gui/task_agent/Docs.hpp"
#include "disk/Disk.hpp"
#include "gui/task_agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <sstream>

namespace GUI::Agent {

namespace docs {

namespace {

namespace fs = std::filesystem;
using json = nlohmann::json;
using text::flat;
using text::fmt;
using text::join;
using text::lower;

std::vector<std::string> strs(const json &j) { return j.get<std::vector<std::string>>(); }

std::string slurp(const fs::path &p) {
  std::ifstream f(p, std::ios::binary);
  assert(f && "研报库文件读不开");
  return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

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

std::atomic<const Lib *> g_loaded{nullptr};

} // namespace

size_t Text::section(size_t p) const { return static_cast<size_t>(std::upper_bound(at.begin(), at.end(), p) - at.begin()) - 1; }
std::string Text::title(size_t k) const { return k == 0 ? std::string("(卷首)") : flat(std::string_view(body).substr(at[k], body.find('\n', at[k]) - at[k])); }

const Text &Lib::text_at(size_t i) const {
  std::lock_guard<std::mutex> lock(mu);
  if (!texts[i])
    texts[i] = load_text(docs[i]);
  return *texts[i];
}
bool Lib::ensure_all_texts(const std::atomic<bool> &cancel) const {
  for (size_t i = 0; i < docs.size(); ++i) {
    if (cancel.load(std::memory_order_relaxed))
      return false;
    text_at(i);
  }
  return true;
}

const Lib &lib() {
  static const std::unique_ptr<const Lib> L = [] {
    auto p = std::make_unique<Lib>();
    Lib &x = *p;
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
    return p;
  }();
  g_loaded.store(L.get(), std::memory_order_release);
  return *L;
}

const Lib *loaded() { return g_loaded.load(std::memory_order_acquire); }

bool has(const std::vector<std::string> &v, const std::string &x) { return std::find(v.begin(), v.end(), x) != v.end(); }

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

Ranks ranked(const Count &c) {
  Ranks v(c.begin(), c.end());
  std::stable_sort(v.begin(), v.end(), [](const auto &a, const auto &b) { return a.second > b.second; });
  return v;
}
std::string ranked_text(const Ranks &v, size_t top) {
  std::string s;
  for (size_t i = 0; i < v.size() && i < top; ++i)
    s += (i ? " · " : "") + v[i].first + " " + std::to_string(v[i].second);
  if (v.size() > top)
    s += " · …另 " + std::to_string(v.size() - top) + " 项";
  return s;
}
std::string named(const Lib &L, const std::string &code) { return code + " " + L.cn.at(code); }

size_t period_ix(const std::string &p) {
  const auto it = std::find(kPeriods.begin(), kPeriods.end(), p);
  assert(it != kPeriods.end() && "未知周期");
  return static_cast<size_t>(it - kPeriods.begin());
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

std::vector<std::string> snippets(const Text &t, const std::string &w) {
  std::vector<std::string> o;
  size_t last = std::string::npos;
  for (size_t p = t.low.find(w); p != std::string::npos && static_cast<int>(o.size()) < kSnips; p = t.low.find(w, p + w.size())) {
    const size_t sec = t.section(p);
    if (sec == last)
      continue;
    last = sec;
    const size_t a = text::bwd(t.body, p, kSnip), b = text::fwd(t.body, p + w.size(), kSnip);
    o.push_back("§" + std::to_string(sec) + " " + t.title(sec) + ": …" + flat(std::string_view(t.body).substr(a, b - a)) + "…");
  }
  return o;
}

std::string conditions(const Call &c) {
  const std::string t = c.text();
  const size_t a = t.find('(');
  return t.size() - a > 2 ? t.substr(a) : "(全库)";
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

} // namespace docs

ToolOut Browse(const Call &c, const std::atomic<bool> &cancel) {
  switch (c.tool) {
  case Tool::SearchDocs:
    return SearchDocs(c, cancel);
  case Tool::SearchFactors:
    return SearchFactors(c, cancel);
  case Tool::ReadDoc:
    return ReadDoc(c, cancel);
  default:
    assert(false && "不是研报库工具");
    return {};
  }
}

} // namespace GUI::Agent
