#include "agent/Prompt.hpp"
#include "agent/Config.hpp"
#include "agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <unordered_map>
#include <utility>

namespace agent {

namespace {

using text::fmt;
using text::num;
using KV = std::vector<std::pair<std::string_view, std::string>>;

constexpr size_t kTitleChars = 40; // 结果标题里每个参数值至多几字 (全文在 agent 自己的回复里)
constexpr size_t kSurfBins = 6;    // 参数曲面: 每个参数至多分几段

std::filesystem::path path_of(Msg m) { return std::filesystem::path(kPromptDir) / (std::string(def(m).id) + ".md"); }

// {{键}} → 值; 模板里的键都须在 kv 里
Message fill(Msg m, const KV &kv) {
  const std::string t = LoadPrompt(m);
  std::string out;
  size_t i = 0;
  for (;;) {
    const size_t a = t.find("{{", i);
    if (a == std::string::npos) {
      out.append(t, i);
      return {m, std::move(out)};
    }
    const size_t b = t.find("}}", a);
    assert(b != std::string::npos && "模板占位 {{ 未闭合");
    const std::string_view key(t.data() + a + 2, b - a - 2);
    const auto it = std::find_if(kv.begin(), kv.end(), [&](const auto &p) { return p.first == key; });
    if (it == kv.end())
      std::fprintf(stderr, "[agent] %s.md: 未知占位 {{%.*s}}\n", def(m).id, static_cast<int>(key.size()), key.data());
    assert(it != kv.end() && "模板占位键未知");
    out.append(t, i, a - i);
    out += it->second;
    i = b + 2;
  }
}

std::string cell(std::string_view s) { // 表格单元: 竖线转义, 换行压成空格
  std::string o;
  for (const char ch : s) {
    if (ch == '|')
      o += "\\|";
    else
      o += ch == '\n' ? ' ' : ch;
  }
  return o;
}

std::string or_none(std::string s, const char *none) { return s.empty() ? none : s; }

int visible(const std::vector<Result> &rs) {
  return static_cast<int>(std::count_if(rs.begin(), rs.end(), [](const Result &r) { return r.visible; }));
}

// 可见且 keep 的跳过前 skip 个, 再列 top 个, 每个一节; 没列完的报总数与接着看的 skip
template <class Keep, class Fn>
std::string list_of(const std::vector<Result> &rs, int skip, int top, Keep &&keep, Fn &&one) {
  std::string s;
  int all = 0, n = 0;
  for (const Result &r : rs)
    if (r.visible && keep(r) && all++ >= skip && n < top) {
      ++n;
      s += one(r);
    }
  if (n < all)
    s += !n ? "(共 " + std::to_string(all) + " 个, skip=" + std::to_string(skip) + " 已过末尾)\n"
            : "(共 " + std::to_string(all) + " 个, 这里是第 " + std::to_string(skip + 1) + "–" + std::to_string(skip + n) + " 个" +
                  (skip + n < all ? "; 接着看给 skip=" + std::to_string(skip + n) : std::string()) + ")\n";
  return s;
}

// 权重 / 代表点的具体式相对模板只差自由参数: 按出现序写 键=值 (无自由参数 = 即模板); 对不上 = 整个具体式
std::string fills(const std::string &tmpl, const std::string &expr) {
  const std::string whole = "`" + expr + "`";
  std::string s;
  size_t i = 0, j = 0;
  while (i < tmpl.size() && j < expr.size()) {
    if (tmpl[i] == expr[j]) {
      ++i, ++j;
      continue;
    }
    const size_t e = expr.find_first_of(",)", j);
    if (expr[j] != '=' || (tmpl[i] != ',' && tmpl[i] != ')') || e == std::string::npos)
      return whole;
    size_t k = i;
    while (k > 0 && std::isalnum(static_cast<unsigned char>(tmpl[k - 1])))
      --k;
    s += (s.empty() ? "" : ", ") + tmpl.substr(k, i - k) + expr.substr(j, e - j);
    j = e;
  }
  if (i != tmpl.size() || j != expr.size())
    return whole;
  return s.empty() ? "(即模板)" : s;
}

// 入库门槛一行 (只对有参数组的): 达 / 差在哪
std::string gate_line(const Result &r, const Gate &g) {
  if (!r.visible || r.verdict != Verdict::Weights)
    return "";
  const std::string miss = GateMiss(g, r);
  return "- " + (miss.empty() ? std::string("达入库门槛") : miss) + "\n";
}

std::string col_of(const std::string &code) { return code.substr(code.find('.', code.find('.') + 1) + 1); }

// 一组叶的列名: 各词位的取值按出现序写 {a,b,…}; 组不是各位取值的全组合则逐个列
std::string kin_name(const std::vector<const Leaf *> &g) {
  const size_t W = LeafWords(g.front()->code).second.size();
  std::vector<std::vector<std::string>> at(W);
  for (const Leaf *l : g) {
    const std::vector<std::string> ws = LeafWords(l->code).second;
    assert(ws.size() == W && "同组叶词数相同");
    for (size_t k = 0; k < W; ++k)
      if (std::find(at[k].begin(), at[k].end(), ws[k]) == at[k].end())
        at[k].push_back(ws[k]);
  }
  size_t all = 1;
  for (const std::vector<std::string> &v : at)
    all *= v.size();
  std::string s;
  if (all != g.size()) {
    for (const Leaf *l : g)
      s += (s.empty() ? "" : ", ") + col_of(l->code);
    return s;
  }
  for (size_t k = 0; k < W; ++k) {
    std::string w;
    for (const std::string &x : at[k])
      w += (w.empty() ? "" : ",") + x;
    s += (k ? "_" : "") + (at[k].size() > 1 ? "{" + w + "}" : w);
  }
  return s;
}

// 按同组 (Leaf::kin) 合并, 组序 = 首个叶的出现序
std::vector<std::vector<const Leaf *>> kin_groups(const std::vector<const Leaf *> &ls) {
  std::vector<std::vector<const Leaf *>> gs;
  std::unordered_map<std::string_view, size_t> at;
  for (const Leaf *l : ls)
    if (const auto [it, fresh] = at.try_emplace(l->kin, gs.size()); fresh)
      gs.push_back({l});
    else
      gs[it->second].push_back(l);
  return gs;
}

struct Family {
  std::string key, title, card; // 族 (Leaf::family) / 族名 / 卡文
};

// 族卡文件: "# <族> <族名>" 起一节, 卡文到下一节
std::vector<Family> families() {
  std::ifstream f(kFamilyFile, std::ios::binary);
  assert(f.is_open() && "族卡文件不在 kFamilyFile");
  std::vector<Family> out;
  for (std::string line; std::getline(f, line);) {
    if (line.starts_with("# ")) {
      const size_t sp = line.find(' ', 2);
      assert(sp != std::string::npos && "族卡标题 = # <族> <族名>");
      out.push_back({line.substr(2, sp - 2), line.substr(sp + 1), ""});
      continue;
    }
    assert((!out.empty() || line.empty()) && "族卡文件以 # <族> <族名> 起头");
    if (!out.empty())
      out.back().card += line + "\n";
  }
  for (Family &x : out)
    while (x.card.ends_with("\n\n"))
      x.card.pop_back();
  return out;
}

// 特征表: 叶按族分节 (族序 = 首次出现), 节首族卡, 节内按 档.层 分组, 每行 列名 中文名 [落盘变换]; 同组的叶合一行 (花括号式 + 个数 + 首个的中文名)
std::string leaves_text(const std::vector<Leaf> &leaves) {
  const std::vector<Family> fams = families();
  std::vector<std::string> order;
  for (const Leaf &l : leaves)
    if (std::find(order.begin(), order.end(), l.family) == order.end())
      order.push_back(l.family);
  std::string s;
  for (const std::string &k : order) {
    const auto f = std::find_if(fams.begin(), fams.end(), [&](const Family &x) { return x.key == k; });
    if (f == fams.end())
      std::fprintf(stderr, "[agent] %s: 族 %s 没有卡\n", kFamilyFile, k.c_str());
    assert(f != fams.end() && "特征族缺卡");
    std::vector<std::pair<std::string, std::vector<const Leaf *>>> groups;
    int n = 0;
    for (const Leaf &l : leaves) {
      if (l.family != k)
        continue;
      ++n;
      const std::string g = LeafWords(l.code).first;
      auto it = std::find_if(groups.begin(), groups.end(), [&](const auto &x) { return x.first == g; });
      if (it == groups.end())
        groups.push_back({g, {}}), it = groups.end() - 1;
      it->second.push_back(&l);
    }
    s += (s.empty() ? "## " : "\n## ") + f->title + " (" + std::to_string(n) + " 个)\n\n" + f->card;
    for (const auto &[g, ls] : groups) {
      s += "\n### " + g + ".*\n";
      for (const std::vector<const Leaf *> &kin : kin_groups(ls)) {
        const Leaf &l = *kin.front();
        s += "- " + (kin.size() == 1 ? col_of(l.code) + " " + cell(l.name_cn) : kin_name(kin) + " (" + std::to_string(kin.size()) + " 个; 首个 " + col_of(l.code) + " " + cell(l.name_cn) + ")") +
             (l.tf.empty() ? "" : " [" + l.tf + "]") + "\n";
      }
    }
  }
  return s;
}

std::string bp_of(double r) { return fmt("%+.1f", r * 1e4); }
std::string bp_spark(const std::vector<double> &v) {
  const auto [lo, hi] = std::minmax_element(v.begin(), v.end());
  return "`" + text::bars(v) + "`  [" + bp_of(*lo) + ", " + bp_of(*hi) + "] bp";
}

// 参数值: 整数不带小数
std::string param(double v) { return v == std::round(v) ? fmt("%.0f", v) : fmt("%.4g", v); }

// 参数曲面: 每个参数按取值分至多 kSurfBins 段 (等点数, 同值不拆), 段内取最好的点 (mono 最高, 同 mono 取目标高的), 列出它的全部参数
std::string surface_text(const Surface &sf) {
  if (sf.slots.empty() || sf.pts.size() < 2)
    return "";
  const size_t n = sf.pts.size(), per = (n + kSurfBins - 1) / kSurfBins;
  const auto g = [](const Stat &st) { return std::max(st.mono, 0.0) * std::max(st.mono, 0.0) * st.ls * st.ric; };
  const auto better = [&](const Stat &x, const Stat &y) { return x.mono != y.mono ? x.mono > y.mono : g(x) > g(y); };
  std::string s = "- 参数曲面 (Search 评过 " + std::to_string(n) + " 个点; 每个参数按取值分段, 段内取最好的点: mono 最高, 同 mono 取目标高的):\n";
  for (size_t j = 0; j < sf.slots.size(); ++j) {
    std::vector<size_t> ix(n);
    for (size_t i = 0; i < n; ++i)
      ix[i] = i;
    const auto v = [&](size_t i) { return sf.pts[ix[i]].v[j]; };
    std::stable_sort(ix.begin(), ix.end(), [&](size_t a, size_t b) { return sf.pts[a].v[j] < sf.pts[b].v[j]; });
    s += "\n| " + sf.slots[j] + " 段 | 最好的点 | mono | 每期毛多空 bp | rIC |\n|---|---|---|---|---|\n";
    for (size_t a = 0; a < n;) {
      size_t b = std::min(n, a + per);
      while (b < n && v(b) == v(b - 1))
        ++b;
      size_t best = a;
      for (size_t i = a; i < b; ++i)
        if (better(sf.pts[ix[i]].st, sf.pts[ix[best]].st))
          best = i;
      const Probe &p = sf.pts[ix[best]];
      std::string at;
      for (size_t q = 0; q < sf.slots.size(); ++q)
        at += (q ? ", " : "") + (sf.slots.size() > 1 ? sf.slots[q] + "=" : std::string()) + param(p.v[q]);
      s += "| " + (v(a) == v(b - 1) ? param(v(a)) : param(v(a)) + "–" + param(v(b - 1))) + " | " + at + " | " + fmt("%.3f", p.st.mono) + " | " +
           fmt("%+.2f", p.st.ls * 1e4) + " | " + fmt("%.4f", p.st.ric) + " |\n";
      a = b;
    }
  }
  return s;
}

// 本层的行 (系统提示 "行与时刻"): 竞价段几行 + 连续竞价的行末时刻
std::string rows_of(const Context &c) {
  if (c.seg == 1)
    return "本层 (" + c.level + ") 每天 1 行: 15:00 算因子, 按当日收盘撮合价开仓 (同上表末行).";
  const size_t o = static_cast<size_t>(c.open);
  const std::string open = o == 1 ? "第 0 行 = 集合竞价段 [09:15, 09:30)"
                                  : "第 0.." + std::to_string(o - 1) + " 行 = 集合竞价段 [09:15, 09:30) 逐分钟一行 (行末 " + c.clock[0] + " … " + c.clock[o - 1] + ")";
  return "本层 (" + c.level + ") 每天 " + std::to_string(c.seg) + " 行: " + open + "; 第 " + std::to_string(o) + " 行起连续竞价 (行末 " + c.clock[o] + ", " + c.clock[o + 1] +
         " … " + c.clock.back() + ", 午休不占行; 末行含收盘竞价).";
}

} // namespace

std::string LoadPrompt(Msg m) {
  std::ifstream f(path_of(m), std::ios::binary);
  assert(f.is_open() && "提示词模板不在 kPromptDir");
  return std::string(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

void SavePrompt(Msg m, const std::string &text) {
  std::ofstream f(path_of(m), std::ios::binary | std::ios::trunc);
  assert(f.is_open());
  f << text;
}

// ---- 结局文本 ----
std::string Text(const Stat &s) {
  char b[128];
  std::snprintf(b, sizeof(b), "mono %.3f · 每期毛多空 %.2fbp · rIC %.4f", s.mono, s.ls * 1e4, s.ric);
  return b;
}

// 目标 ± 标准误 (t = 目标 / 标准误)
std::string Text(double g, double se) {
  char b[64];
  std::snprintf(b, sizeof(b), "%.3g ± %.2g (t %.1f)", g, se, se > 0.0 ? g / se : 0.0);
  return b;
}

std::string Text(const Weight &w) {
  if (!w.has)
    return "无该持有期统计";
  char b[48];
  std::snprintf(b, sizeof(b), " · 有效率 %.0f%%", w.valid * 100.0);
  return Text(w.st) + " · score " + (std::isfinite(w.score) ? Text(w.score, w.se) : std::string("不过门槛")) + b + (w.sign < 0 ? " · 取反" : "");
}

// 显著看曲面估计对标准误, 不看观测目标的 t
std::string Text(const Top &t) {
  char b[96];
  std::snprintf(b, sizeof(b), " · 目标 %.3g ± %.2g", t.g, t.se);
  std::string s = Text(t.st) + b;
  if (t.rep != Rep::Mono) {
    std::snprintf(b, sizeof(b), " · 曲面估计 %.3g", t.mu);
    s += b;
  }
  return s + (t.sign < 0 ? " · 取反" : "");
}

std::string Brief(const Result &r) {
  switch (r.verdict) {
  case Verdict::Unstaged:
  case Verdict::Broken:
    return std::string(def(r.verdict).cn) + ": " + r.error;
  case Verdict::Weights:
    return std::to_string(r.ws.size()) + " 个权重, 最佳 " + Text(*r.best());
  case Verdict::Null:
    return r.top ? std::string(def(r.verdict).cn) + " (" + r.top->miss + "), 代表点 " + Text(*r.top) : std::string(def(r.verdict).cn);
  case Verdict::Pending:
    return def(r.verdict).cn;
  }
  return {};
}

std::string Detail(const Result &r) {
  if (!r.visible)
    return "- 统计不在当前评估期上 (不可比), 不作数\n";
  switch (r.verdict) {
  case Verdict::Unstaged:
  case Verdict::Broken:
    return "- " + std::string(def(r.verdict).cn) + ": " + r.error + "\n";
  case Verdict::Weights: {
    std::string s;
    for (const Weight &w : r.ws)
      s += "- 权重 " + fills(r.tmpl, w.expr) + ": " + Text(w) + "\n";
    return s;
  }
  case Verdict::Null:
    return r.top ? "- Search 无显著权重, 落选原因: " + r.top->miss + "\n- 代表点 (" + def(r.top->rep).cn + ") " + fills(r.tmpl, r.top->expr) + ": " + Text(*r.top) + "\n"
                 : std::string("- Search 无显著权重 (没有一个点算得出该持有期统计)\n");
  case Verdict::Pending:
    return "- " + std::string(def(r.verdict).cn) + "\n";
  }
  return {};
}

// ---- 工具结果 ----
std::string TriedText(const std::vector<Result> &tried, int skip, int top, const Gate &g) {
  return or_none(list_of(tried, skip, top, [](const Result &) { return true; }, [&](const Result &r) { return "### " + r.name + " (" + r.name_cn + ")\n`" + r.tmpl + "`\n" + Detail(r) + gate_line(r, g); }), "(无)\n");
}

std::string LibraryText(const std::vector<Result> &lib, int skip, int top) {
  const auto has = [](const Result &r) { return r.best() != nullptr; };
  std::string s = list_of(lib, skip, top, has, [](const Result &r) {
    const Weight &w = *r.best();
    return "- " + r.name + " (" + r.name_cn + "): `" + r.tmpl + "`" + (r.note.empty() ? "" : " — " + r.note) + "\n  - 最佳权重 " + fills(r.tmpl, w.expr) + ": " + Text(w) + "\n";
  });
  if (const int none = static_cast<int>(std::count_if(lib.begin(), lib.end(), [&](const Result &r) { return r.visible && !has(r); })))
    s += "(另有 " + std::to_string(none) + " 个在本持有期无统计, 不列)\n";
  return or_none(s, "(空)\n");
}

std::string TrialText(const Trial &k, const Gate &g, const std::string &rep) {
  const Staged &st = k.st;
  const Result &r = st.res;
  std::string s = "### " + st.cand.name + " (" + st.cand.name_cn + ")\n`" + st.cand.expr + "`\n" + (k.change.empty() ? "" : "- 改动: " + k.change + "\n") + Detail(r) + gate_line(r, g) + rep;
  if (!r.visible)
    return s;
  if (const Weight *w = r.best(); w && !w->grp.empty())
    s += "- 最佳权重的 20 组每期毛收益 (减组间均值) G1 → G20: " + bp_spark(w->grp) + "\n";
  return s + surface_text(r.surf);
}

std::string LeavesText(const std::vector<Leaf> &leaves, const std::string &query, int n, std::vector<std::string> &shown) {
  text::Filter f;
  f.add(text::words(query), "", [&](size_t i) -> const std::string & { return leaves[i].hay; });
  std::vector<const Leaf *> hits, all;
  for (size_t i = 0; i < leaves.size(); ++i) {
    all.push_back(&leaves[i]);
    if (f(i))
      hits.push_back(&leaves[i]);
  }
  const size_t hit = hits.size();
  if (!hit)
    return "(无命中; 各词单独命中: " + f.alone(leaves.size()) + "; 换个写法或少给几个词)\n";
  // 命中的叶所在的组 (按首个命中的序), 每组列全组; 公式与说明按组内首个命中的叶
  const std::vector<std::vector<const Leaf *>> gs = kin_groups(hits);
  std::unordered_map<std::string_view, std::vector<const Leaf *>> kins;
  for (const std::vector<const Leaf *> &g : kin_groups(all))
    kins.emplace(g.front()->kin, g);
  std::string rows;
  for (size_t i = 0; i < gs.size() && i < static_cast<size_t>(n); ++i) {
    const Leaf &l = *gs[i].front();
    const std::vector<const Leaf *> &kin = kins.at(l.kin);
    for (const Leaf *x : kin)
      shown.push_back(x->code);
    const std::string name = kin.size() == 1 ? l.code : LeafWords(l.code).first + "." + kin_name(kin) + " (" + std::to_string(kin.size()) + " 个; 下列按 " + col_of(l.code) + ")";
    rows += "| " + name + " | " + cell(l.name_cn) + " | " + cell(l.formula) + " | " + cell(l.desc) + " | " + (l.tf.empty() ? "x" : l.tf_def) + " |\n";
  }
  return "命中 " + std::to_string(hit) + " 个叶, 在 " + std::to_string(gs.size()) + " 组里" + (gs.size() > static_cast<size_t>(n) ? ", 列前 " + std::to_string(n) + " 组" : std::string()) +
         ". 同组的叶只差计量 (amt 额 / vol 量 / n 笔) 或阈值 / 窗口 (带数字的词), 列出整组; 公式与说明按组内首个命中的写, 其余照族卡换词; 列出的组整组算查过\n\n"
         "| 叶 | 中文名 | 公式 (原量 x) | 说明 | 落盘值 |\n|---|---|---|---|---|\n" +
         rows;
}

std::string AnswerText(const Asked &a) {
  assert(a.picks.size() == a.qs.size());
  std::string s;
  for (size_t i = 0; i < a.qs.size(); ++i) {
    const Question &q = a.qs[i];
    const Pick &p = a.picks[i];
    s += std::to_string(i + 1) + ". " + q.prompt + "\n- " + or_none(Answered(q, p), "(未选, 你自定)") + "\n";
    if (p.on != q.recommended || (p.other && !p.text.empty()))
      s += "- (未照你的推荐: " + Picked(q, q.recommended) + ")\n";
  }
  return s + "附言: " + or_none(a.note, "(无)") + "\n";
}

// ---- 发 ----
Message SystemMessage(const Context &c, const Gate &g, bool ask, bool fold) {
  std::string ops = "| 算子 | 中文名 | 签名 (元与参数的值域) | 输出域 | 定义 | 说明 |\n|---|---|---|---|---|---|\n";
  for (const Op &o : c.ops)
    ops += "| " + o.name + " | " + cell(o.name_cn) + " | $" + cell(o.sig) + "$ | " + o.out + " | $" + cell(o.tex) + "$ | " + cell(o.note) + " |\n";
  return fill(Msg::System, {{"pool", c.pool},
                            {"pool_cn", c.pool_cn},
                            {"level", c.level},
                            {"seg", std::to_string(c.seg)},
                            {"rows", rows_of(c)},
                            {"hold", c.hold},
                            {"hold_desc", c.hold_desc},
                            {"window", c.window},
                            {"search_mono", num(c.min_mono)},
                            {"ts_root_d", std::to_string(c.ts_root_d)},
                            {"max_d", std::to_string(c.max_d)},
                            {"batch", std::to_string(kBatch)},
                            {"hyp_evals", std::to_string(kHypEvals)},
                            {"hyp_knobs", std::to_string(kHypKnobs)},
                            {"part_chars", std::to_string(kPartChars)},
                            {"max_calls", std::to_string(kMaxCalls)},
                            {"out_chars", std::to_string(kOutChars)},
                            {"max_ops", std::to_string(kMaxOps)},
                            {"good_ops", std::to_string(kGoodOps)},
                            {"gate_mono", num(g.mono)},
                            {"gate_ric", num(g.ric)},
                            {"ask", ask ? "可以用 ask_human 向研究员提一次问 (整个会话只一次)" : "研究员不参与决策, 没有 ask_human; 疑问写 report_issue"},
                            {"history", fold ? "最近 " + std::to_string(kKeepRounds) + "~" + std::to_string(2 * kKeepRounds) +
                                                   " 条消息原样留在对话里, 更早的结果消息会成批折叠 (观察与查询只留标题, 回测只留一行结局; 提交 / 问答 / 假设与判定 / 没做成的原因原样); 还要用的数写进 judge 的 result, 或重调"
                                             : "对话里的结果消息原样保留"},
                            {"tools", ToolsDoc(ask)},
                            {"leaves", leaves_text(c.leaves)},
                            {"ops", ops}});
}

Message FirstMessage(const Context &c, int tried, const std::string &notes, bool ask) {
  return fill(Msg::First, {{"hold", c.hold},
                           {"max_calls", std::to_string(kMaxCalls)},
                           {"tried", std::to_string(tried)},
                           {"library", std::to_string(visible(c.library))},
                           {"notes", notes},
                           {"start", ask ? "要研究员定的事 (从哪个因子起步、往哪个方向挖) 可以先看几眼数据再用 ask_human 一次问完, 也可以不问"
                                         : "自己定下要做的那一个因子 (新起, 或从已试候选 / 正式库里挑) 和方向"}});
}

Message TestMessage(bool ask) {
  std::string tools;
  for (const ToolDef &d : Tools())
    if (d.id != Tool::AskHuman || ask)
      tools += (tools.empty() ? "" : ", ") + std::string(d.name);
  return fill(Msg::Test, {{"tools", tools},
                          {"max_calls", std::to_string(kMaxCalls)},
                          {"ask", ask ? "ask_human 也要测: 问 1 道题 (例: 先测哪类工具), 研究员答完再接着测"
                                      : "本会话没有 ask_human, 不测它"}});
}

Message RoundMessage(const std::vector<Act> &acts, int round, const Board &board, bool evaled, bool test) {
  std::string s;
  for (size_t i = 0; i < acts.size(); ++i) {
    const Act &a = acts[i];
    s += "## [" + RefText({round, static_cast<int>(i) + 1}) + "] " + a.call.text(kTitleChars) + (a.ok ? "" : " — 没做成") + "\n\n" + a.out;
    if (!s.ends_with('\n'))
      s += '\n';
    s += '\n';
  }
  const std::string batch = std::to_string(kBatch);
  std::string next;
  if (test) {
    next = "按自检的做法接着调还没调过的工具与取值 (eval_factor 至多 " + batch + " 个); 对照工具说明查上面每个结果, 有问题即 report_issue. 全部调过后 report_issue 写总评, 再 finish.";
  } else {
    next = "先对照, 再发调用:\n"
           "- **判**: 结果对照各假设的 expect / refute, 哪个被支持、哪个落空、哪个还不清; 证据够的 judge, 不够的补哪个观察\n"
           "- **因**: 为什么是这样, 错在假设的哪一环 (谁 / 什么行为 / 什么盘面 / 何时兑现)\n"
           "- **料**: 有没有更直接捕捉这一现象的特征\n"
           "- **记**: 值得跨会话留下的认识用 note_insight 记\n\n";
    next += evaled ? "本条只诊断 (上一条有落了文件的回测, 本条不能回测): 用观察查本批最好与最差的变体为什么好 / 差, 再对挂了变体的假设 judge; 观察排在 judge 前面, 判支持才引用得到."
                   : "本条可回测: 只挂判为支持的假设 (至多 " + batch + " 个 eval_factor, 各写清 base / change / expect / parts); 按系统提示 \"判优\" 比当前最好版本; 改不动了就 submit_factor 交最好的版本, 再 finish.";
  }
  return fill(Msg::Round, {{"results", s}, {"board", board.text()}, {"next", next}});
}

std::string FoldedText(const std::vector<Act> &acts, int round) {
  std::string s = "# 一条较早回复的调用结果 (已折叠: 观察与查询只留标题, 回测只留结局; 要细节就重调)\n\n";
  for (size_t i = 0; i < acts.size(); ++i) {
    const Act &a = acts[i];
    s += "## [" + RefText({round, static_cast<int>(i) + 1}) + "] " + a.call.text(kTitleChars) + (a.ok ? "" : " — 没做成") + "\n";
    switch (a.ok ? def(a.call.tool).fold : Fold::Keep) {
    case Fold::Keep:
      s += a.out + (a.out.ends_with('\n') ? "" : "\n");
      break;
    case Fold::Brief: {
      const Trial &k = std::get<Trial>(a.got);
      s += "- " + k.st.cand.name + " `" + k.st.cand.expr + "`" + (k.change.empty() ? "" : " (" + text::head(k.change, kTitleChars) + ")") + ": " + Brief(k.st.res) + "\n";
      break;
    }
    case Fold::Title:
      break;
    }
  }
  return s;
}

Message RetryMessage(const std::string &error) { return fill(Msg::Retry, {{"error", error}}); }

} // namespace agent
