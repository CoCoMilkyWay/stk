#include "agent/Prompt.hpp"
#include "agent/Config.hpp"
#include "agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <utility>

namespace agent {

namespace {

using text::fmt;
using text::num;
using KV = std::vector<std::pair<std::string_view, std::string>>;

constexpr size_t kTitleChars = 40; // 结果标题里每个参数值至多几字 (全文在 agent 自己的回复里)
constexpr size_t kSurfBins = 6;    // 参数曲面: 每个参数至多分几段

// 代表点怎么取的 (下标 = Rep)
constexpr const char *kRepCn[] = {"过门槛的峰", "过门槛的点里曲面估计最高的", "mono 最高的点"};
static_assert(std::size(kRepCn) == static_cast<size_t>(Rep::Mono) + 1);

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

// 可见的前 top 个, 每个一节; 余下的只报个数
template <class Fn>
std::string list_of(const std::vector<Result> &rs, int top, Fn &&one) {
  std::string s;
  int n = 0;
  for (const Result &r : rs)
    if (r.visible && n < top) {
      ++n;
      s += one(r);
    }
  if (const int all = visible(rs); all > n)
    s += "(另有 " + std::to_string(all - n) + " 个未列)\n";
  return s;
}

std::string lower(std::string s) { // 只动 ASCII
  for (char &ch : s)
    if (ch >= 'A' && ch <= 'Z')
      ch = static_cast<char>(ch - 'A' + 'a');
  return s;
}

std::string hay_of(const Leaf &l) { return lower(l.code + "\n" + l.name_cn + "\n" + l.formula + "\n" + l.desc + "\n" + l.tf + "\n" + l.family); } // search_leaves 查的字

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

// 特征表: 叶按族分节 (族序 = 首次出现), 节首族卡, 节内按 档.层 分组, 每行 列名 中文名 [落盘变换]
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
    std::vector<std::pair<std::string, std::string>> groups;
    int n = 0;
    for (const Leaf &l : leaves) {
      if (l.family != k)
        continue;
      ++n;
      const size_t dot = l.code.find('.', l.code.find('.') + 1);
      assert(dot != std::string::npos && "叶名 = 档.层.列");
      const std::string g = l.code.substr(0, dot);
      auto it = std::find_if(groups.begin(), groups.end(), [&](const auto &x) { return x.first == g; });
      if (it == groups.end())
        groups.push_back({g, ""}), it = groups.end() - 1;
      it->second += "- " + l.code.substr(dot + 1) + " " + cell(l.name_cn) + (l.tf.empty() ? "" : " [" + l.tf + "]") + "\n";
    }
    s += (s.empty() ? "## " : "\n## ") + f->title + " (" + std::to_string(n) + " 个)\n\n" + f->card;
    for (const auto &[g, rows] : groups)
      s += "\n### " + g + ".*\n" + rows;
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
      s += "- `" + w.expr + "`: " + Text(w) + "\n";
    return s;
  }
  case Verdict::Null:
    return r.top ? "- Search 无显著权重, 落选原因: " + r.top->miss + "\n- 代表点 (" + kRepCn[static_cast<size_t>(r.top->rep)] + ") `" + r.top->expr + "`: " + Text(*r.top) + "\n"
                 : std::string("- Search 无显著权重 (没有一个点算得出该持有期统计)\n");
  case Verdict::Pending:
    return "- " + std::string(def(r.verdict).cn) + "\n";
  }
  return {};
}

// ---- 工具结果 ----
std::string TriedText(const std::vector<Result> &tried, int top) {
  return or_none(list_of(tried, top, [](const Result &r) { return "### " + r.name + " (" + r.name_cn + ")\n`" + r.tmpl + "`\n" + Detail(r); }), "(无)\n");
}

std::string LibraryText(const std::vector<Result> &lib, int top) {
  return or_none(list_of(lib, top,
                         [](const Result &r) {
                           const Weight *w = r.best();
                           return "- " + r.name + " (" + r.name_cn + "): `" + r.tmpl + "`" + (r.note.empty() ? "" : " — " + r.note) + "\n" +
                                  (w ? "  - 最佳权重 `" + w->expr + "`: " + Text(*w) + "\n" : std::string("  - 本持有期无统计\n"));
                         }),
                 "(空)\n");
}

std::string TrialText(const Trial &k, const std::string &rep) {
  const Staged &st = k.st;
  const Result &r = st.res;
  std::string s = "### " + st.cand.name + " (" + st.cand.name_cn + ")\n`" + st.cand.expr + "`\n" + (k.change.empty() ? "" : "- 改动: " + k.change + "\n") + Detail(r) + rep;
  if (!r.visible)
    return s;
  if (const Weight *w = r.best(); w && !w->grp.empty())
    s += "- 最佳权重的 20 组每期毛收益 (减组间均值) G1 → G20: " + bp_spark(w->grp) + "\n";
  return s + surface_text(r.surf);
}

std::string LeavesText(const std::vector<Leaf> &leaves, const std::string &query, int n, std::vector<std::string> &shown) {
  std::vector<std::string> ws;
  for (size_t a = 0, b; a < query.size(); a = b + 1) {
    b = std::min(query.find(' ', a), query.size());
    if (b > a)
      ws.push_back(lower(query.substr(a, b - a)));
  }
  std::string rows;
  int hit = 0;
  for (const Leaf &l : leaves) {
    const std::string hay = hay_of(l);
    if (!std::all_of(ws.begin(), ws.end(), [&](const std::string &w) { return hay.find(w) != std::string::npos; }))
      continue;
    if (++hit > n)
      continue;
    shown.push_back(l.code);
    rows += "| " + l.code + " | " + cell(l.name_cn) + " | " + cell(l.formula) + " | " + cell(l.desc) + " | " + (l.tf.empty() ? "x" : l.tf_def) + " |\n";
  }
  if (!hit) {
    std::string each;
    for (const std::string &w : ws)
      each += (each.empty() ? "" : " · ") + w + " " + std::to_string(std::count_if(leaves.begin(), leaves.end(), [&](const Leaf &l) { return hay_of(l).find(w) != std::string::npos; }));
    return "(无命中; 各词单独命中: " + each + "; 换个写法或少给几个词)\n";
  }
  return "命中 " + std::to_string(hit) + " 个" + (hit > n ? ", 列前 " + std::to_string(n) + " 个" : std::string()) +
         "\n\n| 叶 | 中文名 | 公式 (原量 x) | 说明 | 落盘值 |\n|---|---|---|---|---|\n" + rows;
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
                                                   " 条消息原样留在对话里, 更早的结果消息会成批折叠 (观察与查询只留标题, 回测只留一行结局); 还要用的数在 eval_factor 的 note 里记下, 或重调"
                                             : "对话里的结果消息原样保留"},
                            {"tools", ToolsDoc(ask)},
                            {"leaves", leaves_text(c.leaves)},
                            {"ops", ops}});
}

Message FirstMessage(const Context &c, int tried, const std::string &notes, bool ask) {
  return fill(Msg::First, {{"hold", c.hold},
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
    next = "先想清楚再发调用, 逐条回答:\n"
           "- 上面的结果对照假设板上各假设的 expect / refute: 哪个被支持、哪个落空、哪个还不清? 证据够的用 judge 判 (证据写调用编号), 不够的想清还差哪个观察.\n"
           "- 测出来的为什么是这样? 落空的错在假设的哪一环 (谁 / 什么行为 / 什么盘面 / 何时兑现)? 被支持的, 还有哪部分收益或分层没解释?\n"
           "- 这个现象有没有更直接捕捉它的特征 (search_leaves 查, 族卡对照)? 现有叶做不出来就 request_feature.\n"
           "- 下一步该用哪个工具查什么: 条件分层 (view_layers cond) 看在哪类盘面成立, 成分剥离 (view_explain) 看剥掉已知量还剩多少, 交割单 / 截面 / 个股 / 逐笔重放看行为是否真的发生, 研报库看同行怎么解释.\n"
           "- 得到的认识值得跨会话留下的, note_insight 记.\n";
    next += evaled ? "\n这一条不能回测 (上一条有回测): 用观察诊断本批变体为什么好 / 差, 对挂了变体的假设 judge."
                   : "\n只有判为支持的假设能挂 eval_factor (至多 " + batch + " 个, 每个写清 base / 改动 / 预期 / 功能块). 按系统提示 \"做法\" 的判据比当前最好版本; 改不动了就 submit_factor 交最好的版本, 再 finish.";
  }
  return fill(Msg::Round, {{"results", s}, {"board", board.text()}, {"next", next}});
}

std::string FoldedText(const std::vector<Act> &acts, int round) {
  std::string s = "# 一条较早回复的调用结果 (已折叠: 观察与查询只留标题, 回测只留结局; 要细节就重调)\n\n";
  for (size_t i = 0; i < acts.size(); ++i) {
    const Act &a = acts[i];
    const Tool t = a.call.tool;
    s += "## [" + RefText({round, static_cast<int>(i) + 1}) + "] " + a.call.text(kTitleChars) + (a.ok ? "" : " — 没做成") + "\n";
    const Run run = def(t).run;
    if (run == Run::Eval && a.ok) {
      const Trial &k = std::get<Trial>(a.got);
      s += "- " + k.st.cand.name + " `" + k.st.cand.expr + "`" + (k.change.empty() ? "" : " (" + text::head(k.change, kTitleChars) + ")") + ": " + Brief(k.st.res) + "\n";
    } else if (!a.ok || run == Run::Human || (run == Run::Now && t != Tool::SearchLeaves && t != Tool::ListTried && t != Tool::ListLibrary && t != Tool::ListNotes)) {
      s += a.out + (a.out.ends_with('\n') ? "" : "\n");
    }
  }
  return s;
}

Message RetryMessage(const std::string &err) { return fill(Msg::Retry, {{"error", err}}); }

} // namespace agent
