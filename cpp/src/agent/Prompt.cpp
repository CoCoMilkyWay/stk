#include "agent/Prompt.hpp"
#include "agent/Config.hpp"

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

using KV = std::vector<std::pair<std::string_view, std::string>>;

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

std::string num(double v) {
  char b[32];
  std::snprintf(b, sizeof(b), "%.3g", v);
  return b;
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

std::string Text(const Top &t) {
  std::string s = Text(t.st) + " · 目标 " + Text(t.g, t.se);
  if (t.peak) {
    char b[48];
    std::snprintf(b, sizeof(b), " · 曲面估计 %.3g", t.mu);
    s += b;
  }
  return s + (t.sign < 0 ? " · 取反" : "");
}

std::string Brief(const Result &r) {
  switch (r.verdict) {
  case Verdict::Unstaged:
  case Verdict::Broken:
    return std::string(name(r.verdict)) + ": " + r.error;
  case Verdict::Weights:
    return std::to_string(r.ws.size()) + " 个权重, 最佳 " + Text(*r.best());
  case Verdict::Null:
    return r.top ? std::string(name(r.verdict)) + " (" + r.top->miss + "), 代表点 " + Text(*r.top) : std::string(name(r.verdict));
  case Verdict::Pending:
    return name(r.verdict);
  }
  return {};
}

std::string Detail(const Result &r) {
  if (!r.visible)
    return "- 统计不在当前评估期上 (不可比), 不作数\n";
  switch (r.verdict) {
  case Verdict::Unstaged:
  case Verdict::Broken:
    return "- " + std::string(name(r.verdict)) + ": " + r.error + "\n";
  case Verdict::Weights: {
    std::string s;
    for (const Weight &w : r.ws)
      s += "- `" + w.expr + "`: " + Text(w) + "\n";
    return s;
  }
  case Verdict::Null:
    return r.top ? "- Search 无显著权重, 落选原因: " + r.top->miss + "\n- 代表点 (" + (r.top->peak ? "过门槛的峰" : "mono 最高的点") + ") `" + r.top->expr +
                       "`: " + Text(*r.top) + "\n"
                 : std::string("- Search 无显著权重 (没有一个点算得出该持有期统计)\n");
  case Verdict::Pending:
    return "- " + std::string(name(r.verdict)) + "\n";
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

std::string TrialText(const Staged &st) { return "### " + st.cand.name + " (" + st.cand.name_cn + ")\n`" + st.cand.expr + "`\n" + Detail(st.res); }

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
Message SystemMessage(const Context &c, const Gate &g, bool ask) {
  std::string leaves = "| 叶 | 中文名 | 公式 | 说明 |\n|---|---|---|---|\n";
  for (const Leaf &l : c.leaves)
    leaves += "| " + l.code + " | " + cell(l.name_cn) + " | " + cell(l.formula) + " | " + cell(l.desc) + " |\n";
  std::string ops = "| 算子 | 中文名 | 签名 (元与参数的值域) | 输出域 | 定义 | 说明 |\n|---|---|---|---|---|---|\n";
  for (const Op &o : c.ops)
    ops += "| " + o.name + " | " + cell(o.name_cn) + " | $" + cell(o.sig) + "$ | " + o.out + " | $" + cell(o.tex) + "$ | " + cell(o.note) + " |\n";
  return fill(Msg::System, {{"pool", c.pool},
                            {"pool_cn", c.pool_cn},
                            {"level", c.level},
                            {"seg", std::to_string(c.seg)},
                            {"hold", c.hold},
                            {"hold_desc", c.hold_desc},
                            {"window", c.window},
                            {"search_mono", num(c.min_mono)},
                            {"ts_root_d", std::to_string(c.ts_root_d)},
                            {"max_d", std::to_string(c.max_d)},
                            {"batch", std::to_string(kBatch)},
                            {"max_calls", std::to_string(kMaxCalls)},
                            {"out_chars", std::to_string(kOutChars)},
                            {"max_ops", std::to_string(kMaxOps)},
                            {"good_ops", std::to_string(kGoodOps)},
                            {"gate_mono", num(g.mono)},
                            {"gate_ric", num(g.ric)},
                            {"ask", ask ? "可以用 ask_human 向研究员提一次问 (整个会话只一次)" : "研究员不参与决策, 没有 ask_human; 疑问写 note_issue"},
                            {"tools", ToolsDoc(ask)},
                            {"leaves", leaves},
                            {"ops", ops}});
}

Message FirstMessage(const Context &c, int tried, bool ask) {
  return fill(Msg::First, {{"hold", c.hold},
                           {"tried", std::to_string(tried)},
                           {"library", std::to_string(visible(c.library))},
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

Message RoundMessage(const std::vector<Act> &acts) {
  std::string s;
  for (size_t i = 0; i < acts.size(); ++i) {
    const Act &a = acts[i];
    s += "## [" + std::to_string(i + 1) + "] " + a.call.text() + (a.ok ? "" : " — 没做成") + "\n\n" + a.out;
    if (!s.ends_with('\n'))
      s += '\n';
    s += '\n';
  }
  return fill(Msg::Round, {{"results", s}, {"batch", std::to_string(kBatch)}});
}

Message RetryMessage(const std::string &err) { return fill(Msg::Retry, {{"error", err}}); }

} // namespace agent
