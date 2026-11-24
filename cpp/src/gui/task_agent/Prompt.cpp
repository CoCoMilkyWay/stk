// Prompt — 见头文件
#include "gui/task_agent/Prompt.hpp"
#include "disk/Disk.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"
#include "gui/task_agent/tools/Trial.hpp"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstdio>
#include <initializer_list>
#include <set>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace GUI::Agent {

// =============================================================================
// 内部辅助
// =============================================================================
namespace {

using text::cell;
using text::num;
using KV = std::vector<std::pair<std::string_view, std::string>>;

constexpr size_t kTitleChars = 40;
constexpr size_t kWhyChars = 80;

Message fill(Msg m, const KV &kv) {
  return {m, text::fill(LoadPrompt(m), [&](std::string_view key) -> std::string {
            if (const auto it = std::find_if(kv.begin(), kv.end(), [&](const auto &p) { return p.first == key; }); it != kv.end())
              return it->second;
            const ConstDef *c = const_of(key);
            if (!c)
              std::fprintf(stderr, "[agent] %s.md: 未知占位 {{%.*s}}\n", def(m).id, static_cast<int>(key.size()), key.data());
            assert(c && "模板占位键未知");
            return std::to_string(c->value);
          })};
}

std::string or_none(std::string s, const char *none) { return s.empty() ? none : s; }

std::string remark_text(const std::string &r) {
  return r.empty() ? "" : "\n## 研究员对本会话的要求\n\n" + r + "\n\n只对这一段会话; 与上文的做法冲突处以它为准, 回复协议与会话约束照旧.\n";
}

int visible(const std::vector<Result> &rs) {
  return static_cast<int>(std::count_if(rs.begin(), rs.end(), [](const Result &r) { return r.visible; }));
}

struct Family {
  std::string key, title, card;
};

std::vector<Family> families() {
  std::istringstream f(disk::read(disk::FamilyFile));
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
      std::fprintf(stderr, "[agent] %s: 族 %s 没有卡\n", disk::path(disk::FamilyFile).c_str(), k.c_str());
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
      for (const std::vector<const Leaf *> &kin : KinGroups(ls)) {
        const Leaf &l = *kin.front();
        s += "- " +
             (kin.size() == 1 ? LeafCol(l.code) + " " + cell(l.name_cn)
                              : KinName(kin) + " (" + std::to_string(kin.size()) + " 个; 首个 " + LeafCol(l.code) + " " + cell(l.name_cn) + ")") +
             (l.tf.empty() ? "" : " [" + l.tf + "]") + "\n";
      }
    }
  }
  return s;
}

std::string rows_of(const Context &c) {
  if (c.seg == 1)
    return "本层 (" + c.level + ") 每天 1 行: 15:00 算因子, 按当日收盘撮合价开仓 (同上表末行).";
  const size_t o = static_cast<size_t>(c.open);
  const std::string open = o == 1 ? "第 0 行 = 集合竞价段 [09:15, 09:30)"
                                  : "第 0.." + std::to_string(o - 1) + " 行 = 集合竞价段 [09:15, 09:30) 逐分钟一行 (行末 " + c.clock[0] + " … " + c.clock[o - 1] + ")";
  return "本层 (" + c.level + ") 每天 " + std::to_string(c.seg) + " 行: " + open + "; 第 " + std::to_string(o) + " 行起连续竞价 (行末 " + c.clock[o] + ", " + c.clock[o + 1] +
         " … " + c.clock.back() + ", 午休不占行; 末行含收盘竞价).";
}

// ---- 研究树折叠 (TurnsOf 内部调用) ----
std::string under(std::string_view s) {
  std::string o;
  for (size_t i = 0; i < s.size();) {
    const size_t e = std::min(s.find('\n', i), s.size());
    if (e > i)
      o += "  " + std::string(s.substr(i, e - i)) + "\n";
    i = e + 1;
  }
  return o;
}

std::string fields(const nlohmann::ordered_json &args, std::initializer_list<std::string_view> keys) {
  std::string s;
  for (const auto &[k, v] : args.items())
    if (v.is_string() && std::ranges::contains(keys, std::string_view(k)))
      s += "  - " + k + ": " + v.get_ref<const std::string &>() + "\n";
  return s;
}

std::string why_of(const Act &a) { return text::head(std::string_view(a.out.text).substr(0, a.out.text.find('\n')), kWhyChars); }

std::string tree_of(Rounds msgs) {
  const Rounds done = msgs.first(msgs.size() - 1);
  const Board board = StateOf(done).board;
  struct Step {
    Ref at;
    const Act *a;
  };
  std::vector<Step> steps;
  for (size_t i = 0; i < done.size(); ++i)
    for (size_t k = 0; k < done[i].acts.size(); ++k)
      if (const Act &a = done[i].acts[k]; a.span.ended())
        steps.push_back({{static_cast<int>(i) + 1, static_cast<int>(k) + 1}, &a});
  const auto act_at = [&](Ref r) -> const Act * {
    if (r.round < 1 || static_cast<size_t>(r.round) > done.size())
      return nullptr;
    const std::vector<Act> &as = done[static_cast<size_t>(r.round) - 1].acts;
    return r.call >= 1 && static_cast<size_t>(r.call) <= as.size() && as[static_cast<size_t>(r.call) - 1].span.ended() ? &as[static_cast<size_t>(r.call) - 1] : nullptr;
  };
  const auto on = [](const Act &a, Tool t) { return a.call.tool == t && a.out.ok; };
  const auto staged = [](const Act &a) {
    const TrialRes *k = AsTrial(a);
    return k && k->st.ok();
  };
  const auto gist_of = [](const Act &a, const char *sep) { return a.out.gist.empty() ? std::string() : std::string(sep) + a.out.gist; };

  std::set<Ref> cited;
  std::unordered_map<std::string, std::vector<Step>> by_hyp;
  for (const Step &s : steps) {
    const Act &a = *s.a;
    if (on(a, Tool::Judge)) {
      for (const std::string &e : a.call.strs("evidence"))
        if (Ref r; RefOf(e, r))
          cited.insert(r);
      by_hyp[a.call.str("hyp")].push_back(s);
    } else if (staged(a)) {
      by_hyp[a.call.str("hyp")].push_back(s);
    }
  }

  std::string hyps;
  for (const Hyp &h : board.all()) {
    const bool closed = h.stance == Stance::Refuted || (h.evals >= kHypEvals && !h.pending);
    hyps += "\n### " + h.id + " " + h.title() + " — " + def(h.stance).cn + " · 变体 " + std::to_string(h.evals) + "/" + std::to_string(kHypEvals) + " · 提出 " +
            RefText(h.at) + (closed ? " · 结案" : "") + "\n\n";
    if (!closed)
      hyps += "- 预测\n" + fields(h.fields, {"actor", "why", "expect", "refute"});
    const auto hit = by_hyp.find(h.id);
    if (hit == by_hyp.end())
      continue;
    for (const Step &s : hit->second) {
      const Act &a = *s.a;
      if (on(a, Tool::Judge)) {
        const bool last = s.at == h.judged;
        std::string ev;
        for (const std::string &e : a.call.strs("evidence")) {
          Ref r;
          const Act *x = RefOf(e, r) ? act_at(r) : nullptr;
          ev += (ev.empty() ? "" : "; ") + (x ? CallTitle(*x, r) + gist_of(*x, " → ") : e);
        }
        hyps += "- [" + RefText(s.at) + "] 判 " + def(IdOf<Stance>(a.call.str("verdict"))).cn + " · 证据 " + ev + "\n";
        if (!closed || last)
          hyps += fields(a.call.args, {"result", "insight"});
        if (!closed && last)
          hyps += fields(a.call.args, {"why", "better_feature", "next"});
      } else {
        const TrialRes &k = *AsTrial(a);
        hyps += "- [" + RefText(s.at) + "] 变体 " + k.st.cand.name + " `" + k.st.cand.expr + "` · " + text::head(k.st.cand.note, 60) + " → " + Brief(k.st.res) + "\n";
      }
    }
  }

  std::string rest, notes;
  size_t next = 0;
  for (size_t i = 0; i < msgs.size(); ++i) {
    if (i > 0 && !msgs[i].memo.empty())
      notes += "- 研究员插话 (随第 " + std::to_string(i + 1) + " 条消息)\n" + under(msgs[i].memo);
    for (; next < steps.size() && static_cast<size_t>(steps[next].at.round) == i + 1; ++next) {
      const Step &s = steps[next];
      const Act &a = *s.a;
      if (on(a, Tool::Hypothesize) || on(a, Tool::Judge) || staged(a))
        continue;
      if (!a.out.ok)
        rest += "- " + CallTitle(a, s.at) + ": " + why_of(a) + "\n";
      else if (def(a.call.tool).keep)
        notes += "- " + CallTitle(a, s.at) + "\n" + under(a.out.text);
      else if (!cited.contains(s.at))
        rest += "- " + CallTitle(a, s.at) + gist_of(a, ": ") + "\n";
    }
  }
  if (hyps.empty() && rest.empty() && notes.empty())
    return {};
  std::string s = "# 研究树 (第 1–" + std::to_string(done.size()) +
                  " 轮的各步与结论)\n\n"
                  "各调用的原文只在交回它的那条消息里给一次, 此后收在这里: 假设留预测、各次判定的结论与变体的一行结局, 被引作证据的调用写在那次判定的证据里, "
                  "其余观察留调用一行与关键数 (rIC / mono / 多空等; 已有的不必重看), 查询只留调用一行; 被退回的回复与重发要求不留. 调用编号照旧可作 judge 的证据; 要细节就重调.\n";
  if (!hyps.empty())
    s += "\n## 假设\n" + hyps;
  if (!rest.empty())
    s += "\n## 其余各步 (观察留关键数, 其余只留调用)\n\n" + rest;
  if (!notes.empty())
    s += "\n## 记录 (提交 / 全局记录 / 问答 / 插话)\n\n" + notes;
  return s;
}

} // namespace

// =============================================================================
// 公开函数
// =============================================================================
std::string CallTitle(const Act &a, Ref r) { return "[" + RefText(r) + "] " + a.call.text(kTitleChars) + (a.out.ok ? "" : " — 没做成"); }

std::string ResultsText(const std::vector<Act> &acts, int round) {
  std::string s;
  for (size_t i = 0; i < acts.size(); ++i) {
    const Act &a = acts[i];
    s += "## " + CallTitle(a, {round, static_cast<int>(i) + 1}) + "\n\n" + a.out.text;
    if (!s.ends_with('\n'))
      s += '\n';
    s += '\n';
  }
  return s;
}

std::string Fills(const std::string &tmpl, const std::string &expr) {
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

std::string GateLine(const Result &r, const Gate &g) {
  if (!r.visible || r.verdict != Verdict::Weights)
    return "";
  const std::string miss = GateMiss(g, r);
  return "- " + (miss.empty() ? std::string("达入库门槛") : miss) + "\n";
}

std::string ListTail(int all, int skip, int n) {
  if (n >= all)
    return {};
  return !n ? "(共 " + std::to_string(all) + " 个, skip=" + std::to_string(skip) + " 已过末尾)\n"
            : "(共 " + std::to_string(all) + " 个, 这里是第 " + std::to_string(skip + 1) + "–" + std::to_string(skip + n) + " 个" +
                  (skip + n < all ? "; 接着看给 skip=" + std::to_string(skip + n) : std::string()) + ")\n";
}

std::string LoadPrompt(Msg m) { return disk::read(disk::PromptFile, {.name = def(m).id}); }
void SavePrompt(Msg m, const std::string &text) { disk::write(disk::PromptFile, {.name = def(m).id}, text); }

std::string Text(const Weight &w) {
  if (!w.has)
    return "无该持有期统计";
  char b[192];
  std::snprintf(b, sizeof(b), "中位 rIC %.4f · 每期毛多空 %+.2fbp · 每期净多空 %+.2fbp · 年换手 %.0f 倍", w.score, w.gross * 1e4, w.net * 1e4, w.turn);
  return b;
}

std::string SignLine(const Result &r) {
  const Weight *w = r.best();
  return w ? std::string("符号: ") + SignCn(w->sign) : std::string();
}

std::string Brief(const Result &r) {
  switch (r.verdict) {
  case Verdict::Unstaged:
  case Verdict::Broken:
    return std::string(def(r.verdict).cn) + ": " + r.error;
  case Verdict::Weights:
    return std::to_string(r.ws.size()) + " 个权重, 最佳 " + Text(*r.best());
  case Verdict::Null:
  case Verdict::Pending:
    return def(r.verdict).cn;
  }
  return {};
}

std::string Detail(const Result &r) {
  if (!r.visible && r.verdict != Verdict::Unstaged && r.verdict != Verdict::Broken)
    return "- 统计不在当前评估期上 (不可比), 不作数\n";
  switch (r.verdict) {
  case Verdict::Unstaged:
  case Verdict::Broken:
    return "- " + std::string(def(r.verdict).cn) + ": " + r.error + "\n";
  case Verdict::Weights: {
    const int sign = r.best()->sign;
    std::string s = "- " + SignLine(r) + "\n";
    for (const Weight &w : r.ws)
      s += "- 权重 " + Fills(r.tmpl, w.expr) + (w.sign == sign ? "" : std::string(" (符号 ") + SignCn(w.sign) + ")") + ": " + Text(w) + "\n";
    return s;
  }
  case Verdict::Null:
    return "- Search 无权重 (没有一个参数点算得出该持有期的滚动窗 rIC)\n";
  case Verdict::Pending:
    return "- " + std::string(def(r.verdict).cn) + "\n";
  }
  return {};
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

Message SystemMessage(const Context &c, const Gate &g, bool ask) {
  text::Table ops({"算子", "中文名", "签名 (元与参数的值域)", "输出域", "定义", "说明"});
  for (const Op &o : c.ops)
    ops.add({o.name, o.name_cn, "$" + o.sig + "$", o.out, "$" + o.tex + "$", o.note});
  return fill(Msg::System, {{"pool", c.pool},
                            {"pool_cn", c.pool_cn},
                            {"level", c.level},
                            {"seg", std::to_string(c.seg)},
                            {"rows", rows_of(c)},
                            {"hold", c.hold},
                            {"hold_key", c.hold_key},
                            {"hold_desc", c.hold_desc},
                            {"window", "最近 " + std::to_string(c.years) + " 年, " + c.window},
                            {"srcs", c.srcs},
                            {"roll_parts", std::to_string(c.roll_parts)},
                            {"ts_root_d", std::to_string(c.ts_root_d)},
                            {"max_d", std::to_string(c.max_d)},
                            {"gate_ric", num(g.ric)},
                            {"ask", ask ? "可以用 ask_human 向研究员提一次问 (整个会话只一次)" : "研究员不参与决策, 没有 ask_human; 疑问写 report_issue"},
                            {"tools", ToolsDoc(ask)},
                            {"leaves", leaves_text(c.leaves)},
                            {"ops", ops.str()}});
}

Message FirstMessage(const Context &c, int tried, const std::string &notes, bool ask, const std::string &remark) {
  return fill(Msg::First, {{"hold", c.hold},
                           {"remark", remark_text(remark)},
                           {"tried", std::to_string(tried)},
                           {"library", std::to_string(visible(c.library))},
                           {"notes", notes},
                           {"start", ask ? "要研究员定的事 (从哪个因子起步、往哪个方向挖) 可以先看几眼数据再用 ask_human 一次问完, 也可以不问"
                                         : "自己定下要做的那一个因子 (新起, 或从已试候选 / 正式库里挑) 和方向"}});
}

Message TestMessage(bool ask, const std::string &remark) {
  std::string tools;
  for (const Tool t : kVals<Tool>)
    if (t != Tool::AskHuman || ask)
      tools += (tools.empty() ? "" : ", ") + std::string(def(t).id);
  return fill(Msg::Test, {{"tools", tools},
                          {"remark", remark_text(remark)},
                          {"ask", ask ? "ask_human 也要测: 问 1 道题 (例: 先测哪类工具), 研究员答完再接着测" : "本会话没有 ask_human, 不测它"}});
}

Message RoundMessage(const std::vector<Act> &acts, int round, const Board &board, bool evaled, bool test) {
  const std::string s = ResultsText(acts, round);
  const std::string batch = std::to_string(kBatch);
  std::string next;
  if (test) {
    next = "按自检的做法接着调还没调过的工具与取值 (eval_factor 至多 " + batch +
           " 个); 对照工具说明查上面每个结果, 有问题即 report_issue. 全部调过后 report_issue 写总评, 再 finish.";
  } else {
    next = "先对照, 再发调用:\n"
           "- **判**: 结果对照各假设的 expect / refute, 哪个被支持、哪个落空、哪个还不清; 证据够的 judge, 不够的补哪个观察\n"
           "- **因**: 为什么是这样, 错在假设的哪一环 (谁 / 什么行为 / 什么盘面 / 何时兑现)\n"
           "- **料**: 有没有更直接捕捉这一现象的特征\n"
           "- **缺**: 想刻画的维度现有叶没有或只能近似 → request_feature; 想看的现有工具看不到 → request_tool; 规则或工具可以更好 → report_issue (idea); 都在本条提\n"
           "- **记**: 值得跨会话留下的认识用 note_insight 记\n\n";
    const bool open = std::ranges::any_of(board.all(), [](const Hyp &h) { return h.stance == Stance::Supported && h.evals < kHypEvals; });
    const std::string done = "研究透了就按系统提示 \"判优\" 挑最好的版本 submit_factor, 再 finish.";
    next += evaled ? "本条只诊断 (上一条有落了文件的回测, 本条不能回测): 用观察查本批变体的结果说明了什么 (与预期哪里不符、为什么), 再对挂了变体的假设 judge; 观察排在 judge 前面, 判支持才引用得到."
            : open ? "本条可回测: 只挂判为支持的假设 (至多 " + batch + " 个 eval_factor, 各写清 hyp 与 note), 每个变体只改一处以检验一个问题; " + done
                   : "假设板上还没有能挂变体的假设 (判为支持且变体有余): 本条用观察检验假设并 judge, 或提新假设; 同条里判为支持的, 排在 judge 后面的 eval_factor 即可挂它; " + done;
  }
  return fill(Msg::Round, {{"results", s}, {"board", board.text()}, {"next", next}});
}

Message RetryMessage(const std::string &error) {
  std::string list;
  for (size_t i = 0; i <= error.size();) {
    const size_t e = std::min(error.find('\n', i), error.size());
    list += (list.empty() ? "- " : "\n- ") + error.substr(i, e - i);
    i = e + 1;
  }
  return fill(Msg::Retry, {{"error", list}});
}

std::string MemoText(const std::string &memo) {
  return "\n# 研究员插话\n\n" + memo + "\n\n这是研究员在会话中途给的意见: 与上文的做法冲突处以它为准, 回复协议与会话约束照旧.\n";
}

std::string TextOf(const Round &rd) { return rd.memo.empty() ? rd.user : rd.user + MemoText(rd.memo); }

llm::Turns TurnsOf(Rounds rs) {
  assert(!rs.empty());
  size_t j = rs.size() - 1;
  while (rs[j].kind == Msg::Retry) {
    assert(j > 0 && "重发要求前面有被退回的那条");
    --j;
  }
  llm::Turns t;
  if (j == 0) {
    t.push_back({true, TextOf(rs[0])});
  } else {
    const Round &p = rs[j - 1];
    assert(p.reply.ok() && !p.acts.empty() && "本条前一轮的回复合格且执行了");
    const std::string tree = tree_of(rs.first(j));
    t.push_back({true, TextOf(rs[0]) + (tree.empty() ? "" : "\n---\n\n" + tree)});
    t.push_back({false, p.reply.text});
    t.push_back({true, TextOf(rs[j])});
  }
  for (size_t i = j; i + 1 < rs.size(); ++i) {
    assert(rs[i].reply.ok() && rs[i].acts.empty() && !rs[i].error.empty() && "重发之前的回复被退回");
    t.push_back({false, rs[i].reply.text});
    t.push_back({true, TextOf(rs[i + 1])});
  }
  return t;
}

} // namespace GUI::Agent
