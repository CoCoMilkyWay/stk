// Board — 见头文件
#include "gui/task_agent/Board.hpp"
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Text.hpp"

#include <algorithm>
#include <cassert>
#include <charconv>

namespace GUI::Agent {

bool RefOf(std::string_view s, Ref &r) {
  const size_t hash = s.find('#');
  if (s.size() < 4 || s[0] != 'R' || hash == std::string_view::npos)
    return false;
  const auto whole = [](std::string_view t, int &v) {
    const auto [p, ec] = std::from_chars(t.data(), t.data() + t.size(), v);
    return ec == std::errc() && p == t.data() + t.size() && v >= 1;
  };
  return whole(s.substr(1, hash - 1), r.round) && whole(s.substr(hash + 1), r.call);
}

std::string RefText(Ref r) { return "R" + std::to_string(r.round) + "#" + std::to_string(r.call); }

void Board::add(const Call &c, Ref at) {
  assert(c.tool == Tool::Hypothesize);
  hyps_.push_back({.id = next(), .fields = c.args, .at = at});
}

void Board::judge(const Call &c, Ref at) {
  assert(c.tool == Tool::Judge);
  Hyp &h = hyp(c.str("hyp"));
  h.stance = IdOf<Stance>(c.str("verdict")), h.judged = at, h.verdict = c.args, h.pending = 0;
}

void Board::vary(const Call &c, bool knob, Ref at) {
  assert(c.tool == Tool::EvalFactor);
  Hyp &h = hyp(c.str("hyp"));
  ++h.evals, h.knobs += knob, h.pending = at.round;
}

const Hyp *Board::find(const std::string &id) const {
  const auto it = std::find_if(hyps_.begin(), hyps_.end(), [&](const Hyp &h) { return h.id == id; });
  return it == hyps_.end() ? nullptr : &*it;
}

Hyp &Board::hyp(const std::string &id) {
  const Hyp *h = find(id);
  assert(h && "假设已在板上");
  return const_cast<Hyp &>(*h);
}

std::string Board::text() const {
  if (hyps_.empty())
    return "(还没有假设: 先看数据, 用 hypothesize 写下机制假设与它的预测)\n";
  std::string s = "| 编号 | 假设 | 判定 | 变体 (旋钮类) | 提出 / 最近判定 |\n|---|---|---|---|---|\n";
  std::string owe;
  for (const Hyp &h : hyps_) {
    s += "| " + h.id + " | " + text::head(h.title(), 40) + " | " + def(h.stance).cn + " | " + std::to_string(h.evals) + "/" + std::to_string(kHypEvals) + " (" +
         std::to_string(h.knobs) + "/" + std::to_string(kHypKnobs) + ") | " + RefText(h.at) + (h.judged.round ? " / " + RefText(h.judged) : "") + " |\n";
    if (h.pending)
      owe += (owe.empty() ? "" : ", ") + h.id + " (第 " + std::to_string(h.pending) + " 轮回测的变体)";
  }
  return s + (owe.empty() ? "" : "\n待判: " + owe + " —— 本条回复须对它们各发一个 judge\n");
}

std::string Board::report() const {
  std::string s;
  for (const Hyp &h : hyps_) {
    s += "\n### " + h.id + " " + h.title() + " — " + def(h.stance).cn + " (变体 " + std::to_string(h.evals) + ")\n\n" + FieldsText(Tool::Hypothesize, h.fields, {"title"}, "- ") +
         FieldsText(Tool::Judge, h.verdict, {"hyp", "verdict"}, "- ");
  }
  return s.empty() ? "\n(无)\n" : s;
}

} // namespace GUI::Agent
