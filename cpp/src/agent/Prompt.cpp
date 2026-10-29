#include "agent/Prompt.hpp"
#include "agent/Config.hpp"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <utility>

namespace agent {

namespace {

using KV = std::vector<std::pair<std::string_view, std::string>>;

std::filesystem::path path_of(std::string_view name) { return std::filesystem::path(kPromptDir) / (std::string(name) + ".md"); }

// {{键}} → 值; 模板里的键都须在 kv 里
std::string fill(std::string_view name, const KV &kv) {
  const std::string t = LoadPrompt(name);
  std::string out;
  size_t i = 0;
  for (;;) {
    const size_t a = t.find("{{", i);
    if (a == std::string::npos) {
      out.append(t, i);
      return out;
    }
    const size_t b = t.find("}}", a);
    assert(b != std::string::npos && "模板占位 {{ 未闭合");
    const std::string_view key(t.data() + a + 2, b - a - 2);
    const auto it = std::find_if(kv.begin(), kv.end(), [&](const auto &p) { return p.first == key; });
    if (it == kv.end())
      std::fprintf(stderr, "[agent] %s.md: 未知占位 {{%.*s}}\n", std::string(name).c_str(), static_cast<int>(key.size()), key.data());
    assert(it != kv.end() && "模板占位键未知");
    out.append(t, i, a - i);
    out += it->second;
    i = b + 2;
  }
}

std::string cell(std::string s) { // 表格单元: 竖线转义, 换行压成空格
  std::string o;
  for (const char ch : s)
    o += ch == '|' ? std::string("\\|") : ch == '\n' ? std::string(" ")
                                                     : std::string(1, ch);
  return o;
}

std::string num(double v) {
  char b[32];
  std::snprintf(b, sizeof(b), "%.3g", v);
  return b;
}

std::string stat_of(const Weight &w) {
  char b[256];
  std::snprintf(b, sizeof(b), "score %.3g, 每期毛多空 %.1fbp, rIC %.4f (IR %.2f, t %.1f), mono %.2f, 有效率 %.0f%%", w.score, w.ls * 1e4, w.ric, w.ir, w.t, w.mono,
                w.valid * 100.0);
  return b;
}

// 一个因子文件的结局 (多行, 每行 "- " 起)
std::string outcome(const Result &r) {
  if (!r.error.empty())
    return "- BROKEN: " + r.error + "\n";
  if (r.ws.empty())
    return r.searched ? "- Search 无显著权重 (本持有期 score 不显著为正, 或 mono 不达标)\n" : "- 未评估\n";
  std::string s;
  for (const Weight &w : r.ws)
    s += "- `" + w.expr + "`: " + (w.has ? stat_of(w) : std::string("无该持有期统计")) + "\n";
  return s;
}

// 回复 → JSON 对象 (取首个 '{' 到末个 '}', 容忍外包代码块)
bool object_of(std::string_view text, nlohmann::json &j, std::string &err) {
  const size_t a = text.find('{'), b = text.rfind('}');
  if (a == std::string_view::npos || b == std::string_view::npos || b < a) {
    err = "回复里没有 JSON 对象";
    return false;
  }
  j = nlohmann::json::parse(text.substr(a, b - a + 1), nullptr, false);
  if (j.is_discarded() || !j.is_object()) {
    err = "回复不是合法的 JSON 对象";
    return false;
  }
  return true;
}

// 可缺省的字符串数组字段
bool strings_of(const nlohmann::json &j, const char *key, std::vector<std::string> &out, std::string &err) {
  out.clear();
  if (!j.contains(key))
    return true;
  if (!j[key].is_array()) {
    err = std::string(key) + " 须是字符串数组";
    return false;
  }
  for (const nlohmann::json &x : j[key]) {
    if (!x.is_string()) {
      err = std::string(key) + " 须是字符串数组";
      return false;
    }
    out.push_back(x.get<std::string>());
  }
  return true;
}

} // namespace

std::string GateMiss(const Gate &g, const Result &r) {
  if (!r.error.empty())
    return "BROKEN: " + r.error;
  for (const Weight &w : r.ws)
    if (w.has && w.score >= g.score && w.t >= g.t && w.mono >= g.mono && w.valid >= g.valid)
      return "";
  const Weight *w = r.best();
  if (!w)
    return "本持有期无显著权重";
  std::string s = "最佳权重未达门槛:";
  if (w->score < g.score)
    s += " score " + num(w->score) + " < " + num(g.score);
  if (w->t < g.t)
    s += " t " + num(w->t) + " < " + num(g.t);
  if (w->mono < g.mono)
    s += " mono " + num(w->mono) + " < " + num(g.mono);
  if (w->valid < g.valid)
    s += " 有效率 " + num(w->valid) + " < " + num(g.valid);
  return s;
}

std::string LoadPrompt(std::string_view name) {
  std::ifstream f(path_of(name), std::ios::binary);
  assert(f.is_open() && "提示词模板不在 kPromptDir");
  return std::string(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

void SavePrompt(std::string_view name, const std::string &text) {
  std::ofstream f(path_of(name), std::ios::binary | std::ios::trunc);
  assert(f.is_open());
  f << text;
}

std::string SystemPrompt(const Context &c, const Gate &g) {
  std::string leaves = "| 叶 | 中文名 | 公式 | 说明 |\n|---|---|---|---|\n";
  for (const Leaf &l : c.leaves)
    leaves += "| " + l.code + " | " + cell(l.name_cn) + " | " + cell(l.formula) + " | " + cell(l.desc) + " |\n";
  std::string ops = "| 算子 | 中文名 | 签名 (元与参数的值域) | 输出域 | 定义 | 说明 |\n|---|---|---|---|---|---|\n";
  for (const Op &o : c.ops)
    ops += "| " + o.name + " | " + cell(o.name_cn) + " | $" + cell(o.sig) + "$ | " + o.out + " | $" + cell(o.tex) + "$ | " + cell(o.note) + " |\n";
  std::string lib;
  int n = 0;
  for (const Result &r : c.library) {
    if (n++ == kLibTop)
      break;
    const Weight *w = r.best();
    lib += "- " + r.name + " (" + r.name_cn + "): `" + r.tmpl + "`" + (r.note.empty() ? "" : " — " + r.note) + "\n";
    lib += w ? "  - 最佳权重 `" + w->expr + "`: " + stat_of(*w) + "\n" : std::string("  - 本持有期无统计\n");
  }
  if (lib.empty())
    lib = "(空)\n";
  return fill("system", {{"pool", c.pool},
                         {"pool_cn", c.pool_cn},
                         {"level", c.level},
                         {"seg", std::to_string(c.seg)},
                         {"hold", c.hold},
                         {"hold_desc", c.hold_desc},
                         {"ts_root_d", std::to_string(c.ts_root_d)},
                         {"max_d", std::to_string(c.max_d)},
                         {"batch", std::to_string(kBatch)},
                         {"gate_score", num(g.score)},
                         {"gate_t", num(g.t)},
                         {"gate_mono", num(g.mono)},
                         {"gate_valid", num(g.valid * 100.0)},
                         {"leaves", leaves},
                         {"ops", ops},
                         {"library", lib}});
}

std::string FirstMessage(const Context &c, const std::vector<Result> &tried) {
  std::string s;
  int n = 0;
  for (const Result &r : tried) {
    if (n++ == kTriedTop)
      break;
    s += "### " + r.name + " (" + r.name_cn + ")\n`" + r.tmpl + "`\n" + outcome(r);
  }
  if (s.empty())
    s = "(无)\n";
  return fill("first", {{"hold", c.hold}, {"tried", s}});
}

std::string AnswerMessage(const std::vector<Question> &qs, const std::vector<std::vector<bool>> &picks, const std::string &note) {
  assert(picks.size() == qs.size());
  std::string s;
  for (size_t i = 0; i < qs.size(); ++i) {
    assert(picks[i].size() == qs[i].options.size());
    std::string sel;
    for (size_t j = 0; j < qs[i].options.size(); ++j)
      if (picks[i][j])
        sel += (sel.empty() ? "" : "; ") + qs[i].options[j];
    s += "### " + std::to_string(i + 1) + ". " + qs[i].prompt + "\n- " + (sel.empty() ? std::string("(未选, 你自定)") : sel) + "\n";
  }
  if (s.empty())
    s = "(无问题)\n";
  return fill("answer", {{"answers", s}, {"note", note.empty() ? std::string("(无)") : note}, {"batch", std::to_string(kBatch)}});
}

std::string RoundMessage(const Context &c, const std::vector<Submitted> &subs, const std::vector<Staged> &batch, const std::vector<Result> &results) {
  std::string ss;
  for (const Submitted &x : subs)
    ss += "- " + x.name + ": " + (x.err.empty() ? std::string("已入库") : "未入库, " + x.err) + "\n";
  if (ss.empty())
    ss = "(上次未提交)\n";
  std::string s;
  for (const Staged &st : batch) {
    s += "### " + st.cand.name + " (" + st.cand.name_cn + ")\n`" + st.cand.expr + "`\n";
    if (!st.err.empty()) {
      s += "- 未落文件: " + st.err + "\n";
      continue;
    }
    const auto it = std::find_if(results.begin(), results.end(), [&](const Result &r) { return r.file == st.file; });
    assert(it != results.end() && "已落的候选应在结果里");
    s += outcome(*it);
  }
  return fill("round", {{"hold", c.hold}, {"batch", std::to_string(kBatch)}, {"submits", ss}, {"results", s}});
}

std::string RetryMessage(const std::string &err) { return fill("retry", {{"error", err}}); }

bool ParseQuestions(std::string_view text, std::vector<Question> &out, std::string &err) {
  out.clear();
  nlohmann::json j;
  if (!object_of(text, j, err))
    return false;
  if (!j.contains("questions") || !j["questions"].is_array()) {
    err = "缺数组 questions (没有问题就给空数组)";
    return false;
  }
  for (size_t i = 0; i < j["questions"].size(); ++i) {
    const nlohmann::json &q = j["questions"][i];
    const std::string at = "questions[" + std::to_string(i) + "]";
    if (!q.is_object() || !q.contains("prompt") || !q["prompt"].is_string()) {
      err = at + " 缺字符串字段 prompt";
      return false;
    }
    Question x;
    x.prompt = q["prompt"].get<std::string>();
    if (!strings_of(q, "options", x.options, err) || x.options.size() < 2) {
      err = at + " 的 options 须是至少 2 项的字符串数组";
      return false;
    }
    if (q.contains("multiple")) {
      if (!q["multiple"].is_boolean()) {
        err = at + " 的 multiple 须是布尔";
        return false;
      }
      x.multiple = q["multiple"].get<bool>();
    }
    out.push_back(std::move(x));
  }
  return true;
}

bool ParseMine(std::string_view text, MineReply &out, std::string &err) {
  out = {};
  nlohmann::json j;
  if (!object_of(text, j, err))
    return false;
  if (j.contains("questions")) {
    err = "挖掘阶段不得提问; 疑问写进 issues";
    return false;
  }
  if (j.contains("done")) {
    if (!j["done"].is_boolean()) {
      err = "done 须是布尔";
      return false;
    }
    out.done = j["done"].get<bool>();
  }
  if (!strings_of(j, "submit", out.submit, err) || !strings_of(j, "issues", out.issues, err))
    return false;
  if (!j.contains("factors") || !j["factors"].is_array()) {
    err = "缺数组 factors";
    return false;
  }
  if (out.done != j["factors"].empty()) {
    err = out.done ? "done = true 时 factors 须为空" : "factors 为空 (挖完请置 done = true)";
    return false;
  }
  for (size_t i = 0; i < j["factors"].size(); ++i) {
    const nlohmann::json &f = j["factors"][i];
    Cand c;
    const std::pair<const char *, std::string *> fields[] = {{"name", &c.name}, {"name_cn", &c.name_cn}, {"expr", &c.expr}, {"note", &c.note}};
    for (const auto &[key, dst] : fields) {
      if (f.is_object() && f.contains(key) && f[key].is_string()) {
        *dst = f[key].get<std::string>();
      } else if (std::string_view(key) != "note") {
        err = "factors[" + std::to_string(i) + "] 缺字符串字段 " + key;
        return false;
      }
    }
    out.factors.push_back(std::move(c));
  }
  return true;
}

} // namespace agent
