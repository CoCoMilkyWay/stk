#include "agent/Prompt.hpp"
#include "agent/Config.hpp"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <type_traits>
#include <utility>

namespace agent {

namespace {

using json = nlohmann::json;
using KV = std::vector<std::pair<std::string_view, std::string>>;

std::filesystem::path path_of(Msg m) { return std::filesystem::path(kPromptDir) / (std::string(kPrompts[static_cast<int>(m)]) + ".md"); }

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
      std::fprintf(stderr, "[agent] %s.md: 未知占位 {{%.*s}}\n", kPrompts[static_cast<int>(m)], static_cast<int>(key.size()), key.data());
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

std::string stat_of(const Weight &w) {
  char b[256];
  std::snprintf(b, sizeof(b), "mono %.3f, 每期毛多空 %.2fbp, rIC %.4f; score %.3g, 有效率 %.0f%%", w.mono, w.ls * 1e4, w.ric, w.score, w.valid * 100.0);
  return b;
}

std::string stat_of(const Top &t) {
  char b[256];
  std::snprintf(b, sizeof(b), "mono %.3f, 每期毛多空 %.2fbp, rIC %.4f; 目标 ls·rIC %.3g ± %.3g (标准误)", t.mono, t.ls * 1e4, t.ric, t.g, t.se);
  return b;
}

// 一个因子文件的结局 (多行, 每行 "- " 起)
std::string outcome(const Result &r) {
  if (!r.error.empty())
    return "- BROKEN: " + r.error + "\n";
  if (r.ws.empty() && r.has_top)
    return "- Search 无显著权重, 落选原因: " + r.top.miss + "\n- 目标最高的候选 `" + r.top.expr + "`" + (r.top.sign < 0 ? " (取反)" : "") + ": " + stat_of(r.top) +
           "\n";
  if (r.ws.empty())
    return r.searched ? "- Search 无显著权重 (本持有期 score 不显著为正, 或 mono 不达标)\n" : "- 未评估\n";
  std::string s;
  for (const Weight &w : r.ws)
    s += "- `" + w.expr + "`: " + (w.has ? stat_of(w) : std::string("无该持有期统计")) + "\n";
  return s;
}

std::string or_none(std::string s, const char *none) { return s.empty() ? none : s; }

// 回复 → JSON 对象 (取首个 '{' 到末个 '}', 容忍外包代码块)
bool object_of(std::string_view text, json &j, std::string &err) {
  const size_t a = text.find('{'), b = text.rfind('}');
  if (a == std::string_view::npos || b == std::string_view::npos || b < a) {
    err = "回复里没有 JSON 对象";
    return false;
  }
  j = json::parse(text.substr(a, b - a + 1), nullptr, false);
  if (j.is_discarded() || !j.is_object()) {
    err = "回复不是合法的 JSON 对象";
    return false;
  }
  return true;
}

// 字段值类型: std::string / bool / int64_t
template <class T>
bool is(const json &x) {
  if constexpr (std::is_same_v<T, std::string>)
    return x.is_string();
  else if constexpr (std::is_same_v<T, bool>)
    return x.is_boolean();
  else
    return x.is_number_integer();
}
template <class T>
constexpr const char *kKind = std::is_same_v<T, std::string> ? "字符串" : std::is_same_v<T, bool> ? "布尔"
                                                                                                  : "整数";

// 回复里一个 JSON 对象的逐字段读取: 首个不合协议处记进共用的 err (带字段路径), 之后全部短路
class In {
public:
  In(const json &j, std::string at, std::string &err) : j_(j), at_(std::move(at)), err_(err) {
    if (!j_.is_object())
      fail(at_ + " 须是 JSON 对象");
  }
  bool ok() const { return err_.empty(); }
  void fail(std::string why) {
    if (ok())
      err_ = std::move(why);
  }
  std::string path(const char *k) const { return at_.empty() ? std::string(k) : at_ + "." + k; }
  bool has(const char *k) const { return ok() && j_.contains(k); }

  // 标量; opt = 可缺省 (缺省则 out 不动)
  template <class T>
  In &get(const char *k, T &out, bool opt = false) {
    if (const json *v = field(k, opt, kKind<T>); v && expect(k, is<T>(*v), kKind<T>))
      out = v->get<T>();
    return *this;
  }
  // 标量数组; opt = 可缺省 (缺省 = 空)
  template <class T>
  In &list(const char *k, std::vector<T> &out, bool opt = false) {
    out.clear();
    const std::string kind = std::string(kKind<T>) + "数组";
    const auto elem = [](const json &x) { return is<T>(x); };
    if (const json *v = field(k, opt, kind.c_str()); v && expect(k, v->is_array() && std::all_of(v->begin(), v->end(), elem), kind.c_str()))
      for (const json &x : *v)
        out.push_back(x.get<T>());
    return *this;
  }
  // 对象数组 (必填): 元素交给调用方逐个 In
  const json *objects(const char *k) {
    const json *v = field(k, false, "数组");
    return v && expect(k, v->is_array(), "数组") ? v : nullptr;
  }

private:
  const json *field(const char *k, bool opt, const char *kind) {
    if (!ok())
      return nullptr;
    const auto it = j_.find(k);
    if (it == j_.end()) {
      if (!opt)
        fail("缺" + std::string(kind) + "字段 " + path(k));
      return nullptr;
    }
    return &*it;
  }
  bool expect(const char *k, bool cond, const char *kind) {
    if (!cond)
      fail(path(k) + " 须是" + kind);
    return cond;
  }

  const json &j_;
  std::string at_;
  std::string &err_;
};

std::string at(const char *k, size_t i) { return std::string(k) + "[" + std::to_string(i) + "]"; }

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

std::string Picked(const Question &q, const std::vector<bool> &mask) {
  assert(mask.size() == q.options.size());
  std::string s;
  for (size_t j = 0; j < q.options.size(); ++j)
    if (mask[j])
      s += (s.empty() ? "" : "; ") + q.options[j];
  return s;
}

std::string Answered(const Question &q, const Pick &p) {
  std::string s = Picked(q, p.on);
  if (p.other && !p.text.empty())
    s += (s.empty() ? "" : "; ") + ("其他: " + p.text);
  return s;
}

// ---- 发 ----
Message SystemMessage(const Context &c, const Gate &g) {
  std::string leaves = "| 叶 | 中文名 | 公式 | 说明 |\n|---|---|---|---|\n";
  for (const Leaf &l : c.leaves)
    leaves += "| " + l.code + " | " + cell(l.name_cn) + " | " + cell(l.formula) + " | " + cell(l.desc) + " |\n";
  std::string ops = "| 算子 | 中文名 | 签名 (元与参数的值域) | 输出域 | 定义 | 说明 |\n|---|---|---|---|---|---|\n";
  for (const Op &o : c.ops)
    ops += "| " + o.name + " | " + cell(o.name_cn) + " | $" + cell(o.sig) + "$ | " + o.out + " | $" + cell(o.tex) + "$ | " + cell(o.note) + " |\n";
  std::string lib;
  for (size_t i = 0; i < c.library.size() && i < static_cast<size_t>(kLibTop); ++i) {
    const Result &r = c.library[i];
    const Weight *w = r.best();
    lib += "- " + r.name + " (" + r.name_cn + "): `" + r.tmpl + "`" + (r.note.empty() ? "" : " — " + r.note) + "\n";
    lib += w ? "  - 最佳权重 `" + w->expr + "`: " + stat_of(*w) + "\n" : std::string("  - 本持有期无统计\n");
  }
  return fill(Msg::System, {{"pool", c.pool},
                            {"pool_cn", c.pool_cn},
                            {"level", c.level},
                            {"seg", std::to_string(c.seg)},
                            {"hold", c.hold},
                            {"hold_desc", c.hold_desc},
                            {"ts_root_d", std::to_string(c.ts_root_d)},
                            {"max_d", std::to_string(c.max_d)},
                            {"batch", std::to_string(kBatch)},
                            {"gate_mono", num(g.mono)},
                            {"gate_ric", num(g.ric)},
                            {"leaves", leaves},
                            {"ops", ops},
                            {"library", or_none(std::move(lib), "(空)\n")}});
}

Message FirstMessage(const Context &c, const std::vector<Result> &tried) {
  std::string s;
  for (size_t i = 0; i < tried.size() && i < static_cast<size_t>(kTriedTop); ++i)
    s += "### " + tried[i].name + " (" + tried[i].name_cn + ")\n`" + tried[i].tmpl + "`\n" + outcome(tried[i]);
  return fill(Msg::First, {{"hold", c.hold}, {"tried", or_none(std::move(s), "(无)\n")}});
}

Message AnswerMessage(const Questions &qs, const std::vector<Pick> &picks, const std::string &note) {
  assert(picks.size() == qs.size());
  std::string s;
  for (size_t i = 0; i < qs.size(); ++i) {
    const std::string sel = Answered(qs[i], picks[i]);
    s += "### " + std::to_string(i + 1) + ". " + qs[i].prompt + "\n- " + or_none(sel, "(未选, 你自定)") + "\n";
    if (picks[i].on != qs[i].recommended || (picks[i].other && !picks[i].text.empty()))
      s += "- (未照你的推荐: " + Picked(qs[i], qs[i].recommended) + ")\n";
  }
  return fill(Msg::Answer, {{"answers", or_none(std::move(s), "(无问题)\n")}, {"note", or_none(note, "(无)")}, {"batch", std::to_string(kBatch)}});
}

Message RoundMessage(const Context &c, const std::vector<Submitted> &subs, const std::vector<Staged> &batch) {
  std::string ss;
  for (const Submitted &x : subs)
    ss += "- " + x.name + ": " + (x.err.empty() ? std::string("已入库") : "未入库, " + x.err) + "\n";
  std::string s;
  for (const Staged &st : batch) {
    s += "### " + st.cand.name + " (" + st.cand.name_cn + ")\n`" + st.cand.expr + "`\n";
    s += st.file.empty() ? "- 未落文件: " + st.err + "\n" : outcome(st.res);
  }
  return fill(Msg::Round, {{"hold", c.hold}, {"batch", std::to_string(kBatch)}, {"submits", or_none(std::move(ss), "(上次未提交)\n")}, {"results", s}});
}

Message RetryMessage(const std::string &err) { return fill(Msg::Retry, {{"error", err}}); }

// ---- 收 ----
bool Parse(std::string_view text, Questions &out, std::string &err) {
  out.clear(), err.clear();
  json j;
  if (!object_of(text, j, err))
    return false;
  In in(j, "", err);
  if (!in.has("questions"))
    in.fail("缺数组 questions (没有问题就给空数组)");
  const json *qs = in.objects("questions");
  for (size_t i = 0; in.ok() && i < qs->size(); ++i) {
    In q((*qs)[i], at("questions", i), err);
    Question x;
    std::vector<int64_t> rec;
    q.get("prompt", x.prompt).list("options", x.options).get("multiple", x.multiple, true).list("recommended", rec);
    if (q.ok() && x.options.size() < 2)
      q.fail(q.path("options") + " 须至少 2 项");
    x.recommended.assign(x.options.size(), false);
    bool rec_ok = x.multiple ? !rec.empty() : rec.size() == 1;
    for (const int64_t k : rec) {
      rec_ok = rec_ok && k >= 0 && k < static_cast<int64_t>(x.options.size()) && !x.recommended[static_cast<size_t>(k)];
      if (rec_ok)
        x.recommended[static_cast<size_t>(k)] = true;
    }
    if (!rec_ok)
      q.fail(q.path("recommended") + " 须是选项下标数组 (从 0 起, 不重复; 单选恰 1 个, 多选至少 1 个)");
    out.push_back(std::move(x));
  }
  return in.ok();
}

bool Parse(std::string_view text, MineReply &out, std::string &err) {
  out = {}, err.clear();
  json j;
  if (!object_of(text, j, err))
    return false;
  In in(j, "", err);
  if (in.has("questions"))
    in.fail("挖掘阶段不得提问; 疑问写进 issues");
  in.get("done", out.done, true).list("submit", out.submit, true).list("issues", out.issues, true);
  const json *fs = in.objects("factors");
  if (in.ok() && out.done != fs->empty())
    in.fail(out.done ? "done = true 时 factors 须为空" : "factors 为空 (挖完请置 done = true)");
  if (in.ok() && fs->size() > static_cast<size_t>(kBatch))
    in.fail("factors 至多 " + std::to_string(kBatch) + " 个");
  for (size_t i = 0; in.ok() && i < fs->size(); ++i) {
    In f((*fs)[i], at("factors", i), err);
    Cand c;
    f.get("name", c.name).get("name_cn", c.name_cn).get("expr", c.expr).get("note", c.note, true);
    out.factors.push_back(std::move(c));
  }
  return in.ok();
}

} // namespace agent
