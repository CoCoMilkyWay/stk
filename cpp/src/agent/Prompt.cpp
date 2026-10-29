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

} // namespace

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

std::string SystemPrompt(const Context &c) {
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
  return fill("first", {{"hold", c.hold}, {"batch", std::to_string(kBatch)}, {"tried", s}});
}

std::string RoundMessage(const Context &c, const std::vector<Staged> &batch, const std::vector<Result> &results) {
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
  return fill("round", {{"hold", c.hold}, {"batch", std::to_string(kBatch)}, {"results", s}});
}

std::string RetryMessage(const std::string &err) { return fill("retry", {{"error", err}}); }

bool ParseReply(std::string_view text, std::vector<Cand> &out, std::string &err) {
  out.clear();
  const size_t a = text.find('{'), b = text.rfind('}');
  if (a == std::string_view::npos || b == std::string_view::npos || b < a) {
    err = "回复里没有 JSON 对象";
    return false;
  }
  const nlohmann::json j = nlohmann::json::parse(text.substr(a, b - a + 1), nullptr, false);
  if (j.is_discarded() || !j.is_object()) {
    err = "回复不是合法的 JSON 对象";
    return false;
  }
  if (!j.contains("factors") || !j["factors"].is_array() || j["factors"].empty()) {
    err = "缺非空数组 factors";
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
    out.push_back(std::move(c));
  }
  return true;
}

} // namespace agent
