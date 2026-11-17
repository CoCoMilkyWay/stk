// read_doc — 读研报: 输入 id / part / from / skip → ReadRes (标注卡 / 目录 / 原文一段) → 原文; 展示见 ui/query/ReadDoc.cpp
#include "gui/task_agent/Config.hpp"
#include "gui/task_agent/Docs.hpp"
#include "gui/task_agent/Text.hpp"

namespace GUI::Agent {

using namespace docs;
using json = nlohmann::ordered_json;

std::string ReadRes::text() const {
  if (part == "outline") {
    std::string o = "# " + id + " · 全文 " + std::to_string(chars) + " 字 · " + std::to_string(total) + " 节 (节号 · 标题 · 字数; 读原文用 part=text from=节号)\n";
    for (const Sec &x : secs)
      o += std::to_string(x.k) + " " + x.title + " · " + std::to_string(x.chars) + "\n";
    return o;
  }
  if (part == "text") {
    std::string out = "# " + id + " · 原文, 第 " + std::to_string(sec) + " 节" + (skip ? " 第 " + std::to_string(skip) + " 字" : "") + "起\n\n" + body;
    return out + (end ? "\n(全文完)\n" : "\n…(未完: 接着读 from=" + std::to_string(next_sec) + (next_skip ? " skip=" + std::to_string(next_skip) : "") + ")\n");
  }
  const auto add_factors = [&](std::string &o) {
    for (size_t i = 0; i < factors.size(); ++i) {
      const Factor &f = factors[i];
      const std::string row = std::to_string(f.no) + ". " + f.head + "\n   公式: " + f.formula + "\n   摘录: " + f.evidence + "\n";
      if (i > 0 && text::chars(o) + text::chars(row) > static_cast<size_t>(kOutChars) - kReadTail) {
        o += "…(未完: 第 " + std::to_string(f.no) + ".." + std::to_string(total) + " 个因子接着读 part=card from=" + std::to_string(f.no) + ")\n";
        return;
      }
      o += row;
    }
  };
  if (from > 0) {
    std::string out = "# " + id + " · 标注卡的因子, 第 " + std::to_string(from) + " 个起 (共 " + std::to_string(total) + "; 统计是原文在其股票池与预测期上的数)\n";
    add_factors(out);
    return out;
  }
  std::string o = "# " + id + "\n" + meta + "\n\n## 涉及阶段\n";
  for (const std::string &s : stages)
    o += "- " + s + "\n";
  o += "\n## 结论 (" + std::to_string(findings.size()) + ")\n";
  for (size_t i = 0; i < findings.size(); ++i)
    o += std::to_string(i + 1) + ". " + findings[i] + "\n";
  if (!pre_in.empty() || !pre_out.empty() || !cited.empty()) {
    o += "\n## 引用\n";
    if (!pre_in.empty())
      o += "- 前作 (库内): " + text::join(pre_in, ", ") + "\n";
    if (!pre_out.empty())
      o += "- 前作 (库外): " + text::join(pre_out, "; ") + "\n";
    if (!cited.empty())
      o += "- 被引 (库内): " + text::join(cited, ", ") + "\n";
  }
  if (total) {
    o += "\n## 因子 (" + std::to_string(total) + "; 统计是原文在其股票池与预测期上的数)\n";
    add_factors(o);
  }
  return o;
}

ToolOut ReadDoc(const Call &c, const std::atomic<bool> &) {
  const Lib &L = lib();
  std::string error;
  const std::optional<size_t> di = doc_index(L, c.str("id"), error);
  if (!di)
    return Fail(std::move(error));
  const docs::Doc &d = L.docs[*di];
  auto res = std::make_shared<ReadRes>();
  ReadRes &R = *res;
  R.id = d.id, R.part = c.str("part");
  const size_t from = static_cast<size_t>(c.num("from"));
  if (R.part == "card") {
    const size_t nf = d.factors.size();
    if (from > nf)
      return Fail("from=" + std::to_string(from) + " 超出: 本篇标注卡共 " + std::to_string(nf) + " 个因子 (card 的 from = 从第几个因子列起, 0 = 整卡)");
    R.from = from, R.total = nf;
    for (size_t i = from > 0 ? from - 1 : 0; i < nf; ++i) {
      const docs::Factor &f = L.factors[d.factors[i]];
      R.factors.push_back({i + 1, factor_head(f), text::flat(f.formula), text::head(text::flat(f.evidence), docs::kEvidence)});
    }
    if (from > 0)
      return Done(std::move(res));
    R.meta = "体裁 " + d.genre + " · 资产 " + text::join(d.asset, "+") + " · 主阶段 " + named(L, d.primary) + " · 价值 " + d.value + " · 所在 " + d.dir;
    for (const auto &[k, v] : d.tag.at("pipe").items()) {
      std::vector<std::string> aps;
      for (const std::string &a : v.at("approach").get<std::vector<std::string>>())
        aps.push_back(named(L, a));
      std::string s = named(L, k) + ": 流派 " + text::join(aps, ", ");
      if (v.contains("baseline") && !v["baseline"].empty())
        s += " · 对照 " + text::join(v["baseline"].get<std::vector<std::string>>(), ", ");
      if (v.contains("setup"))
        if (const std::string su = setup_of(v["setup"]); !su.empty())
          s += " · " + su;
      R.stages.push_back(std::move(s));
    }
    for (const std::string &x : d.findings)
      R.findings.push_back(text::flat(x));
    for (const std::string &b : d.builds_on)
      if (L.by_id.count(b))
        R.pre_in.push_back("`" + b + "`");
    R.pre_out = d.external;
    for (const size_t i : d.cited_by)
      R.cited.push_back("`" + L.docs[i].id + "`");
    return Done(std::move(res));
  }
  const docs::Text &t = L.text_at(*di);
  if (R.part == "outline") {
    R.chars = text::chars(t.body), R.total = t.at.size();
    for (size_t k = 0; k < t.at.size(); ++k) {
      const size_t n = text::chars(std::string_view(t.body).substr(t.at[k], t.end(k) - t.at[k]));
      if (k > 0 || n > 0)
        R.secs.push_back({k, n, t.title(k)});
    }
    return Done(std::move(res));
  }
  const size_t skip = static_cast<size_t>(c.num("skip"));
  if (from >= t.at.size())
    return Fail("from=" + std::to_string(from) + " 超出: 本篇共 " + std::to_string(t.at.size()) + " 节 (0.." + std::to_string(t.at.size() - 1) + ")");
  const size_t p = text::fwd(t.body, t.at[from], skip);
  if (skip > 0 && p >= t.end(from))
    return Fail("skip=" + std::to_string(skip) + " 超出: 第 " + std::to_string(from) + " 节共 " + std::to_string(text::chars(std::string_view(t.body).substr(t.at[from], t.end(from) - t.at[from]))) + " 字");
  size_t q = text::fwd(t.body, p, static_cast<size_t>(kOutChars) - kReadTail);
  if (q < t.body.size())
    if (const size_t nl = t.body.rfind('\n', q); nl != std::string::npos && nl > p)
      q = nl + 1;
  R.sec = from, R.skip = skip, R.body = t.body.substr(p, q - p), R.end = q >= t.body.size();
  if (!R.end) {
    const size_t k = t.section(q);
    R.next_sec = k, R.next_skip = text::chars(std::string_view(t.body).substr(t.at[k], q - t.at[k]));
  }
  return Done(std::move(res));
}

json SampleReadDoc(Feed &p) {
  static const char *kParts[] = {"card", "outline", "text"};
  const Lib *L = loaded();
  const std::string id = L && !L->docs.empty() ? L->docs[static_cast<size_t>(p.below(static_cast<int>(L->docs.size())))].id : std::string("2024");
  return {{"id", id}, {"part", kParts[p.below(3)]}, {"from", 0}, {"skip", 0}};
}

} // namespace GUI::Agent
