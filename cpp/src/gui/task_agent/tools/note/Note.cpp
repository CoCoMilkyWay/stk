// 记录 (request_feature / request_tool / note_insight / report_issue): 四个工具同一套落记录 → NoteRes (编号或 ref 不成的原因) → 原文; 展示见 ui/note/Note.cpp
#include "gui/task_agent/tools/Note.hpp"
#include "gui/task_agent/tools/Feed.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

namespace {

// ref 须是同类已有的一条; dry (工作台) 只给编号不落盘
ToolOut file(const Call &c, Seat &s) {
  auto r = std::make_shared<NoteRes>();
  const NoteKind k = NoteOf(c.tool);
  r->kind = k, r->ref = c.str("ref"), r->fields = c.args, r->dry = s.dry;
  r->fields.erase("ref");
  if (!r->ref.empty() && !s.notes.find(k, r->ref)) {
    r->miss = "没有" + std::string(def(k).cn) + " " + r->ref + ": ref 写 list_notes 里同类已有一条的编号 (编号首字母 " + def(k).prefix + "), 新提的不写";
    return Done(std::move(r), false);
  }
  r->id = s.dry ? s.notes.next(k) : s.notes.add(c, s.scope, s.session, s.me).id;
  return Done(std::move(r));
}

} // namespace

std::string NoteRes::text() const {
  if (!miss.empty())
    return miss;
  return "已记为 " + id + (ref.empty() ? "" : " (附议 " + ref + ")") + "; 全局可见: 研究员在界面里看, 后续会话用 list_notes 查得到";
}

std::string NoteRes::title() const { return miss.empty() ? id + " · " + fields.at("title").get<std::string>() : miss; }

ToolOut RequestFeature(const Call &c, Seat &s) { return file(c, s); }
ToolOut RequestTool(const Call &c, Seat &s) { return file(c, s); }
ToolOut NoteInsight(const Call &c, Seat &s) { return file(c, s); }
ToolOut ReportIssue(const Call &c, Seat &s) { return file(c, s); }

json SampleRequestFeature(Feed &p) {
  return {{"title", "示例: 撤单前挂单存续时长的分布"},
          {"define", "lob 逐笔撤单: 每笔撤单距其挂出的交易秒, 按分钟取中位数; 日内可见"},
          {"mechanism", "短存续撤单多 = 试探性挂单 / 幌骗, 可能预示短期反转"},
          {"gap", "现有叶只有撤单额与笔数, 没有存续时长 (查过 " + p.leaf() + ")"},
          {"ref", ""}};
}

json SampleRequestTool(Feed &) {
  return {{"title", "示例: 看因子的年换手与净值"},
          {"want", "给 factor, 看年换手、扣成本后的多空净值、按金额段的冲击"},
          {"purpose", "判断低换手要求下哪个变体更好"},
          {"gap", "eval_factor 只给年换手一个数, 看不到成本后的结果"},
          {"ref", ""}};
}

json SampleNoteInsight(Feed &p) {
  const std::vector<std::string> refs = p.refs(1);
  return {{"title", "示例: 尾盘信号在次日开盘前兑现完"},
          {"evidence", (refs.empty() ? StepTag(1) : refs.front()) + " view_response 的 T+1 收盘点与当日收盘点持平"},
          {"reach", "本作用域; 其他持有期没验证"},
          {"use", "尾盘类因子不宜配长持有期"},
          {"ref", ""}};
}

json SampleReportIssue(Feed &p) { return {{"type", p.pick(Tool::ReportIssue, "type")}, {"title", "示例: 某个结果不好读"}, {"detail", "现在怎样、改成怎样、为什么"}, {"ref", ""}}; }

} // namespace GUI::Agent
