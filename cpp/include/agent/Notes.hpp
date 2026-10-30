#pragma once

// =============================================================================
// Notes — agent 的全局记录: 特征申请 / 工具申请 / 发现 / 建议与 bug (跨会话、跨作用域累积; 只 GUI 线程读写)
// =============================================================================
//   一类一个文件 kGlobalDir/<类>.jsonl (自检会话的在 kGlobalDir/test/), 一条一行: 编号 / 时间 / 作用域 / 出处 (会话目录 · 轮) / 附议哪条 / 字段
//   字段 = 提交它的工具的参数 (去掉 ref), 键按参数表序; 编号 = 类前缀 + 类内序号 (F3 / T1 / I7 / R2); ref = 附议 / 补充同类已有的一条
//   提交 request_feature / request_tool / note_insight / report_issue, 查 list_notes (agent/Acts); 界面 = Mine 页 "候选" 下各类一页 (只读)
// =============================================================================

#include "agent/Tools.hpp"
#include "nlohmann/json.hpp"

#include <array>
#include <cstdint>
#include <iterator>
#include <string>
#include <vector>

namespace agent {

enum class NoteKind : uint8_t { Feature,
                                ToolReq,
                                Insight,
                                Issue };
// 类表 (下标 = NoteKind): id = 文件名 = list_notes 的 kind
struct NoteKindDef {
  const char *id, *cn;
  char prefix;
  Tool tool; // 提交它的工具
};
inline constexpr NoteKindDef kNoteKinds[] = {{"feature", "特征申请", 'F', Tool::RequestFeature},
                                             {"tool", "工具申请", 'T', Tool::RequestTool},
                                             {"insight", "发现", 'I', Tool::NoteInsight},
                                             {"issue", "建议与 bug", 'R', Tool::ReportIssue}};
inline constexpr size_t kNoteKindCount = std::size(kNoteKinds);
static_assert(kNoteKindCount == static_cast<size_t>(NoteKind::Issue) + 1);
inline const NoteKindDef &def(NoteKind k) { return kNoteKinds[static_cast<size_t>(k)]; }
NoteKind NoteOf(Tool t); // 提交工具 → 类 (须是四个提交工具之一)

struct Note {
  NoteKind kind;
  std::string id, time, scope, session, ref;
  int round = 0;
  nlohmann::ordered_json fields;
  std::string title() const { return fields.at("title").get<std::string>(); }
  std::string body() const; // 除标题外的字段, 每个一行 "标签: 值"
};

class Notes {
public:
  void load(bool test); // 读回该目录各类文件 (没有 = 空)
  bool loaded(bool test) const { return test_ == static_cast<int>(test); }
  const std::vector<Note> &of(NoteKind k) const { return notes_[static_cast<size_t>(k)]; } // 旧到新
  const Note *find(NoteKind k, const std::string &id) const;
  int backers(const Note &n) const; // 附议它的条数
  // 记一条 (落盘 + 进表): c = 该类的提交工具的调用, ref 须为空或 find 得到; scope = 作用域名, session = 会话目录
  const Note &add(const Call &c, std::string scope, std::string session, int round);

private:
  int test_ = -1;
  std::array<std::vector<Note>, kNoteKindCount> notes_;
};

// list_notes: kind 空 = 各类条数 + 每类最近 n 条的标题; 否则该类命中的最近 n 条全文. scope 非空 = 只看该作用域提的; query 同 search_leaves 的写法
std::string NotesText(const Notes &notes, const std::string &kind, const std::string &scope, const std::string &query, int n);
// 首条用: 各类条数 (发现另给本作用域的条数)
std::string NotesBrief(const Notes &notes, const std::string &scope);

} // namespace agent
