#pragma once

// =============================================================================
// Notes — 跨会话全局记录板 (I / R / F / T)
// =============================================================================
//   Notes  跨会话全局记录 (output/agent/<main|test>/notes/<类>.jsonl):
//            发现 (I) / 反馈 (R) / 特征申请 (F) / 工具申请 (T); 研究员看过可逐条删; 回退时 drop 删掉对应步提的
// =============================================================================

#include "gui/task_agent/Enum.hpp"
#include "gui/task_agent/Tools.hpp"
#include "nlohmann/json.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace GUI::Agent {

enum class NoteKind : uint8_t { Insight,
                                Issue,
                                Feature,
                                ToolReq };
struct NoteKindDef {
  const char *id, *cn;
  char prefix;
  Tool tool;
};
inline constexpr NoteKindDef kNoteKinds[] = {{"insight", "发现", 'I', Tool::NoteInsight},
                                             {"issue", "反馈", 'R', Tool::ReportIssue},
                                             {"feature", "特征申请", 'F', Tool::RequestFeature},
                                             {"tool", "工具申请", 'T', Tool::RequestTool}};
AGENT_ROWS(NoteKind, kNoteKinds, ToolReq);
NoteKind NoteOf(Tool t);

struct Note {
  NoteKind kind;
  std::string id, time, scope, session, ref;
  int step = 0; // 会话里提它的那一步
  nlohmann::ordered_json fields;
  std::string title() const { return fields.at("title").get<std::string>(); }
  std::string body() const;
};

class Notes {
public:
  void load(bool test);
  bool loaded(bool test) const { return test_ == static_cast<int>(test); }
  const std::vector<Note> &of(NoteKind k) const { return notes_[k]; }
  const Note *find(NoteKind k, const std::string &id) const;
  int backers(const Note &n) const;
  std::string next(NoteKind k) const; // 下一条的编号
  const Note &add(const Call &c, std::string scope, std::string session, int step);
  void erase(NoteKind k, const std::string &id); // 附议它的改为独立一条
  std::vector<const Note *> filed(const std::string &session, int from = 1) const;
  void drop(const std::string &session, int from);

private:
  void rewrite(NoteKind k) const;
  int test_ = -1;
  Per<NoteKind, std::vector<Note>> notes_;
};

std::string NotesBrief(const Notes &notes, const std::string &scope);

} // namespace GUI::Agent
