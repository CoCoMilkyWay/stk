#pragma once

// =============================================================================
// Notes — 研究记录双板: 会话内假设板 Board (H1, H2 …) ↔ 跨会话全局记录板 Notes (F / T / I / R)
// =============================================================================
//   Board  会话内假设与判定 (时间线折叠的纯内存状态):
//            hypothesize 记一条 (待检验) → 观察检验 → judge 判 (支持 / 落空 / 不清, 证据写调用编号 R<轮>#<序>) → 支持的挂 eval_factor 变体 (至多 kHypEvals 个)
//            有变体回测过即待判 (pending): 下一条回复须 judge 它
//   Notes  跨会话全局记录 (output/agent/<main|test>/notes/<类>.jsonl):
//            特征申请 (F) / 工具申请 (T) / 发现 (I) / 建议与 bug (R); 研究员逐条标处理 (待办 / 已做 / 不做 / 搁置 + 回复); 回退时 drop 删掉对应轮提的
// =============================================================================

#include "gui/task_agent/Enum.hpp"
#include "gui/task_agent/Tools.hpp"
#include "nlohmann/json.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace GUI::Agent {

// ---- 调用编号 R<轮>#<序> ----
struct Ref {
  int round = 0, call = 0; // round 0 = 无
  auto operator<=>(const Ref &) const = default;
};
bool RefOf(std::string_view s, Ref &r);
std::string RefText(Ref r);

// ---- 会话假设板 Board ----
enum class Stance : uint8_t { Open,
                              Supported,
                              Refuted,
                              Unclear };
struct StanceDef {
  const char *id, *cn;
};
inline constexpr StanceDef kStances[] = {{"open", "待检验"}, {"supported", "支持"}, {"refuted", "落空"}, {"unclear", "不清"}};
AGENT_ROWS(Stance, kStances, Unclear);

struct Hyp {
  std::string id;
  nlohmann::ordered_json fields; // hypothesize 参数
  Ref at;                        // 提出处
  Stance stance = Stance::Open;
  Ref judged;                     // 最近判定处 (round 0 = 未判)
  nlohmann::ordered_json verdict; // 最近 judge 参数
  int evals = 0;                  // 已落文件的变体数
  int pending = 0;                // 回测过尚未判的变体所在轮 (0 = 无)
  std::string title() const { return fields.at("title").get<std::string>(); }
};

class Board {
public:
  void add(const Call &c, Ref at);
  void judge(const Call &c, Ref at);
  void vary(const Call &c, Ref at);
  std::string next() const { return "H" + std::to_string(hyps_.size() + 1); }
  const Hyp *find(const std::string &id) const;
  const std::vector<Hyp> &all() const { return hyps_; }
  std::string text() const;   // 结果消息附的整张板
  std::string report() const; // 停下报告的一节

private:
  Hyp &hyp(const std::string &id);
  std::vector<Hyp> hyps_;
};

// ---- 跨会话全局记录板 Notes ----
enum class NoteKind : uint8_t { Feature,
                                ToolReq,
                                Insight,
                                Issue };
struct NoteKindDef {
  const char *id, *cn;
  char prefix;
  Tool tool;
};
inline constexpr NoteKindDef kNoteKinds[] = {{"feature", "特征申请", 'F', Tool::RequestFeature},
                                             {"tool", "工具申请", 'T', Tool::RequestTool},
                                             {"insight", "发现", 'I', Tool::NoteInsight},
                                             {"issue", "建议与 bug", 'R', Tool::ReportIssue}};
AGENT_ROWS(NoteKind, kNoteKinds, Issue);
NoteKind NoteOf(Tool t);

enum class NoteState : uint8_t { Open,
                                 Done,
                                 Rejected,
                                 Parked };
struct NoteStateDef {
  const char *id, *cn;
};
inline constexpr NoteStateDef kNoteStates[] = {{"open", "待办"}, {"done", "已做"}, {"rejected", "不做"}, {"parked", "搁置"}};
AGENT_ROWS(NoteState, kNoteStates, Parked);

struct Note {
  NoteKind kind;
  std::string id, time, scope, session, ref;
  int round = 0;
  NoteState state = NoteState::Open;
  std::string reply;
  nlohmann::ordered_json fields;
  std::string title() const { return fields.at("title").get<std::string>(); }
  std::string body() const;
};

class Notes {
public:
  void load(bool test);
  bool loaded(bool test) const { return test_ == static_cast<int>(test); }
  const std::vector<Note> &of(NoteKind k) const { return notes_[static_cast<size_t>(k)]; }
  const Note *find(NoteKind k, const std::string &id) const;
  int backers(const Note &n) const;
  int open(NoteKind k) const;
  const Note &add(const Call &c, std::string scope, std::string session, int round);
  void set(NoteKind k, const std::string &id, NoteState s, std::string reply);
  std::vector<const Note *> filed(const std::string &session, int from = 1) const;
  void drop(const std::string &session, int from);

private:
  void rewrite(NoteKind k) const;
  int test_ = -1;
  std::array<std::vector<Note>, kNum<NoteKind>> notes_;
};

std::string NotesText(const Notes &notes, const std::string &kind, const std::string &scope, const std::string &query, int n);
std::string NotesBrief(const Notes &notes, const std::string &scope);

} // namespace GUI::Agent
