#pragma once

// =============================================================================
// 记录 (request_feature / request_tool / note_insight / report_issue) 的结果: 四个工具共持唯一结构 NoteRes
// =============================================================================
//   四个工具同一套落记录 (FileNote, Notes.cpp): ref 须是同类已有的一条; dry (工作台) 只给编号不落盘
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <string>

namespace GUI::Agent {

struct NoteRes final : Res {
  NoteKind kind = NoteKind::Feature;
  std::string id, ref, miss; // miss 非空 = ref 不成, 没记
  nlohmann::ordered_json fields;
  bool dry = false;
  std::string text() const override;
  void draw() const override;
};

ToolOut FileNote(const Call &c, Seat &s);

ToolOut RequestFeature(const Call &c, Seat &s);
ToolOut RequestTool(const Call &c, Seat &s);
ToolOut NoteInsight(const Call &c, Seat &s);
ToolOut ReportIssue(const Call &c, Seat &s);

nlohmann::ordered_json SampleRequestFeature(Feed &p);
nlohmann::ordered_json SampleRequestTool(Feed &p);
nlohmann::ordered_json SampleNoteInsight(Feed &p);
nlohmann::ordered_json SampleReportIssue(Feed &p);

} // namespace GUI::Agent
