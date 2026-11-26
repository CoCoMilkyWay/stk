#pragma once

// =============================================================================
// 检索 (search_leaves / list_*) 的结果: 每个工具一个 Res; 计算与原文在 tools/query/<工具>.cpp, 展示在 ui/query/<工具>.cpp
// =============================================================================
//   全部当场 (Sync 形): 读候选台 Desk 的上下文与候选 / 全局记录
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <string>
#include <vector>

namespace GUI::Agent {

struct LeavesRes : Res {
  std::string query;
  size_t hit = 0, groups = 0;
  int n = 0;
  bool loose = false; // 没有同时含全部词的, 放宽为含任一词
  std::string alone;  // 各词单独命中数 (放宽 / 零命中时报)
  struct Row {
    std::string leaf, name_cn, formula, desc, value;
  };
  std::vector<Row> rows;
  std::string text() const override;
  std::string title() const override;
  void draw() const override;
};

struct TriedRes : Res {
  std::vector<Result> rows;
  int all = 0, skip = 0;
  Gate gate;
  std::string text() const override;
  std::string title() const override;
  void draw() const override;
};

struct LibraryRes : Res {
  std::vector<Result> rows;
  int all = 0, skip = 0, none = 0; // none = 本持有期无统计 (不列) 的个数
  std::string text() const override;
  std::string title() const override;
  void draw() const override;
};

struct NotesRes : Res {
  std::string kind;
  bool here = false, query = false;
  int n = 0;
  struct Item {
    std::string id, time, scope, title, relation, handled;
    std::vector<std::string> body;
  };
  struct Group {
    NoteKind kind = NoteKind::Feature;
    size_t hit = 0;
    std::vector<Item> items;
  };
  std::vector<Group> groups; // kind 空 = 四类各一组; 否则只那一类
  std::string text() const override;
  std::string title() const override;
  void draw() const override;
};

} // namespace GUI::Agent
