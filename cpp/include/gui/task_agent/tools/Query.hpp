#pragma once

// =============================================================================
// 检索 (search_* / read_doc / list_*) 的结果: 每个工具一个 Res; 计算与原文在 tools/query/<工具>.cpp, 展示在 ui/query/<工具>.cpp
// =============================================================================
//   search_leaves / list_*                    当场 (Sync 形): 读候选台 Desk 的上下文与候选 / 全局记录
//   search_docs / search_factors / read_doc   纯后台 (Pure 形): 只看台面 Bay 的取消旗, 研报库见 Docs.hpp
// =============================================================================

#include "gui/task_agent/tools/Kit.hpp"

#include <string>
#include <utility>
#include <vector>

namespace GUI::Agent {

using Ranks = std::vector<std::pair<std::string, int>>; // 计数降序

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

struct DocsRes : Res {
  std::string cond, alone;
  bool full = false, loose = false;
  size_t hit = 0;
  struct Stage {
    std::string name;
    int n = 0;
    Ranks sub; // 流派
  };
  std::vector<Stage> tree;
  Ranks brokers, years, values;
  struct Doc {
    std::string id, value, primary, approaches, finding;
    size_t factors = 0;
    int hits = 0;
    std::vector<std::string> snippets;
  };
  std::vector<Doc> docs;
  std::string text() const override;
  std::string title() const override;
  void draw() const override;
};

struct FactorsRes : Res {
  std::string cond, alone;
  bool loose = false;
  size_t hit = 0, docs = 0;
  Ranks families, approaches;
  std::vector<std::string> rows, cols; // 数据周期 × 预测期
  std::vector<int> cells;              // [rows × cols]
  struct Row {
    std::string name, family, direction, period, horizon, stats, doc, formula;
  };
  std::vector<Row> list;
  std::string text() const override;
  std::string title() const override;
  void draw() const override;
};

struct ReadRes : Res {
  std::string id, part;
  // part = card
  size_t from = 0, total = 0; // 从第几个因子列起 (0 = 整卡) / 因子总数
  std::string meta;           // 体裁 · 资产 · 主阶段 · 价值 · 所在
  std::vector<std::string> stages, findings, pre_in, pre_out, cited;
  struct Factor {
    size_t no = 0;
    std::string head, formula, evidence;
  };
  std::vector<Factor> factors; // 第 from 个起的全部 (原文按字数上限列到哪算哪)
  // part = outline
  size_t chars = 0;
  struct Sec {
    size_t k = 0, chars = 0;
    std::string title;
  };
  std::vector<Sec> secs;
  // part = text
  size_t sec = 0, skip = 0, next_sec = 0, next_skip = 0;
  std::string body;
  bool end = false;
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
