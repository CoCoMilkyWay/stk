// request_feature — 申请特征: 输入 title / define / mechanism / gap / ref → FeatureNoteRes (编号或 ref 不成的原因) → 原文; 展示见 ui/note/RequestFeature.cpp
#include "gui/task_agent/tools/Note.hpp"

namespace GUI::Agent {

using json = nlohmann::ordered_json;

ToolOut RequestFeature(const Call &c, Seat &s) { return FileNote(c, s, std::make_shared<FeatureNoteRes>()); }

json SampleRequestFeature(Feed &p) {
  return {{"title", "示例: 撤单前挂单存续时长的分布"},
          {"define", "lob 逐笔撤单: 每笔撤单距其挂出的交易秒, 按分钟取中位数; 日内可见"},
          {"mechanism", "短存续撤单多 = 试探性挂单 / 幌骗, 可能预示短期反转"},
          {"gap", "现有叶只有撤单额与笔数, 没有存续时长 (查过 " + p.leaf() + ")"},
          {"ref", ""}};
}

} // namespace GUI::Agent
