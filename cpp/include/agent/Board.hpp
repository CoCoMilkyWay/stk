#pragma once

// =============================================================================
// Board — 假设板: 一段会话里 agent 提出的机制假设与它们的判定 (只 GUI 线程读写)
// =============================================================================
//   hypothesize 记一条 (编号 H1, H2 …, 待检验) → 观察检验 → judge 判 (支持 / 落空 / 不清, 证据 = 调用编号) → 支持的才能挂 eval_factor 变体
//   一个假设至多 kHypEvals 个落了文件的变体, 其中旋钮类 (换平滑 / 换压缩 / 换口径) 至多 kHypKnobs 个;
//   有变体回测过 = 待判 (pending): 下一条回复须 judge 它, 判之前不能再挂变体
//   调用编号 Ref = R<轮>#<序> (结果消息的标题里给出; 轮 = 那条回复所在的轮, 序从 1 起)
//   每条结果消息附整张板 (text), 不进折叠; 执行见 agent/Acts, 回复级约束见 Owed
// =============================================================================

#include "agent/Tools.hpp"
#include "nlohmann/json.hpp"

#include <compare>
#include <cstdint>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

namespace agent {

enum class Stance : uint8_t { Open,
                              Supported,
                              Refuted,
                              Unclear };
// 判定表 (下标 = Stance): id = judge 的 verdict 取值 (Open 不可判); cn = 板上 / 流水的名字
struct StanceDef {
  const char *id, *cn;
};
inline constexpr StanceDef kStances[] = {{"open", "待检验"}, {"supported", "支持"}, {"refuted", "落空"}, {"unclear", "不清"}};
static_assert(std::size(kStances) == static_cast<size_t>(Stance::Unclear) + 1);
inline const StanceDef &def(Stance s) { return kStances[static_cast<int>(s)]; }
Stance StanceOf(std::string_view id); // 须是 kStances 里的 id

struct Ref {
  int round = 0, call = 0; // round 0 = 无
  auto operator<=>(const Ref &) const = default;
};
bool RefOf(std::string_view s, Ref &r); // "R12#3" → Ref; 格式不对 → false
std::string RefText(Ref r);             // Ref → "R12#3"

struct Hyp {
  std::string id;
  nlohmann::ordered_json fields; // hypothesize 的参数
  Ref at;                        // 提出处
  Stance stance = Stance::Open;
  Ref judged;                     // 最近一次判定处 (round 0 = 没判过)
  nlohmann::ordered_json verdict; // 最近一次 judge 的参数
  int evals = 0, knobs = 0;       // 已落文件的变体 / 其中旋钮类
  int pending = 0;                // 回测过而还没判的变体落在哪轮 (0 = 无)
  std::string title() const { return fields.at("title").get<std::string>(); }
};

class Board {
public:
  void clear() { hyps_.clear(); }
  const Hyp &add(const Call &c, Ref at); // c = hypothesize 的调用
  Hyp *find(const std::string &id);
  const std::vector<Hyp> &all() const { return hyps_; }
  std::string text() const;   // 整张板 (结果消息附): 每个假设一行 + 待判的
  std::string report() const; // 结束报告的一节: 每个假设的预测与最终判定

private:
  std::vector<Hyp> hyps_;
};

} // namespace agent
