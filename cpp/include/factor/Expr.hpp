#pragma once

// =============================================================================
// 因子表达式 codec: 文本 ↔ 树 (parse / to_string) + 静态校验
// =============================================================================
//   语法 (前缀函数式; 算子名 = OpTable e_name, 特征 = L1 字段 code 裸写, 参数命名):
//     expr  := Op '(' [expr {',' expr}] {',' param} ')' | feature
//     param := ('d' | 'k' | 'k2') '=' number
//   例:  CsRank(TsMeanRoll(TsLog(amt_todz_5d), d=30))      TsTodMask(k=15, k2=255)
//   规矩:
//     序列参数在前 (个数 = 元数), 命名参数在后 (= OpTable 参数列声明的全部字段, 不多不少, 无隐含默认)
//     无标量常量元 (OpTable 无常量输入; 阈值一律走 k)
//     PascalCase 算子 / snake_case 特征, 大小写敏感, 词法上无歧义
//   静态校验 (parse 内一次做完):
//     算子在 OpTable / 元数对 / 参数声明齐且无多余 / 参数值域 (d ∈ [1, kMaxD]; k 按 OpTable k域 列) /
//     特征存在且可作输入 (LB 标签 / META 门控列不许: 前视) / 逐元值域: 父算子该元 in 为严格域 (INT, 即组 id) 时
//     子算子 out ⊆ in (OpTable in / out 列; 特征叶放行 —— 其整数性与 [0, kMaxGroup) 值域在 eval 前按数据查)
//   类型信息只读 OpTable 的 (k域, in, out) 列, 本头不点任何算子名 (根归一算子集除外: 那是口径定义, 不是类型)
//   节点序 = 前序 (根 = 0, 子树紧随, 左→右): 落盘 params 按 "算子节点前序" 逐节点覆盖字面值 (表达式不动, 参数可更新)
//   本头只依赖 Contract / OpTable, 不依赖 features/: 特征查询由调用方经 FeatureLookup 注入.
//   出错策略: 表达式来自人 / agent / 落盘文件, 不合法是**正常输入** → parse / apply_params 返回 false + err,
//   不 assert; 内部不变量照旧 assert.
// =============================================================================

#include "factor/Contract.hpp"
#include "factor/OpTable.hpp"
#include "factor/Stat/Contract.hpp" // Frame / kTsNormD (root_frame)

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace factor::expr {

// ---- 算子静态表 (OpTable 展开, 下标 = 全局 idx − 1, Stat 不在) ----
struct OpInfo {
  const char *name;
  const char *c_name;
  int arity;
  T t;
  A a;
  Kern kern;
  KDom kdom;          // k 的值域 (NONE = 无 k); 参数集合 = params_str(t, kdom)
  const char *params; // "d,k" 等 (由 (t, kdom) 推出, 与 OpTable 无独立列)
  Dom in[3];          // 逐元自变量值域 (前 arity 个有效)
  Dom out;            // 因变量值域
  const char *op;
  const char *note;
};
#define FACTOR_EXPR_OPINFO(Name, c_name, ar, t, a, kern, kdom, in, out, op, note) \
  OpInfo{#Name, c_name, ar, T::t, A::a, Kern::kern, KDom::kdom, params_str(T::t, KDom::kdom), in, Dom::out, op, note},
inline constexpr OpInfo kOps[] = {OP_ALL(FACTOR_EXPR_OPINFO)};
#undef FACTOR_EXPR_OPINFO
inline constexpr int kOpCount = static_cast<int>(sizeof(kOps) / sizeof(kOps[0]));

// 名字 → kOps 下标; 不在表 → -1
constexpr int op_index(std::string_view name) {
  for (int i = 0; i < kOpCount; ++i)
    if (std::string_view(kOps[i].name) == name)
      return i;
  return -1;
}

// 参数列是否声明了 key ("d" / "k" / "k2"; 逗号分隔, 无空格)
constexpr bool declares(const char *params, std::string_view key) {
  std::string_view s(params);
  while (!s.empty()) {
    const size_t c = s.find(',');
    const std::string_view tok = s.substr(0, c);
    if (tok == key)
      return true;
    if (c == std::string_view::npos)
      break;
    s.remove_prefix(c + 1);
  }
  return false;
}

// ---- 参数值域 ----
inline constexpr int kMaxD = 5 * kSegLen; // 窗长上限 (五个交易日; 再长 GPU ROLL 块 carry 不划算, 也不该在分钟级挖)

// 表的静态一致性: TOD 只许 0 元 POINT (t_D 是隐含输入); GROUP 域的组 id 元 (末元) 必须要求 INT
constexpr bool table_consistent() {
  for (int i = 0; i < kOpCount; ++i) {
    const OpInfo &o = kOps[i];
    if (o.kdom == KDom::TOD && !(o.arity == 0 && o.t == T::POINT))
      return false;
    if (o.a == A::GROUP && !(o.arity >= 2 && o.in[o.arity - 1] == Dom::INT))
      return false;
    if (o.a != A::GROUP)
      for (int k = 0; k < o.arity; ++k)
        if (o.in[k] == Dom::INT || o.in[k] == Dom::BCAST)
          return false;
  }
  return true;
}
static_assert(table_consistent(), "OpTable in / k域 列与 A 域 / 元数不一致");

inline bool is_int(float v) { return std::isfinite(v) && std::floor(v) == v; }

// 有效格的值是否落在值域内 (特征叶元按数据查 in 列; BCAST 不是逐值可判的集合, 特征叶不可能满足)
inline bool dom_holds(Dom d, float v) {
  switch (d) {
  case Dom::REAL:
    return true;
  case Dom::NONNEG:
    return v >= 0.f;
  case Dom::POS:
    return v > 0.f;
  case Dom::UNIT:
    return v >= 0.f && v <= 1.f;
  case Dom::SIGNED:
    return v >= -1.f && v <= 1.f;
  case Dom::BIN:
    return v == 0.f || v == 1.f;
  case Dom::SIGN3:
    return v == 0.f || v == 1.f || v == -1.f;
  case Dom::INT:
    return is_int(v) && v >= 0.f && v < static_cast<float>(kMaxGroup);
  case Dom::BCAST:
    return false;
  }
  return false;
}

// 参数值域校验: nullptr = 合法, 否则一句话原因 (只查该算子声明的字段; k 按 OpTable k域 列)
inline const char *check_params(const OpInfo &o, const Param &p) {
  if (has_d(o.t) && (p.d < 1 || p.d > kMaxD))
    return "d 须为 1..kMaxD (5 个交易日) 的整数";
  if (has_k(o.kdom)) {
    const float k = p.k;
    if (!std::isfinite(k))
      return "k 须有限";
    switch (o.kdom) {
    case KDom::NONE:
    case KDom::ANY:
      break;
    case KDom::GE0:
      if (!(k >= 0.f))
        return "k 须 ≥ 0";
      break;
    case KDom::OPEN01:
      if (!(k > 0.f && k < 1.f))
        return "k 须在 (0, 1)";
      break;
    case KDom::OPEN0_CLOSED1:
      if (!(k > 0.f && k <= 1.f))
        return "k 须在 (0, 1]";
      break;
    case KDom::OPEN0_HALF:
      if (!(k > 0.f && k < 0.5f))
        return "k 须在 (0, 1/2)";
      break;
    case KDom::POSINT_GROUP:
      if (!(is_int(k) && k >= 1.f && k <= static_cast<float>(kMaxGroup)))
        return "k 须为 1..kMaxGroup 的整数 (桶数)";
      break;
    case KDom::TOD:
      if (!(is_int(k) && is_int(p.k2) && k >= 0.f && k < p.k2 && p.k2 <= static_cast<float>(kSegLen)))
        return "须 0 ≤ k < k2 ≤ kSegLen, 皆整数";
      break;
    }
  }
  return nullptr;
}

// 签名 LaTeX (GUI Operators 表 operand 列 / operators.json operand 键), 从表列生成:
//   序列元 x, y, z 按 in 值域, 相邻同域合并 ("x, y \in ℝ"); 再参数 d{=}⟨d⟩ ∈ ℤ₊ / k{=}⟨k⟩ ∈ k域 (TOD 整段自带 k, k2);
//   元与参数间 ";\; ", 占位符 ⟨d⟩ ⟨k⟩ ⟨k2⟩ 由 GUI 换成本轮实际值
inline std::string operand_tex(const OpInfo &o) {
  static constexpr const char *kVar[3] = {"x", "y", "z"};
  std::string s;
  for (int i = 0; i < o.arity;) {
    int j = i + 1;
    while (j < o.arity && o.in[j] == o.in[i])
      ++j;
    if (!s.empty())
      s += ",\\; ";
    for (int k = i; k < j; ++k)
      s += (k > i ? ", " : "") + std::string(kVar[k]);
    s += " \\in ";
    s += dom_tex(o.in[i]);
    i = j;
  }
  bool in_params = false; // 元 → 参数用 ";\;", 参数之间用 ",\;"
  const auto sep = [&] {
    if (!s.empty())
      s += in_params ? ",\\; " : ";\\; ";
    in_params = true;
  };
  if (has_d(o.t)) {
    sep();
    s += R"tex(d{=}⟨d⟩ \in \mathbb{Z}_{+})tex";
  }
  if (o.kdom == KDom::TOD) {
    sep();
    s += kdom_tex(o.kdom);
  } else if (has_k(o.kdom)) {
    sep();
    s += R"tex(k{=}⟨k⟩ \in )tex";
    s += kdom_tex(o.kdom);
  }
  return s;
}

// ---- 树 ----
struct Node {
  int op = -1;                // kOps 下标; -1 = 特征叶
  std::string feat;           // 叶: 特征 code
  int args[3] = {-1, -1, -1}; // 子节点下标 (前 arity 个有效)
  Param p;                    // 本节点参数 (只有 OpTable 声明的字段有意义)
};

struct Expr {
  std::vector<Node> nodes; // 前序, 根 = 0
  int n_ops = 0;           // 算子节点数 (= 落盘 params 数组长度)
  std::string canon;       // 规范串 (含当前参数; parse / apply_params 末尾刷新)

  const Node &root() const {
    assert(!nodes.empty());
    return nodes[0];
  }
  // 第 j 个算子节点 (前序) 的下标
  int op_node(int j) const {
    for (size_t i = 0; i < nodes.size(); ++i)
      if (nodes[i].op >= 0 && j-- == 0)
        return static_cast<int>(i);
    assert(false && "算子节点序号越界");
    return -1;
  }
};

// 静态类型检查的唯一规则: 父算子该元的 in 是严格域 (dom_strict) 时, 子节点 (算子) 的 out 须 ⊆ in;
// 特征叶放行 (值域只能在 eval 前按数据查 dom_holds); 非严格域是语义声明, 越界格由算子自身置无效, 不在此否决
inline bool arg_ok(const Node &child, const OpInfo &parent, int slot) {
  return child.op < 0 || !dom_strict(parent.in[slot]) || dom_sub(kOps[child.op].out, parent.in[slot]);
}
// ---- 序列化 ----
namespace detail {
// 项之间 ", " 分隔: 序列参数 → d → k → k2 (与 OpTable operand 列同序)
inline void sep(std::string &s, bool &first) {
  if (!first)
    s += ", ";
  first = false;
}
inline void to_string_rec(const Expr &e, int i, std::string &s) {
  const Node &n = e.nodes[static_cast<size_t>(i)];
  if (n.op < 0) {
    s += n.feat;
    return;
  }
  const OpInfo &o = kOps[n.op];
  s += o.name;
  s += '(';
  bool first = true;
  for (int a = 0; a < o.arity; ++a) {
    sep(s, first);
    to_string_rec(e, n.args[a], s);
  }
  char buf[48];
  if (declares(o.params, "d")) {
    sep(s, first);
    std::snprintf(buf, sizeof(buf), "d=%d", n.p.d);
    s += buf;
  }
  if (declares(o.params, "k")) {
    sep(s, first);
    std::snprintf(buf, sizeof(buf), "k=%g", static_cast<double>(n.p.k));
    s += buf;
  }
  if (declares(o.params, "k2")) {
    sep(s, first);
    std::snprintf(buf, sizeof(buf), "k2=%g", static_cast<double>(n.p.k2));
    s += buf;
  }
  s += ')';
}
} // namespace detail

// 子树 i 的规范串 (根 i = 0 即整棵)
inline std::string to_string(const Expr &e, int i = 0) {
  std::string s;
  detail::to_string_rec(e, i, s);
  return s;
}

// ---- 特征查询 (调用方注入; factor/ 不认识字段表) ----
enum class FeatState : uint8_t { MISSING,   // 字段表里没有
                                 FORBIDDEN, // 有, 但不许作因子输入 (标签 / _meta)
                                 OK };
using FeatureLookup = std::function<FeatState(std::string_view code)>;

// ---- 解析 ----
namespace detail {

inline bool is_ident0(char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'; }
inline bool is_ident(char c) { return is_ident0(c) || (c >= '0' && c <= '9'); }
inline bool is_ws(char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\r'; }

class Parser {
public:
  Parser(std::string_view src, Expr &out, std::string &err, const FeatureLookup &fl) : s_(src), e_(out), err_(err), fl_(fl) {}

  bool run() {
    e_.nodes.clear();
    e_.n_ops = 0;
    e_.canon.clear();
    int root = -1;
    if (!parse_expr(root))
      return false;
    assert(root == 0);
    skip_ws();
    if (i_ != s_.size())
      return fail(i_, "表达式末尾有多余字符");
    e_.canon = to_string(e_);
    return true;
  }

private:
  std::string_view s_;
  size_t i_ = 0;
  Expr &e_;
  std::string &err_;
  const FeatureLookup &fl_;

  bool fail(size_t pos, const std::string &msg) {
    err_ = "位置 " + std::to_string(pos) + ": " + msg;
    return false;
  }
  void skip_ws() {
    while (i_ < s_.size() && is_ws(s_[i_]))
      ++i_;
  }
  bool peek(char c) {
    skip_ws();
    return i_ < s_.size() && s_[i_] == c;
  }
  bool eat(char c) {
    if (!peek(c))
      return false;
    ++i_;
    return true;
  }
  bool ident(std::string_view &out) {
    skip_ws();
    const size_t b = i_;
    if (b >= s_.size() || !is_ident0(s_[b]))
      return false;
    while (i_ < s_.size() && is_ident(s_[i_]))
      ++i_;
    out = s_.substr(b, i_ - b);
    return true;
  }
  bool number(double &out) {
    skip_ws();
    const size_t b = i_;
    size_t j = i_;
    if (j < s_.size() && (s_[j] == '+' || s_[j] == '-'))
      ++j;
    size_t digits = 0;
    while (j < s_.size() && s_[j] >= '0' && s_[j] <= '9')
      ++j, ++digits;
    if (j < s_.size() && s_[j] == '.') {
      ++j;
      while (j < s_.size() && s_[j] >= '0' && s_[j] <= '9')
        ++j, ++digits;
    }
    if (digits == 0)
      return false;
    if (j < s_.size() && (s_[j] == 'e' || s_[j] == 'E')) {
      size_t k = j + 1;
      if (k < s_.size() && (s_[k] == '+' || s_[k] == '-'))
        ++k;
      size_t ed = 0;
      while (k < s_.size() && s_[k] >= '0' && s_[k] <= '9')
        ++k, ++ed;
      if (ed)
        j = k;
    }
    const std::string tok(s_.substr(b, j - b));
    out = std::strtod(tok.c_str(), nullptr);
    i_ = j;
    return true;
  }
  // 前视: 下一个项是 "ident ws '='" (命名参数) 吗
  bool lookahead_param() {
    const size_t save = i_;
    std::string_view id;
    const bool r = ident(id) && peek('=');
    i_ = save;
    return r;
  }

  // 解析一个表达式项, out = 新节点下标
  bool parse_expr(int &out) {
    const size_t pos0 = (skip_ws(), i_);
    std::string_view id;
    if (!ident(id))
      return fail(pos0, i_ < s_.size() ? std::string("期望算子名或特征 code, 遇到 '") + s_[i_] + "'" : "表达式不完整");
    if (!peek('(')) {
      // 特征叶
      if (op_index(id) >= 0)
        return fail(pos0, "算子 " + std::string(id) + " 后须跟 '('");
      switch (fl_(id)) {
      case FeatState::MISSING:
        return fail(pos0, "字段表里没有特征 " + std::string(id));
      case FeatState::FORBIDDEN:
        return fail(pos0, "特征 " + std::string(id) + " 不许作因子输入 (标签 / 元数据列)");
      case FeatState::OK:
        break;
      }
      out = static_cast<int>(e_.nodes.size());
      Node n;
      n.feat = std::string(id);
      e_.nodes.push_back(std::move(n));
      return true;
    }
    ++i_; // '('
    const int op = op_index(id);
    if (op < 0)
      return fail(pos0, "未知算子 " + std::string(id));
    const OpInfo &o = kOps[op];
    out = static_cast<int>(e_.nodes.size());
    {
      Node n;
      n.op = op;
      e_.nodes.push_back(std::move(n));
    }
    e_.n_ops++;
    int n_args = 0;
    bool got_d = false, got_k = false, got_k2 = false, seen_param = false;
    Param p;
    if (!eat(')')) {
      while (true) {
        skip_ws();
        const size_t pos = i_;
        if (lookahead_param()) {
          seen_param = true;
          std::string_view key;
          ident(key);
          eat('=');
          double v = 0;
          if (!number(v))
            return fail(i_, "参数 " + std::string(key) + " 须为数字");
          if (!declares(o.params, key))
            return fail(pos, std::string(o.name) + " 不声明参数 " + std::string(key) + (o.params[0] ? " (声明: " + std::string(o.params) + ")" : " (无参数)"));
          bool &got = key == "d" ? got_d : (key == "k" ? got_k : got_k2);
          if (got)
            return fail(pos, "参数 " + std::string(key) + " 重复");
          got = true;
          if (key == "d") {
            if (std::floor(v) != v || v < -2e9 || v > 2e9)
              return fail(pos, "d 须为整数");
            p.d = static_cast<int>(v);
          } else if (key == "k") {
            p.k = static_cast<float>(v);
          } else {
            p.k2 = static_cast<float>(v);
          }
        } else {
          if (seen_param)
            return fail(pos, "序列参数须在命名参数之前");
          if (n_args >= 3 || n_args >= o.arity)
            return fail(pos, std::string(o.name) + " 是 " + std::to_string(o.arity) + " 元算子, 序列参数过多");
          int child = -1;
          if (!parse_expr(child))
            return false;
          e_.nodes[static_cast<size_t>(out)].args[n_args++] = child;
        }
        if (eat(')'))
          break;
        if (!eat(','))
          return fail(i_, "期望 ',' 或 ')'");
      }
    }
    if (n_args != o.arity)
      return fail(pos0, std::string(o.name) + " 是 " + std::to_string(o.arity) + " 元算子, 给了 " + std::to_string(n_args) + " 个序列参数");
    if (declares(o.params, "d") && !got_d)
      return fail(pos0, std::string(o.name) + " 缺参数 d");
    if (declares(o.params, "k") && !got_k)
      return fail(pos0, std::string(o.name) + " 缺参数 k");
    if (declares(o.params, "k2") && !got_k2)
      return fail(pos0, std::string(o.name) + " 缺参数 k2");
    if (const char *why = check_params(o, p))
      return fail(pos0, std::string(o.name) + ": " + why);
    Node &n = e_.nodes[static_cast<size_t>(out)];
    n.p = p;
    for (int s = 0; s < o.arity; ++s) { // 值域: 子.out ⊆ 本元.in (OpTable in / out 列, 逐元同一规则)
      const Node &c = e_.nodes[static_cast<size_t>(n.args[s])];
      if (!arg_ok(c, o, s))
        return fail(pos0, std::string(o.name) + " 第 " + std::to_string(s + 1) + " 元要求 " + dom_name(o.in[s]) + ", 而 " + kOps[c.op].name +
                              " 输出 " + dom_name(kOps[c.op].out));
    }
    return true;
  }
};

} // namespace detail

// 解析 + 校验. 失败 → false, err = "位置 N: 原因" (out 内容不可用)
inline bool parse(std::string_view src, const FeatureLookup &fl, Expr &out, std::string &err) {
  err.clear();
  detail::Parser p(src, out, err, fl);
  const bool ok = p.run();
  if (!ok)
    out.nodes.clear(), out.n_ops = 0, out.canon.clear();
  return ok;
}

// ---- 参数覆盖 (落盘 params → 树): 第 j 项对应第 j 个算子节点 (前序); 只给出的键生效, 未声明的键 / 值域不合 → false ----
struct ParamPatch {
  bool has_d = false, has_k = false, has_k2 = false;
  int d = 0;
  float k = 0.f, k2 = 0.f;
};
inline bool apply_params(Expr &e, const std::vector<ParamPatch> &patches, std::string &err) {
  err.clear();
  if (static_cast<int>(patches.size()) != e.n_ops) {
    err = "params 有 " + std::to_string(patches.size()) + " 项, 表达式有 " + std::to_string(e.n_ops) + " 个算子节点";
    return false;
  }
  std::vector<Param> saved;
  saved.reserve(e.nodes.size());
  for (const Node &n : e.nodes)
    saved.push_back(n.p);
  int j = 0;
  for (Node &n : e.nodes) {
    if (n.op < 0)
      continue;
    const OpInfo &o = kOps[n.op];
    const ParamPatch &q = patches[static_cast<size_t>(j)];
    const char *bad = nullptr;
    if (q.has_d && !declares(o.params, "d"))
      bad = "d";
    if (q.has_k && !declares(o.params, "k"))
      bad = "k";
    if (q.has_k2 && !declares(o.params, "k2"))
      bad = "k2";
    if (bad) {
      err = "params[" + std::to_string(j) + "] (" + o.name + ") 不声明参数 " + bad;
    } else {
      if (q.has_d)
        n.p.d = q.d;
      if (q.has_k)
        n.p.k = q.k;
      if (q.has_k2)
        n.p.k2 = q.k2;
      if (const char *why = check_params(o, n.p))
        err = "params[" + std::to_string(j) + "] (" + o.name + "): " + why;
    }
    if (!err.empty()) {
      for (size_t i = 0; i < e.nodes.size(); ++i)
        e.nodes[i].p = saved[i];
      return false;
    }
    ++j;
  }
  e.canon = to_string(e);
  return true;
}

// ---- alpha 因子的口径 (Stat/Contract.hpp【口径 Frame】): 由根算子严格判定, 且根必须是归一算子 ----
//   CS: 根 ∈ {CsRank, CsNormRank, CsZ} (截面居中 → 因子值 = 仓位, β 隔离)
//   TS: 根 = TsRankRoll(d = kTsNormD) (自身 kTsNormDays 日滚动分位; 整数天窗跨日 deseason)
//   其他根 (含 TS 包 CS: 截面不再居中, β 漏) → false + err.
//   再拦根输入的两种静态可判退化 (Stat 里必然全并列 / 20 组填不满, 评出来永远是空), 只读 OpTable out 列 + 树结构:
//     离散值域 (dom_discrete: 0/1 掩码 / 符号 / 桶号, 分位只有几档) / 沿资产轴不变异 (varies_a = false: 每 t 截面全相同)
//   因子文件 / 构建器都走这一个判据; apply_params 后再判 (d 可能被覆盖)

// 节点值是否沿资产轴 a 变异 (结构推导): 特征叶变异; 算子节点 = 非截面广播 (out ≠ BCAST) 且任一输入变异
//   (无特征叶的纯参数树 → 恒 false; TsTodMask 只看 t_D, 每 t 全资产同值)
inline bool varies_a(const Expr &e, int i) {
  const Node &n = e.nodes[static_cast<size_t>(i)];
  if (n.op < 0)
    return true;
  if (kOps[n.op].out == Dom::BCAST)
    return false;
  for (int k = 0; k < kOps[n.op].arity; ++k)
    if (varies_a(e, n.args[k]))
      return true;
  return false;
}

static_assert(stat::kTsNormD == kMaxD, "TS 口径归一窗 (kTsNormDays 日) 须等于 d 上限 kMaxD");
inline bool root_frame(const Expr &e, stat::Frame &out, std::string &err) {
  const Node &r = e.root();
  const std::string_view nm = r.op >= 0 ? std::string_view(kOps[r.op].name) : std::string_view();
  stat::Frame f;
  if (nm == "CsRank" || nm == "CsNormRank" || nm == "CsZ") {
    f = stat::Frame::CS;
  } else if (nm == "TsRankRoll") {
    if (r.p.d != stat::kTsNormD) {
      err = "TS 口径根 TsRankRoll 的 d 须为 " + std::to_string(stat::kTsNormD) + " (" + std::to_string(stat::kTsNormDays) + " 个交易日), 现为 " +
            std::to_string(r.p.d);
      return false;
    }
    f = stat::Frame::TS;
  } else {
    err = "根须是归一算子: CsRank / CsNormRank / CsZ (截面口径) 或 TsRankRoll(d=" + std::to_string(stat::kTsNormD) + ") (时序口径)";
    return false;
  }
  assert(kOps[r.op].arity == 1); // 归一算子都是一元
  const int in = r.args[0];
  const Node &n = e.nodes[static_cast<size_t>(in)];
  if (n.op >= 0 && dom_discrete(kOps[n.op].out)) {
    err = std::string("根的输入 ") + kOps[n.op].name + " 是离散值域 (0/1 掩码 / 符号 / 桶号), 分位只有几档 (退化); 先套窗口统计 (如 TsMeanRoll) 再归一";
    return false;
  }
  if (!varies_a(e, in)) {
    err = std::string("根的输入 ") + (n.op >= 0 ? kOps[n.op].name : n.feat.c_str()) + " 沿资产轴不变异 (无特征叶或被截面广播量抹平), 每 t 截面全相同 (退化)";
    return false;
  }
  out = f;
  return true;
}

} // namespace factor::expr
