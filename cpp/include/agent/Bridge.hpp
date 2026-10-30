#pragma once

// =============================================================================
// Bridge — agent 与项目的唯一接口面
// =============================================================================
//   agent/ 其余代码不碰项目的因子 / 特征 / 服务头, 只经这里 (实现 Bridge.cpp 是唯一 include 它们的 TU). 两半:
//     catalog  作用域目录 (无状态): 可挖的 (池, 因子层, 持有期; 持有期限 Search 档) / 就绪判据 / 会话目录
//     Lab      作用域工作区 (bind 一个作用域): 上下文 ctx (该层可作输入的特征叶 / 算子表 / K 线叶 / 正式因子库在该持有期的统计) + 候选目录 results.
//              三件事:
//              eval  候选的一生: Cand ─stage→ 候选目录里的因子文件 (与正式因子同格式) ─eval→ Result ─adopt (过门槛)→ 正式因子目录;
//                    一次评一个文件: Search (定方向 + 搜自由参数; 具体式只评本身) → Run, 结果写回文件; 评完只重扫它 (results 增量维护);
//                    无显著而有代表点 → 再观察代表点一次 (给 agent 看它的分层差在哪)
//              look  观察: 一组因子 (候选英文名 / 具体根式) + 一组特征式 (+ 本层 K 线叶) → 评估期上整期一块的原料 Look (r16 秩 / 行统计 /
//                    两端持仓 / 特征原值 / K 线 / 门控 / 标签), 不写文件; 汇总成文在 agent/Views
//              book  盘口重放: 一只股票一天里的一段逐笔重放进订单簿 → 逐秒盘口与流量 + 大单事件 + 某一秒的全簿 (Book); 不经特征库, 不限评估期
//              可见: 只有统计与 Search 结局都在作用域此刻评估期上的因子才给 agent 看 (Result::visible), 不同窗口的数不可比
//              四道 (Search / Run / Look / Book) 同构, 首次用到才建; 作用域与评估后端不变的连续会话共用其常驻块 (keep / Life::Worker), 变了即弃
//   worker 协议 (与 agent/Chat.hpp 对仗): 交一件事 (eval / look / book ↔ ask) → poll 逐个取走 Tick, 末个 = End → live 看在跑那步 → busy / cancel
//   全部 GUI 线程调用; 正式因子目录 / 字段表 / 资产轴由项目定, agent 只给候选目录
// =============================================================================

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

struct SharedData;

namespace agent {

struct Scope {
  std::string pool;
  int level = -1; // 因子层下标
  int hold = 0;   // 持有期键
  bool operator==(const Scope &) const = default;
};

struct Choice {
  int key = 0;
  std::string name;
};

// ---- 上下文 ----
struct Leaf {
  std::string code, name_cn, formula, desc; // code = 档.层.列 (模板里的写法); formula / desc 说的是原量 x
  std::string tf, tf_def;                   // 落盘值 = 原量经此元素变换: 简称 / 定义式 (恒等 = 空)
  std::string family;                       // 特征族 = 定义它的节点文件 (features/Node/ 下, 去 .hpp; 如 Lob/TS/OrderFlow/OrderLife); 族卡见 agent/Prompt
};
struct Op {
  std::string name, name_cn, sig, out, tex, note; // sig = 元与参数的值域 (LaTeX); out = 输出值域名
};
// 定向后的一组统计 (Weight / Top 共有)
struct Stat {
  double mono = 0, ls = 0, ric = 0; // 分层单调性 / 每期毛多空 / rIC 均值
};
// 一个权重在作用域持有期下的统计 (has = Run 过且该持有期 n ≥ 3)
struct Weight {
  std::string expr;
  int sign = 1;     // 方向 (−1 = 取反后才是这组统计)
  int hold_src = 0; // 搜出它的持有期 (0 = 手填具体式)
  bool has = false;
  Stat st;
  double score = 0, se = 0, valid = 0; // Search 目标 (不过门槛 = −∞) / 目标的标准误 / 池内有效率 (0..1)
  std::vector<double> grp;             // [kGroups] 组每期毛收益减组间均值 (已定向; has 才有)
};
// Search 无显著时的代表点 (因子文件 search[]) —— 告诉 agent 差在哪. 怎么取的:
//   Peak   过门槛的峰里曲面估计最高的 (差在显著)
//   Gated  没有峰: 过门槛的点里曲面估计最高的 (估计更高处不过门槛, 或估计都 ≤ 0)
//   Mono   没有点过门槛: mono 最高的点
enum class Rep : uint8_t { Peak,
                           Gated,
                           Mono };
struct Top {
  std::string expr;
  int sign = 1;                 // 方向 (−1 = 取反后才是这组统计)
  Stat st;                      // 定向后
  double g = 0, se = 0, mu = 0; // 定向目标 ls·rIC·mono² / 其标准误 / 曲面在此的估计 (Mono 无)
  Rep rep = Rep::Mono;
  std::string miss; // 落选原因
};
// 一个候选的结局 (AGENT.md 结局表): 反馈 / 流水 / 界面同一份分类
enum class Verdict : uint8_t { Unstaged,  // 名字或模板不合法 / 重名, 没落文件
                               Broken,    // 落了文件, 回测算不出
                               Weights,   // 有带本持有期统计的权重
                               Null,      // Search 无显著 (或权重都无本持有期统计)
                               Pending }; // 未评估
// 结局表 (下标 = Verdict): cn = 反馈 / 流水 / 界面的名字
struct VerdictDef {
  const char *cn;
};
inline constexpr VerdictDef kVerdicts[] = {{"未落文件"}, {"BROKEN"}, {"有参数组"}, {"无显著"}, {"未评估"}};
static_assert(std::size(kVerdicts) == static_cast<size_t>(Verdict::Pending) + 1);
inline const VerdictDef &def(Verdict v) { return kVerdicts[static_cast<int>(v)]; }
// Search 评过的一个点: 各自由参数的取值 + 定向统计
struct Probe {
  std::vector<double> v; // 与 Surface::slots 对齐
  Stat st;
};
// Search 的参数曲面 (只有刚评完的 Settle 带; 扫描文件得不到)
struct Surface {
  std::vector<std::string> slots; // 自由参数 "<算子>.<键>", 模板前序; 空 = 具体式
  std::vector<Probe> pts;         // 算得出统计的点
};
// 一个因子文件
struct Result {
  std::string file, name, name_cn, tmpl, note;
  std::string error;                  // 没落文件 (Unstaged) / 回测算不出 (Broken) 的原因
  Verdict verdict = Verdict::Pending; // 扫描时定 (Unstaged 由 stage 定)
  std::optional<Top> top;             // 本持有期 Search 无显著时的代表点
  std::vector<Weight> ws;             // 按 score 降序
  Surface surf;                       // 本次 Search 的曲面 (Settle 才有)
  int ops = 0;                        // 模板算子节点数
  bool visible = true;                // 统计 / Search 结局全在作用域此刻的评估期上 (同池同评估期) 且 ops ≤ kMaxOps; 否则 agent 看不见
  const Weight *best() const { return ws.empty() || !ws.front().has ? nullptr : &ws.front(); }
};
// 个股行情要用的叶 (下标 = Bar): 本层 K 线 + 日层昨收 / 涨跌停价
enum class Bar : uint8_t { Close,
                           High,
                           Low,
                           Volume,
                           Amount,
                           PreClose,
                           LimUp,
                           LimDn };
inline constexpr Bar kBars[] = {Bar::Close, Bar::High, Bar::Low, Bar::Volume, Bar::Amount, Bar::PreClose, Bar::LimUp, Bar::LimDn};
inline constexpr size_t kBarCount = std::size(kBars);
static_assert([] {
  for (size_t i = 0; i < kBarCount; ++i)
    if (static_cast<size_t>(kBars[i]) != i)
      return false;
  return true;
}());
template <class X>
struct PerBar {
  std::array<X, kBarCount> v{};
  constexpr X &operator[](Bar b) { return v[static_cast<size_t>(b)]; }
  constexpr const X &operator[](Bar b) const { return v[static_cast<size_t>(b)]; }
};
struct Context {
  std::string pool, pool_cn, level, hold, hold_desc;
  std::string window;             // 评估期 (标签库与因子池库落盘日交集; 用到别的库的叶再交)
  int first = 0, last = 0;        // 评估期首末日 YYYYMMDD (未就绪 0)
  int seg = 0;                    // 本层每日行数 (d 的单位)
  int open = 0;                   // 每日开头属集合竞价段 [09:15, 09:30) 的行数 (日层 0)
  std::vector<std::string> clock; // [seg] 行的收盘时刻 HH:MM
  int ts_root_d = 0;              // 时序口径根 TsRankRoll 须写死的 d
  int max_d = 0;
  double min_mono = 0; // Search 的 mono 硬门槛
  std::vector<Leaf> leaves;
  std::vector<Op> ops;
  PerBar<std::string> kline;   // 个股行情要用的叶 (全名; 字段表里没有的为空)
  std::vector<Result> library; // 正式因子库, 按 score 降序 (含不可见的, 由用方滤)
};

// ---- 观察 (Lab::look): 评估期上整期一块的原料; 平面 = [T = 天 · seg][A] 行主序, 格 k = Look::at(t, a) ----
template <class X>
using Plane = std::vector<X>;
inline constexpr int kGroups = 20;            // 截面分组数 (= Stat 契约, Bridge.cpp 核对)
inline constexpr uint16_t kRankNone = 0xFFFF; // r16 无效 (同上)
inline constexpr int kRankMax = 65534;        // r16 = round(分位 · kRankMax) (同上)
inline constexpr int kTails = 3;              // 两端组内的尾部: 尾 j 占该组 kTailOfGroup[j] (同 Inspect 尾部分层, 同上)
inline constexpr float kTailOfGroup[kTails] = {0.5f, 0.2f, 0.05f};
// 一个要看的因子: 具体根式 (已校验) + 口径 + 方向 (−1 = 取反后高 = 做多端); label = 给 agent 看的名字 (候选英文名, 或根式本身)
struct LookFactor {
  std::string expr, label;
  bool ts = false;
  int sign = 1;
};
// 一个要看的特征式: root = Lab::feature_of 出的 CsRank 包装规范串; error 非空 = 该式不成, 不算 (原因进 LookFeat::error)
struct LookExpr {
  std::string root, error;
};
struct LookSpec {
  std::vector<LookFactor> factors;
  std::vector<LookExpr> feats;
  bool kline = false; // 另取本层 K 线叶 (Context::kline 里有的)
  std::string code;   // 非空 = 只取该资产一列 (个股视图, 不在池的行也落)
};
// 一级行 (已定向): 组的超额持仓收益 / 真持有只数; ok = 本行有统计
// 尾部 [端][j]: 端 0 = 做多端 G20 内 / 1 = G1 内更极端的一截 (按池内秩), 口径同 grp (满仓按尾部宽度 kTailOfGroup[j] 折算); 只在 ok 行有值
struct LookRow {
  bool ok = false;
  float ric = 0, mkt = 0; // rIC / 本行基准 (超额 = 毛 − mkt)
  float grp[kGroups] = {};
  int cnt[kGroups] = {};
  float tail[2][kTails] = {};
  int tcnt[2][kTails] = {};
};
// 两端组真持有的一格: 行 t 开仓那批里资产 a 在做多端 (top) / 做空端组; ret = 持有毛收益 (超额 = ret − rows[t].mkt)
struct LookCell {
  int t = 0, a = 0;
  bool top = true;
  float ret = 0;
};
struct LookRoot {
  std::string label;           // = LookFactor::label
  std::string error;           // 非空 = 没算成
  Plane<uint16_t> rx;          // r16 池内秩 (已定向: 高 = 做多端; kRankNone 无效)
  std::vector<LookRow> rows;   // [T]
  std::vector<LookCell> cells; // 行序
  // 格 k 的池内分位 ∈ [0, 1] (无效 NaN) / 组号 0..kGroups−1 (= Stat 契约 grp_of; 无效 −1)
  float rank(size_t k) const { return rx[k] == kRankNone ? NAN : static_cast<float>(rx[k]) / kRankMax; }
  int grp(size_t k) const { return rx[k] == kRankNone ? -1 : std::min(rx[k] * kGroups / (kRankMax + 1), kGroups - 1); }
};
struct LookFeat {
  std::string error; // 非空 = 没算成
  Plane<float> v;    // 特征式的值 (无效 NaN; 没取 = 空)
};
struct Look {
  std::string error;              // 非空 = 没算成 (评估域不成 / 资产不在池轴 / 全部根坏), 其余字段空
  std::vector<int> dates;         // [天] YYYYMMDD
  int seg = 0;                    // 每日行数
  int open = 0;                   // = Context::open
  std::vector<std::string> clock; // [seg] 行的收盘时刻 HH:MM
  std::vector<std::string> codes; // [A]
  Plane<uint8_t> gate;            // 当行在池
  Plane<float> label;             // 会话持有期的毛收益 (无效 NaN)
  std::vector<float> mkt;         // [T] 在池且标签有效格的均值 (整截面; 无 → NaN)
  std::vector<LookRoot> factors;  // 与 LookSpec::factors 对齐
  std::vector<LookFeat> feats;    // 与 LookSpec::feats 对齐
  PerBar<LookFeat> kline;         // LookSpec::kline 时取的 K 线叶, 已逆落盘变换 = 原量 (Context::kline 里没有的 v 空)
  int A() const { return static_cast<int>(codes.size()); }
  int T() const { return static_cast<int>(dates.size()) * seg; }
  size_t at(int t, int a) const { return static_cast<size_t>(t) * static_cast<size_t>(A()) + static_cast<size_t>(a); }
};
// 分层单调性: 组号 0..n−1 与 g 的 Spearman (Stat 契约 mono 同一份)
double Mono(const double *g, int n);

// ---- 盘口重放 (Lab::book): 一只股票一天的逐笔重放进订单簿, 不经特征库; 时刻一律钟面秒 (h·3600 + m·60 + s) ----
struct BookSpec {
  std::string code;             // 经 Lab::code_of
  int date = 0;                 // YYYYMMDD
  int from = 0, to = 0, at = 0; // 窗口 [from, to) / 全簿剖面取 at 这一秒末 (∈ 窗口); 都在交易时段 09:15–11:30 / 13:00–14:57 内
  double big = 0;               // 大单门槛 (元); 0 = 当日新挂单额的 99 分位
};
// 窗口里的一秒 (下标 0 = 买侧 / 1 = 卖侧; 额 = 元)
struct BookSec {
  int clock = 0;                                      // 钟面秒
  float bid = 0, ask = 0;                             // 秒末买一 / 卖一 (该侧无挂单 = 0)
  float depth[2] = {};                                // 秒末前 5 价位挂单额
  float ref = 0, matched = 0, imbalance = 0;          // 秒末竞价预撮合: 参考价 / 匹配额 / 未匹配额 (+ 买剩 − 卖剩); 非竞价或簿未交叉 = 0
  float taker[2] = {}, maker[2] = {}, cancel[2] = {}; // 秒内主动成交额 (按主动方) / 新挂额 / 撤单额
  float last = 0;                                     // 秒内末笔成交价 (无成交 = 0)
};
enum class BookKind : uint8_t { New,     // 新挂
                                Cancel,  // 撤单
                                Filled,  // 挂单被吃完
                                Sweep }; // 扫单: 连续竞价里一个主动单连吃 ≥ 2 个价位, 或成交额 ≥ 门槛
struct BookEvent {
  int clock = 0, cs = 0; // 钟面秒 + 10ms
  BookKind kind = BookKind::New;
  int side = 0;     // 委托所在侧 (Sweep = 主动方)
  int levels = 0;   // Sweep 吃过的价位数
  float price = 0;  // 元 (Sweep = 末笔价)
  float first = 0;  // Sweep = 首笔价; 其余 = price
  float amount = 0; // 元: New / Cancel = 本笔委托 / 撤单额; Filled = 申报额; Sweep = 成交额
  float life = -1;  // Cancel / Filled: 距挂出的交易秒 (午休不计); 不知 = −1
};
struct Book {
  BookSpec spec;     // 所放的那段
  std::string error; // 非空 = 没重放成 (当日无逐笔), 以下字段空
  int orders = 0;    // 当日逐笔条数
  float open = 0, high = 0, low = 0, close = 0;
  double amount = 0;                         // 当日成交额 (元)
  double big = 0;                            // 所用大单门槛 (元)
  std::vector<BookSec> secs;                 // 窗口逐秒
  std::vector<BookEvent> events;             // 窗口内的大单事件, 时序
  std::vector<std::pair<float, float>> book; // 剖面: (价, 挂单额 元; + 买 − 卖), 价升序
};
// 钟面秒 → 交易秒 (两个交易时段上的连续编号: 09:15:00 → 0, 13:00:00 → 8100, 14:59:59 → 15299; 午休钳到 8100)
int TradeSec(int clock);

// 入库门槛: 一个权重 mono / rIC 均值都不低于下限才算达标 (多空不设门槛, 只比大小)
struct Gate {
  double mono = 0, ric = 0;
};

// ---- 候选的一生: 进候选目录 (stage) ↔ 进正式库 (adopt) ----
struct Cand {
  std::string name, name_cn, expr, note;
};
struct Staged {
  Cand cand;
  std::string file; // 已落的文件名; 空 = 没落
  Result res;       // 没落 = Unstaged (error = 原因); 落了 = 落时的扫描 (Pending), 评完由 Settle 换成定论
  bool ok() const { return !file.empty(); }
};
struct Adopted {
  std::string name; // 提交的英文名
  std::string err;  // 未入库的原因; 空 = 入库 (自检会话 = 达门槛, 不拷)
  bool ok() const { return err.empty(); }
};

namespace catalog {
std::vector<Choice> pools(); // key = 池下标
std::string pool_cn(const std::string &pool);
std::vector<Choice> levels(const std::string &pool); // 该池能用的因子层
std::vector<Choice> holds(const SharedData &data, int level);
std::string hold_desc(int hold, int level); // 开 / 清仓规则
bool gpu();
bool ready(const SharedData &data, const Scope &sc, bool gpu); // 资产轴就绪且标签库已建
std::string label(const Scope &sc);                            // 池 / 层 / 持有期
std::string dir(const Scope &sc, bool test);                   // 会话目录 kWorkDir/<池>/<层>/<持有期>; test = 其下 test/ (自检会话的候选与日志)
} // namespace catalog

class Lab {
public:
  // 一步 (poll 逐个取走): eval = 进 Search / 进 Run / 评完 (got = 定论 Result; 模板坏的直接评完) / 无显著时再观察代表点 (got = Look);
  // look = 观察完 (got = Look); book = 重放完 (got = Book); 这件事做完 End
  enum class Step : uint8_t { Search,
                              Run,
                              Look,
                              Book,
                              Settle,
                              End };
  struct Tick {
    Step step;
    std::variant<std::monostate, Result, agent::Look, agent::Book> got;
  };
  // 在评那步此刻的样子 (调用方持有, live 整份刷新)
  struct Live {
    double frac = 0.0;   // 该步进度
    std::string message; // 服务给的结论 / 没算成的原因
  };

  Lab();
  ~Lab();

  // 须不忙, 作用域须有该持有期; 重扫上下文与候选目录, 未取走的 Tick 作废; data 须比 Lab 活得久
  // test = 自检: 候选目录换成 test 目录的, adopt 只查不拷
  void bind(const SharedData &data, const Scope &sc, bool test);
  const Scope &scope() const;
  bool test() const;
  const Context &ctx() const;
  const std::vector<Result> &results() const; // 候选目录全部文件, 本持有期 score 降序

  Staged stage(Cand c);                           // 名字 / 模板不合法, 算子超 kMaxOps 或同名已在 → Unstaged
  Adopted adopt(std::string name, const Gate &g); // 候选目录里没有 / 不可见 / 不达门槛 / 正式目录已有同名 → err; 否则拷进正式目录 (test 不拷)

  // 观察的参数 (只查字段表 / 候选目录 / 码表与池轴, 不碰数据): 不成 → false + err (原样给 agent)
  bool factor_of(const std::string &arg, LookFactor &out, std::string &err) const;    // 候选英文名 (取最佳权重; 无显著取代表点) 或具体根式
  bool feature_of(const std::string &src, std::string &root, std::string &err) const; // 具体特征式 (任意子式) → CsRank 包装的规范串
  bool code_of(const std::string &arg, std::string &code, std::string &err) const;    // "000001" / "000001.SZ" → 码表里且在本池资产轴上的码

  void eval(std::string file, bool gpu); // 一个已落的候选文件; 须已 bind 且作用域 ready, 不忙
  void look(LookSpec s);                 // 同上; 参数须经上面三个校验
  void book(BookSpec s);                 // 须已 bind, 不忙; code 须经 code_of, 时刻须在交易时段内
  void cancel();                         // 取消在跑那步; 它收尾即 End
  bool busy() const;                     // eval / look / book 起, 到 End 入队
  std::optional<Tick> poll();
  void live(Live &l) const;

private:
  struct Impl;
  std::unique_ptr<Impl> p_;
};

} // namespace agent
