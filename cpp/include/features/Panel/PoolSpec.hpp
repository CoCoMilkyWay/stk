#pragma once
// 池参数框架: 子池 = 一组参数 (各维一个取值); 一组参数一个池 (独立成员 / 独立库)
//   维表 kDimDefs (维序): 风格 · 端 · 只数 · 周期 · 权重 · ST · 板块 · 融资 · 过滤; 池名 / 中文名 / 网格 / 资产类一律按维表走
//     单选维 (风格 / 端 / 只数 / 周期 / 权重): Spec 存取值下标, 恒起作用
//       风格 = 排序键 (同风格的池共用一份次序); 端 = 键的低端 / 高端; 只数 N = 切片内键在端头取前 N
//       周期 = 调仓日历 (features/Panel/PoolSelect.hpp src_of); 权重 = 篮子成分的权 (等权 / 总市值加权, PoolSelect.hpp Tape::omega)
//     集合维 (ST 起, 切片维): Spec 存取值表的位集, 0 = 不起作用
//       划分维 (ST / 板块 / 融资): 按 all.bar 一列把在市 A 股分成互斥的类, 池取若干类之并 (全部类 = 0 = 不切, canon 归一)
//       过滤维: 各项按 all.bar 一列剔掉一部分 (不互斥, 不是划分), 池剔若干项之并
//       切片 = 各划分维之交 − 各过滤项
//   在市 = 已上市 ∧ 未退市 ∧ 收盘总市值 > 0 ∧ 次日不停牌 ∧ 申万一级行业已知 (固定, 不进维)
//   键 / 切片的原料全是 all.bar L_1day 的基础列 (行 D = D+1 盘前态, 见 Node/Common/Fund/Fund.hpp), 成员由 features/Panel/PoolSelect.hpp 现算 (选股表 Picks)
//   子池表 = output/config/beta/pools.json (Beta → Style 页确认后写; 无文件 = default_pools()); 计算与各页的池表 feature_pools() (features/Storage/FeatureLib.hpp)
//     = all + 子池表, 每进程代读一次 (Style 页落地即进程内重启)
//
//   池名 (库目录名) 按维序以 "." 连, 每维一段 (集合维 = 各位的取值码以 "+" 连, 位序; 0 = all); 中文名同序, 自端起 (端名已含风格):
//     beta.low.200.day.eq.nost.all.all.nonew+nowarn            = 低β.200.日.等权.非ST.全.全.非次新+无预警
//     size.high.400.week.cap.st+sst.sh_main+sz_main.margin.all = 大盘.400.周.加权.ST+*ST.沪主板+深主板.融资.不剔
//   不起作用的段 (集合维 0) 见 on()
//   显示名 (各页) = 简称 short_cn = 端名 + 只数 + 权重后缀 (.等 / .加) (低β200.等); 同简称的池按池表序在只数后加 A / B / C … 区分 (aliases);
//     逐维拆解 (悬停) 见 detail
#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace pool {

// 风格 = 排序键 (行 D 的基础列派生, 见 PoolSelect.cpp kKeyDefs)
enum Style : uint8_t { kStyleSize,
                       kStyleEp,
                       kStyleBp,
                       kStyleDy,
                       kStyleRoe,
                       kStyleInv,
                       kStyleMom,
                       kStyleRev,
                       kStyleTurn,
                       kStyleIvol,
                       kStyleResvol,
                       kStyleBeta,
                       kStyleTrend,
                       kStylePrice };
inline constexpr int kStyles = 14;
// 端: 键的低端 / 高端
enum End : uint8_t { kEndLow,
                     kEndHigh };
inline constexpr int kEnds = 2;
// 只数 (取值下标): 风格池在端头取前 N
enum Count : uint8_t { kCount100,
                       kCount200,
                       kCount400,
                       kCount800 };
inline constexpr int kNCounts = 4;
inline constexpr int kCounts[kNCounts] = {100, 200, 400, 800};
// 周期: 成员源行的日历 (PoolSelect.hpp src_of); 两次调仓之间成员不变, 篮子权随价漂移
enum Rebal : uint8_t { kRebalDay,
                       kRebalWeek };
inline constexpr int kRebals = 2;
// 权重: 成分各自的权 (等权 = 1, 加权 = 总市值)
enum Weight : uint8_t { kWeightEq,
                        kWeightCap };
inline constexpr int kWeights = 2;
// 权重后缀 (简称 / 篮子名的尾)
constexpr const char *weight_suffix(Weight w) { return w == kWeightEq ? ".等" : ".加"; }
// ST 的类 (行 D 的 st_level)
enum St : uint8_t { kStNone,
                    kStSt,
                    kStSst,
                    kStDelist };
inline constexpr int kSts = 4;
// 板块的类 (落盘列 board − 1; fund::Board 1..5)
enum Board : uint8_t { kBoardShMain,
                       kBoardSzMain,
                       kBoardStar,
                       kBoardChiNext,
                       kBoardBj };
inline constexpr int kBoards = 5;
// 融资的类 (is_margin > 0.5)
enum Margin : uint8_t { kMarginOff,
                        kMarginOn };
inline constexpr int kMargins = 2;
// 过滤项 (列 > 0.5 = 剔)
enum Filter : uint8_t { kFltNew,
                        kFltWarn };
inline constexpr int kFilters = 2;

struct StyleDef {
  const char *code;          // 池名风格段
  const char *name_cn;       // 风格名 (Style 页右表)
  const char *key;           // 键 (悬停)
  const char *end_cn[kEnds]; // 风格端中文名
};
inline constexpr StyleDef kStyleDefs[kStyles] = {
    {"size", "市值", "收盘总市值 = close × total_shares", {"小盘", "大盘"}},
    {"ep", "市盈", "EP = 归母净利 TTM / 收盘总市值 (亏损为负)", {"高市盈", "低市盈"}},
    {"bp", "市净", "BP = 归母权益 MRQ / 收盘总市值 (负净资产为负)", {"高市净", "低市净"}},
    {"dy", "股息", "股息率 TTM (近 4 季报告期税前分红 / 收盘总市值)", {"低息", "高息"}},
    {"roe", "ROE", "单季归母净利 / 归母权益 MRQ (权益 ≤ 0 记缺)", {"低ROE", "高ROE"}},
    {"inv", "投资", "总资产 MRQ 同比", {"低投资", "高投资"}},
    {"mom", "动量", "12-1 月动量 P_{D−20} / P_{D−250} − 1 (后复权)", {"低动量", "高动量"}},
    {"rev", "反转", "20 日收益率 P_D / P_{D−20} − 1 (后复权)", {"输家", "赢家"}},
    {"turn", "换手", "20 日均换手 / 250 日均换手", {"低换手", "高换手"}},
    {"ivol", "特波", "近 250 日对等权市场回归的残差标准差", {"低特波", "高特波"}},
    {"resvol", "残波", "残差波动 (三项波动 z 合成, 对 β / log 市值正交; 活跃外缺)", {"低残波", "高残波"}},
    {"beta", "β", "近 156 周周收益对等权市场的 β", {"低β", "高β"}},
    {"trend", "趋势", "均线截面回归的预期 20 日收益", {"低趋势", "高趋势"}},
    {"price", "股价", "收盘价 (元, 不复权)", {"低价", "高价"}},
};

// 一维的一个取值: 池名段 / 中文段 / 悬停
struct DimValue {
  const char *code, *name_cn, *tip;
};
inline constexpr auto kStyleValues = [] {
  std::array<DimValue, kStyles> v{};
  for (int i = 0; i < kStyles; ++i)
    v[i] = {kStyleDefs[i].code, kStyleDefs[i].name_cn, kStyleDefs[i].key};
  return v;
}();
inline constexpr DimValue kEndValues[kEnds] = {
    {"low", "低端", "键的低端 (由小到大取前 N)"},
    {"high", "高端", "键的高端 (由大到小取前 N)"},
};
inline constexpr DimValue kCountValues[kNCounts] = {
    {"100", "100", "切片内键在端头取前 N 只 (键覆盖率 < 80% 的行不建仓)"},
    {"200", "200", "切片内键在端头取前 N 只 (键覆盖率 < 80% 的行不建仓)"},
    {"400", "400", "切片内键在端头取前 N 只 (键覆盖率 < 80% 的行不建仓)"},
    {"800", "800", "切片内键在端头取前 N 只 (键覆盖率 < 80% 的行不建仓)"},
};
inline constexpr DimValue kRebalValues[kRebals] = {
    {"day", "日", "每个交易日按前一交易日收盘重选, 篮子每日回到目标权"},
    {"week", "周", "每周按上周末个交易日收盘重选, 一周成员不变, 篮子权随价漂移"},
};
inline constexpr DimValue kWeightValues[kWeights] = {
    {"eq", "等权", "成分等权"},
    {"cap", "加权", "成分按源行收盘总市值加权"},
};
inline constexpr DimValue kStValues[kSts] = {
    {"nost", "非ST", "st_level = 0 (正常)"},
    {"st", "ST", "ST (不含 *ST)"},
    {"sst", "*ST", "*ST"},
    {"delist", "退市整理", "退市整理期"},
};
inline constexpr DimValue kBoardValues[kBoards] = {
    {"sh_main", "沪主板", "沪市主板 600 / 601 / 603 / 605"},
    {"sz_main", "深主板", "深市主板 000 / 001 / 002 / 003 / 004"},
    {"star", "科创板", "科创板 688 / 689"},
    {"chinext", "创业板", "创业板 300 / 301 / 302 / 309"},
    {"bj", "北交所", "北交所 (代码 .BJ)"},
};
inline constexpr DimValue kMarginValues[kMargins] = {
    {"nonmargin", "非融资", "非融资融券标的"},
    {"margin", "融资", "融资融券标的"},
};
inline constexpr DimValue kFilterValues[kFilters] = {
    {"nonew", "非次新", "剔上市龄 < 365 日历日 (flt_new = 1)"},
    {"nowarn", "无预警", "剔 交易 / 预亏 / 营收 / 分红 四项预警任一 (flt_warn = 1)"},
};

// 维 (维序 = 池名段序); 切片维 = 集合维 = [kDimSt, kDims), 第 k 个切片维 = kDimSt + k
enum Dim : uint8_t { kDimStyle,
                     kDimEnd,
                     kDimN,
                     kDimRebal,
                     kDimWeight,
                     kDimSt,
                     kDimBoard,
                     kDimMargin,
                     kDimFilter };
inline constexpr int kDims = 9;
inline constexpr int kSlices = kDims - kDimSt;
// 维的种类: 单选 (Spec 存下标) / 划分 (存类的位集, 全部类归 0) / 过滤 (存项的位集)
enum DimKind : uint8_t { kKindSingle,
                         kKindPart,
                         kKindFilter };
struct DimDef {
  const char *name_cn;              // 维名
  std::span<const DimValue> values; // 取值表 (单选维: 下标 = Spec::v; 集合维: 位 i = 第 i 类 / 项)
  DimKind kind;
};
inline constexpr DimDef kDimDefs[kDims] = {
    {"风格", kStyleValues, kKindSingle},
    {"端", kEndValues, kKindSingle},
    {"只数", kCountValues, kKindSingle},
    {"周期", kRebalValues, kKindSingle},
    {"权重", kWeightValues, kKindSingle},
    {"ST", kStValues, kKindPart},
    {"板块", kBoardValues, kKindPart},
    {"融资", kMarginValues, kKindPart},
    {"过滤", kFilterValues, kKindFilter},
};
// 维 d 的取值数 (集合维 = 类 / 项数)
constexpr int dim_size(int d) { return static_cast<int>(kDimDefs[d].values.size()); }
constexpr bool is_set(int d) { return kDimDefs[d].kind != kKindSingle; }
constexpr uint16_t full_mask(int d) { return static_cast<uint16_t>((1u << dim_size(d)) - 1); }
static_assert([] {
  for (int d = 0; d < kDims; ++d)
    if (is_set(d) != (d >= kDimSt) || (is_set(d) && dim_size(d) > 8))
      return false;
  return true;
}(),
              "集合维 = 切片维, 位集存进 uint8_t");
// 取名 (线名 / 网格按钮 / 悬停同一套): 维 d 第 i 个取值的中文名 (端名随风格) / 集合维取 0 的中文名 (划分维 = 全, 过滤维 = 不剔)
constexpr const char *value_cn(int d, int i, Style st) { return d == kDimEnd ? kStyleDefs[st].end_cn[i] : kDimDefs[d].values[i].name_cn; }
constexpr const char *none_cn(int d) { return kDimDefs[d].kind == kKindFilter ? "不剔" : "全"; }
// 取值槽 (网格展开 / 线的取值 / 着色同一套): 单选维 = 取值下标; 集合维 (n 个类 / 项) = 单一位 i → i, 多位 (勾选的并 / 合) → n, 0 (全 / 不剔) → n + 1
constexpr int slot_union(int d) { return dim_size(d); }
constexpr int slot_none(int d) { return dim_size(d) + 1; }
static_assert(kStyles <= 16 && kNCounts <= 16, "取值槽存进 uint16_t");

struct Spec {
  std::array<uint8_t, kDims> v{}; // [维] 单选维 = 取值下标, 集合维 = 位集
  Style style() const { return static_cast<Style>(v[kDimStyle]); }
  End end() const { return static_cast<End>(v[kDimEnd]); }
  int n() const { return kCounts[v[kDimN]]; }
  Rebal rebal() const { return static_cast<Rebal>(v[kDimRebal]); }
  Weight weight() const { return static_cast<Weight>(v[kDimWeight]); }
  auto operator<=>(const Spec &) const = default;
};
Spec canon(Spec s);                // 划分维全部类 → 0 (同一池同一份)
Spec unweighted(Spec s);           // 权重归 0: 成员身份 (只差权重的池成员相同)
bool on(const Spec &s, int d);     // 维 d 对 s 起作用 (集合维 0 不起作用)
int slot(const Spec &s, int d);    // 维 d 的取值槽
std::string code(const Spec &s);   // 池名 (库目录名)
Spec parse(std::string_view code); // 不是规范池名 → assert

// 中文名的段 (自端起, 维序); on = 该段起作用 (不起作用的段灰显)
struct Part {
  std::string cn;
  bool on;
};
inline constexpr int kParts = kDims - kDimEnd;
std::array<Part, kParts> parts(const Spec &s);
std::string name_cn(const Spec &s);  // 各段以 "." 连 (Style 页线名)
std::string short_cn(const Spec &s); // 简称 (不带区分字母)
// 逐维拆解 (各页悬停同一份, 维序): 取值 = 中文段; 说明 = 单选维取值的含义 (风格 = 键), 集合维各位 "名 = 含义" 逐行 (0 = 不切 / 不剔)
struct DimDetail {
  std::string val, note;
  bool on;
};
std::array<DimDetail, kDims> detail(const Spec &s);
// 子池表各池的显示名: 简称, 同简称不止一个的按表序加 A / B / C … (过 Z 接 AA)
std::vector<std::string> aliases(const std::vector<Spec> &pools);

// 子池表 (output/config/beta/pools.json, 池表序)
std::vector<Spec> default_pools();
std::vector<Spec> load_pools(); // 文件不存在 = default_pools(); 格式不对 / 重名 → assert
void save_pools(const std::vector<Spec> &pools);

// Style 页参数网格: [维] 勾选位集 (位 i = 该维取值表第 i 个)
//   各维的取值: 单选维 = 勾选的各取值 (至少勾一个); 集合维 (勾选 S, 可空) = S 的各位单独 + S 整体 + 0 (划分维 = 全部类, 过滤维 = 不剔)
//   网格 = 各维取值的笛卡尔积; 风格维由页面置为打开的那一个; 缺省其余各维全勾
struct Grid {
  std::array<uint16_t, kDims> bits = [] {
    std::array<uint16_t, kDims> b{};
    for (int d = 0; d < kDims; ++d)
      b[d] = full_mask(d);
    b[kDimStyle] = 1;
    return b;
  }();
  Style style() const;         // 风格维须只勾一个
  uint16_t slots(int d) const; // 维 d 展开的取值槽 (位): 单选维 = 勾选的; 集合维 = 勾选的各位 + 并 / 合 (勾选不止一个, 划分维全勾的并即全) + 全 / 不剔
  bool operator==(const Grid &) const = default;
};
std::vector<Spec> specs_of(const Grid &g); // 各维的取值槽之积, canon 后去重, 维序 (末维最快)

} // namespace pool
