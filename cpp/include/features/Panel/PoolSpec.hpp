#pragma once
// 池参数框架: 子池 = 一组参数 (各维一个取值); 一组参数一个池 (独立成员 / 独立库)
//   维表 kDimDefs (维序): 风格 · 端 · 只数 · 周期 · ST · 板块 · 融资 · 次新 · 停牌 · 行业 · 预警; 池名 / 中文名 / 网格 / 资产类一律按维表走
//     风格 = 排序键 (同风格的池共用一份次序); 端 = 键的低端 / 高端; 只数 N = 切片内键的一端取前 N (全 = 切片内全选: 风格 / 端不起作用, canon 归一)
//     周期 = 调仓日历 (features/Panel/PoolSelect.hpp src_of)
//     切片维 (ST 起): 各按 all.bar 一列把在市 A 股分类, 取值 0 = 全 (不切), 取值 v > 0 = 第 v − 1 类; 切片 = 各切片维之交
//   键 / 切片的原料全是 all.bar L_1day 的基础列 (行 D = D+1 盘前态, 见 Node/Common/Fund/Fund.hpp), 成员由 features/Panel/PoolSelect.hpp 现算 (选股表 Picks)
//   子池表 = output/config/beta/pools.json (Beta → Style 页确认后写; 无文件 = default_pools()); 计算与各页的池表 feature_pools() (features/Storage/FeatureLib.hpp)
//     = all + 子池表, 进程启动时读一次 (改了重启生效)
//
//   池名 (库目录名) 按维序以 "." 连, 每维一段; 中文名同序, 自端起 (端名已含风格):
//     beta.low.200.day.nost.all.all.old.trade.ind.safe = 低β.200.日.非ST.全.全.非次新.非停牌.行业已知.无预警
//     all.all.all.week.st.all.margin.all.all.all.all  = 全.全.周.ST.全.融资.全.全.全.全
//   不起作用的段 (取值 全 / 全选池的风格端, 池名写 all) 见 on()
//   显示名 (各页) = 简称 short_cn: 风格池 = 端名 + 只数 (低β200); 全选池 = 起作用的 ST / 板块 / 融资 段名 (ST / 沪主板 / 非ST融资; 都不切 = 全选);
//     同简称的池按池表序加 A / B / C … 区分 (aliases); 其余参数悬停见 detail_cn
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
// 只数 (取值下标; 全 = 切片内全选)
enum Count : uint8_t { kCountAll,
                       kCount100,
                       kCount200,
                       kCount400,
                       kCount800 };
inline constexpr int kNCounts = 5;
inline constexpr int kCounts[kNCounts] = {0, 100, 200, 400, 800};
// 周期: 成员源行的日历 (PoolSelect.hpp src_of); 两次调仓之间成员不变, 篮子权随价漂移
enum Rebal : uint8_t { kRebalDay,
                       kRebalWeek };
inline constexpr int kRebals = 2;
// ST (行 D 的 st_level + 1; 全 = 不切)
enum St : uint8_t { kStAll,
                    kStNone,
                    kStSt,
                    kStSst,
                    kStDelist };
inline constexpr int kSts = 5;
// 板块 (落盘列 board; 取值与 fund::Board 同号, 全 = 不切)
enum Board : uint8_t { kBoardAll,
                       kBoardShMain,
                       kBoardSzMain,
                       kBoardStar,
                       kBoardChiNext,
                       kBoardBj };
inline constexpr int kBoards = 6;
// 二值切片 (融资 is_margin / 过滤表 flt_*: 列 > 0.5 = 是): 全 / 否 / 是
enum Flag : uint8_t { kFlagAll,
                      kFlagOff,
                      kFlagOn };
inline constexpr int kFlags = 3;

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
    {"all", "全", "切片内全选 (与风格 / 端无关)"},
    {"100", "100", "切片内键的一端取前 N 只 (键覆盖率 < 80% 的行 = 全取)"},
    {"200", "200", "切片内键的一端取前 N 只 (键覆盖率 < 80% 的行 = 全取)"},
    {"400", "400", "切片内键的一端取前 N 只 (键覆盖率 < 80% 的行 = 全取)"},
    {"800", "800", "切片内键的一端取前 N 只 (键覆盖率 < 80% 的行 = 全取)"},
};
inline constexpr DimValue kRebalValues[kRebals] = {
    {"day", "日", "每个交易日按前一交易日收盘重选, 篮子每日回到目标权"},
    {"week", "周", "每周按上周末个交易日收盘重选, 一周成员不变, 篮子权随价漂移"},
};
inline constexpr DimValue kStValues[kSts] = {
    {"all", "全", "不切 (正常 / ST / *ST / 退市整理全含)"},
    {"nost", "非ST", "st_level = 0"},
    {"st", "ST", "ST (不含 *ST)"},
    {"sst", "*ST", "*ST"},
    {"delist", "退市整理", "退市整理期"},
};
inline constexpr DimValue kBoardValues[kBoards] = {
    {"all", "全", "不切 (五板全含)"},
    {"sh_main", "沪主板", "沪市主板 600 / 601 / 603 / 605"},
    {"sz_main", "深主板", "深市主板 000 / 001 / 002 / 003 / 004"},
    {"star", "科创板", "科创板 688 / 689"},
    {"chinext", "创业板", "创业板 300 / 301 / 302 / 309"},
    {"bj", "北交所", "北交所 (代码 .BJ)"},
};
inline constexpr DimValue kMarginValues[kFlags] = {
    {"all", "全", "不切"},
    {"nonmargin", "非融资", "非融资融券标的"},
    {"margin", "融资", "融资融券标的"},
};
inline constexpr DimValue kNewValues[kFlags] = {
    {"all", "全", "不切"},
    {"old", "非次新", "上市龄 ≥ 365 日历日 (flt_new = 0)"},
    {"new", "次新", "上市龄 < 365 日历日 (flt_new = 1)"},
};
inline constexpr DimValue kSuspValues[kFlags] = {
    {"all", "全", "不切"},
    {"trade", "非停牌", "次日不停牌 (flt_susp = 0)"},
    {"susp", "停牌", "次日停牌 (flt_susp = 1)"},
};
inline constexpr DimValue kIndValues[kFlags] = {
    {"all", "全", "不切"},
    {"ind", "行业已知", "申万一级行业已知 (flt_noind = 0)"},
    {"noind", "行业未知", "申万一级行业未知 (flt_noind = 1)"},
};
inline constexpr DimValue kWarnValues[kFlags] = {
    {"all", "全", "不切"},
    {"safe", "无预警", "交易 / 预亏 / 营收 / 分红 四项预警都无 (flt_warn = 0)"},
    {"warn", "预警", "交易 / 预亏 / 营收 / 分红 四项预警任一 (flt_warn = 1)"},
};

// 维 (维序 = 池名段序); 切片维 = [kDimSt, kDims), 第 k 个切片维 = kDimSt + k
enum Dim : uint8_t { kDimStyle,
                     kDimEnd,
                     kDimN,
                     kDimRebal,
                     kDimSt,
                     kDimBoard,
                     kDimMargin,
                     kDimNew,
                     kDimSusp,
                     kDimInd,
                     kDimWarn };
inline constexpr int kDims = 11;
inline constexpr int kSlices = kDims - kDimSt;
struct DimDef {
  const char *name_cn;              // 维名
  std::span<const DimValue> values; // 取值表 (下标 = Spec::v)
  bool all0;                        // 取值 0 = 全 (不起作用)
};
inline constexpr DimDef kDimDefs[kDims] = {
    {"风格", kStyleValues, false},
    {"端", kEndValues, false},
    {"只数", kCountValues, true},
    {"周期", kRebalValues, false},
    {"ST", kStValues, true},
    {"板块", kBoardValues, true},
    {"融资", kMarginValues, true},
    {"次新", kNewValues, true},
    {"停牌", kSuspValues, true},
    {"行业", kIndValues, true},
    {"预警", kWarnValues, true},
};
// 维 d 的取值数
constexpr int dim_size(int d) { return static_cast<int>(kDimDefs[d].values.size()); }

struct Spec {
  std::array<uint8_t, kDims> v{}; // [维] 取值下标 (全 0 = 在市全选, 日频)
  Style style() const { return static_cast<Style>(v[kDimStyle]); }
  End end() const { return static_cast<End>(v[kDimEnd]); }
  int n() const { return kCounts[v[kDimN]]; } // 0 = 全选
  Rebal rebal() const { return static_cast<Rebal>(v[kDimRebal]); }
  bool keyed() const { return v[kDimN] != kCountAll; }
  auto operator<=>(const Spec &) const = default;
};
Spec canon(Spec s);                // 全选 → 风格 / 端归 0 (同一池同一份)
bool on(const Spec &s, int d);     // 维 d 对 s 起作用 (取值 全 / 全选池的风格端 不起作用)
std::string code(const Spec &s);   // 池名 (库目录名)
Spec parse(std::string_view code); // 不是规范池名 → assert

// 中文名的段 (自端起, 维序); on = 该段起作用 (不起作用的段灰显)
struct Part {
  std::string cn;
  bool on;
};
inline constexpr int kParts = kDims - kDimEnd;
std::array<Part, kParts> parts(const Spec &s);
std::string name_cn(const Spec &s);   // 各段以 "." 连 (Style 页线名, 不带权重后缀)
std::string short_cn(const Spec &s);  // 简称 (不带区分后缀)
std::string detail_cn(const Spec &s); // 全部参数 (悬停, 多行): 池名 / 中文名 / 键与端 / 调仓 / 起作用的切片
// 子池表各池的显示名: 简称, 同简称不止一个的按表序加 A / B / C … (过 Z 接 AA)
std::vector<std::string> aliases(const std::vector<Spec> &pools);

// 子池表 (output/config/beta/pools.json, 池表序)
std::vector<Spec> default_pools();
std::vector<Spec> load_pools(); // 文件不存在 = default_pools(); 格式不对 / 重名 → assert
void save_pools(const std::vector<Spec> &pools);

// Style 页参数网格: [维] 勾选的取值 (位 i = 该维取值表第 i 个), 网格 = 各维笛卡尔积; weight 只是篮子的两条线 (Weight 位), 不进池身份
//   风格维由页面置为打开的那一个; 缺省 ST / 板块 / 融资 全开, 次新 / 停牌 / 行业 / 预警 只勾全
struct Grid {
  std::array<uint16_t, kDims> bits = [] {
    std::array<uint16_t, kDims> b{};
    for (int d = 0; d < kDims; ++d)
      b[d] = d >= kDimNew ? 1 : static_cast<uint16_t>((1u << dim_size(d)) - 1);
    b[kDimStyle] = 1;
    return b;
  }();
  uint8_t weight = 0b11;
  Style style() const; // 风格维须只勾一个
  bool operator==(const Grid &) const = default;
};
std::vector<Spec> specs_of(const Grid &g); // canon 后去重, 维序 (末维最快)

} // namespace pool
