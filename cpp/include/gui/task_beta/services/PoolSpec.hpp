#pragma once
// 池参数框架: 子池 = 风格 (一列排序键, 同风格的池共用) × 端 + 参数; 一组参数一个池 (独立成员 / 独立库)
//   维: 风格端 · 只数 · 周期 · ST · 板块 · 融资 · 过滤
//     ST / 板块 / 融资 各自划分全部在市 A 股 (取值 全 = 不切); 过滤 = 过滤表的子集 (各条件 1 = 滤掉, 剔的条件并起来剔)
//     切片 = ST ∩ 板块 ∩ 融资 ∩ 过滤后的在市资产; 只数 N > 0 = 切片内键的一端取前 N, N = 0 = 切片内全选 (与风格端无关, canon 归一)
//   键 / 切片的原料全是 all.bar L_1day 的基础列 (行 D = D+1 盘前态, 见 Node/Common/Fund/Fund.hpp), 成员由 gui/task_beta/services/PoolSelect.hpp 现算
//   选定的池 = config/pools.json (Beta → Style 页确认后写; 无文件 = default_pools()); 只是本页的选中落地,
//     计算与各页用的池表是编译期 feature_pools() (features/Storage/FeatureLib.hpp), 不读此文件
//
//   池名 (库目录名) 与中文名都按维序以 "." 连, 每维一段:
//     beta_low.200.d.nost.all.all.f0  = 低β.200.日.非ST.全.全.不剔
//     all.all.w.st.all.margin.f3       = 全.全.周.ST.全.融资.剔2      (过滤段: f<剔的条件位, 十六进制>; 中文只写条数, 明细见 filter_cn)
//   不起作用的段 (全 / 不剔 / 全选池的风格端) 见 parts().on
#include <array>
#include <cstdint>
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

struct StyleDef {
  const char *code;          // 池名风格端段 <code>_<end>
  const char *name_cn;       // 风格名 (Style 页右表)
  const char *key;           // 键 (悬停)
  const char *end[kEnds];    // 端名
  const char *end_cn[kEnds]; // 风格端中文名
};
inline constexpr StyleDef kStyleDefs[kStyles] = {
    {"size", "市值", "收盘总市值 = close × total_shares", {"small", "big"}, {"小盘", "大盘"}},
    {"ep", "市盈", "EP = 归母净利 TTM / 收盘总市值 (亏损为负)", {"low", "high"}, {"高市盈", "低市盈"}},
    {"bp", "市净", "BP = 归母权益 MRQ / 收盘总市值 (负净资产为负)", {"low", "high"}, {"高市净", "低市净"}},
    {"dy", "股息", "股息率 TTM (近 4 季报告期税前分红 / 收盘总市值)", {"low", "high"}, {"低息", "高息"}},
    {"roe", "ROE", "单季归母净利 / 归母权益 MRQ (权益 ≤ 0 记缺)", {"low", "high"}, {"低ROE", "高ROE"}},
    {"inv", "投资", "总资产 MRQ 同比", {"low", "high"}, {"低投资", "高投资"}},
    {"mom", "动量", "12-1 月动量 P_{D−20} / P_{D−250} − 1 (后复权)", {"low", "high"}, {"低动量", "高动量"}},
    {"rev", "反转", "20 日收益率 P_D / P_{D−20} − 1 (后复权)", {"low", "high"}, {"输家", "赢家"}},
    {"turn", "换手", "20 日均换手 / 250 日均换手", {"low", "high"}, {"低换手", "高换手"}},
    {"ivol", "特波", "近 250 日对等权市场回归的残差标准差", {"low", "high"}, {"低特波", "高特波"}},
    {"resvol", "残波", "残差波动 (三项波动 z 合成, 对 β / log 市值正交; 活跃外缺)", {"low", "high"}, {"低残波", "高残波"}},
    {"beta", "β", "近 156 周周收益对等权市场的 β", {"low", "high"}, {"低β", "高β"}},
    {"trend", "趋势", "均线截面回归的预期 20 日收益", {"low", "high"}, {"低趋势", "高趋势"}},
    {"price", "股价", "收盘价 (元, 不复权)", {"low", "high"}, {"低价", "高价"}},
};

// 再平衡: 调仓行 = 选股的行; 两次调仓之间成员不变, 篮子权随价漂移
enum Rebal : uint8_t { kRebalDay,
                       kRebalWeek };
inline constexpr int kRebals = 2;
// ST (行 D 的 st_level; 全 = 不切)
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
// 融资 (落盘列 is_margin; 全 = 不切)
enum Margin : uint8_t { kMarginAll,
                        kMarginOn,
                        kMarginOff };
inline constexpr int kMargins = 3;
// 过滤表 (all.bar 列 flt_*, 1 = 滤掉); Spec::filter 的位 k = 剔 kFilterDefs[k]
enum Filter : uint8_t { kFltNew,
                        kFltSusp,
                        kFltNoInd,
                        kFltWarn };
inline constexpr int kFilters = 4;
// 只数 (0 = 全选)
inline constexpr int kCounts[] = {100, 200, 400, 800, 0};
inline constexpr int kNCounts = 5;

// 一维的一个取值: 池名段 / 中文段 / 悬停
struct DimValue {
  const char *code, *name_cn, *tip;
};
inline constexpr DimValue kRebalDefs[kRebals] = {
    {"d", "日", "每个交易日收盘按当日键重选, 篮子每日回到目标权"},
    {"w", "周", "每周末个交易日 (周五) 收盘重选, 下周成员不变, 篮子权随价漂移"},
};
inline constexpr DimValue kStDefs[kSts] = {
    {"all", "全", "不切 (正常 / ST / *ST / 退市整理全含)"},
    {"nost", "非ST", "st_level = 0"},
    {"st", "ST", "ST (不含 *ST)"},
    {"sst", "*ST", "*ST"},
    {"delist", "退市整理", "退市整理期"},
};
inline constexpr DimValue kBoardDefs[kBoards] = {
    {"all", "全", "不切 (五板全含)"},
    {"sh_main", "沪主板", "沪市主板 600 / 601 / 603 / 605"},
    {"sz_main", "深主板", "深市主板 000 / 001 / 002 / 003 / 004"},
    {"star", "科创板", "科创板 688 / 689"},
    {"chinext", "创业板", "创业板 300 / 301 / 302 / 309"},
    {"bj", "北交所", "北交所 (代码 .BJ)"},
};
inline constexpr DimValue kMarginDefs[kMargins] = {
    {"all", "全", "不切"},
    {"margin", "融资", "融资融券标的"},
    {"nonmargin", "非融资", "非融资融券标的"},
};
inline constexpr DimValue kFilterDefs[kFilters] = {
    {"flt_new", "次新", "上市龄 < 365 日历日"},
    {"flt_susp", "停牌", "次日停牌"},
    {"flt_noind", "行业未知", "申万一级行业未知"},
    {"flt_warn", "风险预警", "交易 / 预亏 / 营收 / 分红 四项预警任一"},
};

struct Spec {
  Style style = kStyleSize;
  End end = kEndLow;
  uint16_t n = 200; // 0 = 全选 (风格 / 端不起作用, canon 归一)
  Rebal rebal = kRebalDay;
  St st = kStAll;
  Board board = kBoardAll;
  Margin margin = kMarginAll;
  uint8_t filter = 0; // 位 k = 剔 kFilterDefs[k]
  auto operator<=>(const Spec &) const = default;
};
Spec canon(Spec s);                // n = 0 → 风格 / 端归一 (同一池同一份)
std::string code(const Spec &s);   // 池名 (库目录名)
Spec parse(std::string_view code); // 不是规范池名 → assert

// 中文名的段 (维序); on = 该段起作用 (不起作用的段灰显)
struct Part {
  std::string cn;
  bool on;
};
inline constexpr int kParts = 7;
std::array<Part, kParts> parts(const Spec &s);
std::string name_cn(const Spec &s); // 各段以 "." 连 (篮子行名, 不带权重后缀)
std::string filter_cn(uint8_t f);   // 剔的条件 "次新 / 停牌" (0 → "不剔")

// 选定的池 (config/pools.json, 池表序)
std::vector<Spec> default_pools();
std::vector<Spec> load_pools(); // 文件不存在 = default_pools(); 格式不对 / 重名 → assert
void save_pools(const std::vector<Spec> &pools);

// Style 页参数网格: 各维勾选的取值 (位 i = 该维取值表第 i 个), 网格 = 各维笛卡尔积; weight 只是篮子的两条线 (Weight 位), 不进池身份
//   过滤表每个条件一维: 位 0 = 留, 位 1 = 剔 (只勾剔 = 整个网格剔它; 都勾 = 留 / 剔两组都算); 缺省只留
struct Grid {
  uint8_t end = (1u << kEnds) - 1;
  uint8_t n = (1u << kNCounts) - 1;
  uint8_t rebal = (1u << kRebals) - 1;
  uint8_t st = (1u << kSts) - 1;
  uint8_t board = (1u << kBoards) - 1;
  uint8_t margin = (1u << kMargins) - 1;
  uint8_t filter[kFilters] = {1, 1, 1, 1};
  uint8_t weight = 0b11;
  bool operator==(const Grid &) const = default;
};
std::vector<Spec> specs_of(Style st, const Grid &g); // 风格 st 的网格 (canon 后去重, 维序: 端 → 只数 → ST → 板块 → 融资 → 过滤 → 周期)

} // namespace pool
