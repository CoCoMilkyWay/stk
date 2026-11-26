// 实盘各层的底: 错误边界 + 当日秒 + 市场 + 费率 (券商无关, 适配、上层与账户同用)
//   客户端状态与预期不符 (界面没弹出 / 应答不认识 / 回报失败) 是外部结局, 不是本程序的错: fail 先在当时的段下记一行 (live/Trace.hpp)
//   再抛 Fail, 在各轮 / 各事务 / 各单的边界 (guard) 收成原因文本; 应答畸形时库里抛的异常同样收下, 绝不越过边界崩掉进程.
//   本程序自身的不变式一律 assert
// 编码: 方向 买 +1 / 卖 −1 (live/Sides.hpp); 股数 int64_t; 时刻 int 当日秒 (0..86399, −1 = 无);
//   市场由代码前缀定 (market_of), 新浪码 = 前缀 + 代码, AssetInfo 码 = 代码 + 后缀
#pragma once

#include "define/Fee.hpp"
#include "live/Sides.hpp"
#include "live/Trace.hpp"

#include <cstdint>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

namespace live {

// ---------- 错误边界 ----------

struct Fail {
  std::string why;
};
[[noreturn]] void fail(std::string why);
std::string strf(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
std::optional<double> parse_num(std::string_view s); // 整串是数 (可带尾随空白) → 值; 空 / 非法 → 无 (客户端各处的数字文字同一解析)
// ok 不成立即 fail: 原因原样, 或 printf 格式 + 参数 (只在不成立时才拼, 逐行逐格的校验照用; 参数只收标量与 C 串)
inline void need(bool ok, std::string_view why) {
  if (!ok)
    fail(std::string(why));
}
template <class... A>
  requires(sizeof...(A) > 0)
void need(bool ok, const char *fmt, const A &...a) {
  static_assert(((std::is_arithmetic_v<std::decay_t<A>> || std::is_enum_v<std::decay_t<A>> || std::is_pointer_v<std::decay_t<A>>) && ...),
                "need 的格式参数只收标量与 C 串");
  if (!ok)
    fail(strf(fmt, a...));
}
// 一个错误边界 (各轮 / 各事务 / 各单 / 后台线程): 跑 fn, 外部结局收成原因文本, 空 = 成了.
//   Fail 原样; 别的异常 (客户端 / 行情应答畸形时 JSON 取值、数值转换抛的) 一并收下、先记一行 fail, 不让它越过边界崩掉进程
template <class F>
std::string guard(F &&fn) {
  try {
    fn();
    return {};
  } catch (const Fail &f) {
    return f.why.empty() ? std::string("(无原因)") : f.why;
  } catch (const std::exception &e) {
    trace::ev("fail", {{"why", e.what()}});
    return e.what()[0] ? std::string(e.what()) : std::string("(无原因)");
  }
}

// ---------- 当日秒 ----------

int sec_now();                       // 本地当日秒 0..86399
std::string fmt_sec(int sec);        // 当日秒 → HH:MM:SS (< 0 → "—")
int parse_sec(std::string_view str); // HH:MM:SS → 当日秒 (非法 → -1)
// 只收单不撮合的静默段 [09:25, 09:30) / [11:30, 13:00) 的原因, 不在其中 = 空: 盘口是静止的挂单 (不交叉, 不按竞价看),
// 按它定的扫档 / 对手价到开撮时多已不对, 开撮头几秒盘口又最薄 —— 不下单. 集合竞价 (盘口交叉) 照下, 见 live/Book.hpp order_px
std::string quiet(int sec);

// ---------- 市场 ----------

enum class Mkt : uint8_t { SZ,
                           SH,
                           BJ };
inline constexpr const char *kMktPrefix[3] = {"sz", "sh", "bj"};                   // 新浪
inline constexpr const char *kMktSuffix[3] = {".SZ", ".SH", ".BJ"};                // AssetInfo::codes
Mkt market_of(std::string_view code);                                              // 北 4 / 8 / 92 开头, 沪 5 / 6 / 9, 其余深
inline const char *mkt_name(Mkt m) { return kMktSuffix[static_cast<int>(m)] + 1; } // SZ / SH / BJ
Mkt mkt_named(std::string_view s);                                                 // mkt_name 的逆
inline std::string sina_sym(Mkt m, const std::string &code) { return kMktPrefix[static_cast<int>(m)] + code; }
inline std::string asset_sym(Mkt m, const std::string &code) { return code + kMktSuffix[static_cast<int>(m)]; }

// ---------- 费率 ----------

// 一笔的税佣 (define/Fee.hpp 同一份费率表; 调仓试算与账户日内回推同用)
inline double fee(double amt, int s, const Fee &f = Fee{}) { return amt * f.tax(s == kSell); }

} // namespace live
