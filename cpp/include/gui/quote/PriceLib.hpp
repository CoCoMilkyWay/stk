// PriceLib — 行情的历史价格库 (QuoteHub 持有, 自带 worker): 两侧 (个股 = 资产轴全部, 行 = 全局 id / 篮子 = 篮子表全部, 行 = 篮子下标)
//   × 两种根 (日根 / 分钟根), 一趟合成落盘, 之后只补新日; 账户一侧由账本在日脊上回放 (不落盘).
// 日脊 = 历史日 [D] (各侧同一条; 首日只定成员, 无根). 个股侧的源:
//   库段 [0, L) = all.bar 落盘日, 源 = LibDay (基准 = D−1 行前收, 日根 + L_1min 分钟根); 一趟 lib_pass, 按批:
//     批首先并行读本批各日 (及前一日) 的日根行; 整块 (库段新日起于块界且够 kBlock 日) = 日根行先定行表与各行的格, 定长建块,
//     各日并行合成、分钟根当场写进格, 块末逐行过链再写日根 (不经单日段, 无封块重写); 其余 (块界前 / 不足一块) 走单日段.
//     篮子侧跟得上时同趟: 各日在同一件里用内存中的个股件合成篮子 (BuildDay), 不回读个股段
//   补段 [L, D) = 库末日之后的交易日; 源分两截, 一日只取其一:
//     基本面段 [L, L + F) = 至 AssetInfo::quote_end, 源 = FundDay (bar1d 行 + bar1m 分钟根);
//     新浪段 [L + F, D) = 其后接连有新浪归档 (QuoteService.hpp) 的日, 源 = SinaDay (末快照 + 分钟根); 基本面盖住的归档随即删
//     补段逐日认 (日, 截): 库段没长时, 打头 (日, 截) 都不变的日盘上留着 (只重放链), 其后重算
//   补段的源稠密在资产轴上 ⇒ 库轴外的个股 (新股等) 也有补段的根.
// 篮子侧 = 个股侧的根 (落盘的 SegDay / 同趟的 BuildDay) × 成员 → Lane (一日一件), 跟着个股侧的日脊补到末日:
//   成员: 库段 = 交易日 D 的选股表 (members), 补段 = 库末日之后冻结, 逐日过 drift (漂移比 = 个股侧的 收 / 前收)
//   各日并行合成 (补段的成员先按日序接续出; 一日的合成见 Fold::day), 过链按日序串行;
//   篮子侧只依个股侧 + 篮子表, 一个篮子的全史只依其身份 (BasketTable::key) ⇒ 篮子表一变只重合身份变了的篮子 (refold, 见落盘)
//   末了留交接 (handoff): 末个历史日之后那个交易日的成员表 + 各篮子末日点位, 当日合成接着它;
//   交接一变即交给当日 (构造时给的 bind, 在发布之前 ⇒ 读端拿到新库时当日已换上新交接)
// 有效价 = 存价 × 因子; 只存有日根的行日 (无根的日不过链: 前收与参考价逐日相消, 跳过不改因子):
//   个股: 存价 = 原价, 因子 = 后复权 (自首个有根日 1 起, 逢前收 ≠ 参考价乘 参考价 / 前收)
//   篮子: 存价 = Lane 的比 (前日 = 1), 因子 = 前日点位 (自 kBase 起, 逐日 × 前日比收)
//   分钟根 = 紧凑形 (Bars.hpp Packed; 个股 f16 口径 ⇒ 库段 f16 原价逐位还原, 篮子 = 振幅等分), 价基 / 步长随日根
//   日根带当日资金流摘要 (DayFlow; 个股 = 本只分钟涨跌法, 篮子 = 成员之和): 大范围按日读, 不碰分钟根
// 落盘 <库>/cache/price/ (稀疏段, 段内行主序 ⇒ 一行全史 = 每段一段连续读; 库全量重算连目录一起删; 段 = 只读映射, 与读出的全史共持):
//   head.bin = Head + 资产轴前缀指纹 + 篮子表指纹 + 篮子侧各行的身份 + 日脊 + L + F + 篮子侧日数 + 两侧封块数
//     + 两侧的链 (末日因子 / 参考价; 行数 = 链长). 链: 个股 = 库段末的; 篮子 = 篮子侧库段末的 (篮子侧未及库末 = 其末日的)
//   <侧>/b<首日>.bin = 封好的块 (库段 kBlock 日), <侧>/d<日>.bin = 其后的单日段 (库段尾 + 补段); 库段的单日段每满 kBlock 日即并成一块
//   段 = 段头 (篮子侧记所依的篮子表指纹) + 行表 (键升序; 日 / 分钟位图) + 日根 + 分钟根 (与日根同位, 缺分钟根的占一格空根);
//     写一律 tmp + rename, 头后于段落盘 ⇒ 头外的段开库即删
//   头对得上、资产轴以其为前缀、个股段齐且库段是库日轴的前缀 (pool::extends) → 补段截到留着的日, 只合成库段新日再续算补段;
//   否则清目录从头. 篮子段不齐 → 只清篮子侧. 段齐 = 头所述的段都在且完整 (头 + 长度对得上; 掉电截断的当作缺). 每批落一次头 (取消不丢已合成的)
//   篮子表换了 → 篮子侧截到库段之内, 逐段按新表重排 (refold): 身份对得上的篮子搬旧行 (根与链照旧), 其余各日并行合成、按日序过链;
//     逐段换上, 末了落头. 段头指纹 = 新表的段已换过 (中断后接着换: 链照它重放); 混进第三张表的 (换到一半表又变) → 只清篮子侧
// 读: series = 一行全史 (日根 + 复权倍数; minute = 分钟紧凑根的视图: 指进段的映射, 不解码不拷)
// 账户: 资金流水账本 × 个股连分钟的全史回放 (gui/account/AccountSide.hpp, 与当日同一核); 账本代 / 券商现价 / 戳变了重算
// 服务 (gui/util/Service.hpp, 随对象): 请求 = 要的全史 (标的 × 连不连分钟) + 基本面段末日 + 行情日 + 账本代 + 券商现价 (QuoteHub 每帧并成; 变了即补);
//   每趟: 补库 (只析构中断, 新请求不打断) → 库变了重建日脊 / 篮子全表 (+ 交接的普查) / 个股日脊末段 → 账户 → 读要的 (按 (标的, 戳) 沿用; 新请求打断)
//   → 发布 PriceView (与上份同则不发). 进度: Loading = 合成天 (库段 + 补段 + 篮子侧重排 / 合成, 一趟一次 begin)
// 线程: 库只 worker 读写, 外面只经 PriceView 读; 合成线程池随对象 (kThreads 封顶; 两侧按日并行合成, 篮子全表按篮子并行读)
#pragma once

#include "account/Ledger.hpp" // Spot
#include "define/Members.hpp"
#include "disk/Disk.hpp"
#include "gui/quote/Quote.hpp" // Side / PerMarket / Series / Tape / PriceView
#include "gui/util/Service.hpp"
#include "misc/date.hpp"
#include "misc/exec.hpp"
#include "misc/lru.hpp"
#include "shared/Progress.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

struct FeatureLib;
namespace fundamental {
class Catalog;
}
namespace pool {
struct Picks;
}

namespace GUI::Quote {

// 要一个标的的全史 (篮子不连分钟的恒在, 不必要)
struct Want {
  Ref ref;
  bool minute = false;
  bool operator==(const Want &) const = default;
};
// 一次请求 (QuoteHub 每帧并成)
struct PriceRequest {
  std::vector<Want> wants; // (侧, 键) 升序, 一个标的一项 (连分钟的盖过不连的)
  misc::Ymd mark = 0;      // 基本面段末日 (AssetInfo::quote_end)
  misc::Ymd live = 0;      // 新浪行情日 (换日 = 可能多一份归档)
  uint64_t ledger = 0;     // 账本代 (account::generation)
  account::Spot spot;      // 库外持仓的券商现价 (AccountHub::spot)
  bool operator==(const PriceRequest &) const = default;
};

// 当日合成的交接: 末个历史日之后那个交易日的成员表 (全局 id) + 各篮子末日点位
struct Handoff {
  Members mem;
  std::vector<double> level; // [篮子]
};

struct PriceStore; // 两侧的段 (映射)
struct DayBuild;   // 一侧一日的件 (内存)

class PriceLib : public Service<PriceLib, PriceRequest>, public Published {
  friend Service<PriceLib, PriceRequest>;

public:
  static constexpr const char *kName = "PriceLib";
  static constexpr size_t kKeep = 64, kKeepMinutes = 32; // 留在内存的个股日根全史 / 连分钟的全史数 (请求的上限)
  static constexpr size_t kBlock = 64;
  static constexpr int kThreads = 8; // 合成线程 (每线程 ~60MB: 分钟读 + 一日的件 + 篮子合成件; 定数封顶, 小内存机器同样安全)
  static constexpr double kBase = 1000;
  // 一行的因子链: 末日因子 (0 = 尚无根) / 参考价 (末个收盘, 按前收滚动; 0 = 尚无)
  struct Link {
    double f = 0;
    float ref = 0;
  };

  // bind = 交接的去处 (QuoteService::bind; worker 线程调)
  PriceLib(std::shared_ptr<const Calendar> cal, std::function<void(const Handoff &)> bind);
  ~PriceLib();

  std::shared_ptr<const PriceView> result; // 持 mutex 读 (latest); 空 = 未发布

private:
  static constexpr Phase kStart = Phase::Loading;
  struct Head {
    uint64_t magic = 0, lib = 0; // 库轴指纹
    misc::Ymd first = 0;         // 库首个落盘日
    bool operator==(const Head &) const = default;
  };

  Progress &board() { return *this; }
  bool run(PriceRequest &req);
  void put(std::shared_ptr<const PriceView> v);
  template <class Ar>
  void snap(Ar &ar);
  void save_head();
  PriceStamp stamp() const;
  size_t rows(Side s) const { return link_[s].size(); }                      // 个股 = 资产轴前缀长, 篮子 = 篮子表长
  size_t days(Side s) const { return s == kStock ? dates_.size() : bdays_; } // 一侧落盘的日 (日脊前缀)
  // 头 / 个股段对不上 → 两侧清空从头 (只留库首日); 篮子表 / 篮子段对不上 → 只清篮子侧; 资产轴长了 → 个股行跟上
  void reopen(const FeatureLib &lib, const std::vector<misc::Ymd> &dates);
  void wipe(const std::vector<misc::Ymd> &dates);
  void wipe_baskets();
  void cut_baskets(size_t B); // 篮子侧截到前 B 日 (删其后的单日段; 不切进封好的块)
  bool open_side(Side s);     // 头所述的该侧段全在 → 映射 (头外的删); 缺 → false
  // 个股: 库段补到 dates, 补段补到基本面段末日 mark 及其后接连的新浪归档; 篮子侧跟上, 再定交接; false = 中断 / 判废
  bool sync(const FeatureLib &lib, const std::vector<misc::Ymd> &dates, misc::Ymd mark);
  // 个股库段补到 dates (见文件头); pk 非空 = 篮子侧同趟合成 (须与个股侧同日数); false = 中断 / 判废
  bool lib_pass(const FeatureLib &lib, const std::vector<misc::Ymd> &dates, const pool::Picks *pk);
  // 个股补段 tail (前 F 日基本面段, 其余新浪段): 前 keep 日盘上留着, 其后合成落盘; false = 中断
  bool fill(const fundamental::Catalog &cat, const std::vector<misc::Ymd> &tail, size_t F, size_t keep);
  // 篮子表换了 (篮子侧已截到库段之内): 逐段按新表重排 (见文件头); false = 中断
  bool refold(const pool::Picks &pk);
  // 篮子侧补到日脊末日 (源 = 个股侧的段, 成员见文件头) → 交接; false = 中断
  bool fold(const pool::Picks &pk);
  // 一侧日 d 的件 b 过链 z (就地推进, 填因子) → 落成单日段, 该侧 + d
  template <Side S>
  void commit(misc::Ymd d, DayBuild &b, std::vector<Link> &z);
  template <Side S>
  void seal();        // 该侧库段满 kBlock 日的单日段并成块
  void cut(size_t D); // 日脊截到 D 日 (删两侧其后的单日段)
  // 一行自第 from 日起的全史 (minute = 连分钟根); 无根 → false
  bool series(Ref ref, bool minute, Series &s, size_t from = 0) const;
  void rebuild();                                          // 库变了: 日脊 / 篮子全表 (+ 普查) / 个股日脊末段
  void replay(uint64_t ledger, const account::Spot &spot); // 账本 × 本戳日脊 (+ 券商现价) → account_

  const std::shared_ptr<const Calendar> cal_;
  const std::function<void(const Handoff &)> bind_;
  std::atomic<bool> quit_{false}; // 补库只认它 (析构)
  disk::Key lib_;                 // 所在特征库
  Head head_;
  uint64_t prefix_ = 0;               // 资产轴前 rows(kStock) 条的指纹
  uint64_t table_ = 0;                // 篮子侧所依的篮子表指纹 (BasketTable::hash)
  std::vector<uint64_t> bkeys_;       // [篮子] 篮子侧各行的身份 (BasketTable::key)
  std::vector<misc::Ymd> dates_;      // 日脊 = 个股侧的日
  size_t lib_days_ = 0;               // 库段日数 L
  size_t fund_days_ = 0;              // 基本面段日数 F
  size_t bdays_ = 0;                  // 篮子侧的日数 (≤ 日脊)
  PerMarket<size_t> sealed_{};        // [侧] 封好的块数 (库段 [0, sealed × kBlock))
  PerMarket<std::vector<Link>> link_; // [侧][行] 链 (见文件头 head.bin)
  std::unique_ptr<PriceStore> store_;
  PriceStamp at_;             // 交接所对的戳 (空 = 未定 / 补到一半)
  Members mem_;               // 交接: 成员表
  std::vector<double> level_; // 交接: [篮子] 末日点位
  misc::exec::ForkJoin pool_{misc::exec::fit(kThreads)};
  // worker 私有: 本戳 (built_) 的日脊 / 篮子全表 / 个股日脊末段 / 普查 / 账户 + 沿用的读
  PriceStamp built_;
  std::shared_ptr<const std::vector<misc::Ymd>> spine_;
  std::shared_ptr<const Tape> baskets_;
  std::shared_ptr<const Tails> tails_;
  std::shared_ptr<const Census> census_;
  std::shared_ptr<const Tape> account_; // 空 = 本戳未回放
  Equity equity_;                       // account_ 的摘要
  uint64_t ledger_ = 0;                 // account_ 所依的账本代
  account::Spot spot_;                  // account_ 所依的券商现价
  misc::Lru<uint32_t, Series> daily_;   // 本戳个股日根全史 (留 kKeep 个; 换戳即清)
  misc::Lru<Ref, Series> minute_;       // 本戳连分钟的全史 (两侧; 留 kKeepMinutes 个; 换戳即清)
};

} // namespace GUI::Quote
