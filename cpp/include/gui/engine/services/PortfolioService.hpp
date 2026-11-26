// PortfolioService — 因子组合回测与行情两侧 (篮子 + 个股) 账本:
//   纯计算核: EvalPortfolioGroup (同域因子共享森林出 G20 多头账) ↔ EvalBasketBooks (all 日层一趟出篮子账 + 个股账)
//   薄线程壳: PortfolioService (FactorWorker<PortfolioService, PortfolioRequest>)
#pragma once

#include "factor/Stat/Contract.hpp"
#include "gui/engine/Book.hpp"
#include "gui/engine/Table.hpp"
#include "gui/engine/Worker.hpp"
#include "gui/quote/Quote.hpp"
#include "misc/lru.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

struct SharedData;

namespace GUI::Engine {

// ---- 组合文件 (GUI 线程读写) ----
struct PortfolioEntry {
  std::string pool;
  FactorLevelId lv = 0;
  std::string file;
  std::string expr;
  HoldId hold = kHoldT1;
  int sign = 1;
  std::string key() const {
    return pool + "|" + factor_level(lv).name + "|" + file + "|" + expr + "|" + std::to_string(sign) + "|" + std::string(hold_token(hold));
  }
};
std::vector<std::string> ListPortfolios();
std::vector<PortfolioEntry> LoadPortfolio(const std::string &name);
void SavePortfolio(const std::string &name, const std::vector<PortfolioEntry> &es);
bool RenamePortfolio(const std::string &from, const std::string &to);
void DeletePortfolio(const std::string &name);

// ---- 解析与请求 ----
struct PortfolioItem {
  PortfolioEntry e;
  std::string name_en, name_cn;
  factor::stat::Frame frame = factor::stat::Frame::CS;
  std::string missing;
};
void ResolvePortfolio(const SharedData &data, const std::vector<PortfolioEntry> &es, std::vector<PortfolioItem> &out);

struct FactorTask {
  std::string key;
  FactorScope scope;
  FactorRef f;
  HoldId hold = kHoldT1;
  std::string sig() const { return key + "|" + std::to_string(scope.max_impact); }
};
inline constexpr FactorLevelId kBasketLevel = factor_level_of(L_1day);
struct BasketTask {
  FactorScope scope;
  std::string sig() const;
};
struct PortfolioRequest {
  std::vector<FactorTask> factors;
  std::optional<BasketTask> baskets;
  bool reload = false;
  std::string sig() const;
};
void MakePortfolioRequest(const SharedData &data, std::vector<PortfolioItem> &items, bool reload, bool gpu, PortfolioRequest &req);

// ---- 账与结果 (账列与核 Book 见 gui/engine/Book.hpp) ----
struct PortfolioBook : Book {
  std::vector<misc::Ymd> dates;
  FactorLevelId lv = 0;
  AmtId amt = kAmtFixed;
  std::vector<uint32_t> keys;
  std::string error;
  size_t bytes() const { return Book::bytes() + dates.size() * sizeof(misc::Ymd) + keys.size() * sizeof(uint32_t); }
  size_t lines() const { return keys.size(); }
  size_t line_of(uint32_t key) const {
    const auto it = std::lower_bound(keys.begin(), keys.end(), key);
    return it != keys.end() && *it == key ? static_cast<size_t>(it - keys.begin()) : lines();
  }
  const float *at(int k, size_t line) const {
    assert(line < lines());
    return col[k].n ? col[k].at(static_cast<int>(line)) : nullptr;
  }
  const float *val_of(size_t line, int m, bool other = false) const { // other = 另一种权重 (Profile::alt; 只篮子账有)
    const BookLines &x = other ? pf.alt[m] : pf.val[m];
    assert(line < lines() && x.n);
    return x.at(static_cast<int>(line));
  }
  const float *cap_of(size_t line) const {
    assert(line < lines() && pf.cap.n);
    return pf.cap.at(static_cast<int>(line));
  }
  const float *bal_of(size_t line, int j) const { // j = 两融口 − kValRz
    assert(line < lines() && pf.bal[j].n);
    return pf.bal[j].at(static_cast<int>(line));
  }
};

struct PortfolioResult {
  std::map<std::string, std::shared_ptr<const PortfolioBook>> factors;
  Quote::PerMarket<std::shared_ptr<const PortfolioBook>> quotes;
};

// 纯计算核 (须持 Lease): 同评估域因子共享一张森林 ↔ 行情两侧账一趟算出
Rc EvalPortfolioGroup(std::span<const FactorTask *const> ts, FactorFlow &F,
                      const std::function<void(size_t, std::shared_ptr<const PortfolioBook>)> &on_book);
Rc EvalBasketBooks(const BasketTask &t, FactorFlow &F, Quote::PerMarket<std::shared_ptr<const PortfolioBook>> &out);

class PortfolioService : public FactorWorker<PortfolioService, PortfolioRequest> {
  friend Service<PortfolioService, PortfolioRequest>;

public:
  static constexpr const char *kName = "PortfolioWorker";
  PortfolioService() : FactorWorker(Prio::Interactive) { Start(); }
  ~PortfolioService() { Stop(); }

  PortfolioResult result;
  uint64_t result_gen = 0;
  std::atomic<int> jobs_done{0}, jobs_total{0};

private:
  static constexpr Phase kStart = Phase::Loading;
  bool run(PortfolioRequest &req);
  template <class Set>
  void put(Set &&set);

  misc::Lru<std::string, PortfolioBook> cache_;
  std::string quotes_sig_;
};

} // namespace GUI::Engine
