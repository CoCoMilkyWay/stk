#include "live/Terms.hpp"

#include "disk/Disk.hpp"

#include <cassert>
#include <sstream>
#include <string>
#include <vector>

namespace live::terms {

namespace {

constexpr const char *kPulled = "pulled";
constexpr const char *kHead = "code,name,mkt,ratio,financeable,rate,pledgeable";

} // namespace

Cache load() {
  Cache c;
  if (!disk::exists(disk::LiveTermsFile))
    return c;
  std::istringstream in(disk::read(disk::LiveTermsFile));
  std::string line;
  std::getline(in, line);
  const std::vector<std::string> h = cells(line);
  assert(h.size() == 2 && h[0] == kPulled && "两融条款缓存: 首行");
  c.pulled = misc::parse_ymd(h[1]);
  assert(c.pulled > 0 && "两融条款缓存: 拉于");
  std::getline(in, line);
  assert(line == kHead && "两融条款缓存: 表头");
  while (std::getline(in, line)) {
    const std::vector<std::string> f = cells(line);
    assert(f.size() == 7 && "两融条款缓存: 列数");
    Term &t = c.terms[f[0]];
    t.name = f[1];
    t.mkt = mkt_named(f[2]);
    if (!f[3].empty())
      t.ratio = std::stod(f[3]);
    t.financeable = f[4] == "1";
    t.rate = std::stod(f[5]);
    t.pledgeable = f[6] == "1";
  }
  return c;
}

void save(const Cache &c) {
  assert(c.pulled > 0);
  std::string s = strf("%s,%s\n%s\n", kPulled, misc::dense(c.pulled).c_str(), kHead);
  for (const auto &[code, t] : c.terms) {
    need(t.name.find(',') == std::string::npos, code + " 名称含逗号");
    s += strf("%s,%s,%s,%s,%d,%.10g,%d\n", code.c_str(), t.name.c_str(), mkt_name(t.mkt), t.ratio ? strf("%.10g", *t.ratio).c_str() : "", t.financeable, t.rate,
              t.pledgeable);
  }
  disk::write(disk::LiveTermsFile, {}, s);
}

} // namespace live::terms
