#include "live/Pulled.hpp"

#include "disk/Disk.hpp"
#include "live/Broker.hpp"

#include <cassert>
#include <sstream>
#include <string_view>

namespace live {

namespace {

constexpr const char *kPulled = "pulled";

std::vector<std::string> cells(std::string_view line) {
  std::vector<std::string> f(1);
  for (const char ch : line) {
    if (ch == ',')
      f.emplace_back();
    else
      f.back().push_back(ch);
  }
  return f;
}

} // namespace

Pulled load_pulled(disk::Node file, const char *head) {
  Pulled s;
  if (!disk::exists(file))
    return s;
  std::istringstream in(disk::read(file));
  std::string line;
  std::getline(in, line);
  const std::vector<std::string> h = cells(line);
  assert(h.size() % 2 == 0 && h[0] == kPulled && "缓存首行");
  s.pulled = misc::parse_ymd(h[1]);
  assert(s.pulled > 0 && "缓存拉于");
  for (size_t i = 2; i < h.size(); i += 2)
    s.meta[h[i]] = h[i + 1];
  std::getline(in, line);
  assert(line == head && "缓存表头");
  const size_t n = cells(head).size();
  while (std::getline(in, line)) {
    s.rows.push_back(cells(line));
    assert(s.rows.back().size() == n && "缓存列数");
  }
  return s;
}

void save_pulled(disk::Node file, const char *head, const Pulled &s) {
  assert(s.pulled > 0);
  std::string out = std::string(kPulled) + "," + misc::dense(s.pulled);
  for (const auto &[k, v] : s.meta)
    out += "," + k + "," + v;
  out += "\n" + std::string(head) + "\n";
  for (const std::vector<std::string> &r : s.rows) {
    for (size_t i = 0; i < r.size(); ++i) {
      need(r[i].find(',') == std::string::npos, strf("%s: 「%s」含逗号", head, r[i].c_str()));
      out += (i ? "," : "") + r[i];
    }
    out += "\n";
  }
  disk::write(file, {}, out);
}

} // namespace live
