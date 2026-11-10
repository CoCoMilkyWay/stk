#include "disk/Sweep.hpp"
#include "disk/Disk.hpp"

#include "factor/Stat/Contract.hpp"           // hold_parse / hold_token
#include "features/Storage/FeatureLevels.hpp" // TIER_IDS / has_level
#include "features/Storage/FeatureLib.hpp"    // feature_pools
#include "fundamental/Spec.hpp"               // SPECS
#include "gui/engine/File.hpp"                // ValidFactorName
#include "gui/engine/Table.hpp"               // kFactorLevels
#include "gui/task_agent/Notes.hpp"           // kNoteKinds
#include "gui/task_agent/Prompt.hpp"          // kMsgs
#include "gui/task_agent/Tools.hpp"           // Tools

#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace disk {

namespace fs = std::filesystem;

namespace {

[[noreturn]] void stop(const char *why) {
  std::cerr << "[disk] " << why << std::endl;
  std::abort();
}

std::optional<TierId> tier_of(std::string_view name) {
  for (const TierId t : TIER_IDS)
    if (name == tier_name(t))
      return t;
  return std::nullopt;
}

// <因子>_w<权重号>_top<N>_{log,snap}
bool export_name(std::string_view s) {
  const auto digits = [](std::string_view x) { return !x.empty() && std::all_of(x.begin(), x.end(), [](char c) { return c >= '0' && c <= '9'; }); };
  for (const std::string_view tail : {"_log", "_snap"}) {
    if (!s.ends_with(tail))
      continue;
    std::string_view r = s.substr(0, s.size() - tail.size());
    const size_t top = r.rfind("_top"), w = top == std::string_view::npos ? top : r.rfind("_w", top);
    return w != std::string_view::npos && digits(r.substr(top + 4)) && digits(r.substr(w + 2, top - w - 2)) && GUI::Engine::ValidFactorName(r.substr(0, w));
  }
  return false;
}

fundamental::Shape table_shape(Node n) {
  switch (n) {
  case FundShard:
    return fundamental::Shape::Month;
  case FundDaily:
    return fundamental::Shape::Daily;
  default:
    return fundamental::Shape::Meta;
  }
}

// 名字在真相表里 (写法已由 parse 验过)
bool known(Node n, const Key &k) {
  switch (def(n).seg) {
  case Seg::Pool: {
    const auto pools = feature_pools();
    return std::any_of(pools.begin(), pools.end(), [&](const PoolDef &p) { return k.pool == p.name; });
  }
  case Seg::Tier:
    return tier_of(k.tier).has_value();
  case Seg::Lvl: {
    const std::optional<TierId> t = tier_of(k.tier);
    for (size_t l = 0; t && l < LEVEL_COUNT; ++l)
      if (has_level(*t, l) && k.lvl == level_name(l))
        return true;
    return false;
  }
  case Seg::Level:
    return std::any_of(GUI::Engine::kFactorLevels.begin(), GUI::Engine::kFactorLevels.end(), [&](const auto &l) { return k.level == l.name; });
  case Seg::Hold: {
    int h = 0;
    return factor::stat::hold_parse(k.hold, h) && factor::stat::hold_token(h) == k.hold;
  }
  case Seg::Table:
    return std::any_of(fundamental::SPECS.begin(), fundamental::SPECS.end(), [&](const fundamental::Spec &s) {
      return s.name == k.name && (n == FundAuditSheet || s.shape == table_shape(n));
    });
  case Seg::Factor:
  case Seg::Portfolio:
    return GUI::Engine::ValidFactorName(k.name);
  case Seg::Export:
    return export_name(k.name);
  case Seg::Tool: {
    const auto tools = GUI::Agent::Tools();
    return std::any_of(tools.begin(), tools.end(), [&](const GUI::Agent::ToolDef &t) { return k.name == t.name; });
  }
  case Seg::Note:
    return std::any_of(std::begin(GUI::Agent::kNoteKinds), std::end(GUI::Agent::kNoteKinds), [&](const auto &d) { return k.name == d.id; });
  case Seg::Prompt:
    return std::any_of(std::begin(GUI::Agent::kMsgs), std::end(GUI::Agent::kMsgs), [&](const auto &d) { return k.name == d.id; });
  default:
    return true;
  }
}

bool fits(Node n, const fs::path &p) { return def(n).kind == Kind::File ? fs::is_regular_file(p) : fs::is_directory(p); }

// p 是 n 的哪个子节点 (写法 + 真相表); 不是 → kNodeCount
Node child_of(Node n, const fs::path &p, Key &k) {
  const std::string name = p.filename().string();
  for (int i = 1; i < kNodeCount; ++i) {
    const Node c = static_cast<Node>(i);
    Key kk = k;
    if (def(c).parent == n && fits(c, p) && parse(c, name, kk) && known(c, kk)) {
      k = std::move(kk);
      return c;
    }
  }
  return kNodeCount;
}

// 残留: <子节点实例名>.tmp (文件) / <子目录实例名>.trash.<…>
bool debris(Node n, const fs::path &p, const Key &k) {
  const std::string name = p.filename().string();
  const size_t grave = name.find(".trash.");
  const bool tmp = name.ends_with(".tmp") && fs::is_regular_file(p), dir = grave != std::string::npos && fs::is_directory(p);
  if (!tmp && !dir)
    return false;
  const std::string stem = tmp ? name.substr(0, name.size() - 4) : name.substr(0, grave);
  for (int i = 1; i < kNodeCount; ++i) {
    const Node c = static_cast<Node>(i);
    Key kk = k;
    if (def(c).parent == n && (def(c).kind == Kind::File) == tmp && parse(c, stem, kk))
      return true;
  }
  return false;
}

struct Plan {
  std::vector<fs::path> debris;
  std::vector<std::pair<fs::path, fs::path>> moves;
  std::vector<std::pair<Node, fs::path>> foreign; // (所在目录的节点, 路径)
};

// p (不合布局) 是不是放错地方的基石: 与基石节点同名同类且该节点的布局位置空着 → 整个搬过去 (目录内部不合布局的, 搬后逐组再问)
std::optional<fs::path> relocate(const fs::path &p, const Key &k) {
  const std::string name = p.filename().string();
  for (int i = 1; i < kNodeCount; ++i) {
    const Node c = static_cast<Node>(i);
    if (def(c).core && name == def(c).prefix && fits(c, p) && resolvable(c, k) && !fs::exists(path(c, k)))
      return path(c, k);
  }
  return std::nullopt;
}

void walk(const fs::path &dir, Node n, const Key &k, Plan &plan) {
  std::vector<fs::path> entries;
  for (const fs::directory_entry &e : fs::directory_iterator(dir))
    entries.push_back(e.path());
  std::sort(entries.begin(), entries.end());
  for (const fs::path &p : entries) {
    Key kk = k;
    if (const Node c = child_of(n, p, kk); c != kNodeCount) {
      if (def(c).kind == Kind::Dir)
        walk(p, c, kk, plan);
    } else if (debris(n, p, k)) {
      plan.debris.push_back(p);
    } else if (std::optional<fs::path> to = relocate(p, k)) {
      plan.moves.emplace_back(p, std::move(*to));
    } else {
      plan.foreign.emplace_back(n, p);
    }
  }
}

Plan scan() {
  Plan plan;
  walk(ensure(Output), Output, {}, plan);
  return plan;
}

std::string rel(const fs::path &p) { return p.lexically_relative(root()).string(); }

std::string describe(const fs::path &p) {
  if (fs::is_symlink(p))
    return "  (链接 → " + fs::read_symlink(p).string() + ")";
  if (!fs::is_directory(p))
    return "  (" + std::to_string(fs::file_size(p) >> 10) + " KB)";
  uintmax_t files = 0, bytes = 0;
  for (const fs::directory_entry &e : fs::recursive_directory_iterator(p))
    if (e.is_regular_file())
      ++files, bytes += e.file_size();
  return "  (目录, " + std::to_string(files) + " 个文件, " + std::to_string(bytes >> 20) + " MB)";
}

// 不合布局的按 (所在目录的节点, 名字) 归组, 首见序: 同一张已删的表散在各月目录里 = 一组
std::vector<std::vector<fs::path>> group(const std::vector<std::pair<Node, fs::path>> &foreign) {
  std::vector<std::vector<fs::path>> out;
  std::map<std::pair<Node, std::string>, size_t> at;
  for (const auto &[n, p] : foreign) {
    const auto [i, fresh] = at.try_emplace({n, p.filename().string()}, out.size());
    if (fresh)
      out.emplace_back();
    out[i->second].push_back(p);
  }
  return out;
}

// 一组的写法: 各成员相对路径逐段比, 不同的段写 * (同节点 → 同深度)
std::string pattern(const std::vector<fs::path> &ps) {
  std::vector<std::string> parts;
  for (const fs::path &c : fs::path(rel(ps[0])))
    parts.push_back(c.string());
  for (const fs::path &p : ps) {
    size_t j = 0;
    for (const fs::path &c : fs::path(rel(p))) {
      assert(j < parts.size());
      if (parts[j] != c.string())
        parts[j] = "*";
      ++j;
    }
    assert(j == parts.size());
  }
  std::string out;
  for (const std::string &x : parts)
    out += (out.empty() ? "" : "/") + x;
  return out;
}

std::string describe(const std::vector<fs::path> &ps) {
  if (ps.size() == 1)
    return describe(ps[0]);
  uintmax_t files = 0, bytes = 0;
  for (const fs::path &p : ps) {
    if (fs::is_symlink(p))
      continue;
    if (!fs::is_directory(p)) {
      ++files, bytes += fs::file_size(p);
      continue;
    }
    for (const fs::directory_entry &e : fs::recursive_directory_iterator(p))
      if (e.is_regular_file())
        ++files, bytes += e.file_size();
  }
  return "  (" + std::to_string(ps.size()) + " 处, 共 " + std::to_string(files) + " 个文件, " + std::to_string(bytes >> 20) + " MB)";
}

} // namespace

void sweep() {
  Plan plan = scan();
  while (!plan.debris.empty() || !plan.moves.empty()) {
    for (const fs::path &p : plan.debris) {
      std::cout << "[disk] 清残留 " << rel(p) << std::endl;
      fs::remove_all(p);
    }
    for (const auto &[from, to] : plan.moves) {
      std::cout << "[disk] 迁移 " << rel(from) << " → " << rel(to) << std::endl;
      fs::create_directories(to.parent_path());
      fs::rename(from, to);
    }
    plan = scan();
  }
  if (plan.foreign.empty())
    return;

  const std::vector<std::vector<fs::path>> groups = group(plan.foreign);
  std::cout << "[disk] " << plan.foreign.size() << " 处不合布局, " << groups.size() << " 组 (disk/Disk.hpp DISK_LAYOUT):" << std::endl;
  std::vector<std::string> name, what;
  for (const std::vector<fs::path> &g : groups) {
    name.push_back(pattern(g));
    what.push_back(describe(g));
    std::cout << "  " << name.back() << what.back() << std::endl;
  }
  if (!isatty(STDIN_FILENO))
    stop("不合布局的文件须先清掉: 在终端里运行一次, 逐组确认删除");
  for (size_t i = 0; i < groups.size(); ++i) {
    std::cout << "删除 " << name[i] << what[i] << " ? [Y/n] " << std::flush;
    std::string a;
    std::getline(std::cin, a);
    if (!(a.empty() || a == "y" || a == "Y"))
      stop("不合布局的文件须先清掉, 未删则不运行");
    for (const fs::path &p : groups[i])
      fs::remove_all(p);
  }
}

} // namespace disk
