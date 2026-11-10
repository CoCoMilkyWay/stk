#include "disk/Sweep.hpp"
#include "disk/Disk.hpp"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <map>
#include <span>
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

// 子节点表: 父为 n 的节点 = kid[off[n], off[n + 1]), 按表序
struct Tree {
  std::array<std::uint16_t, kNodeCount + 1> off{};
  std::array<Node, kNodeCount> kid{};
};
constexpr Tree kTree = [] {
  Tree t;
  for (int i = 1; i < kNodeCount; ++i)
    ++t.off[kDefs[i].parent + 1];
  for (int n = 0; n < kNodeCount; ++n)
    t.off[n + 1] += t.off[n];
  std::array<std::uint16_t, kNodeCount + 1> at = t.off;
  for (int i = 1; i < kNodeCount; ++i)
    t.kid[at[kDefs[i].parent]++] = static_cast<Node>(i);
  return t;
}();
std::span<const Node> kids(Node n) { return {kTree.kid.data() + kTree.off[n], kTree.kid.data() + kTree.off[n + 1]}; }

// 基石的整条路径全是字面: 布局位置唯一, 与键无关
static_assert([] {
  for (int i = 0; i < kNodeCount; ++i)
    for (Node n = static_cast<Node>(i); kDefs[i].core && n != Root; n = kDefs[n].parent)
      if (kDefs[n].seg != Seg::Lit)
        return false;
  return true;
}());

// 名为 name 的一项 (dir = 盘上是目录) 是 n 的哪个子节点 (类 ∧ 写法 ∧ 值域), 段值记进 k; 不是 → kNodeCount
Node child(Node n, std::string_view name, bool dir, Key &k) {
  for (const Node c : kids(n))
    if (is_dir(c) == dir && parse(c, name, k))
      return c;
  return kNodeCount;
}

// 残留: <子文件实例名>.tmp / <子目录实例名>.trash.<…>
bool debris(Node n, std::string_view name, bool dir, Key k) {
  const size_t cut = dir ? name.find(".trash.") : name.ends_with(".tmp") ? name.size() - 4
                                                                         : std::string_view::npos;
  return cut != std::string_view::npos && child(n, name.substr(0, cut), dir, k) != kNodeCount;
}

// 放错地方的基石: 与基石节点同名同类, 且其布局位置空着 → 整个搬过去 (目录内部不合布局的, 搬后逐组再问)
Node stray(std::string_view name, bool dir) {
  for (int i = 1; i < kNodeCount; ++i) {
    const Node c = static_cast<Node>(i);
    if (def(c).core && is_dir(c) == dir && name == def(c).prefix && !fs::exists(path(c)))
      return c;
  }
  return kNodeCount;
}

struct Plan {
  std::vector<fs::path> debris;
  std::vector<std::pair<fs::path, fs::path>> moves;
  std::vector<std::pair<Node, fs::path>> foreign; // (所在目录的节点, 路径)
};

// 每项一次分类: 空目录 (残留) / 布局内 (目录则下行) / 残留 / 放错地方的基石 / 不合布局
void walk(const fs::path &at, Node n, const Key &k, Plan &plan) {
  std::vector<fs::directory_entry> entries{fs::directory_iterator(at), fs::directory_iterator()};
  std::sort(entries.begin(), entries.end());
  for (const fs::directory_entry &e : entries) {
    const fs::path &p = e.path();
    const std::string name = p.filename().string();
    const bool dir = e.is_directory();
    Key kk = k;
    if (dir && !e.is_symlink() && fs::is_empty(p)) {
      plan.debris.push_back(p);
    } else if (const Node c = child(n, name, dir, kk); c != kNodeCount) {
      if (def(c).kind == Kind::Dir)
        walk(p, c, kk, plan);
    } else if (debris(n, name, dir, k)) {
      plan.debris.push_back(p);
    } else if (const Node c = stray(name, dir); c != kNodeCount) {
      plan.moves.emplace_back(p, path(c));
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

// 残留 / 放错地方的基石处理到不再冒出 (删掉的目录让上层成空目录 → 下一轮清), 余下不合布局的
std::vector<std::pair<Node, fs::path>> settle() {
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
  return std::move(plan.foreign);
}

// 不合布局的先全部列出, 再逐组问删
void ask(const std::vector<std::pair<Node, fs::path>> &foreign) {
  const std::vector<std::vector<fs::path>> groups = group(foreign);
  std::cout << "[disk] " << foreign.size() << " 处不合布局, " << groups.size() << " 组 (disk/Disk.hpp DISK_LAYOUT):" << std::endl;
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

} // namespace

void sweep() {
  for (std::vector<std::pair<Node, fs::path>> foreign = settle(); !foreign.empty(); foreign = settle())
    ask(foreign);
}

} // namespace disk
