#pragma once

// 模型表: 名 + 参数轴 (数值 / 类别, 网格取值) + 组合约束 + 构造; 测试台 (main.cpp) 与交互轨迹 (trace.cpp) 共用
//   参数单位都是根 (1m = 分钟), band 的 n 以日计 (构造时 × 每日根数)
#include "Model.hpp"

#include <cmath>
#include <memory>
#include <vector>

using P = std::vector<float>;

struct Axis {
  const char *name;
  std::vector<float> v;
  std::vector<const char *> label = {}; // 非空 = 类别轴 (label[i] 对应 v[i], 平原不跨类别平滑)
};

// ok / make 的第二参 = 每日根数
struct Kind {
  const char *name;
  std::vector<Axis> grid;
  bool (*ok)(const P &, double); // 组合约束 (nullptr = 全要)
  std::unique_ptr<Model> (*make)(const P &, double);
  bool trail; // 测试台另叠 Trail 外挂 (k = kTrail) 各跑一份
};

inline constexpr float kTrail = 3;

// clang-format off
inline const Kind kKinds[] = {
    {"hold", {}, nullptr, [](const P &, double) -> std::unique_ptr<Model> { return std::make_unique<Hold>(); }, false},
    {"ma", {{"fast", {5, 10, 20}}, {"slow", {20, 40, 60, 120, 250}}}, [](const P &p, double) { return 2 * p[0] <= p[1]; },
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Ma>(p[0], p[1]); }, true},
    {"tsmom", {{"L", {10, 20, 40, 60, 120, 250}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<TsMom>(p[0]); }, true},
    {"ens", {{"lo", {5, 10, 20}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Ens>(p[0]); }, true},
    {"kama", {{"n", {10, 20}}, {"slow", {30, 60}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Kama>(p[0], p[1]); }, true},
    {"donchian", {{"n", {20, 40, 60, 120}}, {"m", {10, 20, 40}}}, [](const P &p, double) { return p[1] < p[0]; },
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Donchian>(p[0], p[1]); }, false},
    {"chandelier", {{"n", {10, 20, 60}}, {"k", {2, 3, 4, 5}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Chandelier>(p[0], p[1]); }, false},
    {"rebound", {{"d", {0.5, 1, 1.5}}, {"n", {5, 10, 20}}, {"k", {2, 3, 4}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Rebound>(p[0], p[1], p[2]); }, false},
    {"revert", {{"L", {60, 120, 250}}, {"g", {0.5, 1, 1.5}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Revert>(p[0], p[1]); }, false},
    {"gate", {{"L", {60, 120, 250}}, {"g", {-1, -0.5, 0}}, {"f", {5, 10, 20}}, {"h", {0, 0.25, 0.5}}}, nullptr,
     [](const P &p, double) -> std::unique_ptr<Model> { return std::make_unique<Gate>(p[0], p[1], p[2], p[3]); }, false},
    {"band", {{"c", {0, 1}, {kCenters[0], kCenters[1]}}, {"w", {0, 1, 2}, {kWidths[0], kWidths[1], kWidths[2]}},
              {"n", {0.5, 2, 10, 60, 120}}, {"k", {0.25, 0.5, 1, 2}}, {"N", {1, 4}}, {"h", {0, 0.5}}},
     [](const P &p, double S) { return p[2] * S >= 5; },
     [](const P &p, double S) -> std::unique_ptr<Model> {
       return std::make_unique<Band>(static_cast<int>(p[0]), static_cast<int>(p[1]), std::lround(p[2] * S), p[3], static_cast<int>(p[4]), p[5]);
     }, false},
};
// clang-format on
