#pragma once

// 模型表: 名 + 参数轴 (数值 / 类别, 网格取值) + 共列轴数 + 组合约束 + 映射; 测试台 (main.cpp) 与交互轨迹 (trace.cpp) 共用
//   参数单位都是根 (1m = 分钟); 以日计的轴 (band 的 n) 经 to_bars 乘每日根数, ok / rule 拿到的都是根
#include "Model.hpp"

#include <cmath>
#include <vector>

using P = std::vector<float>;

struct Axis {
  const char *name;
  std::vector<float> v;
  std::vector<const char *> label = {}; // 非空 = 类别轴 (label[i] 对应 v[i], 平原不跨类别平滑)
  bool day = false;                     // 以日计
};

struct Kind {
  const char *name;
  std::vector<Axis> grid;
  size_t share;                   // 前 share 根轴定指标列: 只在其后轴上不同的配置同组, 测试台逐资产共用一份列缓存
  bool (*ok)(const P &);          // 组合约束 (nullptr = 全要)
  Sig (*rule)(Tape &, const P &); // 映射
  bool trail;                     // 测试台另叠 Trail 外挂 (k = kTrail) 各跑一份
};

inline constexpr float kTrail = 3;

inline size_t len(float x) { return static_cast<size_t>(std::lround(x)); }

// 轴单位 → 根: 以日计的轴乘每日根数 S
inline P to_bars(const Kind &K, P p, double S) {
  for (size_t i = 0; i < p.size(); ++i)
    if (K.grid[i].day)
      p[i] *= static_cast<float>(S);
  return p;
}

// clang-format off
inline const Kind kKinds[] = {
    {"hold", {}, 0, nullptr, [](Tape &t, const P &) { return hold(t); }, false},
    {"ma", {{"fast", {5, 10, 20}}, {"slow", {20, 40, 60, 120, 250}}}, 2, [](const P &p) { return 2 * p[0] <= p[1]; },
     [](Tape &t, const P &p) { return ma(t, len(p[0]), len(p[1])); }, true},
    {"tsmom", {{"L", {10, 20, 40, 60, 120, 250}}}, 1, nullptr,
     [](Tape &t, const P &p) { return tsmom(t, len(p[0])); }, true},
    {"ens", {{"lo", {5, 10, 20}}}, 1, nullptr,
     [](Tape &t, const P &p) { return ens(t, len(p[0])); }, true},
    {"kama", {{"n", {10, 20}}, {"slow", {30, 60}}}, 2, nullptr,
     [](Tape &t, const P &p) { return kama(t, len(p[0]), len(p[1])); }, true},
    {"donchian", {{"n", {20, 40, 60, 120}}, {"m", {10, 20, 40}}}, 2, [](const P &p) { return p[1] < p[0]; },
     [](Tape &t, const P &p) { return donchian(t, len(p[0]), len(p[1])); }, false},
    {"chandelier", {{"n", {10, 20, 60}}, {"k", {2, 3, 4, 5}}}, 2, nullptr,
     [](Tape &t, const P &p) { return chandelier(t, len(p[0]), p[1]); }, false},
    {"rebound", {{"d", {0.5, 1, 1.5}}, {"n", {5, 10, 20}}, {"k", {2, 3, 4}}}, 3, nullptr,
     [](Tape &t, const P &p) { return rebound(t, p[0], len(p[1]), p[2]); }, false},
    {"revert", {{"L", {60, 120, 250}}, {"g", {0.5, 1, 1.5}}}, 2, nullptr,
     [](Tape &t, const P &p) { return revert(t, len(p[0]), p[1]); }, false},
    {"gate", {{"L", {60, 120, 250}}, {"g", {-1, -0.5, 0}}, {"f", {5, 10, 20}}, {"h", {0, 0.25, 0.5}}}, 4, nullptr,
     [](Tape &t, const P &p) { return gate(t, len(p[0]), p[1], len(p[2]), p[3]); }, false},
    {"band", {{"c", {0, 1}, {kCenters[0], kCenters[1]}}, {"w", {0, 1, 2}, {kWidths[0], kWidths[1], kWidths[2]}},
              {"n", {0.5, 2, 10, 60, 120}, {}, true}, {"k", {0.25, 0.5, 1, 2}}, {"N", {1, 4}}, {"h", {0, 0.5}}}, 3,
     [](const P &p) { return p[2] >= 5; },
     [](Tape &t, const P &p) { return band(t, static_cast<int>(p[0]), static_cast<int>(p[1]), len(p[2]), p[3], static_cast<int>(p[4]), p[5]); }, false},
};
// clang-format on
