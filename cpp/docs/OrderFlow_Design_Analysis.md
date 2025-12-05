# OrderFlow 数据流设计分析

## 1. 核心时间索引系统

### 1.1 时间映射定义 (FeaturesDefine.hpp)

**全局约定**:
- Trading seconds: `0-15299` (TRADE_SECONDS_PER_DAY = 15300)
- 早盘: `0-8099` → 09:15:00 - 11:29:59
- 午休: `8100` (映射点)
- 午盘: `8100-15299` → 13:00:00 - 14:59:59

**关键函数契约**:
```cpp
// FeaturesDefine.hpp:250-258
inline constexpr size_t tick2index(uint8_t hour, uint8_t minute, uint8_t second);
// Input: 时钟时间 (HH:MM:SS)
// Output: tick_idx ∈ [0, 15299]
// Contract: 午休时间映射到 8100

// FeaturesDefine.hpp:274-293
inline constexpr ClockTime index2tick(size_t index);
// Input: tick_idx ∈ [0, 15299]
// Output: 时钟时间 (HH:MM:SS)
// Contract: 双射关系（除午休映射点外一一对应）
```

**设计原则**:
- 时间索引是**全局唯一标识符**
- 所有时间相关操作必须通过 `tick2index`/`index2tick` 转换
- 午休时段处理：映射到午盘开始点，避免时间轴断裂

---

## 2. Tensor 存储假设

### 2.1 Tensor 结构定义 (FeatureStoreConfig.hpp)

**存储布局**:
```
tensor.T[level] = 该 level 的行数
level=0 (L0): 预期 T[0] = 15300 (密集存储)
level=1 (L1): 预期 T[1] = 255 (密集存储)
```

**访问模式**:
```cpp
// DataLoader.hpp:243
for (size_t t = 0; t < tensor.T[0]; ++t) {
  // t 的语义假设: t ∈ [0, tensor.T[0])
  // 访问: TENSOR_GET(tensor, 0, t, field_offset, asset_idx)
}
```

### 2.2 Tensor 行索引的两种可能解释

**解释 A: 密集时间索引** (当前代码假设)
```
t = 0     → tick_idx = 0     (09:15:00)
t = 1     → tick_idx = 1     (09:15:01)
...
t = 15299 → tick_idx = 15299 (14:59:59)
```
- **假设**: `tensor.T[0] == 15300`
- **假设**: 第 `t` 行对应 tick_idx = `t`
- **稀疏性**: 通过 `_depth_valid`/`_data_valid` 标记无效行

**解释 B: 稀疏事件索引** (可能的实际情况)
```
t = 0     → tick_idx = 5     (09:15:05, 第一个有效事件)
t = 1     → tick_idx = 12    (09:15:12, 第二个有效事件)
...
t = N-1   → tick_idx = 15280 (14:59:40, 最后有效事件)
t = N...  → 填充行 (valid=0)
```
- **假设**: `tensor.T[0]` 可能远小于 15300
- **需求**: 需要额外字段存储真实 tick_idx
- **问题**: 当前代码用 `t` 作为 `tick_idx` (第285行)

---

## 3. 数据加载层 (DataLoader.hpp)

### 3.1 L0 数据加载契约

**函数**: `load_l0` (第211-293行)

**输入假设**:
```cpp
// 第243行
for (size_t t = 0; t < tensor.T[0]; ++t) {
```
- `t`: tensor 的行索引
- 假设: `t` 可以直接用作 tick_idx

**输出契约**:
```cpp
// 第285行
day.push(t, depth_valid, data_valid, mid, bp, ap, bv, av);
```
- 存储 `tick_idx = t`
- 下游依赖: 所有使用 `tick.tick_idx` 的代码假设它是 tick_idx

**过滤逻辑**:
```cpp
// 第252-253行
if (!depth_valid && !data_valid)
  continue;
```
- 稀疏化策略: 跳过双 invalid 的行
- **关键**: 跳过后仍使用原 `t` 值

### 3.2 稀疏存储模式

**Day 结构** (OrderFlow.hpp:184-209):
```cpp
struct Day {
  std::vector<Tick> ticks;  // 稀疏数组
  
  struct Tick {
    size_t tick_idx;        // 存储的是 tick_idx
    bool depth_valid, data_valid;
    // ... 数据字段 ...
  };
};
```

**设计意图**:
- 只存储 valid 的 tick
- `tick_idx` 保留原始时间信息
- 通过 `tick_idx` 重建时间轴

---

## 4. 绘图缓存层 (OrderFlow.cpp)

### 4.1 PlotData 构建 (build_plot, 第348-399行)

**坐标转换**:
```cpp
// 第372行
double global_x = day.to_global_x(i);

// 第159-161行 (Day::to_global_x)
return static_cast<double>(day_idx * OrderFlowConst::L0_CAPACITY + ticks[i].tick_idx);
```

**关键假设**:
- `ticks[i].tick_idx` 是 tick_idx
- `global_x` 是跨天的全局 tick_idx
- 多天数据: day_0 ∈ [0, 15299], day_1 ∈ [15300, 30599], ...

**反向映射** (第379-382行):
```cpp
size_t global_tick_idx = day_base + tick.tick_idx;
if (global_tick_idx < plot.tick_idx_map.size()) {
  plot.tick_idx_map[global_tick_idx] = plot_idx;
}
```
- **目的**: O(1) 从 tick_idx 查找稀疏数组索引
- **大小**: `tick_idx_map.size() == num_days * 15300` (密集)
- **值**: `plot_idx` (稀疏数组中的位置)

### 4.2 HeatmapMerged 构建 (build_heatmap_merged, 第401-540行)

**时间坐标使用**:
```cpp
// 第475行
size_t global_tick_idx = day_base + tick.tick_idx;

// 第432行 (矩形扩展)
last_rect.tick_end = global_tick_idx + 1;

// 第459-463行 (新矩形)
level.rects.push_back({global_tick_idx,      // tick_start
                       global_tick_idx + 1,  // tick_end
                       price_high,
                       price_low,
                       amount_rmb});
```

**矩形时间语义**:
- `tick_start`, `tick_end`: 全局 tick_idx
- 闭区间: `[tick_start, tick_end)`
- **隐含假设**: 相邻 tick 间的空白由前向填充

### 4.3 HeatmapColored 构建 (build_heatmap_colored, 第584-616行)

**坐标输出**:
```cpp
// 第604-609行
double x1 = static_cast<double>(merged_rect.tick_start);
double x2 = static_cast<double>(merged_rect.tick_end);
double y1 = static_cast<double>(merged_rect.price_high);
double y2 = static_cast<double>(merged_rect.price_low);

heatmap_colored.rects.push_back({x1, y1, x2, y2, color});
```

**契约**:
- `x1`, `x2` 直接是 tick_idx
- 下游渲染层假设这些坐标可直接传给 ImPlot

---

## 5. 渲染层 (TabOrderFlow.cpp)

### 5.1 X 轴设置 (RenderL0Plot, 第222-235行)

**轴范围定义**:
```cpp
const size_t day_idx = of.l0.days.empty() ? 0 : of.l0.days[0].day_idx;
const double x_min = static_cast<double>(day_idx * OrderFlowConst::L0_CAPACITY);
const double x_max = static_cast<double>((day_idx + 1) * OrderFlowConst::L0_CAPACITY);

ImPlot::SetupAxisLimits(ImAxis_X1, x_min, x_max, cond);
```

**假设**:
- X 轴单位是 tick_idx
- 每天占用 `[day_idx*15300, (day_idx+1)*15300)` 区间

### 5.2 X 轴格式化器 (L0TimeFormatter, 第43-54行)

**转换逻辑**:
```cpp
const size_t global_x = static_cast<size_t>(value);
const size_t tick_idx = global_x % OrderFlowConst::L0_CAPACITY;

ClockTime ct = index2tick(tick_idx);

return std::snprintf(buff, size, "%02d:%02d", ct.hour, ct.minute);
```

**契约**:
- **输入**: ImPlot 传入的 X 坐标 (`value`)
- **假设**: `value` 是 tick_idx
- **输出**: 对应的时钟时间字符串

**调用时机**:
- Tick 标签显示
- Hover tooltip
- 任意 X 坐标的格式化

### 5.3 线图渲染 (第298-316行)

**数据绑定**:
```cpp
ImPlot::PlotStairs("Best Bid", of.l0.plot.x.data(), of.l0.plot.best_bid.data(),
                   static_cast<int>(of.l0.plot.x.size()));
```

**假设**:
- `plot.x[i]` 是 tick_idx
- ImPlot 的 Stairs 模式在稀疏点间前向填充

### 5.4 热力图渲染 (第287-291行)

**坐标使用**:
```cpp
for (const auto &rect : of.l0.heatmap_colored.rects) {
  ImVec2 p_min = ImPlot::PlotToPixels(rect.x1, rect.y1);
  ImVec2 p_max = ImPlot::PlotToPixels(rect.x2, rect.y2);
  draw_list->AddRectFilled(p_min, p_max, rect.color);
}
```

**假设**:
- `rect.x1`, `rect.x2` 是 tick_idx
- ImPlot 将其正确映射到屏幕坐标

---

## 6. 数据流契约总结

### 6.1 整体数据流

```
Tensor (密集?)              DataLoader               OrderFlow.Day (稀疏)
┌─────────────┐            ┌──────────┐            ┌──────────────┐
│ t=0: valid=1│            │          │            │ tick[0]:     │
│ t=1: valid=0│───filter──>│  跳过    │            │   tick_idx=0 │
│ t=2: valid=1│            │          │            │ tick[1]:     │
│ t=3: valid=1│            │          │            │   tick_idx=2 │
│ ...         │            │          │            │ tick[2]:     │
│ t=15299     │            │          │            │   tick_idx=3 │
└─────────────┘            └──────────┘            └──────────────┘
      ↓                                                    ↓
   假设 t 是                                         假设 tick_idx 是
 tick_idx                                    tick_idx
```

```
OrderFlow.Day              build_plot              PlotData
┌──────────────┐          ┌─────────┐            ┌──────────────┐
│ tick[i]:     │          │         │            │ x[j]:        │
│  tick_idx=k  │────────>│  转换   │──────────>│   global_x=k │
│  depth_valid │          │         │            │ y[j]:        │
│  mid_price   │          └─────────┘            │   mid_price  │
└──────────────┘                                  └──────────────┘
      ↓                                                  ↓
  稀疏存储                                          稀疏存储
 真实时间索引                                      保留时间索引
```

```
PlotData                   ImPlot                  屏幕
┌──────────────┐          ┌─────────┐            ┌──────────┐
│ x[]: trading_│          │ Stairs  │            │ 连续折线 │
│      second  │────────>│  模式   │──────────>│ (前向填充)│
│ y[]: price   │          │         │            │          │
└──────────────┘          └─────────┘            └──────────┘
                                ↓
                          L0TimeFormatter
                          ┌─────────────┐
                          │ x → HH:MM   │
                          └─────────────┘
```

### 6.2 关键假设链

**假设 1**: Tensor 行索引的语义
```
DataLoader.hpp:285
day.push(t, ...)
         └─ 假设: t 是 tick_idx
```

**假设 2**: tick_idx 的语义
```
OrderFlow.cpp:160
day_idx * L0_CAPACITY + ticks[i].tick_idx
                        └─ 假设: tick_idx 是 tick_idx
```

**假设 3**: plot.x 的语义
```
TabOrderFlow.cpp:298
ImPlot::PlotStairs(..., of.l0.plot.x.data(), ...)
                        └─ 假设: x 是 tick_idx
```

**假设 4**: L0TimeFormatter 的输入
```
TabOrderFlow.cpp:46
const size_t tick_idx = global_x % L0_CAPACITY;
                        └─ 假设: global_x 是 tick_idx
```

### 6.3 一致性要求

**所有层必须对 "索引" 有统一的解释**:

| 层 | 变量名 | 语义要求 | 验证方法 |
|---|---|---|---|
| Tensor | `t` | tick_idx ∈ [0,15299] | `tensor.T[0] == 15300` |
| Day | `tick_idx` | tick_idx ∈ [0,15299] | `tick_idx == 原 tensor 行号` |
| PlotData | `x[i]` | global tick_idx | `x[i] == day_idx*15300 + tick_idx` |
| Formatter | `value` | global tick_idx | `index2tick(value % 15300)` 有效 |

---

## 7. 稀疏性处理策略

### 7.1 存储层面

**策略**: 稀疏向量 + 时间索引
```cpp
// 不存储:
vector<float> prices[15300];  // 密集，浪费内存

// 而是存储:
struct Tick {
  size_t tick_idx;  // 时间索引
  float price;      // 数据
};
vector<Tick> ticks;  // 稀疏
```

### 7.2 查询层面

**策略**: 反向索引表
```cpp
// OrderFlow.cpp:379-382
vector<size_t> tick_idx_map;  // 大小: 15300 * num_days
tick_idx_map[tick_idx] = plot_idx;  // 映射: 密集索引 → 稀疏索引
```

**用途**:
- O(1) 从 tick_idx 查找对应数据
- 支持拖拽时的快速 snap

### 7.3 渲染层面

**策略**: ImPlot Stairs 模式 + 前向填充
```cpp
// TabOrderFlow.cpp:298
ImPlot::PlotStairs("Best Bid", x_data, y_data, count);
```

**行为**:
- 稀疏点: `(x[i], y[i])`, `(x[i+1], y[i+1])`
- 渲染: 在 `[x[i], x[i+1])` 区间绘制水平线 `y = y[i]`

---

## 8. 设计原则总结

### 8.1 时间索引统一原则

**原则**: 所有层使用相同的时间索引定义
- 全局唯一: tick_idx ∈ [0, 15299]
- 转换函数: `tick2index` / `index2tick`
- 禁止: 任意的时间表示方式

### 8.2 稀疏存储原则

**原则**: 只存储有效数据，但保留完整时间信息
- 存储: `(tick_idx, data)` 对
- 不存储: 无效时间点的占位符
- 索引: 通过 `tick_idx` 重建时间轴

### 8.3 契约明确原则

**原则**: 每层的输入输出语义必须明确
- 输入假设: 函数期望的数据语义
- 输出保证: 函数提供的数据语义
- 文档: 关键变量的语义注释

### 8.4 验证点

**关键验证**:
1. `tensor.T[0]` 的值和含义
2. tensor 第 `t` 行与 tick_idx 的对应关系
3. `tick_idx` 的赋值是否正确
4. formatter 接收的 X 坐标语义

---

## 9. 潜在的设计模糊点

### 9.1 Tensor 行号的双重解释

**可能性 A**: 行号即时间
```
tensor[t] 对应 tick_idx = t
要求: tensor.T[0] == 15300
```

**可能性 B**: 行号是序号
```
tensor[t] 是第 t 个有效事件
需要: 额外字段存储 tick_idx
```

**当前代码倾向**: 假设 A (第285行直接用 `t`)

### 9.2 稀疏填充的语义

**问题**: 相邻稀疏点之间的区间如何解释？

**热力图**: 扩展矩形覆盖整个区间
```cpp
// OrderFlow.cpp:432
last_rect.tick_end = global_tick_idx + 1;  // 一直扩展
```
- 语义: 挂单在区间内保持不变

**线图**: Stairs 前向填充
```cpp
// TabOrderFlow.cpp:298
ImPlot::PlotStairs(...)
```
- 语义: 价格在区间内保持不变

**一致性**: 两者语义对齐

### 9.3 多天数据的连接

**设计**: 全局偏移
```cpp
global_x = day_idx * 15300 + tick_idx
```

**假设**:
- 每天独立: `[0, 15299]`
- 跨天不连续: day_0 最后一个 tick 和 day_1 第一个 tick 之间有隔夜间隔
- 渲染: X 轴自动显示这个间隔

---

## 10. 代码审查检查清单

### 10.1 时间索引正确性

- [ ] tensor 行号 `t` 的语义是什么？
- [ ] `tick_idx` 的赋值是否使用了正确的时间信息？
- [ ] `global_x` 的计算是否正确？
- [ ] formatter 的输入假设是否与实际一致？

### 10.2 稀疏性处理

- [ ] 跳过无效行后，是否保留了正确的时间索引？
- [ ] 反向索引表的构建是否正确？
- [ ] 稀疏点之间的填充语义是否符合预期？

### 10.3 契约一致性

- [ ] 所有层对 "索引" 的理解是否一致？
- [ ] 数据传递时是否保持了语义不变？
- [ ] 坐标转换是否正确？

### 10.4 调试验证

**建议添加的断言**:
```cpp
// DataLoader.hpp
assert(tensor.T[0] == 15300 || "Tensor 不是密集存储");
assert(t < 15300 || "行号超出 tick_idx 范围");

// OrderFlow.cpp
assert(tick.tick_idx < 15300 || "tick_idx 超出范围");
assert(global_x >= 0 && global_x < num_days * 15300 || "全局坐标越界");

// TabOrderFlow.cpp
assert(value >= x_min && value <= x_max || "formatter 接收到越界坐标");
```

我遇到的现象：
L0 X 轴上的时间标签（如 09:30, 09:45, 10:00）显示正确
从图上看出来， 线图， 热力图都是对的， 画的都能对上
但是depth anchor显示的tick_idx/time 和正确的时间差15-5分钟， 不均匀
depth panel显示的数据也是对的， 但是是按照那个错误的时间来显示的
