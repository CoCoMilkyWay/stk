# 通达信（国金证券版）本地数据存储说明

安装目录软链：`/home/chuyin/work/tdx/zd_gjzq` → `~/.wine/drive_c/zd_gjzq`

## 目录树（与本机实际一致）

```
gjzq/
├── vipdoc/                      行情/K线/财务数据根目录
│   ├── sh/  sz/  bj/            沪/深/北 市场
│   │   ├── lday/    *.day       日线（本机：sh 4779 / sz 4279 / bj 331 个文件，合计 ~314M）
│   │   ├── minline/ *.lc1       1分钟线（本机 ~2.5G）
│   │   ├── fzline/  *.lc5       5分钟线（本机为空，需在客户端下载）
│   │   └── eday/    *.day       扩展日线（本机为空）
│   ├── ds/                      大商所相关（fzline/lday/minline，本机为空）
│   ├── ot/                      期权（空）
│   └── cw/                      财务数据
│       ├── gpcw.txt             季度财务包下载清单（zip 名 + md5 + size）
│       ├── gpszsh.txt           个股财务文件清单（gpsz*.dat 名 + md5 + size）
│       └── （gpcw*.zip / gpsz*.dat 本机尚未下载）
│
├── T0002/                       用户配置与缓存
│   ├── hq_cache/                行情元数据缓存（~71M）
│   │   ├── base.dbf             代码表（FoxBase DBF，14415 条 × 481 字段）
│   │   ├── base.map             base.dbf 索引/映射
│   │   ├── gbbq                 除权除息数据（~5.5M，新版有混淆，需社区方案解密）
│   │   ├── gbbq.map             gbbq 索引
│   │   ├── *.tnf / shs.tnf      代码表/板块配置
│   │   ├── spec*.txt            ETF/LOF/基金/事件等专题清单
│   │   └── *.dat / *.cfg        板块、券商、提示等缓存
│   ├── cw_cache/                财务缓存（本机为空）
│   ├── lc  lc_data/             历史缓存/数据
│   ├── wt_data  wt_cache/       委托/交易缓存
│   ├── zst_cache/               分时图缓存
│   ├── ds_cache  info_cache  ext_cache  cache  cloud_cache/  其它缓存（多为空）
│   ├── blocknew/  signals/      自定义板块/信号
│   ├── user.ini  usercomm.ini   用户配置
│   └── bigdata*.dax / *.zip     大数据/资讯包
│
├── connect.cfg                  连接配置（含 F10Cache1Day=1）
└── webs/  TCPlugins/  QHPlugins/  funcs/  funcs_jy/  dlls/  前端/插件/公式/交易
```

## K 线二进制结构（社区公开格式，可直接解析）

### `.day`（日线，32 字节/条，小端）

| 偏移 | 长度 | 类型 | 字段          |
| ---- | ---- | ---- | ------------- |
| 0    | 4    | u32  | 日期 YYYYMMDD |
| 4    | 4    | u32  | 开盘价 ×100   |
| 8    | 4    | u32  | 最高价 ×100   |
| 12   | 4    | u32  | 最低价 ×100   |
| 16   | 4    | u32  | 收盘价 ×100   |
| 20   | 4    | f32  | 成交额（元）  |
| 24   | 4    | u32  | 成交量（手）  |
| 28   | 4    | u32  | 保留          |

文件大小恒为 32 的整数倍；条数 = size / 32。
示例 `sh600000.day`：2021-08-02 ～ 2026-07-10，共 1196 条。

### `.lc1`（1 分钟线，32 字节/条，小端）

| 偏移 | 长度 | 类型 | 字段                                                                                       |
| ---- | ---- | ---- | ------------------------------------------------------------------------------------------ |
| 0    | 2    | u16  | 打包日期：`year = dpack/2048 + 2004`，`month = (dpack%2048)/100`，`day = (dpack%2048)%100` |
| 2    | 2    | u16  | 打包时间：`hh = tpack/60`，`mm = tpack%60`                                                 |
| 4    | 4    | f32  | 开盘价                                                                                     |
| 8    | 4    | f32  | 最高价                                                                                     |
| 12   | 4    | f32  | 最低价                                                                                     |
| 16   | 4    | f32  | 收盘价                                                                                     |
| 20   | 4    | f32  | 成交额                                                                                     |
| 24   | 4    | u32  | 成交量                                                                                     |
| 28   | 4    | u32  | 保留                                                                                       |

### `.lc5`（5 分钟线）

结构与 `.lc1` 相同，仅周期不同；本机 `fzline/` 为空，需在客户端补下载。

## 财务数据结构

### 当前状态
- `vipdoc/cw/` 仅含两个清单 txt，**实体 zip/dat 未下载**。
- `T0002/cw_cache/` 为空。
- 需在通达信客户端执行「财务数据下载」后，`vipdoc/cw/` 才会出现 `gpcw*.zip` / `gpsz*.dat`。

### 清单格式（CSV）
`vipdoc/cw/gpcw.txt`：每行 `文件名,md5,大小`，例如
```
gpcw20260331.zip,c92721db6083f55e8ee9e01a285b9032,5114834
```
`vipdoc/cw/gpszsh.txt`：每行 `文件名,md5,大小`，例如
```
gpsz302132.dat,142f0ad6289b09cc20d5af17483b7518,278226
```

### `gpcw*.dat`（下载并解压后）
通达信财务数据按报告期组织，单条记录较长（数百字节），包含每股收益、净资产、营收、净利润、现金流、十大股东等字段；字段布局随版本变化，解析需参照社区对应版本的字段表。本机暂无此文件，待下载后再补充字段表。

## 其它可用的元数据

### `T0002/hq_cache/base.dbf`
标准 FoxBase/dBase III DBF，14415 条记录，每条 481 字节，含代码、名称、市场等基础信息。可用标准 DBF 库（如 Python `dbfread`）直接读取。

### `T0002/hq_cache/gbbq` + `gbbq.map`
除权除息。新版 `gbbq` 经过混淆/加密，需采用社区公开的解密方案（按 stockcode 哈希做字节异或）后才能按定长记录解析；`gbbq.map` 为代码到文件偏移的索引。

## 可用性结论

| 数据              | 可用           | 说明                   |
| ----------------- | -------------- | ---------------------- |
| 日线 `.day`       | 是             | 格式稳定，直接解析     |
| 1 分钟 `.lc1`     | 是             | 注意覆盖区间未必全历史 |
| 5 分钟 `.lc5`     | 否（本机为空） | 需在客户端下载         |
| 财务 `gpcw*`      | 否（未下载）   | 需在客户端下载后再解析 |
| 代码表 `base.dbf` | 是             | 标准 DBF               |
| 除权除息 `gbbq`   | 需解密         | 按社区方案处理后可用   |

## 运行时数据（不落盘）

### 持仓明文表（`scripts/mem/`）
- 来源：客户端查过一次持仓后，解密明文驻留在 `TdxW.exe` 堆里，扫 `/proc/<pid>/mem` 即得。只扫可写私有匿名段（Wine 堆，约 580M，全量 0.23s）。
- 格式：表头 `0||<n>||P00`；`\r\n` 分行、`|` 分列、行首 6 位代码、GBK。列布局分完整（≥22 列）与精简（18 列）两种，完整视图列号已对照客户端持仓表核过，见 `positions.COLS`。
- 选块：每次查询在网络缓冲多留一份 `n=行数` 的应答副本（历史快照，永不更新）；客户端在用的活动块 `n=0`、全进程唯一，新查询到来即释放旧块、另建新块。只认活动块，不唯一即 assert。
- 换代：新旧活动块在同一堆段来回换址，重读先只扫该段（106M）；按址复读旧块比字节（约 0.03ms）即知客户端是否重查过（`positions.follow`）。点「刷新」后约 100ms 换代（`positions.wait`）。
- 接口：快照 `Snap(pid, win, raw, rows)` 为不可变值，`==` 即同一代；`read` 全量 / 段内，`follow` 按址盯换代，`wait` 轮询 `follow` 等客户端重查。
- 校验：连读两次一致才采信；代码 6 位、列数一致、沪深标志合法、无替换字符、市值 ≈ 现价 × 余额，任一不过即 assert（`positions.parse`）。
- 权限：`ptrace_scope≠0` 时用 `scripts/.sudo_pass` 自动放开（`common.ensure_access`）。

### 消息桥（`scripts/bridge/`）
- 构成：`shim.dll` 注入 `TdxW.exe` 后开 TCP 控制通道；Linux 侧 `shimctl.py` 负责编译、注入（`ensure`）与发命令（`Shim`）。
- 构建：产物按源码哈希命名 `shim_<H>.dll`，端口 `20000 + H % 10000`；改源码即换新 DLL、新端口，旧产物自动清理。
- 协议：请求一行 `cmd arg...`，应答 0..N 行 JSON 加末行 `ok` / `err <原因>`；`target` 为 `0x..` 或 `class=<类名>`。

| 类别   | 命令                                            |
| ------ | ----------------------------------------------- |
| 窗口树 | `tree` `enum` `find` `treegen` `treewatch`      |
| 控件   | `gettext` `settext` `rect` `detail` `highlight` |
| 动作   | `post` `send` `click` `clickat`                 |
| 输入   | `mouse` `key` `char`                            |
| 绘制层 | `gdi` `gdidump` `gdiwatch`                      |
| 其它   | `noraise` `spy` `ping` `help`                   |

- 窗口树：节点 `{depth,hwnd,id,class,title,rect,style,exstyle,vis,en}`；WinEvent 钩子维护变更版本号。
- 快照 / 推送成对：`treegen` / `treewatch`（版本号）、`gdidump` / `gdiwatch`（绘制层）；长连接无变更 1s 推心跳 `{}`。
- 图层记录：`{hwnd,off,items:[[l,t,r,b,v,文字?]]}`，客户区坐标；`detail` 与绘制层同形，`v` 分别为状态位与颜色。
- 子项：`detail` 在目标线程内一次读完 Button / ListBox / Header / ListView / TreeView / 弹出菜单的可见项。
- 输入：一律进程内 `PostMessage`，不动真实光标、不抢前台；鼠标投给捕获窗口或坐标下最深子窗口，键盘投给焦点窗口。
- 不抬窗：把各模块 IAT 里的 `NtUserSetWindowPos` / `NtUserDeferWindowPosAndBand` 换成去掉 z 序变化的包装；新弹出的菜单、对话框仍会到最前。
- 绘制层：自绘控件（`CFQS_*` / `MHPToolBar` / `Afx*`）上的按钮只是画出来的字。改写 gdi32/user32 的 18 个绘制函数入口（`TdxW.exe` 有 VMProtect 壳、不走 IAT；Wine 的 win32u 层已是字形索引，故钩 API 入口），按"后画盖先画"维护每个窗口可见的文字与色块。
- 管不到：内嵌 CEF 网页（消息中心等）。

### 控制台（`scripts/console.py` + `console.html`）
- 启动：`python3 scripts/console.py [port]`（默认 8765）是唯一入口，依次 `ensure`、`noraise on`、`gdi on`，再起 HTTP 服务并 `xdg-open` 用系统浏览器打开。
- 接口：`GET /api/ping|treewatch|gdiwatch|poswatch|slippage`；`POST /api/shim {cmd,args}` 透传 shim 命令，`/api/clicktext {text,class?,title?}` 点画着该文字的按钮，`/api/refresh` 点交易栏「刷新」并等内存换代。任何失败统一回 `{err}`。
- 布局图：按屏幕坐标与 z 序还原窗口，标准控件按类画、自绘窗口用绘制层；勾「交互」后鼠标、滚轮、键盘原样投给客户端。
- 列表：可折叠窗口树，自绘窗口下挂其绘制元素（每段文字一项）；与布局图双向 hover、选中。
- 持仓：内存持仓 × 新浪实时价，列为代码 / 仓位 / 名称 / 滑点小盘口 / 当日盈亏(%) / 盈亏(%) / 总市值 / 融资市值 / 可卖市值 / 担保折算率；点表头排序，数字右对齐，放得下即全展开。「刷新」点客户端刷新按钮，「自动刷新」兜底（默认 60s）；下方常驻 5 档滑点表。
- 下单：持仓行「买 / 卖」各 最小单位 / 1W / 5W 按钮，`POST /api/order` 后台走闪电窗（主窗敲代码切图表 → 打开闪电窗核代码 → 填价量 → 确认框核代码/价/量 → 读回报），`GET /api/ordwatch` 推每单步骤状态，卡住即停在该步、窗口留给人工。委托价（对手价 / 对手价偏 1% / 本方价）与买入方式（担保品 / 融资）为页头统一下拉。
- 工具：选中控件读写、点击、闪烁；原始 post / send；spy 抓消息。
- 同步：窗口树与绘制层各走一条 SSE；标题、列表内容等无事件的变化按「轮询 fps」补拉。

### 持仓推送（`GET /api/poswatch`）
- 单一后台循环 `Hub`：每 100ms 复读活动块，换代即重读内存；有订阅者时每 1.5s 取一次行情；内存或行情变了才算一帧推给全部订阅者，15s 无帧推心跳 `{}`。每 60s 全量扫一次，补全进程唯一性校验。
- 列口径：量、成本、券商盈亏取自内存快照，市值 / 盈亏按 Δ价 × 量 平移（保留券商成本口径）；盈亏% = 现价 / 参考成本价 − 1；当日盈亏 = (现价 − 昨收) × 量，今日有买卖的留空；仓位分母由快照「市值 / 仓位」反推再随市值平移。
- 前端仅在持仓页可见且标签在前台时订阅，断开即停取行情。

### 行情（新浪 `hq.sinajs.cn`）
- `pytdx` 已不可用：各行情服务器（含客户端自己连的国金服务器）对其报价请求一律回 0 只。
- 新浪批量接口一次取全部持仓的现价、昨收、5 档（量单位股），需带 `Referer`；keep-alive 长连接（约 31ms/次），闲置 10s 重建。全进程只此一路请求，开几个页面都不加量，约 40 次/分，防限频。

### 5 档盘口滑点（`GET /api/slippage`）
新浪 5 档，相对 micro price 计算持仓在 10W/5W/1W 买、1W/5W/10W 卖下的滑点，附 ±1%/±2% 价位、实时市值占比与市值加权 aggregate。集合竞价（卖一 ≤ 买一）不出滑点。

### 已探索未完成
- 程序化下单：闪电窗 UI 自动化已接线、未实机跑通（键盘切代码一步最没把握）；卖券还款的命令 id 未录到。
- 资金 / 净值：与持仓是不同的内存块，布局未探。
- 交易插件：`TCPlugins/AddinFlatJy.dll` 的 `Addin_GetObject` 各 vtable slot 已用 uprobe 观察，未落地。
- 交易抓包：透明代理录到出站 7703 的字节流，协议未解析。

## 脚本

```
scripts/
├── console.py       唯一入口：Web 控制台后端
├── console.html     控制台前端（布局图 / 持仓 / 列表 / 工具）
├── bridge/          消息桥（通用基建）
│   ├── shimctl.py   编译 / 注入 / 控制通道
│   └── shim/
│       ├── shim.c   注入 DLL：行命令 → JSON 记录
│       ├── inject.c 注入器（远程 LoadLibraryA）
│       └── .clangd  交叉编译配置
└── mem/             读内存（依赖具体布局）
    ├── common.py    找进程 / 读堆段 / ptrace 放权 / 连读一致
    └── positions.py 持仓明文表：选活动块 / 换代检测 / 解析 / 校验
```

- 本地文件（gitignore）：构建产物 `shim_<H>.dll` / `inject.exe`、sudo 密码 `.sudo_pass`。
- 工具链：`shimctl.py` 优先 clang + mingw 运行时，次选 `i686-w64-mingw32-gcc`，都没有就 `sudo apt-get install`。

## 注意事项
- 软件运行时可能正在写入这些文件，读取建议在收盘后或关闭客户端后进行，避免读到半写状态。
- 个人本地缓存用于研究/回测一般可行，但行情/财务数据不得二次分发。
- 文件格式信息来源于社区逆向公开资料，不同版本可能存在差异，解析时以本机实际字节为准并加断言校验。
