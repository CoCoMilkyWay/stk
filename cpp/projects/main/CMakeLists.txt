cmake_minimum_required(VERSION 3.25)
project(Main VERSION 1.0.0 LANGUAGES CXX C)

# ============================================================================
# Toolchain: Linux + Clang + C++23
# ============================================================================
if(NOT LINUX)
    message(FATAL_ERROR "只支持 Linux (vendored Arrow 只有 .so, 链接走 lld). Current system: ${CMAKE_SYSTEM_NAME}")
endif()
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "This project requires Clang compiler. Current compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

get_filename_component(CPP_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../.. ABSOLUTE)
set(SRC_ROOT  ${CPP_ROOT}/src)
set(INC_ROOT  ${CPP_ROOT}/include)
set(PKG_ROOT  ${CPP_ROOT}/package)
set(FONT_ROOT ${CPP_ROOT}/fonts)

# ============================================================================
# 选择轴: 一轴 = cache 名 + 取值集 (首项为默认) + 每取值一行表 <轴>_<取值>_<列>
#   axis(<轴> <取值>...)        声明 + 断言取值合法 (py/main.py 传 -D<轴>=<取值>)
#   axis_pick(<轴> <列>...)     <轴>_<列> = <轴>_<当前取值>_<列>
#   三轴两两正交; 轴内互斥由"一个 cache 变量只有一个值"自然保证
# ============================================================================
function(axis name)
    set(${name} ${ARGV1} CACHE STRING "${ARGN}")
    set_property(CACHE ${name} PROPERTY STRINGS ${ARGN})
    if(NOT ${name} IN_LIST ARGN)
        message(FATAL_ERROR "${name} = ${${name}}, 须为 ${ARGN} 之一")
    endif()
    set(${name}_VALUES ${ARGN} PARENT_SCOPE)
endfunction()

function(axis_pick name)
    foreach(col ${ARGN})
        set(${name}_${col} "${${name}_${${name}}_${col}}" PARENT_SCOPE)
    endforeach()
    message(STATUS "${name}: ${${name}}")
endfunction()

axis(BUILD_MODE  PRODUCTION TSAN DEBUG PROFILE ASSERT)
axis(GUI_BACKEND OpenGL Vulkan)
axis(GPU_BACKEND Stub CUDA)
set(CUDA_ARCH "native" CACHE STRING "GPU 架构算力号, native = 探测本机 GPU")

# 模式表: 五模式同形, 四列
#   OPT  优化级        C / C++ / CUDA
#   CC   调试 / 插桩   C / C++ (nvcc 的 -forward-unknown-to-host-compiler 会把 clang 专属 flag 转给 gcc 宿主)
#   LD   链接
#   DEF  宏            C / C++ / CUDA (NDEBUG 只在 PRODUCTION: 其余模式 .cu 的断言同样生效)
set(BUILD_MODE_TSAN_OPT         -O1)
set(BUILD_MODE_TSAN_CC          -g -fsanitize=thread -fno-omit-frame-pointer)
set(BUILD_MODE_TSAN_LD          -fsanitize=thread)
set(BUILD_MODE_TSAN_DEF         TSAN_MODE)

set(BUILD_MODE_DEBUG_OPT        -O0)
set(BUILD_MODE_DEBUG_CC         -g)
set(BUILD_MODE_DEBUG_LD         "")
set(BUILD_MODE_DEBUG_DEF        DEBUG_MODE)

set(BUILD_MODE_PROFILE_OPT      -O2)
set(BUILD_MODE_PROFILE_CC       -g)
set(BUILD_MODE_PROFILE_LD       "")
set(BUILD_MODE_PROFILE_DEF      PROFILE_MODE)

set(BUILD_MODE_ASSERT_OPT       -O2)
set(BUILD_MODE_ASSERT_CC        "")
set(BUILD_MODE_ASSERT_LD        "")
set(BUILD_MODE_ASSERT_DEF       ASSERT_MODE)

set(BUILD_MODE_PRODUCTION_OPT   -O2)
set(BUILD_MODE_PRODUCTION_CC    "")
set(BUILD_MODE_PRODUCTION_LD    "")
set(BUILD_MODE_PRODUCTION_DEF   NDEBUG)

# GUI 后端表: SRC 入口 / IMPL imgui 后端 (package/imgui/backends/imgui_impl_<IMPL>.cpp) / LINK 图形库; find_package(<取值>)
set(GUI_BACKEND_OpenGL_SRC      gui/Run_OpenGL.cpp)
set(GUI_BACKEND_OpenGL_IMPL     opengl3)
set(GUI_BACKEND_OpenGL_LINK     OpenGL::GL)

set(GUI_BACKEND_Vulkan_SRC      gui/Run_Vulkan.cpp)
set(GUI_BACKEND_Vulkan_IMPL     vulkan)
set(GUI_BACKEND_Vulkan_LINK     Vulkan::Vulkan)

# GPU 后端表: SRC GpuRun.hpp 的实现 / LINK 运行时
set(GPU_BACKEND_Stub_SRC        factor/op_check_gpu_stub.cpp)
set(GPU_BACKEND_Stub_LINK       "")

set(GPU_BACKEND_CUDA_SRC        factor/op_check_gpu.cu)
set(GPU_BACKEND_CUDA_LINK       CUDA::cudart)

# ============================================================================
# CUDA (GPU_BACKEND=CUDA): 先于一切编译参数启用, 模式表对 C / C++ / CUDA 一视同仁
# ============================================================================
# native 须本机有可见 NVIDIA GPU (驱动已装); 探不到 → 本次配置退回 Stub (缓存里的 CUDA 不动, 插卡后重配即恢复).
# 显式给算力号 = 交叉编译, 不探测
if(GPU_BACKEND STREQUAL "CUDA" AND CUDA_ARCH STREQUAL "native")
    execute_process(COMMAND nvidia-smi -L RESULT_VARIABLE CUDA_SMI_RC OUTPUT_VARIABLE CUDA_SMI_OUT ERROR_QUIET)
    if(NOT CUDA_SMI_RC EQUAL 0 OR NOT CUDA_SMI_OUT MATCHES "GPU [0-9]+:")
        message(WARNING "factor: 本机无可见 NVIDIA GPU (nvidia-smi -L 失败), GPU 后端回退 Stub")
        set(GPU_BACKEND Stub)
    endif()
endif()

if(GPU_BACKEND STREQUAL "CUDA")
    # 必须走 nvcc: clang 21 最高只认到 CUDA 12.8, 本机是 13.x (13 删掉了 clang 依赖的
    # texture_fetch_functions.h 等旧头), -x cuda 直接编不过.
    # 宿主侧仍是 clang++ -std=c++23, CUDA 侧是 nvcc+g++ -std=c++20;
    # 两边只通过 GpuRun.hpp 的 POD 接口相见, libstdc++ ABI 相同, 可直接链接.
    # apt 的 cuda-toolkit-* 装到 /usr/local/cuda-<ver>/ 并挂 /usr/local/cuda 软链, 但不进 PATH;
    # 这里显式给上, 省掉每个终端的 export. 先探再 enable: enable_language(CUDA) 只认 PATH,
    # 且找不到 nvcc 时只抛 CMake 内部错.
    find_program(CMAKE_CUDA_COMPILER nvcc PATHS /usr/local/cuda/bin)
    if(NOT CMAKE_CUDA_COMPILER)
        message(FATAL_ERROR "找不到 nvcc: sudo apt install cuda-toolkit-13-0 (NVIDIA apt 源)\n")
    endif()
    set(CMAKE_CUDA_ARCHITECTURES ${CUDA_ARCH})
    enable_language(CUDA)
    set(CMAKE_CUDA_STANDARD 20)
    set(CMAKE_CUDA_STANDARD_REQUIRED ON)
    find_package(CUDAToolkit REQUIRED)
    # CUB 头 (TS/CS Gpu.cuh 的 #include <cub/cub.cuh>): CUDA 13 起从 include/cub/ 挪到
    # include/cccl/cub/, nvcc 自动把 include/cccl 加进搜索路径, 故源码里写法不变.
    # 两种布局都认; 都找不到 = toolkit 装残了 (pip 的 nvidia-* 运行时包不含 CUB), 配置期就炸.
    find_path(CUDA_CUB_DIR cub/cub.cuh
        PATHS ${CUDAToolkit_INCLUDE_DIRS} ${CUDAToolkit_TARGET_DIR}/include
        PATH_SUFFIXES cccl
        NO_DEFAULT_PATH)
    if(NOT CUDA_CUB_DIR)
        message(FATAL_ERROR "缺 cub/cub.cuh: sudo apt install cuda-toolkit-13-0 (NVIDIA apt 源)\n")
    endif()
    message(STATUS "factor: GPU 后端 CUDA (arch=${CUDA_ARCH}, CUDA ${CUDAToolkit_VERSION}, CUB ${CUDA_CUB_DIR}) → op_check + app_main")
else()
    message(STATUS "factor: GPU 后端 Stub (op_check 两方对拍; GUI Operators 页 GPU 列 n/a)")
endif()

axis_pick(BUILD_MODE  OPT CC LD DEF)
axis_pick(GUI_BACKEND SRC IMPL LINK)
axis_pick(GPU_BACKEND SRC LINK)
find_package(${GUI_BACKEND} REQUIRED)

# ============================================================================
# 全局编译参数: 只出自模式表 + 下方数学档, 构建类型自带的 flag 一律清空 (含 CUDA 的 -O3 -DNDEBUG)
# ============================================================================
get_property(BUILD_LANGS GLOBAL PROPERTY ENABLED_LANGUAGES)
foreach(lang ${BUILD_LANGS})
    foreach(config RELEASE DEBUG RELWITHDEBINFO MINSIZEREL)
        set(CMAKE_${lang}_FLAGS_${config} "")
    endforeach()
endforeach()

# genex 必须整体加引号, 否则里面的 ; 会先被当成 add_compile_options 的参数分隔符
set(CC_FLAGS ${BUILD_MODE_CC} -march=native -fcolor-diagnostics -fansi-escape-codes -Wno-overriding-option)
add_compile_options(${BUILD_MODE_OPT} "$<$<COMPILE_LANGUAGE:C,CXX>:${CC_FLAGS}>")
add_compile_definitions(${BUILD_MODE_DEF})
# 链接加速: lld (LLVM 自带, 并行) — 增量迭代每轮都要重链, 默认 GNU ld 是常数项瓶颈
add_link_options(${BUILD_MODE_LD} -fuse-ld=lld)

# 第三方头 (zstd / zlib / microtex / tracy / arrow) 随各自目标的 usage requirements 走, 不进全局
include_directories(${INC_ROOT} ${PKG_ROOT} ${CMAKE_CURRENT_BINARY_DIR}/generated)
# Boost 头在 C++23 下会触发 -Wdeprecated-declarations 等, 用 SYSTEM 抑制第三方头警告.
include_directories(SYSTEM ${PKG_ROOT}/boost)

# 断言 ARGN 各表两两不交, 且并集 = ${all}; 成员判定走标志变量 (O(1)): in_all_<文件> / in_part_<文件> = 所在表
function(assert_partition what all)
    foreach(file ${${all}})
        set(in_all_${file} TRUE)
    endforeach()
    foreach(part ${ARGN})
        foreach(file ${${part}})
            if(NOT DEFINED in_all_${file})
                message(FATAL_ERROR "${what}: ${part} 里的 ${file} 不存在")
            endif()
            if(DEFINED in_part_${file})
                message(FATAL_ERROR "${what}: ${file} 重复归类 (${in_part_${file}} / ${part})")
            endif()
            set(in_part_${file} ${part})
        endforeach()
    endforeach()
    foreach(file ${${all}})
        if(NOT DEFINED in_part_${file})
            message(FATAL_ERROR "${what}: ${file} 未归类, 须归入 ${ARGN} 之一")
        endif()
    endforeach()
endfunction()

# ============================================================================
# 源表: src/ 全量按目标分区 (配置期断言不重不漏)
#   SRC_OP_CHECK            因子算子对拍 CLI
#   <轴>_<取值>_SRC          GUI_BACKEND / GPU_BACKEND 每个取值一份, 当前取值入链
#   SRC_APP                 其余全部 → app_main
# ============================================================================
file(GLOB_RECURSE SRC_ALL CONFIGURE_DEPENDS RELATIVE ${SRC_ROOT}
    ${SRC_ROOT}/*.cpp
    ${SRC_ROOT}/*.c
    ${SRC_ROOT}/*.cu)

set(SRC_OP_CHECK factor/op_check.cpp)
set(SRC_APP      ${SRC_ALL})
set(SRC_PARTS    SRC_APP SRC_OP_CHECK)
list(REMOVE_ITEM SRC_APP ${SRC_OP_CHECK})
foreach(axis GUI_BACKEND GPU_BACKEND)
    foreach(value ${${axis}_VALUES})
        list(REMOVE_ITEM SRC_APP ${${axis}_${value}_SRC})
        list(APPEND SRC_PARTS ${axis}_${value}_SRC)
    endforeach()
endforeach()
assert_partition(源表 SRC_ALL ${SRC_PARTS})

# ============================================================================
# 数学档: src/ 每个文件恰属一档 (配置期断言不重不漏; 新文件必须先在此归档); 三方库一律 PRECISE (见 Vendor 段)
#   FAST     -ffast-math: 不写 isnan, 无定义用位图 / 守卫显式表示
#   PRECISE  NaN / inf 有语义: 缺失值 (api / parquet / 基本面) / 退化判据 (factor 契约头 isfinite) / microtex POS_INF
#            -fno-fast-math 会把 clang 在 Linux 上默认的 -fmath-errno 开回来: sqrt/log/pow 变成带 errno 旁路的 libm 调用,
#            含它们的内层循环整段不能自动向量化 (factor CPU 后端吞吐掉一个量级). 没人读 errno, 单独关掉;
#            IEEE / NaN / inf 语义不受影响 (只影响 errno 写不写).
#   .cu 经 nvcc -Xcompiler 转给宿主编译器 (逗号分隔多个宿主参数)
# ============================================================================
set(MATH_FAST_FLAGS    -ffast-math)
set(MATH_PRECISE_FLAGS -fno-fast-math -fno-math-errno)

# 两表同一段序 (shared → misc → api → fundamental → codec → features → worker → gui → agent → factor → 入口), 段内按路径序; 某档无文件的段留空
set(MATH_FAST
    # shared
    shared/Asset.cpp
    shared/Feature.cpp
    shared/OrdersDay.cpp
    # misc
    misc/archive.cpp
    misc/file_check.cpp
    misc/logging.cpp
    misc/perf.cpp
    misc/system_metrics.cpp
    # api
    # fundamental
    # codec
    codec/L2_Validator.cpp
    codec/OrderEncoder.cpp
    codec/OrderFrame.cpp
    # features
    features/FeatureMeta.cpp
    # worker: Lob 档管线 + 档无关的截面 / 落盘
    worker/crosssectional_worker.cpp
    worker/encoding_worker.cpp
    worker/feature_progress.cpp
    worker/io_worker.cpp
    worker/lob_pipeline.cpp
    # gui
    gui/Gui.cpp
    gui/Run_OpenGL.cpp
    gui/Run_Vulkan.cpp
    gui/Tasks.cpp
    gui/task_database/services/AuditService.cpp
    gui/task_database/services/EncodingService.cpp
    gui/task_database/ui/TabBrowser.cpp
    gui/task_database/ui/TabEncode.cpp
    gui/task_database/ui/TabOverview.cpp
    gui/task_features/TaskFeatures.cpp
    gui/task_features/services/ComputeService.cpp
    gui/task_features/services/CorrService.cpp
    gui/task_features/services/DistService.cpp
    gui/task_features/services/PreviewService.cpp
    gui/task_features/services/TransformService.cpp
    gui/task_features/ui/TabCompute.cpp
    gui/task_icon_bar/NetworkService.cpp
    gui/task_icon_bar/TaskIconBar.cpp
    gui/task_settings/TaskSettings.cpp
    gui/task_system_info/TaskSystemInfo.cpp
    # agent
    # factor
    # 入口
    main.cpp
)

set(MATH_PRECISE
    # shared
    shared/Analysis.cpp
    shared/AssetAxis.cpp
    shared/AssetInfo.cpp
    shared/Config.cpp
    shared/Corr.cpp
    shared/Dist.cpp
    shared/OrderFlow.cpp
    shared/Pool.cpp
    shared/Preview.cpp
    shared/Transform.cpp
    # misc
    misc/date.cpp
    misc/format.cpp
    misc/fs.cpp
    misc/parquet.cpp
    # api
    api/bigquant/client.cpp
    api/tushare/client.cpp
    # fundamental
    fundamental/Audit.cpp
    fundamental/Remote.cpp
    fundamental/Scan.cpp
    fundamental/Spec.cpp
    fundamental/Store.cpp
    fundamental/Sync.cpp
    # features
    features/Bar1mDay.cpp
    features/Method/CS.cpp
    features/Method/Fundamental.cpp
    # worker: Bar 档管线 (TS: Fund 缺失值 / PoolKey 母集 / cs_valid 依赖 NaN 比较); 采样 meta.json (nlohmann 序列化 isfinite)
    worker/bar_pipeline.cpp
    worker/feature_profile.cpp
    # gui
    gui/task_database/TaskDatabase.cpp
    gui/task_database/services/AssetLoader.cpp
    gui/task_database/services/FundamentalService.cpp
    gui/task_database/services/ScanService.cpp
    gui/task_database/services/StateManager.cpp
    gui/task_database/ui/CrossSectionAnalysis.cpp
    gui/task_database/ui/TabTable.cpp
    gui/task_features/services/OrderFlowService.cpp
    gui/task_features/services/ReplayService.cpp
    gui/task_features/ui/Common.cpp
    gui/task_features/ui/TabCorr.cpp
    gui/task_features/ui/TabDist.cpp
    gui/task_features/ui/TabFeature.cpp
    gui/task_features/ui/TabOrderFlow.cpp
    gui/task_features/ui/TabTransform.cpp
    # gui: 因子契约头 (Contract.hpp: isfinite 入口/出口) 的消费者
    gui/task_factors/TaskFactors.cpp
    gui/task_factors/services/FactorsBook.cpp
    gui/task_factors/services/FactorsEngine.cpp
    gui/task_factors/services/FactorsFile.cpp
    gui/task_factors/services/FactorsLoad.cpp
    gui/task_factors/services/FactorsProbe.cpp
    gui/task_factors/services/FactorsScope.cpp
    gui/task_factors/services/FactorsTable.cpp
    gui/task_factors/services/InspectService.cpp
    gui/task_factors/services/KlineService.cpp
    gui/task_factors/services/LensService.cpp
    gui/task_factors/services/OperatorsService.cpp
    gui/task_factors/services/PortfolioService.cpp
    gui/task_factors/services/RunService.cpp
    gui/task_factors/services/SearchService.cpp
    gui/task_factors/ui/TabFactors.cpp
    gui/task_factors/ui/TabInspect.cpp
    gui/task_factors/ui/TabOperators.cpp
    gui/task_factors/ui/TabPortfolio.cpp
    # gui: microtex 头 (nums.h: POS_INF = infinity()) 的消费者, -ffinite-math-only 下是 UB
    gui/task_tools/TaskTools.cpp
    gui/task_tools/ui/TabLatex.cpp
    gui/util/Latex.cpp
    # agent: Bridge = agent 里唯一的因子契约头消费者; Views = 观察的统计 (NaN 即无效); Docs = 研报统计 (原文没给 = NaN); 其余 = nlohmann 消费者 (请求 / 回复 / 落盘 JSON, isfinite)
    agent/Acts.cpp
    agent/Board.cpp
    agent/Bridge.cpp
    agent/Chat.cpp
    agent/Docs.cpp
    agent/Journal.cpp
    agent/Llm.cpp
    agent/Mine.cpp
    agent/Notes.cpp
    agent/Prompt.cpp
    agent/Tools.cpp
    agent/Views.cpp
    agent/ui/TaskAgent.cpp
    # factor: 因子契约头的消费者 = op_check 本体与 GPU 后端
    factor/op_check.cpp
    factor/op_check_gpu.cu
    factor/op_check_gpu_stub.cpp
    # 入口
)
assert_partition(数学档 SRC_ALL MATH_FAST MATH_PRECISE)

# 数学档 → 编译参数: C / C++ 直给, CUDA 经 -Xcompiler 逗号拼接转给宿主; src 按文件 / vendor 按目标, 同一份
function(math_options out math)
    set(cc ${MATH_${math}_FLAGS})
    list(TRANSFORM cc PREPEND "$<$<COMPILE_LANGUAGE:C,CXX>:")
    list(TRANSFORM cc APPEND ">")
    list(JOIN MATH_${math}_FLAGS "," nvcc)
    set(${out} ${cc} "$<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=${nvcc}>" PARENT_SCOPE)
endfunction()

foreach(math FAST PRECISE)
    math_options(math_opts ${math})
    set(math_files ${MATH_${math}})
    list(TRANSFORM math_files PREPEND ${SRC_ROOT}/)
    set_source_files_properties(${math_files} PROPERTIES COMPILE_OPTIONS "${math_opts}")
endforeach()

# ============================================================================
# 特征档 (tier) = 数据源, 一档一套节点表 / 字段表 / 特征库 (C++ 侧档注册见 FeaturesDefine.hpp TIER_LEVELS):
#   Lob  Node/Lob/{TS,CS}/<分类>/   Level-2 逐笔 → 订单簿还原 (池轴; features/<pool>/)
#   Bar  Node/Bar/{TS,CS}/<分类>/   分钟 K (bar1m, 非订单簿还原) + 基本面 (全轴; features/all/)
#   两档共用的节点类放 Node/Common/<分类>/ (只许 class, 不许 NODE_ / FIELDS_), 各档节点文件 include 它再各写各的 NODE_ / FIELDS_.
#
# 每档生成物 (build 目录 generated/features/<Tier>/), 切开"字段表消费者"与"节点代码消费者"的编译依赖:
#   FieldsGenerated.hpp  宏文本汇总, 不 #include 任何节点头 —— NODE_/FIELDS_/helper 宏
#                        原样抽自节点文件 (逐 token 一致) + <Tier>_NODES(N) + <Tier>_L*_FIELDS(X) + <tier>::node_deps.
#                        消费者 = FeatureStoreConfig (写端布局/指纹) / FeatureMeta.cpp (GUI 元数据).
#                        只改节点实现体 → 本文件内容不变 (configure_file 不重写) → 消费者不重编.
#   NodesGenerated.hpp   本档全部节点 #include + <Tier>_NODE_PREV_* 声明链 —— 只给本档构图端用.
#                        一个 TU 只许 include 一档的 NodesGenerated (两档节点文件的 NODE_ 宏同名).
#   FeatureCategoriesGenerated.hpp  本档一级 / 二级分类表 (namespace <tier>)
#   两档 FieldsGenerated 同处一个 TU (FeatureMeta.cpp / FeatureStoreConfig), 而节点 / 字段 owner 可同名
#   (Fund / Kline / Meta) 且内容不同 —— 所以内联的 NODE_ / FIELDS_ 宏改名加档前缀 (<Tier>_NODE_x / <Tier>_FIELDS_L_1min_x);
#   helper 宏原名内联, 两档同名必须逐字一致 (否则 FATAL_ERROR).
#   节点实例和落盘列直接写在节点文件里 (TS/ 时序节点, CS/ 截面列; 方法在 Method/):
#     #define NODE_<Name>(N)         N(<Name>, (NodeType), (ctor inputs), compute_trigger, flush_trigger)
#     #define FIELDS_<LVL>_<Owner>(X, CAT1) X(code, CAT1, ...)   LVL ∈ DAG_LEVELS; Owner = 节点名, 或非节点列 (CS/LABEL/FLAG) 的任意名字
#   依赖 = ctor inputs 里出现的 "<Up>." (Up.out() / Depth.bid_qty ...), 不需要任何 include.
#   此处: glob → 正则抽 NODE_/FIELDS_ → 分层拓扑排序 (同层按发现序 = 路径序) → 套 templates/<文件>.hpp.in 生成
#     <Tier>_NODES(N) / <Tier>_NODE_PREV_<Name> (声明链, 让顺序错误变成编译错误) /
#     <Tier>_<LVL>_FIELDS(X) 和 #include 清单.
#   CMAKE_CONFIGURE_DEPENDS 盯住每个文件内容: 改动后下次 build 自动重新 configure.
# ============================================================================
set(DAG_SCAN_ROOT     ${INC_ROOT}/features)
set(DAG_TEMPLATE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/templates)
# 落盘层名 (与 FeaturesDefine.hpp FeatureLevel / TIER_LEVELS 一致)
set(DAG_LEVELS L_sec L_1min L_5min L_15min L_30min L_60min L_1day)
string(REPLACE ";" "|" DAG_LEVEL_RE "${DAG_LEVELS}")

# src 的全部 #define/#undef 行 → out_text (#define 加 #ifndef 守卫: 节点头先 include 时跳过; 整段冠 "// ---- rel ----", 无宏为空)
#                                 + out_names (原名, 发现序; NodesGenerated 先 #undef 再 include 节点头, 免重复定义诊断).
# 分号先换占位符 (CMake 列表分隔符), 逐行还原.
# tier 非空: NODE_ / FIELDS_ 改名加档前缀 (见段首); helper 原名, 跨档同名须逐字一致 (GLOBAL 属性记首见定义)
function(dag_inline_macros src rel tier out_text out_names)
    string(REPLACE ";" "@SEMICOLON@" src_nosemi "${src}")
    string(REGEX MATCHALL "(^|\n)[ \t]*#[ \t]*(define|undef)[ \t][^\n]*" macro_lines "${src_nosemi}")
    set(text "")
    set(names "")
    foreach(macro_line ${macro_lines})
        string(REGEX REPLACE "^\n" "" macro_line "${macro_line}")
        string(REPLACE "@SEMICOLON@" ";" macro_line "${macro_line}")
        if(NOT macro_line MATCHES "#[ \t]*define[ \t]+([A-Za-z0-9_]+)")
            string(APPEND text "${macro_line}\n")
            continue()
        endif()
        set(macro_name ${CMAKE_MATCH_1})
        list(APPEND names ${macro_name})
        if(tier AND macro_name MATCHES "^(NODE|FIELDS)_")
            string(REGEX REPLACE "#([ \t]*)define([ \t]+)(NODE|FIELDS)_" "#\\1define\\2${tier}_\\3_" macro_line "${macro_line}")
            set(macro_name ${tier}_${macro_name})
        elseif(tier)
            get_property(helper_seen GLOBAL PROPERTY DAG_HELPER_${macro_name} SET)
            if(NOT helper_seen)
                set_property(GLOBAL PROPERTY DAG_HELPER_${macro_name} "${macro_line}")
            endif()
            get_property(helper_text GLOBAL PROPERTY DAG_HELPER_${macro_name})
            if(NOT helper_text STREQUAL macro_line)
                message(FATAL_ERROR "helper 宏 ${macro_name} 在两档定义不一致 (${rel}); 两档 FieldsGenerated 同处一个 TU, 同名须逐字一致或改名")
            endif()
        endif()
        string(APPEND text "#ifndef ${macro_name}\n${macro_line}\n#endif\n")
    endforeach()
    if(NOT text STREQUAL "")
        set(text "// ---- ${rel} ----\n${text}\n")
    endif()
    set(${out_text} "${text}" PARENT_SCOPE)
    set(${out_names} "${names}" PARENT_SCOPE)
endfunction()

# 逐项套 prefix / suffix, 以 sep 拼接 (生成文件里的表行 / 初始化列表)
function(dag_join out sep prefix suffix)
    set(items "")
    foreach(item ${ARGN})
        list(APPEND items "${prefix}${item}${suffix}")
    endforeach()
    list(JOIN items "${sep}" joined)
    set(${out} "${joined}" PARENT_SCOPE)
endfunction()

# 读节点头 → 续行拼成一行; 读即登记 CMAKE_CONFIGURE_DEPENDS (内容一改, 下次 build 自动重新 configure)
function(dag_read file out)
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${file})
    file(READ ${file} src)
    string(REGEX REPLACE "\\\\\n" " " src "${src}")
    set(${out} "${src}" PARENT_SCOPE)
endfunction()

# Common 只放共用 class: 有 NODE_ / FIELDS_ 宏即配置期报错 (实例必须落在某一档).
# 其 #define/#undef 行 (两档共用的 helper 宏, 如 LABEL_GROUPS) 先内联进每档 FieldsGenerated: 字段表宏可引用它们, 而 FieldsGenerated 不 include 节点头.
# 不进 NodesGenerated 的 #undef 清单: Common 头 #pragma once, 若已被 include 过, 撤掉就再也定义不回来;
# 两处定义同出一源逐字一致, 重复定义合法.
file(GLOB_RECURSE DAG_COMMON_FILES CONFIGURE_DEPENDS ${DAG_SCAN_ROOT}/Node/Common/*.hpp)
list(SORT DAG_COMMON_FILES)
set(DAG_COMMON_MACROS "")
foreach(common_file ${DAG_COMMON_FILES})
    file(RELATIVE_PATH common_rel ${INC_ROOT} ${common_file})
    dag_read(${common_file} common_src)
    if(common_src MATCHES "#define (NODE|FIELDS)_")
        message(FATAL_ERROR "Node/Common 只放两档共用的 class, NODE_ / FIELDS_ 写到 Node/<Tier>/ 下: ${common_file}")
    endif()
    dag_inline_macros("${common_src}" ${common_rel} "" common_text common_names)
    string(APPEND DAG_COMMON_MACROS "${common_text}")
endforeach()

# 一档三段 (宏, 共享 dag_generate 的函数作用域):
#   dag_scan  节点文件 → 节点 / 依赖 / 字段 / 分类 / 宏
#   dag_sort  节点 → 拓扑序
#   dag_emit  → 三个生成文件 (模板 = templates/<文件>.hpp.in)
# 成员判定走标志变量 (O(1)): DAG_DEPS_<名> 已定义 ⇔ <名> 是本档节点; DAG_DONE_<名> 已定义 ⇔ <名> 已排入拓扑序
macro(dag_scan)
    file(GLOB_RECURSE DAG_SCAN_FILES CONFIGURE_DEPENDS
        ${DAG_TIER_ROOT}/TS/*.hpp
        ${DAG_TIER_ROOT}/CS/*.hpp)
    list(SORT DAG_SCAN_FILES)

    set(DAG_NODES "")                          # 全部节点名 (发现序)
    set(DAG_FIELD_OWNERS "")                   # 有任一 FIELDS_ 宏的名字 (节点或非节点列; 发现序)
    set(DAG_CATEGORIES "")                     # Node/TS/<分类> 或 Node/CS/<分类> 的目录名
    set(DAG_CAT2S "")                          # 字段表中实际出现过的二级分类 token
    set(DAG_INCLUDES "")                       # 有 NODE_ / FIELDS_ 的节点文件 (include/ 下相对)
    set(DAG_GEN_MACROS "${DAG_COMMON_MACROS}") # 抽取的 #define/#undef 行 (FieldsGenerated 内联)
    set(DAG_MACRO_NAMES "")                    # 抽取宏名 (NodesGenerated 先 #undef; 不含 Common)
    foreach(scan_file ${DAG_SCAN_FILES})
        file(RELATIVE_PATH scan_rel ${INC_ROOT} ${scan_file})
        file(RELATIVE_PATH scan_op_rel ${DAG_TIER_ROOT} ${scan_file})
        string(REPLACE "/" ";" scan_parts "${scan_op_rel}")
        list(LENGTH scan_parts scan_parts_len)
        if(scan_parts_len LESS 3)
            message(FATAL_ERROR "节点文件必须放在 Node/${DAG_TIER}/TS/<分类>/ 或 Node/${DAG_TIER}/CS/<分类>/ 下: ${scan_rel}")
        endif()
        list(GET scan_parts 1 scan_cat)
        list(APPEND DAG_CATEGORIES ${scan_cat})
        dag_read(${scan_file} scan_src)
        set(scan_defs "${scan_src}")
        string(REGEX REPLACE "(^|\n) *//[^\n]*" "\n" scan_defs "${scan_defs}")
        string(REGEX MATCHALL "(^|\n) *#define NODE_[A-Za-z0-9_]+\\(" node_defs "${scan_defs}")
        string(REGEX MATCHALL "(^|\n) *#define FIELDS_(${DAG_LEVEL_RE})_[A-Za-z0-9_]+\\(" field_defs "${scan_defs}")
        string(REGEX MATCHALL "X\\([^,\n]+, *CAT1, *[A-Za-z0-9_]+," cat2_uses "${scan_defs}")
        foreach(cat2_use ${cat2_uses})
            string(REGEX REPLACE ".*CAT1, *([A-Za-z0-9_]+),.*" "\\1" cat2 "${cat2_use}")
            list(APPEND DAG_CAT2S ${cat2})
        endforeach()
        if(NOT node_defs AND NOT field_defs)
            continue()
        endif()
        list(APPEND DAG_INCLUDES ${scan_rel})
        dag_inline_macros("${scan_src}" ${scan_rel} ${DAG_TIER} scan_text scan_names)
        string(APPEND DAG_GEN_MACROS "${scan_text}")
        list(APPEND DAG_MACRO_NAMES ${scan_names})
        foreach(node_def ${node_defs})
            string(REGEX REPLACE ".*#define NODE_([A-Za-z0-9_]+)\\(" "\\1" node_name "${node_def}")
            if(DEFINED DAG_DEPS_${node_name})
                message(FATAL_ERROR "NODE_${node_name} 重复定义 (${scan_rel})")
            endif()
            list(APPEND DAG_NODES ${node_name})
            # 依赖 = 该行里所有 "<Ident>." (先全收, 排序时只认节点名)
            # 单口引用 Up.out(Up.port) / Up.y[Up.port] 另记成 "Up.port" (GUI 依赖解析按口过滤, 不把上游整节点的字段都算进来);
            # 其余形态 (Up.out() / Up.outs() / Up.hist() / Up.y / Up.member) = 整节点
            string(REGEX MATCH "(^|\n) *#define NODE_${node_name}\\([^\n]*" node_line "${scan_defs}")
            set(port_ref_re "[A-Za-z_][A-Za-z0-9_]*\\.(out\\( *[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z0-9_]+ *\\)|y\\[ *[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z0-9_]+ *\\])")
            string(REGEX MATCHALL "${port_ref_re}" port_refs "${node_line}")
            string(REGEX REPLACE "${port_ref_re}" " " node_rest "${node_line}")
            string(REGEX MATCHALL "[A-Za-z_][A-Za-z0-9_]*\\." node_refs "${node_rest}")
            set(DAG_DEPS_${node_name} "")    # 节点名 (拓扑排序用)
            set(DAG_DEPREFS_${node_name} "") # "Up" / "Up.port" (node_deps::TABLE 用)
            foreach(ref ${node_refs})
                string(REGEX REPLACE "\\.$" "" ref "${ref}")
                list(APPEND DAG_DEPS_${node_name} ${ref})
                list(APPEND DAG_DEPREFS_${node_name} ${ref})
            endforeach()
            foreach(ref ${port_refs})
                string(REGEX REPLACE "^([A-Za-z_][A-Za-z0-9_]*)\\.(out\\(|y\\[) *[A-Za-z_][A-Za-z0-9_]*\\.([A-Za-z0-9_]+) *.$" "\\1;\\3" up_port "${ref}")
                list(GET up_port 0 up)
                list(GET up_port 1 port)
                list(APPEND DAG_DEPS_${node_name} ${up})
                list(APPEND DAG_DEPREFS_${node_name} "${up}.${port}")
            endforeach()
        endforeach()
        # 每 (层, owner) 记两张同形表的 CAT1 位: FIELDS = 一级分类, FILES = 节点文件路径
        foreach(field_def ${field_defs})
            string(REGEX REPLACE ".*#define FIELDS_(${DAG_LEVEL_RE})_([A-Za-z0-9_]+)\\(" "\\1;\\2" lvl_owner "${field_def}")
            list(GET lvl_owner 0 lvl)
            list(GET lvl_owner 1 owner)
            if(DEFINED DAG_FIELDS_${lvl}_${owner})
                message(FATAL_ERROR "FIELDS_${lvl}_${owner} 重复定义 (${scan_rel})")
            endif()
            set(DAG_FIELDS_${lvl}_${owner} "${scan_cat}")
            set(DAG_FILES_${lvl}_${owner} "${scan_rel}")
            list(APPEND DAG_FIELD_OWNERS ${owner})
        endforeach()
    endforeach()
    # 去重保首见序
    foreach(dag_list CATEGORIES CAT2S FIELD_OWNERS MACRO_NAMES)
        list(REMOVE_DUPLICATES DAG_${dag_list})
    endforeach()
endmacro()

# 分层拓扑排序: 每轮按发现序扫一遍剩余节点, 依赖已就位即排入 (同轮后者看得见前者), 一轮无进展即成环.
macro(dag_sort)
    set(DAG_ORDER "")
    set(dag_remaining ${DAG_NODES})
    while(dag_remaining)
        set(dag_pending "")
        foreach(node_name ${dag_remaining})
            set(ready TRUE)
            foreach(dep ${DAG_DEPS_${node_name}})
                if(DEFINED DAG_DEPS_${dep} AND NOT DEFINED DAG_DONE_${dep})
                    set(ready FALSE)
                    break()
                endif()
            endforeach()
            if(ready)
                list(APPEND DAG_ORDER ${node_name})
                set(DAG_DONE_${node_name} TRUE)
            else()
                list(APPEND dag_pending ${node_name})
            endif()
        endforeach()
        if(dag_pending STREQUAL dag_remaining)
            message(FATAL_ERROR "节点依赖成环: ${dag_remaining}")
        endif()
        set(dag_remaining ${dag_pending})
    endwhile()
endmacro()

macro(dag_emit)
    # NodesGenerated 在 include 节点头前先 #undef 全部抽取宏 (FieldsGenerated 可能已定义同名守卫宏),
    # 让节点头内的定义成为现行定义, 两种 include 顺序都无重复定义诊断.
    dag_join(DAG_GEN_UNDEFS   "" "#undef "     "\n"   ${DAG_MACRO_NAMES})
    dag_join(DAG_GEN_INCLUDES "" "#include \"" "\"\n" ${DAG_INCLUDES})
    dag_join(DAG_GEN_NODES    "" " \\\n  ${DAG_TIER}_NODE_" "(N)" ${DAG_ORDER})

    set(DAG_GEN_PREV "")
    set(dag_prev DAG_Root)
    foreach(node_name ${DAG_ORDER})
        string(APPEND DAG_GEN_PREV "#define ${DAG_TIER}_NODE_PREV_${node_name} ${dag_prev}\n")
        set(dag_prev DAG_${node_name})
    endforeach()
    set(DAG_LAST ${dag_prev})

    # 节点上游依赖 (仅保留指向真实节点的引用, 过滤掉 tick_data 等源; 去重)
    # 生成 <tier>::node_deps::TABLE: {node, deps[], count}, 项 = "Up" (整节点) 或 "Up.port" (单口), 供 GUI 特征依赖解析使用
    set(DAG_GEN_DEP_ARRAYS "")
    set(DAG_GEN_DEP_TABLE "")
    set(dep_idx 0)
    foreach(node_name ${DAG_ORDER})
        set(dep_list "")
        foreach(dep ${DAG_DEPREFS_${node_name}})
            string(REGEX REPLACE "\\..*$" "" dep_node "${dep}")
            if(DEFINED DAG_DEPS_${dep_node})
                list(APPEND dep_list ${dep})
            endif()
        endforeach()
        list(REMOVE_DUPLICATES dep_list)
        list(LENGTH dep_list dep_count)
        dag_join(dep_items ", " "\"" "\"" ${dep_list})
        string(APPEND DAG_GEN_DEP_ARRAYS "  inline constexpr const char* const deps_${dep_idx}[] = {${dep_items}};\n")
        string(APPEND DAG_GEN_DEP_TABLE "  {\"${node_name}\", deps_${dep_idx}, ${dep_count}},\n")
        math(EXPR dep_idx "${dep_idx} + 1")
    endforeach()

    # 字段: 先按节点拓扑序, 再非节点列 (按发现序)
    set(dag_field_order ${DAG_ORDER})
    foreach(owner ${DAG_FIELD_OWNERS})
        if(NOT DEFINED DAG_DEPS_${owner})
            list(APPEND dag_field_order ${owner})
        endif()
    endforeach()
    # 层名与 FeaturesDefine.hpp TIER_LEVELS 一致; 每层两张同形表 (同一展开, 行序一致, 只差 CAT1 位; 档没有的层为空表, C++ 侧断言):
    #   <Tier>_<LVL>_FIELDS(X)  CAT1 = 一级分类
    #   <Tier>_<LVL>_FILES(X)   CAT1 = 节点文件路径 (include/ 下相对; GUI 元数据用)
    set(DAG_GEN_FIELDS "")
    foreach(lvl ${DAG_LEVELS})
        foreach(table FIELDS FILES)
            string(APPEND DAG_GEN_FIELDS "#define ${DAG_TIER}_${lvl}_${table}(X)")
            foreach(owner ${dag_field_order})
                if(DEFINED DAG_${table}_${lvl}_${owner})
                    string(APPEND DAG_GEN_FIELDS " \\\n  ${DAG_TIER}_FIELDS_${lvl}_${owner}(X, \"${DAG_${table}_${lvl}_${owner}}\")")
                endif()
            endforeach()
            string(APPEND DAG_GEN_FIELDS "\n\n")
        endforeach()
    endforeach()

    list(LENGTH DAG_ORDER DAG_N)
    foreach(cats CATEGORIES CAT2S)
        list(SORT DAG_${cats})
        list(LENGTH DAG_${cats} DAG_${cats}_N)
        dag_join(DAG_GEN_${cats} "" "  \"" "\",\n" ${DAG_${cats}})
    endforeach()

    # 内容不变不重写 (消费者不重编)
    foreach(gen FeatureCategoriesGenerated FieldsGenerated NodesGenerated)
        configure_file(${DAG_TEMPLATE_ROOT}/${gen}.hpp.in ${DAG_GEN_DIR}/${gen}.hpp @ONLY NEWLINE_STYLE UNIX)
    endforeach()
endmacro()

function(dag_generate DAG_TIER DAG_NS)
    set(DAG_TIER_ROOT ${DAG_SCAN_ROOT}/Node/${DAG_TIER})
    set(DAG_GEN_DIR   ${CMAKE_CURRENT_BINARY_DIR}/generated/features/${DAG_TIER})
    dag_scan()
    dag_sort()
    dag_emit()
endfunction()

dag_generate(Lob lob)
dag_generate(Bar bar)

# ============================================================================
# Vendor: 源码内置三方库, 一律 模式参数 (目录级继承) + PRECISE 数学档; 各库 CMakeLists 不写死优化 / 数学参数
# ============================================================================
# Threading support for zstd
set(CMAKE_THREAD_LIBS_INIT "-lpthread" CACHE STRING "pthread library")
set(CMAKE_HAVE_THREADS_LIBRARY 1)
set(Threads_FOUND TRUE CACHE BOOL "Threads found" FORCE)

foreach(glfw_opt GLFW_BUILD_EXAMPLES GLFW_BUILD_TESTS GLFW_BUILD_DOCS GLFW_INSTALL)
    set(${glfw_opt} OFF CACHE BOOL "" FORCE)
endforeach()

# 有 CMakeLists 的: package/<目录> → build/vendor/<目录>, 一律 EXCLUDE_FROM_ALL (只随链接者构建); 单文件的: 直接 OBJECT
set(VENDOR_DIRS zlib-1.3.1 zstd-1.5.7 tinyxml2 microtex glfw imgui implot)
foreach(vendor_dir ${VENDOR_DIRS})
    add_subdirectory(${PKG_ROOT}/${vendor_dir} ${CMAKE_BINARY_DIR}/vendor/${vendor_dir} EXCLUDE_FROM_ALL)
endforeach()

add_library(tracy  OBJECT ${PKG_ROOT}/tracy/public/TracyClient.cpp)
add_library(yyjson OBJECT ${PKG_ROOT}/yyjson/yyjson.c)
target_include_directories(tracy SYSTEM PUBLIC ${PKG_ROOT}/tracy/public/tracy)

set(VENDOR_TARGETS z libzstd_static tinyxml2 microtex glfw imgui implot tracy yyjson)
math_options(vendor_math PRECISE)
foreach(vendor ${VENDOR_TARGETS})
    target_compile_options(${vendor} PRIVATE ${vendor_math})
endforeach()

# Vendored Arrow C++ Flight + Parquet (基本面数据 sync: api/bigquant + api/tushare; Flight 走明文 gRPC, 无 TLS 依赖)
#   headers + 预编译 .so 由 vendor_from_pyarrow.sh 从 pyarrow manylinux wheel 提取
#   Arrow 头文件用 std::aligned_storage, 在 C++23 下被 deprecated; IMPORTED 目标的头目录按 SYSTEM 处理, 抑制第三方头警告.
set(ARROW_ROOT ${PKG_ROOT}/arrow)
if(NOT EXISTS ${ARROW_ROOT}/include/arrow/flight/client.h)
    message(FATAL_ERROR
        "Vendored Arrow headers missing: ${ARROW_ROOT}/include/arrow/flight/client.h\n"
        "Run: ${ARROW_ROOT}/vendor_from_pyarrow.sh")
endif()
#   直接链带版本号的真实 .so: exFAT 等无 symlink 的盘上, git 把 lib*.so 符号链接检出为纯文本, lld 会当链接脚本解析失败
foreach(arrow_lib arrow_flight parquet arrow)
    file(GLOB arrow_so CONFIGURE_DEPENDS ${ARROW_ROOT}/lib/lib${arrow_lib}.so.[0-9]*)
    list(LENGTH arrow_so arrow_so_n)
    if(NOT arrow_so_n EQUAL 1)
        message(FATAL_ERROR
            "Expected exactly one ${ARROW_ROOT}/lib/lib${arrow_lib}.so.<ver>, found ${arrow_so_n}: ${arrow_so}\n"
            "Run: ${ARROW_ROOT}/vendor_from_pyarrow.sh")
    endif()
    add_library(arrow::${arrow_lib} SHARED IMPORTED)
    set_target_properties(arrow::${arrow_lib} PROPERTIES
        IMPORTED_LOCATION ${arrow_so}
        INTERFACE_INCLUDE_DIRECTORIES ${ARROW_ROOT}/include)
endforeach()

# ============================================================================
# Targets
#   factor_gpu  因子 GPU 后端 (GpuRun.hpp 的唯一实现): 一份 object, 两个可执行文件共用, 只编一次
#   op_check    因子算子三方对拍 (stream 实盘流式 / cpu 挖掘向量 / gpu 挖掘 CUDA; OpTable.hpp 展开)
#               只依赖 factor/*.hpp, 无 GUI / Arrow / Python; 退化判据 (disp_ok / den_ok) 与 guard 依赖受控浮点 → PRECISE
#   app_main    GUI 主程序: Factors→Operators 页经同一 GpuRun.hpp 调 GPU
# ============================================================================
add_library(factor_gpu OBJECT ${SRC_ROOT}/${GPU_BACKEND_SRC})
target_link_libraries(factor_gpu PUBLIC ${GPU_BACKEND_LINK})

add_executable(op_check ${SRC_ROOT}/${SRC_OP_CHECK})
target_link_libraries(op_check PRIVATE factor_gpu)

# OpenSSL: agent/Llm 的 LLM 接口走 HTTPS (asio ssl::stream)
#   须链在 arrow 前: vendored libparquet 静态打包了一份 libcrypto 并导出无版本符号 (X509_* / EVP_* / ERR_* …),
#   排在前面会抢占全局符号查找, asio / libssl 调到它 → 结构体不对位崩溃. NEEDED 序 = 链接序
find_package(OpenSSL REQUIRED)

list(TRANSFORM SRC_APP PREPEND ${SRC_ROOT}/)
add_executable(app_main ${SRC_APP} ${SRC_ROOT}/${GUI_BACKEND_SRC})
target_link_libraries(app_main PRIVATE
    factor_gpu
    libzstd_static
    z
    implot
    microtex
    tracy
    yyjson
    OpenSSL::SSL
    OpenSSL::Crypto
    arrow::arrow_flight
    arrow::parquet
    arrow::arrow
    dl
)
# 运行时让 ld.so 找到 vendored libarrow*.so
set_target_properties(app_main PROPERTIES
    BUILD_RPATH "${ARROW_ROOT}/lib"
    INSTALL_RPATH "${ARROW_ROOT}/lib"
)

# ============================================================================
# Install and Post-Build
# ============================================================================

install(TARGETS app_main RUNTIME DESTINATION bin)

# Custom target for running
add_custom_target(run_main
    COMMAND app_main
    DEPENDS app_main
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running app_main"
)

# 运行时资源: build/<名> ← RES_<名> (字体 / MicroTeX 资源)
set(RES_fonts ${FONT_ROOT})
set(RES_res   ${MICROTEX_RES_DIR})
foreach(res fonts res)
    add_custom_command(TARGET app_main POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${RES_${res}} ${CMAKE_BINARY_DIR}/${res}
        COMMENT "Copying ${res} to build directory")
endforeach()