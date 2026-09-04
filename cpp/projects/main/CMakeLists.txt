cmake_minimum_required(VERSION 3.20)
project(Main VERSION 1.0.0 LANGUAGES CXX C)

# ============================================================================
# Platform Detection
# ============================================================================
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(IS_WINDOWS TRUE)
    set(IS_MACOS FALSE)
    set(IS_LINUX FALSE)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(IS_WINDOWS FALSE)
    set(IS_MACOS TRUE)
    set(IS_LINUX FALSE)
else()
    set(IS_WINDOWS FALSE)
    set(IS_MACOS FALSE)
    set(IS_LINUX TRUE)
endif()

# ============================================================================
# Project Configuration
# ============================================================================

# Force Clang compiler (clang-cl on Windows, AppleClang on macOS)
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
    message(FATAL_ERROR "This project requires Clang compiler. Current compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()

# C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# ============================================================================
# Build Mode Options
# ============================================================================

option(PROFILE_MODE "Profile mode: /Od /Zi, disable optimizations for profiling" OFF)
option(DEBUG_MODE "Debug mode: /Od /Zi for debugging" OFF)
option(TSAN_MODE "ThreadSanitizer mode: /O1 /Zi for race detection" OFF)
option(ASSERT_MODE "Assert mode: -O3 with assert checks enabled" OFF)
option(USE_VULKAN_BACKEND "Use Vulkan backend instead of OpenGL for GUI rendering" OFF)

# Clear default CMAKE build type flags to avoid conflicts
set(CMAKE_CXX_FLAGS_RELEASE "")
set(CMAKE_CXX_FLAGS_DEBUG "")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "")
set(CMAKE_CXX_FLAGS_MINSIZEREL "")
set(CMAKE_C_FLAGS_RELEASE "")
set(CMAKE_C_FLAGS_DEBUG "")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "")
set(CMAKE_C_FLAGS_MINSIZEREL "")

# Build mode configuration
if(TSAN_MODE)
    message(STATUS "Build mode: TSAN (ThreadSanitizer - race detection)")
    add_compile_definitions(TSAN_MODE)
    if(IS_WINDOWS)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O1 /Zi -fsanitize=thread -fno-omit-frame-pointer")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O1 -g -fsanitize=thread -fno-omit-frame-pointer")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
    endif()

elseif(DEBUG_MODE)
    message(STATUS "Build mode: DEBUG (interactive debugging)")
    add_compile_definitions(DEBUG_MODE)
    if(IS_WINDOWS)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Od /Zi -march=native")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG:FULL")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -march=native")
    endif()

elseif(PROFILE_MODE)
    message(STATUS "Build mode: PROFILE (tracy instrumentation)")
    add_compile_definitions(PROFILE_MODE TRACY_ENABLE TRACY_NO_SYSTEM_TRACING TRACY_NO_SAMPLING)
    if(IS_WINDOWS)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 /Zi -march=native /fp:fast")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /OPT:REF /OPT:ICF")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g -march=native -ffast-math")
    endif()

elseif(ASSERT_MODE)
    message(STATUS "Build mode: ASSERT")
    add_compile_definitions(ASSERT_MODE)
    if(IS_WINDOWS)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 -march=native /fp:fast")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /OPT:REF /OPT:ICF")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -march=native -ffast-math")
    endif()

else()
    message(STATUS "Build mode: PRODUCTION")
    if(IS_WINDOWS)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 -DNDEBUG -march=native /fp:fast")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /OPT:REF /OPT:ICF")
    else()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -DNDEBUG -march=native -ffast-math")
    endif()
endif()

# ============================================================================
# Global Compile Options
# ============================================================================

add_compile_options(-fcolor-diagnostics -fansi-escape-codes -Wno-overriding-option)

# Platform-specific defines
if(IS_WINDOWS)
    add_compile_definitions(_WIN32_WINNT=0x0A00 WIN32_LEAN_AND_MEAN NOMINMAX)
endif()

# GUI backend selection
if(USE_VULKAN_BACKEND)
    add_compile_definitions(USE_VULKAN_BACKEND)
    message(STATUS "GUI Pipeline: Vulkan")
else()
    add_compile_definitions(USE_OPENGL_BACKEND)
    message(STATUS "GUI Pipeline: OpenGL")
endif()

# ============================================================================
# External Dependencies
# ============================================================================

include_directories(
    ../../include
    ../../package
    ../../package/zstd-1.5.7/lib/
    ../../package/zlib-1.3.1/
    ../../package/microtex/src
    ../../package/tracy/public/tracy
)

# Boost 头在 C++23 下会触发 -Wdeprecated-declarations 等, 用 SYSTEM 抑制第三方头警告.
include_directories(SYSTEM ../../package/boost)

# ============================================================================
# Vendored Arrow C++ Flight + Parquet (基本面数据 sync: api/bigquant + api/tushare)
#   headers + 预编译 .so 由 vendor_from_pyarrow.sh 从 pyarrow manylinux wheel 提取
# ============================================================================
set(ARROW_VENDOR ${CMAKE_CURRENT_SOURCE_DIR}/../../package/arrow)
set(ARROW_INCLUDE ${ARROW_VENDOR}/include)
set(ARROW_LIB ${ARROW_VENDOR}/lib)

if(NOT EXISTS ${ARROW_INCLUDE}/arrow/flight/client.h)
    message(FATAL_ERROR
        "Vendored Arrow headers missing: ${ARROW_INCLUDE}/arrow/flight/client.h\n"
        "Run: ${ARROW_VENDOR}/vendor_from_pyarrow.sh")
endif()
if(NOT EXISTS ${ARROW_LIB}/libarrow_flight.so)
    message(FATAL_ERROR
        "Vendored Arrow shared libs missing: ${ARROW_LIB}/libarrow_flight.so\n"
        "Run: ${ARROW_VENDOR}/vendor_from_pyarrow.sh")
endif()

# Arrow 头文件用 std::aligned_storage, 在 C++23 下被 deprecated.
# 用 SYSTEM include 抑制第三方头里的 -Wdeprecated-declarations 等警告.
include_directories(SYSTEM ${ARROW_INCLUDE})

# ============================================================================
# Source Files
# ============================================================================

set(SOURCES
    ../../src/shared/Config.cpp
    ../../src/shared/Asset.cpp
    ../../src/shared/AssetAxis.cpp
    ../../src/shared/EncodeDayRecord.cpp
    ../../src/shared/AssetInfo.cpp
    ../../src/shared/OrderFlow.cpp
    ../../src/shared/Feature.cpp
    ../../src/shared/Dist.cpp
    ../../src/shared/TimeSeries.cpp
    ../../src/shared/Transform.cpp
    ../../src/shared/TaskState.cpp
    ../../src/misc/logging.cpp
    ../../src/misc/archive.cpp
    ../../src/misc/file_check.cpp
    ../../src/misc/date.cpp
    ../../src/misc/fs.cpp
    ../../src/misc/parquet.cpp
    ../../src/misc/schedule.cpp
    ../../src/misc/system_metrics.cpp
    ../../src/api/bigquant/dai.cpp
    ../../src/api/bigquant/spec.cpp
    ../../src/api/bigquant/pipeline.cpp
    ../../src/api/tushare/http.cpp
    ../../src/api/tushare/spec.cpp
    ../../src/api/tushare/parse.cpp
    ../../src/api/tushare/pipeline.cpp
    ../../package/yyjson/yyjson.c
    ../../src/codec/json_config.cpp
    ../../src/codec/binary_decoder_L2.cpp
    ../../src/codec/binary_encoder_L2.cpp
    ../../src/codec/L2_Validator.cpp
    ../../src/worker/encoding_worker.cpp
    ../../src/worker/sequential_worker.cpp
    ../../src/worker/crosssectional_worker.cpp
    ../../src/worker/io_worker.cpp
    ../../src/features/Method/Fundamental.cpp
    ../../src/features/Method/CS.cpp
    ../../src/gui/Gui.cpp
    ../../src/gui/Tasks.cpp
    ../../src/gui/coro/CoroManager.cpp
    ../../src/gui/task_icon_bar/TaskIconBar.cpp
    ../../src/gui/task_icon_bar/CoroNetwork.cpp
    ../../src/gui/task_database/services/AssetLoader.cpp
    ../../src/gui/task_database/services/StateManager.cpp
    ../../src/gui/task_database/services/FundamentalService.cpp
    ../../src/gui/task_database/services/ScanService.cpp
    ../../src/gui/task_database/services/L2DatabaseService.cpp
    ../../src/gui/task_database/services/EncodingService.cpp
    ../../src/gui/task_database/ui/TabOverview.cpp
    ../../src/gui/task_database/ui/TabTable.cpp
    ../../src/gui/task_database/ui/TabBrowser.cpp
    ../../src/gui/task_database/ui/TabEncode.cpp
    ../../src/gui/task_database/ui/CrossSectionAnalysis.cpp
    ../../src/gui/task_database/TaskDatabase.cpp
    ../../src/gui/task_features/services/ComputeService.cpp
    ../../src/gui/task_features/services/DistService.cpp
    ../../src/gui/task_features/services/TimeSeriesService.cpp
    ../../src/gui/task_features/services/TransformService.cpp
    ../../src/gui/task_features/ui/TabFeature.cpp
    ../../src/gui/task_features/ui/TabCompute.cpp
    ../../src/gui/task_features/ui/TabTransform.cpp
    ../../src/gui/task_features/ui/TabDist.cpp
    ../../src/gui/task_features/ui/TabTimeSeries.cpp
    ../../src/gui/task_features/ui/TabOrderFlow.cpp
    ../../src/gui/task_features/TaskFeatures.cpp
    ../../src/gui/task_settings/TaskSettings.cpp
    ../../src/gui/task_system_info/TaskSystemInfo.cpp
    ../../src/gui/task_tools/TaskTools.cpp
    ../../src/gui/task_tools/ui/TabLatex.cpp
    ../../src/main.cpp
    ../../package/tracy/public/TracyClient.cpp
)

# Add GUI backend implementation
if(USE_VULKAN_BACKEND)
    list(APPEND SOURCES ../../src/gui/Run_Vulkan.cpp)
else()
    list(APPEND SOURCES ../../src/gui/Run_OpenGL.cpp)
endif()

set(HEADERS
    ../../include/api/bigquant/dai.hpp
    ../../include/api/bigquant/spec.hpp
    ../../include/api/bigquant/pipeline.hpp
    ../../include/api/tushare/http.hpp
    ../../include/api/tushare/spec.hpp
    ../../include/api/tushare/parse.hpp
    ../../include/api/tushare/pipeline.hpp
    ../../include/misc/date.hpp
    ../../include/misc/fs.hpp
    ../../include/misc/parquet.hpp
    ../../include/misc/schedule.hpp
    ../../include/misc/system_metrics.hpp
    ../../include/misc/cross_platform.hpp
    ../../package/yyjson/yyjson.h
    ../../include/misc/timer.hpp
    ../../include/misc/print.hpp
    ../../include/misc/progress_single.hpp
    ../../include/misc/progress_parallel.hpp
    ../../include/misc/affinity.hpp
    ../../include/misc/logging.hpp
    ../../include/misc/file_check.hpp
    ../../include/misc/archive.hpp
    ../../include/misc/profiler.hpp
    ../../include/define/CBuffer.hpp
    ../../include/codec/json_config.hpp
    ../../include/codec/binary_decoder_L2.hpp
    ../../include/codec/binary_encoder_L2.hpp
    ../../include/codec/L2_DataType.hpp
    ../../include/codec/L2_Validator.hpp
    ../../include/features/FeaturesDefine.hpp
    ../../include/features/TimeIndex.hpp
    ../../include/features/Backend/FeatureRead.hpp
    ../../include/features/Backend/FeatureStoreConfig.hpp
    ../../include/features/Backend/FeatureStore.hpp
    ../../include/features/Backend/ZstdHelper.hpp
    ../../include/features/ComputeGraph.hpp
    ../../include/features/CoreSequential.hpp
    ../../include/features/CoreCrosssection.hpp
    ../../include/lob/LimitOrderBookDefine.hpp
    ../../include/lob/LimitOrderBook.hpp
    ../../include/worker/encoding_worker.hpp
    ../../include/worker/sequential_worker.hpp
    ../../include/worker/crosssectional_worker.hpp
    ../../include/worker/io_worker.hpp
    ../../include/shared/Config.hpp
    ../../include/shared/Asset.hpp
    ../../include/shared/AssetInfo.hpp
    ../../include/shared/OrderFlow.hpp
    ../../include/shared/Feature.hpp
    ../../include/shared/Dist.hpp
    ../../include/shared/TimeSeries.hpp
    ../../include/shared/Transform.hpp
    ../../include/shared/TaskState.hpp
    ../../include/shared/SharedData.hpp
    ../../include/gui/Config.hpp
    ../../include/gui/Gui.hpp
    ../../include/gui/Tasks.hpp
    ../../include/gui/coro/CoroManager.hpp
    ../../include/gui/task_icon_bar/TaskIconBar.hpp
    ../../include/gui/task_icon_bar/CoroNetwork.hpp
    ../../include/gui/task_database/models/SharedTypes.hpp
    ../../include/gui/task_database/services/ClassifierService.hpp
    ../../include/gui/task_database/services/FundamentalService.hpp
    ../../include/gui/task_database/services/ScanService.hpp
    ../../include/gui/task_database/services/L2DatabaseService.hpp
    ../../include/gui/task_database/services/StateManager.hpp
    ../../include/gui/task_database/ui/TabOverview.hpp
    ../../include/gui/task_database/ui/TabTable.hpp
    ../../include/gui/task_database/ui/TabBrowser.hpp
    ../../include/gui/task_database/TaskDatabase.hpp
    ../../include/gui/task_settings/TaskSettings.hpp
    ../../include/gui/task_system_info/TaskSystemInfo.hpp
    ../../include/gui/task_terminal/TaskTerminal.hpp
)

# ============================================================================
# features/NodesGenerated.hpp (生成到 build 目录) — 计算图节点表 + 落盘字段表.
#   节点实例和落盘列直接写在算子文件里 (include/features/Operator/**/*.hpp: TS/ 时序节点, CS/ 按源节点分文件的截面列; 方法在 Method/):
#     #define NODE_<Name>(N)         N(<Name>, (OpType), (ctor inputs), compute_trigger, flush_trigger)
#     #define FIELDS_<LVL>_<Owner>(X) X(code, ...)   LVL ∈ {L0, L1, DEPTH}; Owner = 节点名, 或非节点列 (CS/LABEL/FLAG/META) 的任意名字
#   依赖 = ctor inputs 里出现的 "<Up>." (Up.out() / DepthData.bid_qty ...), 不需要任何 include.
#   此处: glob → 正则抽 NODE_/FIELDS_ → 分层拓扑排序 (同层按发现序 = 路径序) → 生成
#     NODES(N) / NODE_PREV_<Name> (声明链, 让顺序错误变成编译错误) /
#     LEVEL_{0,1}_FIELDS(X) / DEPTH_FIELDS(X) 和 #include 清单.
#   CMAKE_CONFIGURE_DEPENDS 盯住每个文件内容: 改动后下次 build 自动重新 configure.
# ============================================================================
set(DAG_SCAN_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../../include/features)
file(GLOB_RECURSE DAG_SCAN_FILES CONFIGURE_DEPENDS ${DAG_SCAN_ROOT}/Operator/*.hpp)
list(SORT DAG_SCAN_FILES)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${DAG_SCAN_FILES})

set(DAG_NODES "")        # 全部节点名 (发现序)
set(DAG_FIELD_OWNERS "") # 有任一 FIELDS_ 宏的名字 (节点或非算子列)
set(DAG_GEN_INCLUDES "")
foreach(scan_file ${DAG_SCAN_FILES})
    file(RELATIVE_PATH scan_rel ${CMAKE_CURRENT_SOURCE_DIR}/../../include ${scan_file})
    file(READ ${scan_file} scan_src)
    string(REGEX REPLACE "\\\\\n" " " scan_src "${scan_src}") # 续行拼成一行
    string(REGEX MATCHALL "#define NODE_[A-Za-z0-9_]+\\(" node_defs "${scan_src}")
    string(REGEX MATCHALL "#define FIELDS_(L0|L1|DEPTH)_[A-Za-z0-9_]+\\(" field_defs "${scan_src}")
    if(NOT node_defs AND NOT field_defs)
        continue()
    endif()
    string(APPEND DAG_GEN_INCLUDES "#include \"${scan_rel}\"\n")
    foreach(node_def ${node_defs})
        string(REGEX REPLACE "#define NODE_([A-Za-z0-9_]+)\\(" "\\1" node_name "${node_def}")
        if(node_name IN_LIST DAG_NODES)
            message(FATAL_ERROR "NODE_${node_name} 重复定义 (${scan_rel})")
        endif()
        list(APPEND DAG_NODES ${node_name})
        set(DAG_FILE_${node_name} ${scan_rel})
        # 依赖 = 该行里所有 "<Ident>." (先全收, 排序时只认节点名)
        string(REGEX MATCH "#define NODE_${node_name}\\([^\n]*" node_line "${scan_src}")
        string(REGEX MATCHALL "[A-Za-z_][A-Za-z0-9_]*\\." node_refs "${node_line}")
        set(DAG_DEPS_${node_name} "")
        foreach(ref ${node_refs})
            string(REGEX REPLACE "\\.$" "" ref "${ref}")
            list(APPEND DAG_DEPS_${node_name} ${ref})
        endforeach()
    endforeach()
    foreach(field_def ${field_defs})
        string(REGEX REPLACE "#define FIELDS_(L0|L1|DEPTH)_([A-Za-z0-9_]+)\\(" "\\1;\\2" lvl_owner "${field_def}")
        list(GET lvl_owner 0 lvl)
        list(GET lvl_owner 1 owner)
        if(DAG_HAS_${lvl}_${owner})
            message(FATAL_ERROR "FIELDS_${lvl}_${owner} 重复定义 (${scan_rel})")
        endif()
        set(DAG_HAS_${lvl}_${owner} TRUE)
        if(NOT owner IN_LIST DAG_FIELD_OWNERS)
            list(APPEND DAG_FIELD_OWNERS ${owner})
        endif()
    endforeach()
endforeach()

# 分层拓扑排序: 每轮取出全部依赖已就位的节点 (按发现序), 无进展即成环.
set(DAG_ORDER "")
set(dag_remaining ${DAG_NODES})
while(dag_remaining)
    set(dag_progress FALSE)
    foreach(node_name ${dag_remaining})
        set(ready TRUE)
        foreach(dep ${DAG_DEPS_${node_name}})
            if(dep IN_LIST DAG_NODES AND NOT dep IN_LIST DAG_ORDER)
                set(ready FALSE)
                break()
            endif()
        endforeach()
        if(ready)
            list(APPEND DAG_ORDER ${node_name})
            list(REMOVE_ITEM dag_remaining ${node_name})
            set(dag_progress TRUE)
        endif()
    endforeach()
    if(NOT dag_progress)
        message(FATAL_ERROR "节点依赖成环: ${dag_remaining}")
    endif()
endwhile()

set(DAG_GEN_NODES "#define NODES(N)")
set(DAG_GEN_PREV "")
set(dag_prev DAG_Root)
foreach(node_name ${DAG_ORDER})
    string(APPEND DAG_GEN_NODES " \\\n  NODE_${node_name}(N)")
    string(APPEND DAG_GEN_PREV "#define NODE_PREV_${node_name} ${dag_prev}\n")
    set(dag_prev DAG_${node_name})
endforeach()
string(APPEND DAG_GEN_PREV "#define DAG_LAST ${dag_prev}\n")
# 字段: 先按节点拓扑序, 再非算子列 (按发现序)
set(dag_field_order ${DAG_ORDER})
foreach(owner ${DAG_FIELD_OWNERS})
    if(NOT owner IN_LIST DAG_NODES)
        list(APPEND dag_field_order ${owner})
    endif()
endforeach()
set(DAG_GEN_FIELDS "")
foreach(lvl L0 L1 DEPTH) # 层名与 FeaturesDefine.hpp ALL_LEVELS 一致, 生成 <LVL>_FIELDS(X)
    string(APPEND DAG_GEN_FIELDS "#define ${lvl}_FIELDS(X)")
    foreach(owner ${dag_field_order})
        if(DAG_HAS_${lvl}_${owner})
            string(APPEND DAG_GEN_FIELDS " \\\n  FIELDS_${lvl}_${owner}(X)")
        endif()
    endforeach()
    string(APPEND DAG_GEN_FIELDS "\n\n")
endforeach()
list(LENGTH DAG_ORDER DAG_N)
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/generated/features/NodesGenerated.hpp
"#pragma once

// AUTO-GENERATED by cpp/projects/main/CMakeLists.txt — 不要手动编辑.
// 由 include/features/Operator/**/*.hpp 里的 NODE_/FIELDS_ 宏扫描生成
// (${DAG_N} 个节点, 按输入引用分层拓扑排序). 改动后下次 build 自动重新 configure.

${DAG_GEN_INCLUDES}
// 节点表 (拓扑序 = DAG 声明序 = 同一触发域内执行序)
${DAG_GEN_NODES}

// 声明链: DAG_<name> : NODE_PREV_<name>, 节点只看得到排在自己前面的节点, 顺序错误即编译错误
${DAG_GEN_PREV}
// 落盘字段表 (按层; 行序 = 列序)
${DAG_GEN_FIELDS}")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/generated)

# ============================================================================
# Executable Configuration
# ============================================================================

add_executable(app_main ${SOURCES} ${HEADERS})

# Disable fast-math for specific files to avoid infinity issues (Windows: /fp:precise, Unix: -fno-fast-math)
if(IS_WINDOWS)
    set(PRECISE_MATH_FLAG "/fp:precise")
else()
    set(PRECISE_MATH_FLAG "-fno-fast-math")
endif()

# api / parquet 路径用 NaN 标记缺失值, 与 -ffast-math
# (-ffinite-math-only) 冲突会出错, 必须 precise math.
set_source_files_properties(
    ../../src/misc/date.cpp
    ../../src/misc/fs.cpp
    ../../src/misc/parquet.cpp
    ../../src/misc/schedule.cpp
    ../../src/api/bigquant/dai.cpp
    ../../src/api/bigquant/spec.cpp
    ../../src/api/bigquant/pipeline.cpp
    ../../src/api/tushare/http.cpp
    ../../src/api/tushare/spec.cpp
    ../../src/api/tushare/parse.cpp
    ../../src/api/tushare/pipeline.cpp
    ../../src/features/Method/Fundamental.cpp
    ../../src/features/Method/CS.cpp
    ../../src/codec/json_config.cpp
    ../../src/shared/Config.cpp
    ../../src/shared/AssetAxis.cpp
    ../../src/shared/AssetInfo.cpp
    ../../src/shared/Dist.cpp
    ../../src/shared/Transform.cpp
    ../../src/gui/task_database/services/AssetLoader.cpp
    ../../src/gui/task_database/services/FundamentalService.cpp
    ../../src/gui/task_database/services/ScanService.cpp
    ../../src/gui/task_database/services/L2DatabaseService.cpp
    ../../src/gui/task_database/services/StateManager.cpp
    ../../src/gui/task_database/TaskDatabase.cpp
    ../../src/gui/task_database/ui/TabTable.cpp
    ../../src/gui/task_database/ui/CrossSectionAnalysis.cpp
    ../../src/gui/task_features/ui/TabFeature.cpp
    ../../src/gui/task_features/ui/TabTransform.cpp
    ../../src/gui/task_features/ui/TabDist.cpp
    ../../src/gui/task_features/ui/TabTimeSeries.cpp
    ../../src/gui/task_features/ui/TabOrderFlow.cpp
    ../../src/gui/task_tools/TaskTools.cpp
    ../../src/gui/task_tools/ui/TabLatex.cpp
    PROPERTIES COMPILE_OPTIONS "${PRECISE_MATH_FLAG}"
)

# ============================================================================
# GUI Dependencies and Libraries
# ============================================================================

message(STATUS "")
message(STATUS "=== Checking GUI Dependencies ===")

# Platform check
if(IS_WINDOWS)
    message(STATUS "Platform: Windows (using Win32 for windowing)")
elseif(IS_MACOS)
    message(STATUS "Platform: macOS (using Metal/Cocoa for windowing)")
else()
    message(STATUS "Platform: Linux (using X11 for windowing)")
endif()

# Check graphics API
if(USE_VULKAN_BACKEND)
    find_package(Vulkan QUIET)
    if(NOT Vulkan_FOUND)
        message(FATAL_ERROR "Vulkan not found! Install Vulkan SDK from https://vulkan.lunarg.com/")
    endif()
    message(STATUS "✓ Vulkan found: ${Vulkan_LIBRARIES}")
else()
    find_package(OpenGL QUIET)
    if(NOT OpenGL_FOUND)
        message(FATAL_ERROR "OpenGL not found! OpenGL should be available via graphics drivers")
    endif()
    message(STATUS "✓ OpenGL found: ${OPENGL_LIBRARIES}")
endif()

message(STATUS "✓ Windows SDK available (Win32 API)")
message(STATUS "All GUI dependencies satisfied")
message(STATUS "")

# Threading support for zstd
if(NOT IS_WINDOWS)
    set(CMAKE_THREAD_LIBS_INIT "-lpthread" CACHE STRING "pthread library")
    set(CMAKE_HAVE_THREADS_LIBRARY 1)
    set(Threads_FOUND TRUE CACHE BOOL "Threads found" FORCE)
endif()

# Add third-party libraries
add_subdirectory(../../package/zlib-1.3.1 ${CMAKE_BINARY_DIR}/zlib EXCLUDE_FROM_ALL)
add_subdirectory(../../package/zstd-1.5.7 ${CMAKE_BINARY_DIR}/zstd EXCLUDE_FROM_ALL)
add_subdirectory(../../package/tinyxml2 ${CMAKE_BINARY_DIR}/tinyxml2)
add_subdirectory(../../package/microtex ${CMAKE_BINARY_DIR}/microtex)

set(GLFW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(GLFW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(GLFW_BUILD_DOCS OFF CACHE BOOL "" FORCE)
set(GLFW_INSTALL OFF CACHE BOOL "" FORCE)
add_subdirectory(../../package/glfw ${CMAKE_BINARY_DIR}/glfw)
add_subdirectory(../../package/imgui ${CMAKE_BINARY_DIR}/imgui)
add_subdirectory(../../package/implot ${CMAKE_BINARY_DIR}/implot)

# Link all libraries
target_link_libraries(app_main PRIVATE
    libzstd_static
    z
    implot
    microtex
)

# vendored Arrow C++ Flight + Parquet (Flight 走明文 gRPC, 无 TLS 依赖)
target_link_libraries(app_main PRIVATE
    ${ARROW_LIB}/libarrow_flight.so
    ${ARROW_LIB}/libparquet.so
    ${ARROW_LIB}/libarrow.so
    dl
)

# 运行时让 ld.so 找到 vendored libarrow*.so
set_target_properties(app_main PROPERTIES
    BUILD_RPATH "${ARROW_LIB}"
    INSTALL_RPATH "${ARROW_LIB}"
)

if(IS_WINDOWS)
    target_link_libraries(app_main PRIVATE
        pdh
        iphlpapi
        setupapi
        dxgi
        advapi32
        ws2_32
        dbghelp
    )
endif()

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

# Copy fonts directory to build directory
add_custom_command(TARGET app_main POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_SOURCE_DIR}/../../fonts
    ${CMAKE_BINARY_DIR}/fonts
    COMMENT "Copying fonts to build directory"
)

# Copy MicroTeX resources
add_custom_command(TARGET app_main POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${MICROTEX_RES_DIR}
    ${CMAKE_BINARY_DIR}/res
    COMMENT "Copying MicroTeX resources"
)

# Print configuration summary
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Output directory: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")
