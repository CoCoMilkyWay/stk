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
    ../../package/zstd-1.5.7/lib/
    ../../package/zlib-1.3.1/
    ../../package/microtex/src
    ../../package/tracy/public/tracy
)

# ============================================================================
# Source Files
# ============================================================================

set(SOURCES
    ../../src/shared/Config.cpp
    ../../src/shared/Asset.cpp
    ../../src/shared/AssetInfo.cpp
    ../../src/shared/OrderFlow.cpp
    ../../src/shared/Feature.cpp
    ../../src/shared/Dist.cpp
    ../../src/shared/TimeSeries.cpp
    ../../src/shared/Transform.cpp
    ../../src/shared/TaskState.cpp
    ../../src/misc/logging.cpp
    ../../src/misc/file_check.cpp
    ../../src/codec/json_config.cpp
    ../../src/codec/binary_decoder_L2.cpp
    ../../src/codec/binary_encoder_L2.cpp
    ../../src/worker/encoding_worker.cpp
    ../../src/worker/sequential_worker.cpp
    ../../src/worker/crosssectional_worker.cpp
    ../../src/worker/io_worker.cpp
    ../../src/gui/Gui.cpp
    ../../src/gui/Tasks.cpp
    ../../src/gui/coro/CoroManager.cpp
    ../../src/gui/task_icon_bar/TaskIconBar.cpp
    ../../src/gui/task_icon_bar/CoroNetwork.cpp
    ../../src/gui/task_database/services/AssetLoader.cpp
    ../../src/gui/task_database/services/StateManager.cpp
    ../../src/gui/task_database/services/BaostockService.cpp
    ../../src/gui/task_database/services/ScanService.cpp
    ../../src/gui/task_database/services/L2DatabaseService.cpp
    ../../src/gui/task_database/services/EncodingService.cpp
    ../../src/gui/task_database/infrastructure/DataManager.cpp
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
    ../../include/misc/timer.hpp
    ../../include/misc/print.hpp
    ../../include/misc/progress_single.hpp
    ../../include/misc/progress_parallel.hpp
    ../../include/misc/affinity.hpp
    ../../include/misc/logging.hpp
    ../../include/misc/file_check.hpp
    ../../include/misc/profiler.hpp
    ../../include/define/CBuffer.hpp
    ../../include/codec/json_config.hpp
    ../../include/codec/binary_decoder_L2.hpp
    ../../include/codec/binary_encoder_L2.hpp
    ../../include/codec/L2_DataType.hpp
    ../../include/features/FeaturesDefine.hpp
    ../../include/features/backend/FeatureReader.hpp
    ../../include/features/backend/FeatureStoreConfig.hpp
    ../../include/features/backend/FeatureStore.hpp
    ../../include/features/backend/ZstdHelper.hpp
    ../../include/features/FeaturesHour/Hour_Sequential.hpp
    ../../include/features/FeaturesMinute/Minute_Sequential.hpp
    ../../include/features/ComputeGraph.hpp
    ../../include/features/FeaturesTick/Tick_Sequential.hpp
    ../../include/features/FeaturesHour/Hour_Crosssection.hpp
    ../../include/features/FeaturesMinute/Minute_Crosssection.hpp
    ../../include/features/FeaturesTick/Tick_Crosssection.hpp
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
    ../../include/gui/task_database/models/BaostockData.hpp
    ../../include/gui/task_database/services/ClassifierService.hpp
    ../../include/gui/task_database/services/BaostockService.hpp
    ../../include/gui/task_database/services/ScanService.hpp
    ../../include/gui/task_database/services/L2DatabaseService.hpp
    ../../include/gui/task_database/services/StateManager.hpp
    ../../include/gui/task_database/infrastructure/BaostockClient.hpp
    ../../include/gui/task_database/infrastructure/BaostockPool.hpp
    ../../include/gui/task_database/infrastructure/DataManager.hpp
    ../../include/gui/task_database/ui/TabOverview.hpp
    ../../include/gui/task_database/ui/TabTable.hpp
    ../../include/gui/task_database/ui/TabBrowser.hpp
    ../../include/gui/task_database/TaskDatabase.hpp
    ../../include/gui/task_settings/TaskSettings.hpp
    ../../include/gui/task_system_info/TaskSystemInfo.hpp
    ../../include/gui/task_terminal/TaskTerminal.hpp
)

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

set_source_files_properties(
    ../../src/codec/json_config.cpp
    ../../src/shared/Config.cpp
    ../../src/shared/AssetInfo.cpp
    ../../src/shared/Dist.cpp
    ../../src/gui/task_database/infrastructure/DataManager.cpp
    ../../src/gui/task_database/services/AssetLoader.cpp
    ../../src/gui/task_database/services/BaostockService.cpp
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
