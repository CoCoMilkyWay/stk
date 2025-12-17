cmake_minimum_required(VERSION 3.20)
project(Main VERSION 1.0.0 LANGUAGES CXX C)

# Force Clang compiler (clang-cl on Windows)
if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    message(FATAL_ERROR "This project requires Clang compiler. Current compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()

# Set C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Build mode options
option(PROFILE_MODE "Profile mode: -O0 -g, disable optimizations for profiling" OFF)
option(DEBUG_MODE "Debug mode: -O0 -g3 -ggdb for GDB debugging" OFF)
option(TSAN_MODE "ThreadSanitizer mode: -O1 -g for race detection" OFF)
option(ASSERT_MODE "Assert mode: -O3 with assert checks enabled" OFF)

# GUI backend selection
set(USE_VULKAN_BACKEND OFF CACHE BOOL "Use Vulkan backend instead of OpenGL for GUI rendering" FORCE)

# Clear default CMAKE build type flags to avoid conflicts
set(CMAKE_CXX_FLAGS_RELEASE "")
set(CMAKE_CXX_FLAGS_DEBUG "")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "")
set(CMAKE_CXX_FLAGS_MINSIZEREL "")
set(CMAKE_C_FLAGS_RELEASE "")
set(CMAKE_C_FLAGS_DEBUG "")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "")
set(CMAKE_C_FLAGS_MINSIZEREL "")

# Enable colored diagnostics for Clang
add_compile_options(-fcolor-diagnostics -fansi-escape-codes)

# Compiler flags configuration
if(TSAN_MODE)
    message(STATUS "Build mode: TSAN (ThreadSanitizer - race detection)")
    add_compile_definitions(TSAN_MODE)

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O1 /Zi -fsanitize=thread")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")

elseif(DEBUG_MODE)
    message(STATUS "Build mode: DEBUG (interactive debugging)")
    add_compile_definitions(DEBUG_MODE)

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Od /Zi")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-optimize-sibling-calls")

elseif(PROFILE_MODE)
    message(STATUS "Build mode: PROFILE (gperftools)")
    add_compile_definitions(PROFILE_MODE)

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Od /Zi -march=native")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline-functions")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mllvm -inline-threshold=0")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-optimize-sibling-calls")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-unroll-loops")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-vectorize")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-slp-vectorize")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin")

elseif(ASSERT_MODE)
    message(STATUS "Build mode: ASSERT")
    add_compile_definitions(ASSERT_MODE)

    # Optimization level (no NDEBUG - asserts enabled)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2")
    # Architecture optimization (clang-cl specific)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
    # Fast math
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /fp:fast")
    # Link-time optimization
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto=thin")

    # Linker optimization (Windows-style)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /OPT:REF /OPT:ICF")

else()
    message(STATUS "Build mode: PRODUCTION")

    # Optimization level and definitions
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /O2 -DNDEBUG")
    # Architecture optimization (clang-cl specific)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
    # Fast math
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /fp:fast")
    # Link-time optimization
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto=thin")

    # Linker optimization (Windows-style)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /OPT:REF /OPT:ICF")
endif()

# Output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# External libraries
set(ZSTD_LIB_PATH ${CMAKE_CURRENT_SOURCE_DIR}/../../package/zstd-1.5.7/libzstd.a)
include_directories(../../include)
include_directories(../../package/zstd-1.5.7)

# Source files
set(SOURCES
    ../../src/shared/Config.cpp
    ../../src/shared/Asset.cpp
    ../../src/shared/GuiState.cpp
    ../../src/shared/OrderFlow.cpp
    ../../src/shared/Feature.cpp
    ../../src/shared/Dist.cpp
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
    ../../src/gui/task_features/ui/TabFeature.cpp
    ../../src/gui/task_features/ui/TabCompute.cpp
    ../../src/gui/task_features/ui/TabOrderFlow.cpp
    ../../src/gui/task_features/ui/TabDist.cpp
    ../../src/gui/task_features/TaskFeatures.cpp
    ../../src/gui/task_settings/TaskSettings.cpp
    ../../src/gui/task_system_info/TaskSystemInfo.cpp
    ../../src/main.cpp
)

# Add GUI backend pipeline based on selection
if(USE_VULKAN_BACKEND)
    list(APPEND SOURCES ../../src/gui/Run_Vulkan.cpp)
    add_compile_definitions(USE_VULKAN_BACKEND)
    message(STATUS "GUI Pipeline: Vulkan")
else()
    list(APPEND SOURCES ../../src/gui/Run_OpenGL.cpp)
    add_compile_definitions(USE_OPENGL_BACKEND)
    message(STATUS "GUI Pipeline: OpenGL")
endif()

set(HEADERS
    ../../include/misc/timer.hpp
    ../../include/misc/print.hpp
    ../../include/misc/progress_single.hpp
    ../../include/misc/progress_parallel.hpp
    ../../include/misc/affinity.hpp
    ../../include/misc/logging.hpp
    ../../include/misc/file_check.hpp
    ../../include/define/CBuffer.hpp
    ../../include/codec/json_config.hpp
    ../../include/codec/binary_decoder_L2.hpp
    ../../include/codec/binary_encoder_L2.hpp
    ../../include/codec/L2_DataType.hpp
    ../../include/features/FeaturesDefine.hpp
    ../../include/features/backend/FeatureReader.hpp
    ../../include/features/backend/FeatureStoreConfig.hpp
    ../../include/features/backend/FeatureStore.hpp
    ../../include/features/FeaturesHour/Hour_Sequential.hpp
    ../../include/features/FeaturesMinute/Minute_Sequential.hpp
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
    ../../include/shared/GuiState.hpp
    ../../include/shared/OrderFlow.hpp
    ../../include/shared/Feature.hpp
    ../../include/shared/Dist.hpp
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

# Disable fast-math for JSON library and UI math to avoid infinity issues (Windows: /fp:precise)
set_source_files_properties(
    ../../src/codec/json_config.cpp
    ../../src/shared/Config.cpp
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
    ../../src/gui/task_features/ui/TabOrderFlow.cpp
    ../../src/gui/task_features/ui/TabDist.cpp
    PROPERTIES COMPILE_OPTIONS "/fp:precise"
)

# Executable
add_executable(app_main ${SOURCES} ${HEADERS})

target_include_directories(app_main PRIVATE
    ../../include
    ../../package/zstd-1.5.7
)

target_link_libraries(app_main PRIVATE ${ZSTD_LIB_PATH} z)

if(PROFILE_MODE)
    target_link_libraries(app_main PRIVATE profiler)
endif()

install(TARGETS app_main RUNTIME DESTINATION bin)

# Custom target for running
add_custom_target(run_main
    COMMAND app_main
    DEPENDS app_main
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running app_main"
)

# Print configuration info
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Output directory: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")

# ============================================================================
# GUI Libraries (ImGui, ImPlot, GLFW, OpenGL)
# ============================================================================

message(STATUS "")
message(STATUS "=== Checking GUI Dependencies ===")

# Platform check
if(NOT WIN32)
    message(FATAL_ERROR "This project only supports Windows")
endif()
message(STATUS "Platform: Windows (using Win32 for windowing)")

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

# Check Windows SDK
message(STATUS "✓ Windows SDK available (Win32 API)")

message(STATUS "All GUI dependencies satisfied")

message(STATUS "")

# Setup GLFW library from local source
set(GLFW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(GLFW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(GLFW_BUILD_DOCS OFF CACHE BOOL "" FORCE)
set(GLFW_INSTALL OFF CACHE BOOL "" FORCE)
add_subdirectory(../../package/glfw ${CMAKE_BINARY_DIR}/glfw)

# Setup ImGui library from local source
set(IMGUI_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../package/imgui)
set(IMGUI_SOURCE
    ${IMGUI_SOURCE_DIR}/imgui.cpp
    ${IMGUI_SOURCE_DIR}/imgui_demo.cpp
    ${IMGUI_SOURCE_DIR}/imgui_draw.cpp
    ${IMGUI_SOURCE_DIR}/imgui_tables.cpp
    ${IMGUI_SOURCE_DIR}/imgui_widgets.cpp
    ${IMGUI_SOURCE_DIR}/backends/imgui_impl_glfw.cpp
)

# Add backend-specific ImGui implementation
if(USE_VULKAN_BACKEND)
    list(APPEND IMGUI_SOURCE ${IMGUI_SOURCE_DIR}/backends/imgui_impl_vulkan.cpp)
else()
    list(APPEND IMGUI_SOURCE ${IMGUI_SOURCE_DIR}/backends/imgui_impl_opengl3.cpp)
endif()

add_library(imgui STATIC ${IMGUI_SOURCE})
target_include_directories(imgui SYSTEM PUBLIC
    ${IMGUI_SOURCE_DIR}
    ${IMGUI_SOURCE_DIR}/backends
)

# Link graphics libraries
if(USE_VULKAN_BACKEND)
    target_link_libraries(imgui PUBLIC glfw Vulkan::Vulkan)
else()
    target_link_libraries(imgui PUBLIC glfw OpenGL::GL)
endif()

# Setup ImPlot library from local source
set(IMPLOT_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../package/implot)
set(IMPLOT_SOURCE
    ${IMPLOT_SOURCE_DIR}/implot.cpp
    ${IMPLOT_SOURCE_DIR}/implot_demo.cpp
    ${IMPLOT_SOURCE_DIR}/implot_items.cpp
)
add_library(implot STATIC ${IMPLOT_SOURCE})
target_include_directories(implot SYSTEM PUBLIC ${IMPLOT_SOURCE_DIR})
target_link_libraries(implot PUBLIC imgui)

# Disable fast-math for ImPlot library to support infinity/NaN (Windows: /fp:precise)
target_compile_options(implot PRIVATE /fp:precise)

# Link GUI libraries to main executable
target_link_libraries(app_main PRIVATE implot)

# Copy fonts directory to build directory
add_custom_command(TARGET app_main POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_SOURCE_DIR}/../../fonts
    ${CMAKE_BINARY_DIR}/fonts
    COMMENT "Copying fonts to build directory"
)

