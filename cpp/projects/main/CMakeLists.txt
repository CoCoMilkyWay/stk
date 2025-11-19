cmake_minimum_required(VERSION 3.20)
project(Main VERSION 1.0.0 LANGUAGES CXX C)

# Force Clang compiler
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
option(DEBUG_MODE   "Debug mode: -O0 -g3 -ggdb for GDB debugging" OFF)
option(TSAN_MODE    "ThreadSanitizer mode: -O1 -g for race detection" OFF)

# Clear default CMAKE build type flags to avoid conflicts
set(CMAKE_CXX_FLAGS_RELEASE "")
set(CMAKE_CXX_FLAGS_DEBUG "")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "")
set(CMAKE_CXX_FLAGS_MINSIZEREL "")
set(CMAKE_C_FLAGS_RELEASE "")
set(CMAKE_C_FLAGS_DEBUG "")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "")
set(CMAKE_C_FLAGS_MINSIZEREL "")

# Compiler flags configuration
if(TSAN_MODE)
    message(STATUS "Build mode: TSAN (ThreadSanitizer - race detection)")
    add_compile_definitions(TSAN_MODE)
    
    set(CMAKE_CXX_FLAGS "-O1 -g -Wall -Wextra -fsanitize=thread")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")

elseif(DEBUG_MODE)
    message(STATUS "Build mode: DEBUG (GDB - interactive debugging)")
    add_compile_definitions(DEBUG_MODE)
    
    set(CMAKE_CXX_FLAGS "-O0 -g3 -ggdb -Wall -Wextra")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-optimize-sibling-calls")

elseif(PROFILE_MODE)
    message(STATUS "Build mode: PROFILE (gperftools)")
    add_compile_definitions(PROFILE_MODE)
    
    set(CMAKE_CXX_FLAGS "-O0 -g -Wall -Wextra -march=native")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline-functions")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mllvm -inline-threshold=0")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-optimize-sibling-calls")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-unroll-loops")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-vectorize")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-slp-vectorize")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin")

else()
    message(STATUS "Build mode: PRODUCTION")
    
    # Optimization level and definitions
    # set(CMAKE_CXX_FLAGS "-O3 -DNDEBUG")
    set(CMAKE_CXX_FLAGS "-O3")
    # Warning flags
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
    # Architecture optimization
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
    # Specific optimizations
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -funroll-loops")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ffast-math")
    # Link-time optimization
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto=thin")
    
    # Linker selection
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=lld")
    # Linker optimization level
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-O3")
    # Linker features
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--icf=all")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--as-needed")
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
    ../../src/codec/json_config.cpp
    ../../src/codec/binary_decoder_L2.cpp
    ../../src/codec/binary_encoder_L2.cpp
    ../../src/misc/logging.cpp
    ../../src/misc/file_check.cpp
    ../../src/worker/encoding_worker.cpp
    ../../src/worker/sequential_worker.cpp
    ../../src/worker/crosssectional_worker.cpp
    ../../src/worker/io_worker.cpp
    ../../src/main.cpp
)

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
    ../../include/lob/LimitOrderBook.hpp
    ../../include/worker/shared_state.hpp
    ../../include/worker/encoding_worker.hpp
    ../../include/worker/sequential_worker.hpp
    ../../include/worker/crosssectional_worker.hpp
    ../../include/worker/io_worker.hpp
)

# Disable fast-math for JSON library to avoid infinity issues
set_source_files_properties(../../src/codec/json_config.cpp PROPERTIES
    COMPILE_OPTIONS "-fno-fast-math"
)

# Executable
add_executable(app_main ${SOURCES} ${HEADERS})

target_include_directories(app_main PRIVATE
    ../../include
    ../../package/zstd-1.5.7
)

target_link_libraries(app_main PRIVATE ${ZSTD_LIB_PATH})

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
message(STATUS "app_main sources: ${SOURCES}")

