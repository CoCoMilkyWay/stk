cmake_minimum_required(VERSION 3.10 FATAL_ERROR)

# Set policy to suppress MSVC ASM warning
if(POLICY CMP0194)
    cmake_policy(SET CMP0194 NEW)
endif()

# Suppress warnings
if(MSVC)
    # MSVC or clang-cl (MSVC-compatible frontend)
    add_compile_options(/W0)
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # LLVM Clang / Apple Clang / GCC
    add_compile_options(-Wno-unused-function)
endif()

# Configure zstd to only build static library
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_CONTRIB OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)

# Add the cmake subdirectory for zstd library
add_subdirectory(build/cmake)
