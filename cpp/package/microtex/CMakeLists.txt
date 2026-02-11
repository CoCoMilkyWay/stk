cmake_minimum_required(VERSION 3.16)
project(microtex LANGUAGES CXX)

# Collect all source files
file(GLOB_RECURSE MICROTEX_SOURCES
    src/atom/*.cpp
    src/box/*.cpp
    src/core/*.cpp
    src/fonts/*.cpp
    src/utils/*.cpp
    src/res/builtin/*.cpp
    src/res/font/*.cpp
    src/res/parser/*.cpp
    src/res/reg/*.cpp
    src/res/sym/*.cpp
    src/latex.cpp
    src/render.cpp
    src/platform/imgui/graphic_imgui.cpp
)

add_library(microtex STATIC ${MICROTEX_SOURCES})

target_include_directories(microtex PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(microtex PRIVATE tinyxml2)
target_link_libraries(microtex PRIVATE imgui)

# ImGui backend flag
target_compile_definitions(microtex PUBLIC BUILD_IMGUI)

# Disable Windows CRT secure warnings
target_compile_definitions(microtex PUBLIC _CRT_SECURE_NO_WARNINGS)

# microtex needs infinity support, override fast-math
if(MSVC OR (CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND WIN32))
    target_compile_options(microtex PRIVATE /fp:precise)
else()
    target_compile_options(microtex PRIVATE -fno-fast-math)
endif()

target_compile_options(microtex PRIVATE
    -Wno-tautological-constant-out-of-range-compare
    -Wno-switch
    -Wno-unused-result
)

# Export resource path for copying
set(MICROTEX_RES_DIR ${CMAKE_CURRENT_SOURCE_DIR}/res PARENT_SCOPE)

