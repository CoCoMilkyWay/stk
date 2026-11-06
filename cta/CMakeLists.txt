cmake_minimum_required(VERSION 3.28)
project(cta CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

# 断言常开 (不设 NDEBUG); CTA_DIR = 本目录 (库落 data/, 源在 ../output, 前端在 web/)
add_executable(cta_bench src/main.cpp)
add_executable(cta_trace src/trace.cpp)
foreach(app cta_bench cta_trace)
  target_compile_definitions(${app} PRIVATE CTA_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
  target_compile_options(${app} PRIVATE -O2 -march=native -g -Wall -Wextra)
  target_link_libraries(${app} PRIVATE Threads::Threads)
endforeach()
