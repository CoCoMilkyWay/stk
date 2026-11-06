cmake_minimum_required(VERSION 3.28)
project(cta CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

# 断言常开 (不设 NDEBUG)
add_executable(cta_bench src/main.cpp)
target_compile_options(cta_bench PRIVATE -O2 -march=native -g -Wall -Wextra)
target_link_libraries(cta_bench PRIVATE Threads::Threads)
