cmake_minimum_required(VERSION 3.16)
project(implot LANGUAGES CXX)

# Collect source files
set(IMPLOT_SOURCE
    implot.cpp
    implot_demo.cpp
    implot_items.cpp
)

add_library(implot STATIC ${IMPLOT_SOURCE})

target_include_directories(implot SYSTEM PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(implot PUBLIC imgui)

# Use precise math for implot
if(IS_WINDOWS)
    target_compile_options(implot PRIVATE /fp:precise)
else()
    target_compile_options(implot PRIVATE -fno-fast-math)
endif()

