#pragma once
#include <iostream>
#include <iomanip>
#include <tuple>
#include <type_traits>
#include <sstream>

constexpr int DEFAULT_WIDTH = 8;  // Default width for each field

// Forward declaration
template<typename T>
void print(const T& value);

// For STL containers
template<typename T>
concept Iterable = requires(T t) {
    std::begin(t);
    std::end(t);
};

// Print for iterable containers (except std::string)
template<typename T>
requires Iterable<T> && (!std::is_same_v<T, std::string>)
void print(const T& container) {
    std::cout << "[";
    bool first = true;
    for (const auto& item : container) {
        if (!first) std::cout << ", ";
        print(item); // recursive
        first = false;
    }
    std::cout << "]";
}

// Print for std::pair
template<typename T1, typename T2>
void print(const std::pair<T1, T2>& p) {
    std::cout << "(";
    print(p.first);
    std::cout << ", ";
    print(p.second);
    std::cout << ")";
}

// Print for std::tuple
template<std::size_t Index = 0, typename... Ts>
void print_tuple(const std::tuple<Ts...>& t) {
    if constexpr (Index < sizeof...(Ts)) {
        if constexpr (Index > 0) std::cout << ", ";
        print(std::get<Index>(t));
        print_tuple<Index + 1>(t);
    }
}

template<typename... Ts>
void print(const std::tuple<Ts...>& t) {
    std::cout << "(";
    print_tuple(t);
    std::cout << ")";
}

// Fallback print (for primitive types, string, etc.)
template<typename T>
void print(const T& value) {
    std::stringstream ss;
    ss << value;
    std::string str = ss.str();
    if (str.length() > DEFAULT_WIDTH) {
        str = str.substr(0, DEFAULT_WIDTH - 3) + "...";
    }
    std::cout << std::left << std::setw(DEFAULT_WIDTH) << str;
}

// Variadic print
inline void println() {
    std::cout << std::endl;
}

template<typename First, typename... Rest>
void println(const First& first, const Rest&... rest) {
    print(first);
    if constexpr (sizeof...(rest) > 0) {
        std::cout << " ";
        println(rest...);
    } else {
        std::cout << std::endl;
    }
}
