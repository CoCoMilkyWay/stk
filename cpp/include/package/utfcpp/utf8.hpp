// Copyright 2006 Nemanja Trifunovic
// SPDX-License-Identifier: BSL-1.0

#include "utf8/checked.hpp"
#include <string_view>

namespace utf8
{
    // std::string overloads
    inline void append16(utfchar32_t cp, std::u16string& s)
    {
        append16(cp, std::back_inserter(s));
    }

    inline std::string utf16to8(const std::u16string& s)
    {
        std::string result;
        utf16to8(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::u16string utf8to16(const std::string& s)
    {
        std::u16string result;
        utf8to16(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::string utf32to8(const std::u32string& s)
    {
        std::string result;
        utf32to8(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::u32string utf8to32(const std::string& s)
    {
        std::u32string result;
        utf8to32(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    // std::string_view overloads
    inline std::string utf16to8(std::u16string_view s)
    {
        std::string result;
        utf16to8(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::u16string utf8to16(std::string_view s)
    {
        std::u16string result;
        utf8to16(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::string utf32to8(std::u32string_view s)
    {
        std::string result;
        utf32to8(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::u32string utf8to32(std::string_view s)
    {
        std::u32string result;
        utf8to32(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::size_t find_invalid(std::string_view s)
    {
        std::string_view::const_iterator invalid = find_invalid(s.begin(), s.end());
        return (invalid == s.end()) ? std::string_view::npos : static_cast<std::size_t>(invalid - s.begin());
    }

    inline bool is_valid(std::string_view s)
    {
        return is_valid(s.begin(), s.end());
    }

    inline std::string replace_invalid(std::string_view s, char32_t replacement)
    {
        std::string result;
        replace_invalid(s.begin(), s.end(), std::back_inserter(result), replacement);
        return result;
    }

    inline std::string replace_invalid(std::string_view s)
    {
        std::string result;
        replace_invalid(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline bool starts_with_bom(std::string_view s)
    {
        return starts_with_bom(s.begin(), s.end());
    }

    // std::u8string overloads (C++20)
    inline std::u8string utf16tou8(const std::u16string& s)
    {
        std::u8string result;
        utf16to8(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::u8string utf16tou8(std::u16string_view s)
    {
        std::u8string result;
        utf16to8(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::u16string utf8to16(const std::u8string& s)
    {
        std::u16string result;
        utf8to16(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::u16string utf8to16(const std::u8string_view& s)
    {
        std::u16string result;
        utf8to16(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::u8string utf32tou8(const std::u32string& s)
    {
        std::u8string result;
        utf32to8(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::u8string utf32tou8(const std::u32string_view& s)
    {
        std::u8string result;
        utf32to8(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::u32string utf8to32(const std::u8string& s)
    {
        std::u32string result;
        utf8to32(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::u32string utf8to32(const std::u8string_view& s)
    {
        std::u32string result;
        utf8to32(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline std::size_t find_invalid(const std::u8string& s)
    {
        std::u8string::const_iterator invalid = find_invalid(s.begin(), s.end());
        return (invalid == s.end()) ? std::string_view::npos : static_cast<std::size_t>(invalid - s.begin());
    }

    inline bool is_valid(const std::u8string& s)
    {
        return is_valid(s.begin(), s.end());
    }

    inline std::u8string replace_invalid(const std::u8string& s, char32_t replacement)
    {
        std::u8string result;
        replace_invalid(s.begin(), s.end(), std::back_inserter(result), replacement);
        return result;
    }

    inline std::u8string replace_invalid(const std::u8string& s)
    {
        std::u8string result;
        replace_invalid(s.begin(), s.end(), std::back_inserter(result));
        return result;
    }

    inline bool starts_with_bom(const std::u8string& s)
    {
        return starts_with_bom(s.begin(), s.end());
    }

} // namespace utf8
