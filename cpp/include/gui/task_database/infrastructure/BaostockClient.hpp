// Data source: Baostock (证券宝) - http://www.baostock.com
// Free, open-source financial data API for Chinese stock market
// Provides: stock metadata, historical K-data, adjust factors, trading calendar
//
// BaoStock C++ Async Client - Clean and High-Performance Implementation
// Uses C++20 Coroutines with Boost.Asio

#pragma once

#include "misc/cross_platform.hpp"
#include "package/nlohmann/json.hpp"
#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <array>
#include <cstdint>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <zlib.h>

namespace GUI::Database {

using boost::asio::awaitable;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;
using json = nlohmann::json;

// ============================================================================
// Constants - All protocol constants defined here
// ============================================================================

// Server Configuration
constexpr const char *SERVER_HOST = "www.baostock.com";
constexpr uint16_t SERVER_PORT = 10030;
constexpr const char *CLIENT_VERSION = "00.8.90";

// Protocol Constants
constexpr const char *MESSAGE_SPLIT = "\x01";
constexpr size_t MESSAGE_HEADER_LENGTH = 21;
constexpr size_t BODY_LENGTH_FIELD_WIDTH = 10;
constexpr int PER_PAGE_COUNT = 10000;
constexpr const char *COMPRESSED_DELIMITER = "<![CDATA[]]>\n";
constexpr size_t COMPRESSED_DELIMITER_LENGTH = 13;

// Message Types - Login/Logout
constexpr const char *MSG_TYPE_LOGIN_REQUEST = "00";
constexpr const char *MSG_TYPE_LOGIN_RESPONSE = "01";
constexpr const char *MSG_TYPE_LOGOUT_REQUEST = "02";
constexpr const char *MSG_TYPE_LOGOUT_RESPONSE = "03";

// Message Types - K-Line Data
constexpr const char *MSG_TYPE_GETKDATA_REQUEST = "11";
constexpr const char *MSG_TYPE_GETKDATA_RESPONSE = "12";
constexpr const char *MSG_TYPE_GETKDATAPLUS_REQUEST = "95";
constexpr const char *MSG_TYPE_GETKDATAPLUS_RESPONSE = "96";

// Message Types - Adjustment Factor
constexpr const char *MSG_TYPE_ADJUSTFACTOR_REQUEST = "15";
constexpr const char *MSG_TYPE_ADJUSTFACTOR_RESPONSE = "16";

// Message Types - Stock Info
constexpr const char *MSG_TYPE_QUERYSTOCKBASIC_REQUEST = "45";
constexpr const char *MSG_TYPE_QUERYSTOCKBASIC_RESPONSE = "46";
constexpr const char *MSG_TYPE_QUERYSTOCKINDUSTRY_REQUEST = "59";
constexpr const char *MSG_TYPE_QUERYSTOCKINDUSTRY_RESPONSE = "60";

// Message Types - Trading Calendar
constexpr const char *MSG_TYPE_QUERYTRADEDATES_REQUEST = "33";
constexpr const char *MSG_TYPE_QUERYTRADEDATES_RESPONSE = "34";

// Error Codes
constexpr const char *BSERR_SUCCESS = "0";
constexpr const char *BSERR_USER_NOT_LOGIN = "10001001";
constexpr const char *BSERR_NETWORK_ERROR = "10002007";
constexpr const char *BSERR_PARAM_ERROR = "10004006";

// Default Values
constexpr const char *DEFAULT_USER = "anonymous";
constexpr const char *DEFAULT_PASSWORD = "123456";

// ============================================================================
// Data Structures
// ============================================================================

// Response result for API calls
struct QueryResult {
  std::string error_code;
  std::string error_msg;
  std::vector<std::string> fields;
  std::vector<std::vector<std::string>> records;

  bool success() const { return error_code == BSERR_SUCCESS; }
};

// ============================================================================
// Helper Functions
// ============================================================================

namespace detail {

// Pad string with zeros
inline std::string pad_zero(int value, size_t width, bool left_pad) {
  std::string str = std::to_string(value);
  if (str.length() >= width)
    return str;

  std::string padding(width - str.length(), '0');
  return left_pad ? (padding + str) : (str + padding);
}

// Calculate CRC32 checksum
inline std::string calculate_crc32(const std::string &data) {
  uint32_t crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, reinterpret_cast<const Bytef *>(data.c_str()), data.length());
  return std::to_string(crc);
}

// Build message header
inline std::string build_header(const std::string &msg_type, size_t body_length) {
  return std::string(CLIENT_VERSION) + MESSAGE_SPLIT +
         msg_type + MESSAGE_SPLIT +
         pad_zero(body_length, BODY_LENGTH_FIELD_WIDTH, true);
}

// Build complete message
inline std::string build_message(const std::string &msg_type, const std::string &body) {
  std::string header = build_header(msg_type, body.length());
  std::string head_body = header + body;
  std::string crc = calculate_crc32(head_body);
  std::string result = head_body + MESSAGE_SPLIT + crc + "\n";
  return result;
}

// Check if response ends with compressed data delimiter
inline bool has_compressed_suffix(const std::string &response) {
  return response.size() >= COMPRESSED_DELIMITER_LENGTH &&
         response.compare(response.size() - COMPRESSED_DELIMITER_LENGTH,
                          COMPRESSED_DELIMITER_LENGTH,
                          COMPRESSED_DELIMITER) == 0;
}

// Decompress zlib-compressed data with dynamic buffer resizing
inline std::optional<std::string> decompress_zlib(const unsigned char *data, size_t length) {
  if (!data || length == 0) return std::nullopt;

  uLongf buffer_size = static_cast<uLongf>(length) * 4;
  for (int attempt = 0; attempt < 6; ++attempt) {
    std::vector<unsigned char> buffer(buffer_size);
    uLongf actual_size = buffer_size;
    int result = uncompress(buffer.data(), &actual_size, data, length);
    if (result == Z_OK) {
      return std::string(reinterpret_cast<char *>(buffer.data()), actual_size);
    }
    if (result != Z_BUF_ERROR) break;
    buffer_size *= 2;
  }
  return std::nullopt;
}

// Normalize response: decompress if needed
// Follows official Python implementation (baostock/util/socketutil.py:74-84)
inline std::pair<std::string, bool> normalize_response(std::string response) {
  bool is_decompressed = false;

  if (response.size() <= MESSAGE_HEADER_LENGTH) {
    return {std::move(response), is_decompressed};
  }

  // Parse header: version|msg_type|body_length
  std::string header = response.substr(0, MESSAGE_HEADER_LENGTH);
  std::vector<std::string> header_parts;
  std::stringstream header_ss(header);
  std::string part;
  while (std::getline(header_ss, part, MESSAGE_SPLIT[0])) {
    header_parts.push_back(part);
  }

  if (header_parts.size() < 3) {
    return {std::move(response), is_decompressed};
  }

  std::string msg_type = header_parts[1];
  
  // Only MSG_TYPE_GETKDATAPLUS_RESPONSE ("96") uses compression
  // This matches Python's COMPRESSED_MESSAGE_TYPE_TUPLE = (MESSAGE_TYPE_GETKDATAPLUS_RESPONSE, )
  if (msg_type != MSG_TYPE_GETKDATAPLUS_RESPONSE) {
    return {std::move(response), is_decompressed};
  }

  // Check for compressed data delimiter <![CDATA[]]>\n
  if (!has_compressed_suffix(response)) {
    return {std::move(response), is_decompressed};
  }

  // Extract compressed length from header (body_length field)
  int compressed_len = 0;
  try {
    compressed_len = std::stoi(header_parts[2]);
  } catch (...) {
    return {std::move(response), is_decompressed};
  }

  if (compressed_len <= 0) {
    return {std::move(response), is_decompressed};
  }

  // Decompress: receive[MESSAGE_HEADER_LENGTH:MESSAGE_HEADER_LENGTH + compressed_len]
  const unsigned char *compressed_data =
      reinterpret_cast<const unsigned char *>(response.data() + MESSAGE_HEADER_LENGTH);
  
  auto decompressed_opt = decompress_zlib(compressed_data, compressed_len);
  if (decompressed_opt.has_value()) {
    std::string result = header + decompressed_opt.value();
    is_decompressed = true;
    return {std::move(result), is_decompressed};
  }

  return {std::move(response), is_decompressed};
}

// Parse response message
// Follows official Python implementation (baostock/data/resultset.py:87-111)
inline QueryResult parse_response(const std::string &response, bool is_decompressed = false) {
  QueryResult result;

  // Extract body (skip header and CRC)
  std::string body;
  if (is_decompressed) {
    body = response.substr(MESSAGE_HEADER_LENGTH);
    if (!body.empty() && body.back() == '\n') {
      body.pop_back();
    }
  } else {
    size_t crc_pos = response.rfind(MESSAGE_SPLIT);
    if (crc_pos == std::string::npos) {
      result.error_code = "parse_error";
      result.error_msg = "Invalid message format (no CRC separator)";
      return result;
    }
    body = response.substr(MESSAGE_HEADER_LENGTH, crc_pos - MESSAGE_HEADER_LENGTH);
  }

  // Split body by MESSAGE_SPLIT
  std::vector<std::string> parts;
  std::stringstream ss(body);
  std::string part;
  while (std::getline(ss, part, MESSAGE_SPLIT[0])) {
    parts.push_back(part);
  }

  if (parts.size() < 2) {
    result.error_code = "parse_error";
    result.error_msg = "Insufficient message parts";
    return result;
  }

  // Always parse error_code and error_msg first (parts[0] and parts[1])
  result.error_code = parts[0];
  result.error_msg = parts[1];

  // Display server errors clearly in terminal
  if (result.error_code != BSERR_SUCCESS) {
    std::cout << "[Baostock Server Error] Code: " << result.error_code 
              << ", Message: " << result.error_msg << std::endl;
    return result;
  }

  // Only parse data if error_code == "0" (success)
  if (parts.size() > 6) {
    if (!parts[6].empty()) {
      try {
        auto j = json::parse(parts[6]);
        if (j.contains("record") && j["record"].is_array()) {
          for (const auto &rec : j["record"]) {
            std::vector<std::string> row;
            for (const auto &val : rec) {
              row.push_back(val.is_string() ? val.get<std::string>() : val.dump());
            }
            result.records.push_back(row);
          }
        }
      } catch (...) {
      }
    }

    for (size_t i = 8; i < parts.size(); ++i) {
      if (parts[i].find(',') != std::string::npos) {
        std::string fields_str = parts[i];
        std::stringstream fields_ss(fields_str);
        std::string field;
        while (std::getline(fields_ss, field, ',')) {
          result.fields.push_back(field);
        }
        break;
      }
    }
  }

  return result;
}

} // namespace detail

// ============================================================================
// BaoStock Async Client - Main Class
// ============================================================================

class BaostockClient {
private:
  boost::asio::io_context &io_context_;
  tcp::socket socket_;
  bool logged_in_;
  std::string user_id_;

public:
  explicit BaostockClient(boost::asio::io_context &io_context)
      : io_context_(io_context), socket_(io_context), logged_in_(false) {}

  // Connect to server
  awaitable<bool> connect() {
    try {
      tcp::resolver resolver(io_context_);
      auto endpoints = co_await resolver.async_resolve(
          SERVER_HOST, std::to_string(SERVER_PORT), use_awaitable);
      [[maybe_unused]] auto ep = co_await boost::asio::async_connect(
          socket_, endpoints, use_awaitable);
      co_return true;
    } catch (const std::exception &e) {
      std::cerr << "Baostock connect error: " << e.what() << std::endl;
      co_return false;
    }
  }

  // Send request and receive response
  awaitable<QueryResult> send_request(const std::string &msg_type, const std::string &body) {
    try {
      std::string message = detail::build_message(msg_type, body);
      co_await boost::asio::async_write(socket_,
                                        boost::asio::buffer(message), use_awaitable);

      std::string response;
      std::array<char, 8192> buffer;

      while (true) {
        size_t n = co_await socket_.async_read_some(
            boost::asio::buffer(buffer), use_awaitable);
        response.append(buffer.data(), n);

        if (detail::has_compressed_suffix(response)) {
          break;
        } else if (!response.empty() && response.back() == '\n') {
          if (response.size() > MESSAGE_HEADER_LENGTH) {
            break;
          }
        }
      }

      auto normalized = detail::normalize_response(std::move(response));
      co_return detail::parse_response(normalized.first, normalized.second);
    } catch (const std::exception &e) {
      QueryResult error_result;
      error_result.error_code = "network_error";
      error_result.error_msg = e.what();
      co_return error_result;
    }
  }

  // Login
  awaitable<bool> login(const std::string &user = DEFAULT_USER,
                        const std::string &password = DEFAULT_PASSWORD) {
    if (!socket_.is_open()) {
      bool connected = co_await connect();
      if (!connected) {
        co_return false;
      }
    }

    std::string body = "login" + std::string(MESSAGE_SPLIT) + user + MESSAGE_SPLIT +
                       password + MESSAGE_SPLIT + "0";
    auto result = co_await send_request(MSG_TYPE_LOGIN_REQUEST, body);

    if (result.success()) {
      logged_in_ = true;
      user_id_ = user;
      co_return true;
    } else {
      std::cerr << "Baostock login failed: " << result.error_msg << std::endl;
      co_return false;
    }
  }

  // Logout
  awaitable<void> logout() {
    if (!logged_in_)
      co_return;

    try {
      auto now = std::chrono::system_clock::now();
      auto now_t = std::chrono::system_clock::to_time_t(now);
      std::tm now_tm = safe_localtime(&now_t);
      char timestamp[15];
      std::strftime(timestamp, sizeof(timestamp), "%Y%m%d%H%M%S", &now_tm);

      std::string body = "logout" + std::string(MESSAGE_SPLIT) + user_id_ +
                         MESSAGE_SPLIT + std::string(timestamp);
      co_await send_request(MSG_TYPE_LOGOUT_REQUEST, body);
      socket_.close();
      logged_in_ = false;
    } catch (...) {
    }
  }

  // Query adjustment factors
  awaitable<QueryResult> query_adjust_factor(
      const std::string &code,
      const std::string &start_date = "1900-01-01",
      const std::string &end_date = "") {
    std::string end = end_date.empty() ? std::string("2100-01-01") : end_date;

    std::string body = "query_adjust_factor" + std::string(MESSAGE_SPLIT) +
                       user_id_ + MESSAGE_SPLIT +
                       "1" + MESSAGE_SPLIT +
                       "10000" + MESSAGE_SPLIT +
                       code + MESSAGE_SPLIT +
                       start_date + MESSAGE_SPLIT +
                       end;
    co_return co_await send_request(MSG_TYPE_ADJUSTFACTOR_REQUEST, body);
  }

  // Query history K-line data (Plus version with compression)
  awaitable<QueryResult> query_history_k_data_plus(
      const std::string &code,
      const std::string &fields,
      const std::string &start_date,
      const std::string &end_date,
      const std::string &frequency = "d",
      const std::string &adjustflag = "3") {
    std::string body = "query_history_k_data_plus" + std::string(MESSAGE_SPLIT) +
                       user_id_ + MESSAGE_SPLIT +
                       "1" + MESSAGE_SPLIT +
                       "10000" + MESSAGE_SPLIT +
                       code + MESSAGE_SPLIT +
                       fields + MESSAGE_SPLIT +
                       start_date + MESSAGE_SPLIT +
                       end_date + MESSAGE_SPLIT +
                       frequency + MESSAGE_SPLIT +
                       adjustflag;
    co_return co_await send_request(MSG_TYPE_GETKDATAPLUS_REQUEST, body);
  }

  // Query stock basic info
  awaitable<QueryResult> query_stock_basic(const std::string &code = "", const std::string &code_name = "") {
    std::string body = "query_stock_basic" + std::string(MESSAGE_SPLIT) +
                       user_id_ + MESSAGE_SPLIT +
                       "1" + MESSAGE_SPLIT +
                       "10000" + MESSAGE_SPLIT +
                       code + MESSAGE_SPLIT +
                       code_name;
    co_return co_await send_request(MSG_TYPE_QUERYSTOCKBASIC_REQUEST, body);
  }

  // Query stock industry classification
  awaitable<QueryResult> query_stock_industry(const std::string &code = "", const std::string &date = "") {
    std::string body = "query_stock_industry" + std::string(MESSAGE_SPLIT) +
                       user_id_ + MESSAGE_SPLIT +
                       "1" + MESSAGE_SPLIT +
                       "10000" + MESSAGE_SPLIT +
                       code + MESSAGE_SPLIT +
                       date;
    co_return co_await send_request(MSG_TYPE_QUERYSTOCKINDUSTRY_REQUEST, body);
  }

  // Query trade dates
  awaitable<QueryResult> query_trade_dates(
      const std::string &start_date,
      const std::string &end_date) {
    std::string body = "query_trade_dates" + std::string(MESSAGE_SPLIT) +
                       user_id_ + MESSAGE_SPLIT +
                       "1" + MESSAGE_SPLIT +
                       "10000" + MESSAGE_SPLIT +
                       start_date + MESSAGE_SPLIT +
                       end_date;
    co_return co_await send_request(MSG_TYPE_QUERYTRADEDATES_REQUEST, body);
  }

  bool is_logged_in() const { return logged_in_; }
  bool is_connected() const { return socket_.is_open(); }
};

} // namespace GUI::Database
