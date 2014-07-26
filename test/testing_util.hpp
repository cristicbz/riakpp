#ifndef RIAKPP_TESTING_UTIL_HPP_
#define RIAKPP_TESTING_UTIL_HPP_

#include <system_error>
#include <gtest/gtest.h>

namespace boost {
namespace asio {
class io_service;
}  // namespace asio

namespace system {
class error_code;
}  // namespace system
}  // namespace boost

namespace riak {
namespace testing {

namespace io = boost::asio;
namespace ip = io::ip;

using namespace ::testing;
using io::ip::tcp;
using asio_error = boost::system::error_code;

constexpr auto errc_success = static_cast<std::errc>(0);
extern const asio_error asio_success;

inline uint16_t random_port() {
  std::random_device device;
  std::default_random_engine engine{device()};
  return static_cast<uint16_t>(engine() % (65536 - 10001) + 10001);
}

inline void do_nothing() {}

template <class Connection, class Handler = void (*)(void)>
inline void send_and_expect(Connection& connection, std::string request,
                     uint64_t deadline_ms, std::errc expect_errc,
                     std::string expect_reply, Handler&& handler = do_nothing) {
  connection.async_send(
      {std::move(request), deadline_ms},
      [expect_errc, expect_reply, handler](std::error_code ec,
                                           std::string& reply) {
      auto expect_ec = std::make_error_code(expect_errc);
      EXPECT_EQ(expect_ec, ec) << ec.message() << " vs " << expect_ec.message();
      if (expect_ec == ec) {
        EXPECT_EQ(expect_reply, reply);
      }
      handler();
    });
}

}  // namespace testing
}  // namespace riak

#endif  // #ifndef RIAKPP_TESTING_UTIL_HPP_
