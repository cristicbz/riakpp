#ifndef RIAKPP_LENGTH_FRAMED_CONNECTION_HPP_
#define RIAKPP_LENGTH_FRAMED_CONNECTION_HPP_

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <system_error>
#include <vector>

#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>

#include "endpoint_vector.hpp"
#include "transient.hpp"

namespace boost {
namespace system {
class error_code;
}  // namespace system
namespace asio {
class io_service;
}  // namespace asio
}  // namespace boost

namespace riak {

class length_framed_connection {
 public:
  using response_type = std::string;
  using error_type = std::error_code;
  using handler_type = std::function<void(error_type, response_type&)>;

  static constexpr uint64_t no_deadline = -1;
  static constexpr uint64_t default_connection_timeout = 1500;

  struct request_type {
    request_type() = default;
    explicit request_type(std::string payload) : payload(std::move(payload)) {}
    request_type(std::string payload, uint64_t deadline_ms)
        : payload(std::move(payload)), deadline_ms{deadline_ms} {}

    std::string payload;
    uint64_t deadline_ms = no_deadline;
  };

  length_framed_connection(
      boost::asio::io_service& io_service, endpoint_iterator endpoints_begin,
      endpoint_iterator endpoints_end,
      uint64_t connection_timeout_ms = default_connection_timeout);

  void async_send(request_type request, handler_type handler);

  bool accepts_request() const { return accepts_requests_; }

 private:
  void connect();
  void connect_at(endpoint_iterator current_endpoint);
  void write_request();
  void read_response();
  void report(std::errc ec);

  inline void report(boost::system::error_code ec);

  template <class Handler>
  auto wrap(Handler&& handler)
      -> decltype(std::declval<transient<length_framed_connection>>().wrap(
          std::declval<boost::asio::strand>().wrap(handler))) {
    return transient_.wrap(strand_.wrap(handler));
  }

  boost::asio::strand strand_;
  boost::asio::deadline_timer timer_;
  boost::asio::ip::tcp::socket socket_;
  boost::asio::ip::tcp::resolver resolver_;

  std::atomic<bool> accepts_requests_{true};
  const endpoint_iterator endpoints_begin_;
  const endpoint_iterator endpoints_end_;
  const uint64_t connection_timeout_ms_ = 0;

  handler_type on_response_;
  std::string payload_buffer_;
  uint32_t length_buffer_ = 0;
  uint64_t deadline_ms_ = 0;

  transient<length_framed_connection> transient_;
};

}  // namespace riak

#endif  // #ifndef RIAKPP_LENGTH_FRAMED_CONNECTION_HPP_
