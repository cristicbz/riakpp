#include "length_framed_unbuffered_connection.hpp"

#include <array>

#include <boost/asio/write.hpp>
#include <boost/asio/read.hpp>

#include "byte_order.hpp"
#include "check.hpp"
#include "debug_log.hpp"

namespace riak {

namespace ph = std::placeholders;
namespace asio = boost::asio;
namespace ip = boost::asio::ip;

length_framed_unbuffered_connection::length_framed_unbuffered_connection(
    asio::io_service& io_service,
    const std::vector<ip::tcp::endpoint>& endpoints)
    : io_service_{io_service},
      socket_{io_service},
      endpoints_{endpoints} {}

length_framed_unbuffered_connection::~length_framed_unbuffered_connection() {
  cancelled_ = true;
  std::unique_lock<std::mutex> lock{mutex_};
  while (has_active_request_) on_request_finished_.wait(lock);
}

void length_framed_unbuffered_connection::send_and_consume_request(
    request& new_request) {
  // If there's another active request, kill the process.
  RIAKPP_CHECK(!has_active_request_.exchange(true))
      << "Unbuffered connection called again before request completion.";

  // Send the request, but first reconnect asynchrnonously if disconnected.
  current_request_ = std::move(new_request);
  if (!socket_.is_open()) {
    reconnect(std::bind(&self_type::send_current_request, this));
  } else {
    send_current_request();
  }
}

void length_framed_unbuffered_connection::shutdown() {
  cancelled_ = true;
}

void length_framed_unbuffered_connection::send_current_request() {
  RIAKPP_CHECK(has_active_request_);
  if (abort_request()) return;

  request_length_ = current_request_.message.size();
  request_length_ = riak::byte_order::host_to_network_long(request_length_);
  std::array<boost::asio::const_buffer, 2> buffers = {
      {asio::const_buffer(reinterpret_cast<uint8_t*>(&request_length_), 4),
       asio::buffer(current_request_.message.data(),
                    current_request_.message.size())}};
  auto callback =
      std::bind(&self_type::wait_for_response, this, ph::_1, ph::_2);

  asio::async_write(socket_, buffers, callback);
}

template <class Handler>
void length_framed_unbuffered_connection::reconnect(Handler on_connection,
                                                    size_t endpoint_index) {
  RIAKPP_CHECK(has_active_request_);
  RIAKPP_CHECK_LT(endpoint_index, endpoints_.size());
  if (abort_request()) return;
  socket_.async_connect(
      endpoints_[endpoint_index],
      [this, endpoint_index, on_connection](boost::system::error_code error) {
        if (abort_request()) return;
        if (error) {
          if (endpoint_index == endpoints_.size() - 1) {
            if (socket_.is_open()) socket_.close();
            finalize_request(error);
          } else {
            reconnect(on_connection, endpoint_index + 1);
          }
        } else {
          on_connection();
        }
      });
}

void length_framed_unbuffered_connection::wait_for_response(
    boost::system::error_code error, size_t) {
  RIAKPP_CHECK(has_active_request_);
  if (abort_request()) return;
  if (error) {
    finalize_request(error);
    return;
  }

  std::string().swap(current_request_.message);
  auto length_buffer = asio::buffer(
      reinterpret_cast<uint8_t*>(&response_length_), sizeof(response_length_));
  auto callback =
      std::bind(&self_type::wait_for_response_body, this, ph::_1, ph::_2);

  asio::async_read(socket_, length_buffer, callback);
}

void length_framed_unbuffered_connection::wait_for_response_body(
    boost::system::error_code error, size_t) {
  RIAKPP_CHECK(has_active_request_);
  if (abort_request()) return;
  if (error) {
    finalize_request(error);
    return;
  }

  auto callback = std::bind(&self_type::on_response, this, ph::_1, ph::_2);
  response_length_ = riak::byte_order::network_to_host_long(response_length_);
  response_.resize(response_length_);
  auto response_buffer = asio::buffer(&response_[0], response_length_);

  asio::async_read(socket_, response_buffer, callback);
}

void length_framed_unbuffered_connection::on_response(
    boost::system::error_code error, size_t) {
  RIAKPP_CHECK(has_active_request_);
  if (abort_request()) return;
  finalize_request(error);
}

void length_framed_unbuffered_connection::finalize_request(
    boost::system::error_code error) {
  // Convert boost::system_error to std::system_error.
  finalize_request(std::make_error_code(static_cast<std::errc>(error.value())));
}

void length_framed_unbuffered_connection::finalize_request(
    std::error_code error) {
  RIAKPP_CHECK(has_active_request_);
  has_active_request_ = false;

  auto reset_active_state = [&] {
    current_request_.reset();
    std::string().swap(response_);
    response_length_ = request_length_ = 0;
  };

  if (current_request_.on_response) {
    auto on_response_closure = std::bind(
        current_request_.on_response, std::move(response_), std::move(error));
    reset_active_state();
    on_response_closure();
  } else {
    reset_active_state();
  }

  // It's very important to notify while still holding the mutex (despite
  // usual practice): if we set the flag to false, then unlock the mutex 'this'
  // may be destroyed before calling on_request_finished_.notify_one(), making
  // on_request_finished_ a dangling pointer.
  on_request_finished_.notify_one();
}

bool length_framed_unbuffered_connection::abort_request() {
  if (cancelled_) {
    has_active_request_ = false;
    on_request_finished_.notify_one();
    return true;
  }

  // Return cancelled_ is not a good idea: .notify_one() can destroy 'this' and
  // cancelled_ with it.
  return false;
}

}  // namespace riak
