#include "length_framed_unbuffered_connection.hpp"

#include <array>

#include <boost/asio/write.hpp>
#include <boost/asio/read.hpp>

#include "byte_order.hpp"
#include "check.hpp"

namespace riak {

namespace ph = std::placeholders;
namespace asio = boost::asio;
namespace ip = boost::asio::ip;

length_framed_unbuffered_connection::length_framed_unbuffered_connection(
    boost::asio::io_service& io_service,
    const std::vector<boost::asio::ip::tcp::endpoint>& endpoints)
    : io_service_{io_service},
      socket_{io_service},
      endpoints_{endpoints} {}

void length_framed_unbuffered_connection::send_and_consume_request(
    request& new_request) {
  current_request_ = new_request;
  if (!socket_.is_open()) {
    reconnect(std::bind(&self_type::send_current_request, this));
  } else {
    send_current_request();
  }
}

void length_framed_unbuffered_connection::send_current_request() {
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
  RIAKPP_CHECK_LT(endpoint_index, endpoints_.size());

  if (socket_.is_open()) socket_.close();
  socket_.async_connect(
      endpoints_[endpoint_index],
      [this, endpoint_index, on_connection](boost::system::error_code error) {
        if (error) {
          if (endpoint_index == endpoints_.size() - 1) {
            if (socket_.is_open()) socket_.close();
            fail(error);
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
  std::string().swap(current_request_.message);
  if (error) {
    fail(error);
    return;
  }
  auto length_buffer = asio::buffer(
      reinterpret_cast<uint8_t*>(&response_length_), sizeof(response_length_));
  auto callback =
      std::bind(&self_type::wait_for_response_body, this, ph::_1, ph::_2);
  asio::async_read(socket_, length_buffer, callback);
}

void length_framed_unbuffered_connection::wait_for_response_body(
    boost::system::error_code error, size_t) {
  if (error) {
    fail(error);
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
  if (error) {
    fail(error);
    return;
  }

  if (current_request_.on_response) {
    auto response_closure = std::bind(current_request_.on_response,
                                      std::move(response_), std::error_code{});
    reset();
    io_service_.dispatch(response_closure);
  }
}

void length_framed_unbuffered_connection::fail(
    boost::system::error_code error) {
  // Convert boost::system_error to std::system_error.
  fail(std::make_error_code(static_cast<std::errc>(error.value())));
}

void length_framed_unbuffered_connection::fail(std::error_code error) {
  if (current_request_.on_response) {
    auto response_closure =
        std::bind(current_request_.on_response, std::string{}, error);
    io_service_.dispatch(response_closure);
  } else {
    reset();
    if (socket_.is_open()) socket_.close();
  }
}

void length_framed_unbuffered_connection::reset() {
  current_request_.reset();
  std::string().swap(response_);
  response_length_ = request_length_ = 0;
}

}  // namespace riak
