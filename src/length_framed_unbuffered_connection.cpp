#include "length_framed_unbuffered_connection.hpp"

#include <array>

#include <boost/asio/write.hpp>
#include <boost/asio/read.hpp>

#include "check.hpp"
#include "byte_order.hpp"

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
  current_request_ = std::move(new_request);
  if (!socket_.is_open()) {
    io_service_.dispatch([&] {
        reconnect();
        if (socket_.is_open()) send_current_request();
    });
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

void length_framed_unbuffered_connection::reconnect() {
  boost::system::error_code error;
  for (const auto& endpoint : endpoints_) {
    socket_.close();
    socket_.connect(endpoint, error);

    if (!error) break;
  }

  if (error) { fail(error); return; }
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
    io_service_.dispatch(std::bind(current_request_.on_response,
                                   std::move(response_), std::error_code{}));
    reset();
  }
}

void length_framed_unbuffered_connection::fail(
    boost::system::error_code error) {
  // Convert boost::system_error to std::system_error.
  fail(std::make_error_code(static_cast<std::errc>(error.value())));
}

void length_framed_unbuffered_connection::fail(std::error_code error) {
  if (current_request_.on_response) {
    io_service_.dispatch(
        std::bind(current_request_.on_response, std::string{}, error));
  }

  socket_.close();
  reset();
}

void length_framed_unbuffered_connection::reset() {
  current_request_.reset();
  std::string().swap(response_);
  response_length_ = request_length_ = 0;
}

}  // namespace riak
