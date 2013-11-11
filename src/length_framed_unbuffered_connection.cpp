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

struct length_framed_unbuffered_connection::active_request_state {
  request request;
  std::string response;
  uint32_t request_length = 0;
  uint32_t response_length = 0;
  std::atomic<bool> done{false};
};

length_framed_unbuffered_connection::length_framed_unbuffered_connection(
    asio::io_service& io_service,
    const std::vector<ip::tcp::endpoint>& endpoints)
    : strand_{io_service},
      socket_{io_service},
      endpoints_{endpoints},
      blocker_{*this} {}

length_framed_unbuffered_connection::~length_framed_unbuffered_connection() {
  shutdown();
  blocker_.destroy();
}

void length_framed_unbuffered_connection::send_and_consume_request(
    request& new_request) {
  // If there's another active request, kill the process.
  RIAKPP_CHECK(!has_active_request_.exchange(true))
      << "Unbuffered connection called again before request completion.";

  // Send the request, but first reconnect asynchronously if disconnected.
  auto new_request_state = std::make_shared<active_request_state>();
  new_request_state->request = std::move(new_request);
  current_request_state_ = new_request_state;
  if (!socket_.is_open()) {
    reconnect(std::move(new_request_state));
  } else {
    send_active_request(std::move(new_request_state));
  }
}

void length_framed_unbuffered_connection::shutdown() {
  if (auto state = current_request_state_.lock()) {
    state->done = true;
    auto blocking_this = blocker_.new_ptr();
    strand_.post([blocking_this] { blocking_this->socket_.close(); });
  }
}

void length_framed_unbuffered_connection::reconnect(shared_request_state state,
                                                    size_t endpoint_index) {
  RIAKPP_CHECK_LT(endpoint_index, endpoints_.size());

  auto blocking_this = blocker_.new_ptr();
  auto callback = [blocking_this, state, endpoint_index](
      asio_error error) {
    if (error) {
      if (endpoint_index == blocking_this->endpoints_.size() - 1) {
        if (blocking_this->socket_.is_open()) blocking_this->socket_.close();
        blocking_this->finalize_request(std::move(state), error);
      } else {
        blocking_this->reconnect(std::move(state), endpoint_index + 1);
      }
    } else {
      blocking_this->send_active_request(std::move(state));
    }
  };

  if (socket_.is_open()) socket_.close();
  socket_.async_connect(endpoints_[endpoint_index],
                        strand_.wrap(std::move(callback)));
}

void length_framed_unbuffered_connection::send_active_request(
    shared_request_state state) {
  if (state->done) return;

  auto& content = state->request.message;
  auto& length = state->request_length;
  length = byte_order::host_to_network_long(content.size());

  std::array<asio::const_buffer, 2> buffers = {{
      asio::buffer(reinterpret_cast<const uint8_t*>(&length), sizeof(length)),
      asio::buffer(content.data(), content.size())}};

  asio::async_write(
      socket_, std::move(buffers),
      strand_.wrap(std::bind(&self_type::wait_for_response, blocker_.new_ptr(),
                             state, ph::_1, ph::_2)));
}

void length_framed_unbuffered_connection::wait_for_response(
    shared_request_state state, asio_error error, size_t /*bytes*/) {
  if (handle_abort_conditions(state, error)) return;

  auto& length = state->response_length;
  auto handler = std::bind(&self_type::wait_for_response_body,
                           blocker_.new_ptr(), state, ph::_1, ph::_2);
  asio::async_read(
      socket_,
      asio::buffer(reinterpret_cast<uint8_t*>(&length), sizeof(length)),
      strand_.wrap(std::move(handler)));
}

void length_framed_unbuffered_connection::wait_for_response_body(
    shared_request_state state, asio_error error, size_t /*bytes*/) {
  if (handle_abort_conditions(state, error)) return;

  auto& length = state->response_length;
  auto& content = state->response;
  length = byte_order::host_to_network_long(length);
  content.resize(length);

  auto handler = std::bind(&self_type::on_response,
                           blocker_.new_ptr(), state, ph::_1, ph::_2);
  asio::async_read(socket_, asio::buffer(&content[0], length),
                   strand_.wrap(std::move(handler)));
}

void length_framed_unbuffered_connection::on_response(
    shared_request_state state, asio_error error, size_t /*bytes*/) {
  if (state->done) return;
  finalize_request(std::move(state), error);
}

void length_framed_unbuffered_connection::finalize_request(
    shared_request_state state, asio_error error) {
  // Convert boost::system_error to std::system_error.
  finalize_request(std::move(state),
                   std::make_error_code(static_cast<std::errc>(error.value())));
}

void length_framed_unbuffered_connection::finalize_request(
    shared_request_state state, std::error_code error) {
  RIAKPP_CHECK(current_request_state_.lock() == state);
  has_active_request_.store(false);
  state->done.store(true);
  if (state->request.on_response) {
    auto handler = std::bind(state->request.on_response,
                             std::move(state->response), error);
    state.reset();
    handler();
  }
}

bool length_framed_unbuffered_connection::handle_abort_conditions(
    shared_request_state state, asio_error error) {
  if (state->done) return true;
  if (error) {
    finalize_request(state, error);
    return true;
  }
  return false;
}

}  // namespace riak
