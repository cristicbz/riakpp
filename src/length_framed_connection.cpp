#include "length_framed_connection.hpp"

#include <array>
#include <utility>

#include <boost/asio/buffer.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>

#include "byte_order.hpp"
#include "check.hpp"
#include "debug_log.hpp"
#include "endpoint_vector.hpp"

namespace riak {
namespace io = boost::asio;
using io::ip::tcp;

template <class Handler>
inline void set_timer(io::deadline_timer& timer, uint64_t milliseconds,
                      Handler&& handler) {
  if (milliseconds != length_framed_connection::no_deadline) {
    timer.expires_from_now(boost::posix_time::milliseconds(milliseconds));
    timer.async_wait(std::forward<Handler>(handler));
  }
}

length_framed_connection::length_framed_connection(
    boost::asio::io_service& io_service, endpoint_iterator endpoints_begin,
    endpoint_iterator endpoints_end, uint64_t connection_timeout_ms)
    : strand_{io_service},
      timer_{io_service},
      socket_{io_service},
      resolver_{io_service},
      endpoints_begin_{endpoints_begin},
      endpoints_end_{endpoints_end},
      connection_timeout_ms_{connection_timeout_ms},
      transient_{*this} {}

void length_framed_connection::async_send(request_type request,
                                          handler_type handler) {
  RIAKPP_CHECK(accepts_requests_.exchange(false));
  payload_buffer_ = std::move(request.payload);
  on_response_ = std::move(handler);
  deadline_ms_ = request.deadline_ms;
  strand_.dispatch(transient_.wrap([this] { connect(); }));
}

void length_framed_connection::connect() { connect_at(endpoints_begin_); }

void length_framed_connection::connect_at(endpoint_iterator current_endpoint) {
  RIAKPP_CHECK(!accepts_requests_);

  if (socket_.is_open()) {
    write_request();
  } else if (current_endpoint == endpoints_end_) {
    report(std::errc::connection_refused);
  } else {
    set_timer(timer_, connection_timeout_ms_,
              wrap([this](boost::system::error_code ec) {
                if (!ec) {
                  socket_.shutdown(tcp::socket::shutdown_both, ec);
                  socket_.close();
                }
              }));
    socket_.async_connect(
        *current_endpoint,
        wrap([this, current_endpoint](boost::system::error_code ec) {
          if (!ec) {
            write_request();
          } else {
            if (socket_.is_open()) {
              socket_.shutdown(tcp::socket::shutdown_both, ec);
              socket_.close();
            }
            connect_at(current_endpoint + 1);
          }
        }));
  }
}

void length_framed_connection::write_request() {
  RIAKPP_CHECK(!accepts_requests_);

  length_buffer_ = byte_order::host_to_network_long(payload_buffer_.size());
  std::array<io::const_buffer, 2> buffers = {
      {io::buffer(&length_buffer_, sizeof(length_buffer_)),
       io::buffer(payload_buffer_, payload_buffer_.size())}};

  io::async_write(socket_, std::move(buffers),
                  wrap([this](boost::system::error_code ec, size_t) {
                    if (!ec) {
                      read_response();
                      set_timer(timer_, deadline_ms_,
                                wrap([this](boost::system::error_code ec) {
                                  if (!ec) report(std::errc::timed_out);
                                }));
                    } else {
                      report(ec);
                    }
                  }));
}

void length_framed_connection::read_response() {
  RIAKPP_CHECK(!accepts_requests_);
  io::async_read(
      socket_, io::buffer(&length_buffer_, sizeof(length_buffer_)),
      wrap([this](boost::system::error_code ec, size_t) {
        if (ec) {
          if (ec.value() != io::error::operation_aborted) report(ec);
          return;
        }
        length_buffer_ = byte_order::network_to_host_long(length_buffer_);
        payload_buffer_.resize(length_buffer_);
        io::async_read(socket_, io::buffer(&payload_buffer_[0], length_buffer_),
                       wrap([this](boost::system::error_code ec, size_t) {
                         if (ec.value() != io::error::operation_aborted) {
                           report(ec);
                         }
                       }));
      }));
}

void length_framed_connection::report(std::errc ec) {
  auto error_code = std::make_error_code(ec);
  if (error_code) {
    payload_buffer_.clear();
    if (socket_.is_open()) socket_.close();
  }
  auto postable_handler = std::bind(std::move(on_response_), error_code,
                                    std::move(payload_buffer_));
  accepts_requests_.store(true);
  timer_.cancel();
  strand_.get_io_service().post(postable_handler);
}

inline void length_framed_connection::report(boost::system::error_code ec) {
  switch (ec.value()) {
    case io::error::eof:
      report(std::errc::not_connected);
      break;
    default:
      report(static_cast<std::errc>(ec.value()));
      break;
  }
}

}  // namespace riak
