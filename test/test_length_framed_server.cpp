#include "test_length_framed_server.hpp"

#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/deadline_timer.hpp>

#include <algorithm>
#include <random>
#include <gtest/gtest.h>

#include "byte_order.hpp"
#include "check.hpp"
#include "debug_log.hpp"

namespace riak {
namespace testing {

const asio_error asio_success{};

test_length_framed_server::test_length_framed_server()
    : port_{random_port()},
      acceptor_{io_service_, tcp::endpoint{tcp::v4(), port_}} {}

test_length_framed_server::~test_length_framed_server() {
  if (expected_sessions_ != static_cast<decltype(expected_sessions_)>(-1)) {
    EXPECT_EQ(max_sessions_, expected_sessions_);
  }
}

test_length_framed_server::session::session(test_length_framed_server& server)
    : server_(server), socket_{server.io_service()}, transient_{*this} {}

test_length_framed_server::session::~session() {
  RIAKPP_DLOG << "Destroyed session " << this;
}

void test_length_framed_server::session::close_session() {
  boost::system::error_code ec;
  socket_.shutdown(tcp::socket::shutdown_both, ec);
  if (socket_.is_open()) socket_.close();
  server_.delete_session(this);
}

void test_length_framed_server::run(size_t expected_sessions,
                                    uint64_t timeout_ms) {
  io_service_.reset();
  reply_counts_.clear();
  max_sessions_ = 0;
  expected_sessions_ = expected_sessions;
  if (timeout_ms > 0) {
    auto timer = std::make_shared<io::deadline_timer>(io_service_);
    timer->expires_from_now(boost::posix_time::milliseconds(timeout_ms));
    timer->async_wait(
        [this, timer, timeout_ms](boost::system::error_code ec) {
          if (!ec) {
            stop();
            ASSERT_TRUE(false) << "Timed out after " << timeout_ms << "ms.";
          }
        });
  }
  accept();
  io_service_.run();
}

void test_length_framed_server::stop() {
  io_service_.stop();
}

void test_length_framed_server::delete_session(session* which) {
  io_service_.post([this, which] {
    RIAKPP_DLOG << "Removed session " << which;
    sessions_.erase(
        std::remove_if(sessions_.begin(), sessions_.end(),
                       [&](const std::unique_ptr<session>& current) {
                           return current.get() == which;
                       }),
        sessions_.end());
  });
}

void test_length_framed_server::accept() {
  session* new_session = nullptr;
  sessions_.emplace_back(new_session = new session{*this});
  RIAKPP_DLOG << "New session " << new_session;
  acceptor_.async_accept(sessions_.back()->socket(),
      [this, new_session] (boost::system::error_code ec) {
        RIAKPP_DLOG << "Conected session " << new_session;
        ASSERT_FALSE(ec) << ec.message();
        ASSERT_LE(sessions_.size(), expected_sessions_);
        if (sessions_.size() > max_sessions_) max_sessions_ = sessions_.size();
        new_session->wait_for_request();
        accept();
      });
}

void test_length_framed_server::session::wait_for_request() {
  RIAKPP_CHECK(!processing_request_);
  processing_request_ = true;
  io::async_read(
      socket_, io::buffer(&length_buffer_, sizeof(length_buffer_)),
      transient_.wrap([this] (boost::system::error_code ec, size_t length) {
          if (ec == io::error::operation_aborted) {
            return;
          } else if (ec) {
            server_.on_receive(ec, {});
            close_session();
            return;
          }
          ASSERT_EQ(sizeof(length_buffer_), length);
          length_buffer_ = byte_order::network_to_host_long(length_buffer_);
          payload_buffer_.resize(length_buffer_);
          io::async_read(
              socket_, io::buffer(&payload_buffer_[0], length_buffer_),
              [this] (boost::system::error_code ec, size_t length) {
                  if (ec == io::error::operation_aborted) {
                    return;
                  } else if (ec) {
                    server_.on_receive(ec, {});
                    close_session();
                    return;
                  }
                  ASSERT_EQ(length_buffer_, length);
                  RIAKPP_DLOG << "Received ec=" << ec.message()
                              << " msg=" << payload_buffer_;
                  ++server_.reply_counts_.emplace(this, 0).first->second;
                  reply(server_.on_receive(ec, payload_buffer_));
              });
      }));
}

void test_length_framed_server::session::reply(response with) {
  if (with.type == response::type_close) {
    RIAKPP_DLOG << "Closing socket.";
    close_session();
  } else if (with.type == response::type_defer) {
    RIAKPP_DLOG << "Waiting " << with.milliseconds << "ms, then sending '"
                << with.message << "'.";
    auto timer = std::make_shared<io::deadline_timer>(server_.io_service());
    timer->expires_from_now(boost::posix_time::milliseconds(with.milliseconds));
    timer->async_wait(
        transient_
            .wrap([this, with, timer](boost::system::error_code ec) mutable {
              if (!ec) reply(response{std::move(with.message)});
            }));
  } else {
    RIAKPP_CHECK_EQ(response::type_message, with.type);
    payload_buffer_ = std::move(with.message);
    length_buffer_ = byte_order::host_to_network_long(payload_buffer_.size());
    std::array<io::const_buffer, 2> buffers = {{
        io::buffer(&length_buffer_, sizeof(length_buffer_)),
        io::buffer(payload_buffer_, payload_buffer_.size())}};

    RIAKPP_DLOG << "Replying with '" << payload_buffer_ << "'.";
    io::async_write(
        socket_, std::move(buffers), transient_.wrap(
        [this, with] (boost::system::error_code ec, size_t length) {
            if (ec == io::error::operation_aborted) {
              return;
            } else if (ec) {
              close_session();
              if (!with.allow_errors) ASSERT_FALSE(ec) << ec.message();
              return;
            }
            ASSERT_EQ(payload_buffer_.size() + sizeof(length_buffer_), length);
            processing_request_ = false;
            RIAKPP_DLOG << "Replied, ec=" << ec.message()
                        << ". Waiting on request.";
            wait_for_request();
        }));
  }
}

}  // namespace testing
}  // namespace riak
