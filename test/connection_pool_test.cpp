#include <boost/asio/io_service.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <system_error>
#include <thread>
#include <utility>

#include "connection_pool.hpp"
#include "length_framed_connection.hpp"
#include "testing_util.hpp"
#include "test_length_framed_server.hpp"
#include "thread_pool.hpp"

namespace riak {
namespace testing {
namespace {
constexpr auto allow_errors = response::allow_errors_yes;

TEST(ConnectionPoolTest, SequencedMessages) {
  InSequence sequence;
  mock_server server;
  thread_pool threads{4};
  std::unique_ptr<connection_pool<length_framed_connection>> pool{
      new connection_pool<length_framed_connection>{
          threads.io_service(), "localhost", server.port(), 2, 4096, 1000}};

  send_and_expect(*pool, "okay1", 300, errc_success, "okay1_reply", [&] {
  send_and_expect(*pool, "okay2", 300, errc_success, "okay2_reply", [&] {
  send_and_expect(*pool, "timeout1", 60, std::errc::timed_out, "");
  }); });

  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("okay1")))
      .WillOnce(Return(response{30, "okay1_reply"}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("okay2")))
      .WillOnce(Return(response{50, "okay2_reply"}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("timeout1")))
      .WillOnce(Return(response{70, "timeout1_reply", allow_errors}));
  server.expect_eof_and_close();

  // A single connection should be neccessary, since the messages are sent only
  // when the previous ones are received.
  server.run(1);
}

TEST(ConnectionPoolTest, ManyMessages) {
  for (uint32_t num_connections = 1; num_connections < 17;
       num_connections += 3) {
    mock_server server;
    thread_pool threads{4};
    std::atomic<uint32_t> msgs_received{0};
    constexpr uint32_t msgs_to_send = 1000;

    std::unique_ptr<connection_pool<length_framed_connection>> pool{
        new connection_pool<length_framed_connection>{
            threads.io_service(), "localhost", server.port(), num_connections,
            4096, 1000}};

    // Expect 'msgs_to_send' messages (in any order) and reply to 'msg' with
    // 'msg_reply'.
    for (uint32_t i = 0; i < msgs_to_send; ++i) {
      EXPECT_CALL(server, on_receive(Eq(asio_success), _))
          .WillOnce(Invoke([&, i](asio_error, std::string request) {
            return response{request + "_reply"};
          }))
          .RetiresOnSaturation();
    }
    server.expect_eof_and_close();  // The client will close the connection.
    std::thread server_thread{[&] { server.run(num_connections, 20000); }};

    // Send 'okay1', 'okay2' ... from two different threads at the same time
    // and kill the connection pool when all message replies have been received.
    auto stop_when_done = [&] {
      if (++msgs_received == msgs_to_send) {
        pool.reset();
        threads.io_service().stop();
      }
    };

    std::thread first_half{[&] {
      for (auto i = 0; i < msgs_to_send / 2; ++i) {
        send_and_expect(*pool, "okay" + std::to_string(i), 20000, errc_success,
                        "okay" + std::to_string(i) + "_reply", stop_when_done);
      }
    }};

    std::thread second_half{[&] {
      for (auto i = msgs_to_send / 2; i < msgs_to_send; ++i) {
        send_and_expect(*pool, "okay" + std::to_string(i), 20000, errc_success,
                        "okay" + std::to_string(i) + "_reply", stop_when_done);
      }
    }};

    first_half.join();
    second_half.join();
    server_thread.join();

    // Compute the variance of the reply counts of each connection and ensure
    // it is less than 20% -- a fair load balancing.
    size_t i_connection = 0;
    double variance = 0.0;
    double mean = static_cast<double>(msgs_to_send) / num_connections;
    std::stringstream counts_string_builder;
    for (auto count : server.reply_counts()) {
      EXPECT_LT(0, count);
      counts_string_builder << count << " ";
      variance += (count - mean) * (count - mean) / num_connections;
      ++i_connection;
    }
    EXPECT_LE(sqrt(variance), 2 * msgs_to_send / 100)
        << "FLAKY: This test checks for the variance of a load balancer, "
           "it is statistically possible that the test fails on a normal run. "
           "The counts are: " << counts_string_builder.str();
  }
}

TEST(ConnectionPoolTest, ConnectionRefused) {
  for (int i_run = 0; i_run < 100; ++i_run) {
    constexpr uint32_t msgs_to_send = 20;
    std::atomic<uint32_t> msgs_to_receive{msgs_to_send};
    thread_pool threads{4};
    std::unique_ptr<connection_pool<length_framed_connection>> pool{
        new connection_pool<length_framed_connection>{
            threads.io_service(), "localhost", random_port(), 3, 4096, 1000}};

    for (size_t i_msg = 0; i_msg < msgs_to_send; ++i_msg) {
      send_and_expect(*pool, "a", 5000, std::errc::connection_refused, "", [&] {
        if (--msgs_to_receive == 0) {
          pool.reset();
          threads.io_service().stop();
        }
      });
    }
    threads.io_service().run();
  }
}

}  // namespace
}  // namespace testing
}  // namespace riak
