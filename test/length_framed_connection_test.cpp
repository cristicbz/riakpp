#include <boost/asio/io_service.hpp>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <system_error>
#include <thread>
#include <utility>

#include "debug_log.hpp"
#include "length_framed_connection.hpp"
#include "test_length_framed_server.hpp"
#include "testing_util.hpp"

namespace riak {
namespace testing {
namespace {

constexpr auto connect_timeout_ms = 100;
constexpr auto allow_errors = response::allow_errors_yes;
constexpr auto no_deadline = length_framed_connection::no_deadline;

class threaded_connection {
 public:
  threaded_connection(mock_server& server, endpoint_vector endpoints)
      : endpoints_{std::move(endpoints)}, server_(server) { run(); }

  threaded_connection(mock_server& server)
      : threaded_connection{
            server, {{ip::address_v4{{{127, 0, 0, 1}}}, server.port()}}} {}

  ~threaded_connection() { stop(); }

  void run() {
    if (!running_) {
      work_.reset(new io::io_service::work{service_});
      conn_.reset(new length_framed_connection{
          service_, endpoints_.begin(), endpoints_.end(), connect_timeout_ms});
      thread_ = std::thread{[&] {
        service_.run();
        service_.reset();
      }};
      running_ = true;
    }
  }

  void defer_stop() {
    server_.post([&] { stop(); });
  }

  void stop() {
    if (running_) {
      service_.stop();
      thread_.join();
      conn_.reset();
      running_ = false;
    }
  }

  void stop_reset_first() {
    if (running_) {
      service_.stop();
      thread_.join();
      running_ = false;
    }
  }

  io::io_service& io_service() { return service_; }
  length_framed_connection* operator->() { return conn_.get(); }
  length_framed_connection& operator*() { return *conn_; }

 private:
  endpoint_vector endpoints_;
  io::io_service service_;
  std::unique_ptr<io::io_service::work> work_;
  mock_server& server_;
  std::thread thread_;
  std::unique_ptr<length_framed_connection> conn_;
  bool running_ = false;
};

TEST(LengthFramedConnectionTest, TwoMessages) {
  mock_server server;
  threaded_connection conn{server};
  InSequence sequence;

  // Monads without the syntax sugar...
  send_and_expect(*conn, "hello", 1000, errc_success, "world", [&] {
  send_and_expect(*conn, "hello again", 1000, errc_success, "hi there", [&] {
  conn.defer_stop(); // Will close the socket, causing an EOF server-side.
  }); });

  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("hello")))
      .WillOnce(Return(response{"world"}));

  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("hello again")))
      .WillOnce(Return(response{"hi there"}));

  server.expect_eof_and_close();
  server.run(1);
}

TEST(LengthFramedConnectionTest, Timeouts) {
  mock_server server;
  threaded_connection conn{server};
  InSequence sequence;

  send_and_expect(*conn, "okay1", 60, errc_success, "okay1_reply", [&] {
  send_and_expect(*conn, "okay2", 100, errc_success, "okay2_reply", [&] {
  send_and_expect(*conn, "timeout1", 60, std::errc::timed_out, "");
  }); });

  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("okay1")))
      .WillOnce(Return(response{30, "okay1_reply"}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("okay2")))
      .WillOnce(Return(response{50, "okay2_reply"}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("timeout1")))
      .WillOnce(Return(response{70, "timeout1_reply", allow_errors}));

  // EOF from timeout1
  server.expect_eof([&] {
  send_and_expect(*conn, "timeout2", 100, std::errc::timed_out, "");
  });
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("timeout2")))
      .WillOnce(Return(response{110, "timeout2_reply", allow_errors}));

  // EOF from timeout2
  server.expect_eof([&] {
  send_and_expect(*conn, "okay3", 100, errc_success, "okay3_reply", [&] {
  send_and_expect(*conn, "timeout3", 100, std::errc::timed_out, "");
  }); });
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("okay3")))
      .WillOnce(Return(response{50, "okay3_reply"}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("timeout3")))
      .WillOnce(Return(response{110, "timeout3_reply", allow_errors}));

  // EOF from timeout3
  server.expect_eof([&] {
  send_and_expect(*conn, "okay4", 60, errc_success, "okay4_reply", [&] {
  send_and_expect(*conn, "timeout4", 60, std::errc::timed_out, "", [&] {
  conn.defer_stop(); // Will close the socket, causing an EOF server-side.
  }); }); });
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("okay4")))
      .WillOnce(Return(response{30, "okay4_reply"}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("timeout4")))
      .WillOnce(Return(response{70, "timeout4_reply", allow_errors}));

  server.expect_eof_and_close();

  server.run(1);
}

TEST(LengthFramedConnectionTest, DisconnectReconnect) {
  mock_server server;
  threaded_connection conn{server};

  InSequence sequence;
  send_and_expect(*conn, "a", no_deadline, errc_success, "ra", [&] {
  send_and_expect(*conn, "x", no_deadline, std::errc::not_connected, "", [&] {
  send_and_expect(*conn, "b", no_deadline, errc_success, "rb", [&] {
  send_and_expect(*conn, "c", no_deadline, errc_success, "rc", [&] {
  send_and_expect(*conn, "y", no_deadline, std::errc::not_connected, "", [&] {
  send_and_expect(*conn, "d", no_deadline, errc_success, "rd", [&] {
  conn.defer_stop();
  }); }); }); }); }); });

  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("a")))
      .WillOnce(Return(response{"ra"}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("x")))
      .WillOnce(Return(response{}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("b")))
      .WillOnce(Return(response{"rb"}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("c")))
      .WillOnce(Return(response{"rc"}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("y")))
      .WillOnce(Return(response{}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("d")))
      .WillOnce(Return(response{"rd"}));

  server.expect_eof_and_close();
  server.run(1);
}

TEST(LengthFramedConnectionTest, DisconnectAndClose) {
  mock_server server;
  threaded_connection conn{server};

  InSequence sequence;
  send_and_expect(*conn, "x", no_deadline, std::errc::not_connected, "", [&] {
  server.stop();
  });

  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("x")))
      .WillOnce(Return(response{}));

  server.run(1);
}

TEST(LengthFramedConnectionTest, EmptyRequestAndReply) {
  mock_server server;
  threaded_connection conn{server};

  InSequence sequence;
  send_and_expect(*conn, "", no_deadline, errc_success, "a", [&] {
  send_and_expect(*conn, "b", no_deadline, errc_success, "", [&] {
  send_and_expect(*conn, "", no_deadline, errc_success, "", [&] {
  conn.defer_stop();
  }); }); });

  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("")))
      .WillOnce(Return(response{"a"}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("b")))
      .WillOnce(Return(response{""}));
  EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("")))
      .WillOnce(Return(response{""}));

  server.expect_eof_and_close();
  server.run(1);
}

TEST(LengthFramedConnectionTest, ConnectionRefused) {
    io::io_service conn_service;
    io::io_service::work work{conn_service};
    {
      auto endpoints = endpoint_vector{{ip::address_v4{{{1, 2, 3, 4}}}, 60000}};
      // This times out if we are connected to the internet.
      length_framed_connection conn{conn_service, endpoints.begin(),
                                    endpoints.end(), connect_timeout_ms};

      send_and_expect(
          conn, "a", no_deadline, std::errc::connection_refused, "", [&] {
      send_and_expect(
          conn, "a", no_deadline, std::errc::connection_refused, "", [&] {
      conn_service.stop(); }); });

      conn_service.run();
      conn_service.reset();
    }
    {
      auto endpoints =
          endpoint_vector{{ip::address_v4{{{127, 0, 0, 1}}}, 60000}};
      length_framed_connection conn{conn_service, endpoints.begin(),
                                    endpoints.end(), no_deadline};

      send_and_expect(
          conn, "a", no_deadline, std::errc::connection_refused, "", [&] {
      send_and_expect(
          conn, "a", no_deadline, std::errc::connection_refused, "", [&] {
      conn_service.stop(); }); });

      conn_service.run();
      conn_service.reset();
    }
    {
      mock_server server;
      threaded_connection conn{
          server,
          {{ip::address_v4{{{1, 2, 3, 4}}}, 60000},
           {ip::address_v4{{{127, 0, 0, 1}}}, 60000},
           {ip::address_v4{{{127, 0, 0, 1}}}, server.port()}}};
      InSequence sequence;

      send_and_expect(*conn, "hello", 1000, errc_success, "world", [&] {
      conn.defer_stop();
      });

      EXPECT_CALL(server, on_receive(Eq(asio_success), Eq("hello")))
          .WillOnce(Return(response{"world"}));
      server.expect_eof_and_close();
      server.run(1, 1000);
    }
}

TEST(LengthFramedConnectionTest, DestroyScenarios) {
  {
    mock_server server;
    threaded_connection conn{server};
  }
  {
    io::io_service conn_service;
    {
      auto endpoints = endpoint_vector{{ip::address_v4{{{1, 2, 3, 4}}}, 60000}};
      length_framed_connection conn{conn_service, endpoints.begin(),
                                    endpoints.end()};
      send_and_expect(conn, "a", no_deadline, std::errc::connection_refused,
                      "", [&] { conn_service.stop(); });
      // Destroy before running conn_service.
    }
    conn_service.run();
  }
}

}  // namespace
}  // namespace testing
}  // namespace riak
