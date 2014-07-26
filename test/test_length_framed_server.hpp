#ifndef RIAKPP_TEST_LENGTH_FRAMED_SERVER_HPP_
#define RIAKPP_TEST_LENGTH_FRAMED_SERVER_HPP_

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <cstdint>
#include <unordered_map>

#include <gmock/gmock.h>

#include "testing_util.hpp"
#include "transient.hpp"

namespace riak {
namespace testing {

struct response {
  enum response_type {
   type_message,
   type_defer,
   type_close,
   type_invalid
  };

  enum allow_errors {
    allow_errors_no,
    allow_errors_yes
  };

  explicit response(allow_errors allow = allow_errors_no)
      : type{type_close}, allow_errors{allow} {}
  explicit response(std::string message, allow_errors allow = allow_errors_no)
      : type{type_message}, message{std::move(message)}, allow_errors{allow} {}
  explicit response(uint64_t milliseconds, std::string message,
           allow_errors allow_errors = allow_errors_no)
      : type{type_defer},
        message{std::move(message)},
        milliseconds{milliseconds},
        allow_errors{allow_errors} {}

  const response_type type = type_invalid;
  const std::string message;
  const uint64_t milliseconds = 0;
  const allow_errors allow_errors = allow_errors_no;
};

class test_length_framed_server {
 public:
  test_length_framed_server();
  virtual ~test_length_framed_server();

  boost::asio::io_service& io_service() { return io_service_; }

  virtual response on_receive(boost::system::error_code ec,
                              const std::string& message) = 0;

  uint16_t port() const { return port_; }

  template <class Handler>
  void post(Handler&& handler) {
    io_service().post(std::forward<Handler>(handler));
  }

  void run(size_t expected_sessions = -1, uint64_t timeout_ms = 5000);
  void stop();

  std::vector<size_t> reply_counts() {
    std::vector<size_t> counts;
    for (auto& session_and_count : reply_counts_) {
      counts.push_back(session_and_count.second);
    }
    return counts;
  }

 private:
  class session;
  friend class session;

  void accept();
  void delete_session(session* which);

  class session {
   public:
    explicit session(test_length_framed_server& server);
    ~session();

    void wait_for_request();
    void reply(response with);

    boost::asio::ip::tcp::socket& socket() { return socket_; }

   private:
    void close_session();

    test_length_framed_server& server_;
    boost::asio::ip::tcp::socket socket_;

    bool processing_request_{false};
    std::string payload_buffer_;
    uint32_t length_buffer_ = 0;

    transient<session> transient_;
  };

  boost::asio::io_service io_service_;
  uint16_t port_;
  boost::asio::ip::tcp::acceptor acceptor_;

  std::unordered_map<session*, uint32_t> reply_counts_;
  std::vector<std::unique_ptr<session>> sessions_;
  size_t max_sessions_ = 0;
  size_t expected_sessions_ = 0;
};

class mock_server : public test_length_framed_server {
 public:
  template <class Handler>
  void expect_eof(Handler&& handler) {
    EXPECT_CALL(*this, on_receive(AnyOf(Eq(io::error::eof),
                                          Eq(io::error::connection_reset)),
                                    Eq("")))
        .WillOnce(InvokeWithoutArgs([handler]() ->response {
            handler();
            return response{};
        }));
  }

  void expect_eof_and_close() {
    expect_eof([this] { stop(); });
  }

  MOCK_METHOD2(on_receive, response(asio_error ec, const std::string &));
};


}  // namespace testing
}  // namespace riak

#endif  // #ifndef RIAKPP_TEST_LENGTH_FRAMED_SERVER_HPP_
