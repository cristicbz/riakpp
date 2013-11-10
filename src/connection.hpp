#ifndef RIAKPP_CONNECTION_HPP_
#define RIAKPP_CONNECTION_HPP_

#include <cstdint>
#include <functional>
#include <string>
#include <system_error>

namespace riak {
// TODO(cristicbz): Add method for cancelling requests. Destructors should
// cancel all requests then wait for them to return.
class connection {
 public:
  typedef std::function<void(std::string, std::error_code)>
      response_handler;

  struct request {
    request() noexcept {}
    inline request(std::string messsage, int64_t deadline_ms,
                   response_handler on_response) noexcept;

    inline void reset() noexcept;

    std::string message;
    int64_t deadline_ms = -1;
    response_handler on_response;
  };

  virtual ~connection() {}

  inline void send(const std::string& message, int64_t deadline_ms,
                   const response_handler& on_response);

  virtual void send_and_consume_request(request& request) = 0;
};

void connection::send(const std::string& message, int64_t deadline_ms,
                      const response_handler& on_response) {
  request new_request{message, deadline_ms, on_response};
  send_and_consume_request(new_request);
}

connection::request::request(std::string message, int64_t deadline_ms,
                             response_handler on_response) noexcept
    : message{std::move(message)},
      deadline_ms{deadline_ms},
      on_response{std::move(on_response)} {}

void connection::request::reset() noexcept {
  deadline_ms = -1;
  std::string().swap(message);
  response_handler().swap(on_response);
}

}  // namespace riak

#endif  // #ifndef RIAKPP_CONNECTION_HPP_
