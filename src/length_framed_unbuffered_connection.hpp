#ifndef RIAKPP_LENGTH_FRAMED_UNBUFFERED_CONNECTION_HPP_
#define RIAKPP_LENGTH_FRAMED_UNBUFFERED_CONNECTION_HPP_

#include "blocking_object.hpp"
#include "connection.hpp"

#include <boost/asio/strand.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <vector>
#include <atomic>


namespace riak {

// TODO(cristicbz): Deadline is currently not enforced.
// TODO(cristicbz): Need better error handling.
class length_framed_unbuffered_connection : public connection {
 public:
  length_framed_unbuffered_connection(
      boost::asio::io_service& io_service,
      const std::vector<boost::asio::ip::tcp::endpoint>& endpoints);

  ~length_framed_unbuffered_connection();

  virtual void send_and_consume_request(request& new_request) override;

  virtual void shutdown() override;

 private:
  struct active_request_state;

  typedef length_framed_unbuffered_connection self_type;
  typedef std::shared_ptr<active_request_state> shared_request_state;
  typedef boost::system::error_code asio_error;

  void reconnect(shared_request_state state, size_t endpoint_index = 0);
  void send_active_request(shared_request_state state);
  void on_timeout(shared_request_state state, asio_error error);
  void wait_for_response(shared_request_state, asio_error error, size_t bytes);
  void wait_for_response_body(shared_request_state state, asio_error error,
                              size_t bytes);
  void on_response(shared_request_state state, asio_error error, size_t bytes);

  void finalize_request(shared_request_state state, asio_error error);
  void finalize_request(shared_request_state state, std::error_code error);
  bool handle_abort_conditions(shared_request_state state, asio_error error);

  boost::asio::io_service::strand strand_;
  boost::asio::ip::tcp::socket socket_;
  const std::vector<boost::asio::ip::tcp::endpoint>& endpoints_;

  std::atomic<bool> has_active_request_{false};
  std::weak_ptr<active_request_state> current_request_state_;

  blocking_object<self_type> blocker_;
};

}  // namespace riak

#endif  // #ifndef RIAKPP_LENGTH_FRAMED_UNBUFFERED_CONNECTION_HPP_

