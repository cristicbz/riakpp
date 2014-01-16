#ifndef RIAKPP_LENGTH_FRAMED_UNBUFFERED_CONNECTION_HPP_
#define RIAKPP_LENGTH_FRAMED_UNBUFFERED_CONNECTION_HPP_

#include "blocking_counter.hpp"
#include "connection.hpp"

#include <boost/asio/strand.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <vector>
#include <atomic>

namespace riak {

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
  friend struct active_request_state;

  typedef length_framed_unbuffered_connection self_type;
  typedef std::shared_ptr<active_request_state> shared_request_state;
  typedef boost::system::error_code asio_error;

  void start_request(shared_request_state state);
  void connect(shared_request_state state, size_t endpoint_index = 0);
  void write_request(shared_request_state state);
  void wait_for_length(shared_request_state, asio_error error);
  void wait_for_content(shared_request_state state, asio_error error);
  void report(shared_request_state state, asio_error error);
  void report_std_error(shared_request_state state, std::error_code error);

  boost::asio::io_service::strand strand_;
  boost::asio::ip::tcp::socket socket_;
  boost::asio::deadline_timer deadline_timer_;
  const std::vector<boost::asio::ip::tcp::endpoint>& endpoints_;

  std::atomic<bool> has_active_request_{false};
  std::weak_ptr<active_request_state> current_request_state_;

  blocking_counter request_counter_;
};

}  // namespace riak

#endif  // #ifndef RIAKPP_LENGTH_FRAMED_UNBUFFERED_CONNECTION_HPP_

