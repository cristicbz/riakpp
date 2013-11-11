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
  typedef length_framed_unbuffered_connection self_type;

  template<class Handler>
  void reconnect(Handler on_connection, size_t endpoint_index = 0);

  void send_current_request();
  void on_timeout(boost::system::error_code error);
  void wait_for_response(boost::system::error_code error, size_t);
  void wait_for_response_body(boost::system::error_code error, size_t);
  void on_response(boost::system::error_code error, size_t);
  void finalize_request(boost::system::error_code error);
  void finalize_request(std::error_code code);
  bool abort_request();

  blocking_object<self_type> blocker_;

  boost::asio::io_service::strand strand_;
  boost::asio::ip::tcp::socket socket_;
  const std::vector<boost::asio::ip::tcp::endpoint>& endpoints_;

  std::atomic<bool> cancelled_{false};
  std::atomic<bool> has_active_request_{false};

  connection::request current_request_;
  uint32_t request_length_ = 0;
  uint32_t response_length_ = 0;
  std::string response_;
};

}  // namespace riak

#endif  // #ifndef RIAKPP_LENGTH_FRAMED_UNBUFFERED_CONNECTION_HPP_

