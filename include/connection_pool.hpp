#ifndef RIAKPP_CONNECTION_POOL_HPP_
#define RIAKPP_CONNECTION_POOL_HPP_

#include "async_queue.hpp"
#include "connection.hpp"

#include <boost/asio/ip/tcp.hpp>

#include <cstdint>
#include <string>
#include <system_error>
#include <vector>

namespace boost {
namespace asio {
class io_service;
}  // namespace asio
}  // namespace boost

namespace riak {

class connection_pool : public connection {
 public:
  // TODO(cristicbz): We should get rid of this exception -- there's a question
  // of reporting errors from a constructor though...
  struct hostname_resolution_failed : public std::system_error {
    hostname_resolution_failed()
        : system_error{std::make_error_code(
              std::errc::address_not_available)} {}
  };

  static constexpr size_t default_num_sockets = 8;
  static constexpr size_t default_highwatermark = 1024;

  connection_pool(const std::string& host, uint16_t port,
                  boost::asio::io_service& io_service,
                  size_t num_sockets = default_num_sockets,
                  size_t highwatermark = default_highwatermark);


  ~connection_pool();

  virtual void send_and_consume_request(request& new_request) override;

 private:
  void resolve(const std::string& host, int16_t port);
  void add_worker_for(connection& sub_connection);

  async_queue<request> request_queue_;
  std::vector<std::unique_ptr<connection>> connections_;
  std::vector<boost::asio::ip::tcp::endpoint> endpoints_;
  boost::asio::io_service& io_service_;
};

}  // namespace riak

#endif  // #ifndef RIAKPP_CONNECTION_POOL_HPP_
