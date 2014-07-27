#ifndef RIAKPP_CONNECTION_POOL_HPP_
#define RIAKPP_CONNECTION_POOL_HPP_

#include <boost/asio/io_service.hpp>
#include <boost/system/error_code.hpp>

#include <cstddef>
#include <cstdint>
#include <vector>

#include "async_queue.hpp"
#include "check.hpp"
#include "endpoint_vector.hpp"
#include "transient.hpp"

namespace riak {

template <class Connection>
class connection_pool {
 public:
  using connection_type = Connection;
  using error_type = typename connection_type::error_type;
  using handler_type = typename connection_type::handler_type;
  using request_type = typename connection_type::request_type;
  using response_type = typename connection_type::response_type;

  static constexpr size_t default_highwatermark = 4096;
  static constexpr size_t default_num_connections = 6;

  connection_pool(boost::asio::io_service& io_service, std::string hostname,
                  uint16_t port,
                  size_t num_connections = default_num_connections,
                  size_t highwatermark = default_highwatermark);
  ~connection_pool();

  void async_send(request_type request, handler_type handler);

 private:
  struct packaged_request {
    packaged_request(request_type request, handler_type handler)
        : request{std::move(request)}, handler{std::move(handler)} {}
    request_type request;
    handler_type handler;
  };

  void resolve(size_t num_connections, std::string hostname, uint16_t port);
  void report_resolution_error(boost::system::error_code asio_error);
  void create_connections(size_t num_connections);
  void notify_connection_ready(connection_type& connection);
  void send_request(connection_type& connection, packaged_request packaged);

  boost::asio::io_service& io_service_;
  std::vector<std::unique_ptr<connection_type>> connections_;
  async_queue<packaged_request> request_queue_;
  endpoint_vector endpoints_;

  transient<connection_pool> transient_;
};

template <class Connection>
connection_pool<Connection>::connection_pool(
    boost::asio::io_service& io_service, std::string hostname, uint16_t port,
    size_t num_connections, size_t highwatermark)
    : io_service_(io_service),
      request_queue_{highwatermark, num_connections},
      transient_{*this} {
  connections_.reserve(num_connections);
  resolve(num_connections, std::move(hostname), port);
}

template <class Connection>
connection_pool<Connection>::~connection_pool() {
  request_queue_.close();
  transient_.reset();
  connections_.clear();
}

template <class Connection>
void connection_pool<Connection>::async_send(request_type request,
                                             handler_type handler) {
  request_queue_.emplace(std::move(request), std::move(handler));
}

template <class Connection>
void connection_pool<Connection>::resolve(size_t num_connections,
                                          std::string hostname, uint16_t port) {
  using resolver = boost::asio::ip::tcp::resolver;
  auto* resolver_raw = new resolver{io_service_};
  auto query = resolver::query{std::move(hostname), std::to_string(port)};
  auto self_ref = transient_.ref();
  resolver_raw->async_resolve(
      std::move(query),
      transient_.wrap([this, resolver_raw, num_connections](
          boost::system::error_code ec,
          boost::asio::ip::tcp::resolver::iterator endpoint_begin) {
        std::unique_ptr<resolver> resolver_destroyer{resolver_raw};
        if (ec) {
          report_resolution_error(ec);
        } else {
          endpoints_.assign(endpoint_begin, decltype(endpoint_begin) {});
          create_connections(num_connections);
        }
      }));
}

template <class Connection>
void connection_pool<Connection>::report_resolution_error(
    boost::system::error_code asio_error) {
  request_queue_.async_pop(
      transient_.wrap([this, asio_error](packaged_request packaged) {
        io_service_.post(
            std::bind(std::move(packaged.handler),
                      error_type{asio_error.value(), std::generic_category()},
                      response_type{}));
        report_resolution_error(asio_error);
      }));
}

template <class Connection>
void connection_pool<Connection>::create_connections(size_t num_connections) {
  RIAKPP_CHECK_GE(endpoints_.size(), 0);
  for (size_t i_conn = 0; i_conn < num_connections; ++i_conn) {
    connections_.emplace_back(
        new connection_type{io_service_, endpoints_.begin(), endpoints_.end()});
  }

  for (auto& connection_ptr : connections_) {
    notify_connection_ready(*connection_ptr);
  }
}

template <class Connection>
void connection_pool<Connection>::notify_connection_ready(
    connection_type& connection) {
  request_queue_.async_pop(
      transient_.wrap([this, &connection](packaged_request packaged) {
        send_request(connection, std::move(packaged));
      }));
}

template <class Connection>
void connection_pool<Connection>::send_request(connection_type& connection,
                                               packaged_request packaged) {
  using namespace std::placeholders;
  auto call_and_notify =
    [this, &connection](handler_type& original_handler, error_type error,
                        response_type& response) {
    notify_connection_ready(connection);
    io_service_.post(
        std::bind(std::move(original_handler), error, std::move(response)));
  };
  auto wrapped = transient_.wrap(std::bind(
      std::move(call_and_notify), std::move(packaged.handler), _1, _2));
  connection.async_send(std::move(packaged.request), std::move(wrapped));
}

}  // namespace riak

#endif  // #ifndef RIAKPP_CONNECTION_POOL_HPP_
