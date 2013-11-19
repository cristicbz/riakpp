#include "connection_pool.hpp"

#include "length_framed_unbuffered_connection.hpp"

#include <functional>

namespace riak {
namespace ph = std::placeholders;
namespace ip = boost::asio::ip;

connection_pool::~connection_pool() {
  broker_.close();
  for (auto& c : connections_) c->shutdown();
  connections_.clear();
}

connection_pool::connection_pool(const std::string &host, uint16_t port,
                                 size_t num_threads, size_t num_sockets,
                                 size_t highwatermark)
    : broker_{highwatermark, num_sockets}, thread_pool_{num_threads},
      io_service_(thread_pool_.io_service()) {
  resolve(host, port);
  for (size_t i_socket = 0; i_socket < num_sockets; ++i_socket) {
    connections_.emplace_back(
        new length_framed_unbuffered_connection{io_service_, endpoints_});
    add_worker_for(*connections_.back());
  }
}

void connection_pool::add_worker_for(connection& sub_connection) {
  broker_.add_worker([this, &sub_connection](request& new_request) {
    // Wrap the request handler: first, notify the broker the connection is
    // ready again, then call the actual handler.
    auto real_handler = std::move(new_request.on_response);
    new_request.on_response = [this, &sub_connection, real_handler](
        const std::string& response, std::error_code error) {
      add_worker_for(sub_connection);  // Notify broker.
      real_handler(response, error);   // Call real handler.
    };

    // Send the request with the wrapped handler.
    sub_connection.send_and_consume_request(new_request);
  });
}

void connection_pool::resolve(const std::string& host, int16_t port) {
  ip::tcp::resolver resolver{io_service_};
  ip::tcp::resolver::query query{host, std::to_string(port)};
  boost::system::error_code error_code;
  auto endpoint_iter = resolver.resolve(query, error_code);
  if (error_code) throw hostname_resolution_failed{};

  endpoints_.assign(endpoint_iter, decltype(endpoint_iter){});
}

void connection_pool::send_and_consume_request(request& new_request) {
  broker_.add_work(std::move(new_request));
}

}  // namespace riak
