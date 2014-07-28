#include "riakpp/client.hpp"
#include <boost/asio/io_service.hpp>

int main() {
  boost::asio::io_service io_service;
  boost::asio::io_service::work work{io_service};
  riak::client client{io_service, "localhost", 8087};

  client.async_store("example_bucket", "example_key", "hello, world!",
                     [&](std::error_code ec) {
    if (ec) std::cerr << "Oh no! Error: " << ec.message() << ".\n";
    io_service.stop();
  });
  io_service.run();
}
