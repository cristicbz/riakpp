#include "riakpp/client.hpp"

int main() {
  riak::client client{"localhost", 8087};

  client.async_store("example_bucket", "example_key", "hello, world!",
               [&](std::error_code ec) {
    if (ec) std::cerr << "Oh no! (" << ec.message() << ").\n";
    client.stop_managed();  // Unblocks main thread.
  });

  client.run_managed();  // Block until client.managed_stop().
}
