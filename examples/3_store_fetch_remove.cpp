#include "example_util.hpp"
#include "riakpp/client.hpp"

// Store, fetch and remove a value, using the preferred, asynchronous method.
int main(int argc, char* argv[]) {
  // Parse [host:port] from the command line arguments.
  std::string hostname = "localhost";
  uint16_t port = 8087;
  if (!hostport_from_argv(argc, argv, hostname, port)) return 1;

  // Create the client object. DNS resolution and connection is performed
  // lazily so any errors will be reported on the callback from the first
  // operation. Try running './store_fetch 1.2.3.4' or something to see this.
  riak::client client{hostname, port};

  // Helper for callbacks. On failure, save the error and bail.
  std::error_code error;
  auto should_bail = [&] (std::error_code to_check) -> bool {
    if (to_check) {
      error = to_check;
      client.stop_managed();
      return true;
    }
    return false;
  };

  // We'll perform the following operations in order:
  //   1. Fetch a new object 'example_bucket/example_key'.
  //   2. Store 'hello' there
  //   3. Fetch it again and check its value.
  //   4. Remove it.
  client.async_fetch("example_bucket", "example_key",
                     [&](std::error_code ec, riak::object initial) {
    if (should_bail(ec)) return;
    if (!initial.exists()) {
      std::cout << "Fetched new object, storing 'hello'..." << std::endl;
      initial.value() = "hello";
    } else {
      std::cout << "Fetched existing object '" << initial.value()
                << "'. Appending 'hello'." << std::endl;
      initial.value() += "hello";
    }

    client.async_store(initial, [&](std::error_code ec) {
      if (should_bail(ec)) return;
      std::cout << "Stored. Refetching..." << std::endl;

      client.async_fetch("example_bucket", "example_key",
                         [&](std::error_code ec, riak::object refetched) {
        if (should_bail(ec)) return;
        std::cout << "Fetched '" << refetched.value() << "'. Removing..."
                  << std::endl;

        client.async_remove(refetched, [&](std::error_code ec) {
          if (should_bail(ec)) return;
          std::cout << "Removed." << std::endl;

          // Stopping the client will unblock the main thread.
          client.stop_managed();
        });
      });
    });
  });

  // Block (and run callbacks) until .managed_stop() is called. If we didn't
  // have this line, the client will be destroyed without waiting for
  // operations to complete. Blocking destructors are trouble.
  client.run_managed();
  if (error) {
    std::cerr << "ERROR: " << error.message() << std::endl;
    return 1;
  }
  std::cout << "Everything ok, clean exit." << std::endl;
  return 0;
}
