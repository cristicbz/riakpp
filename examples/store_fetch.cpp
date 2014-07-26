#include "example_util.hpp"
#include "riakpp/client.hpp"

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
      client.managed_stop();
      return true;
    }
    return false;
  };

  // We'll perform the following operations in order:
  //   1. Store 'hello' at 'my_bucket/my_key'.
  //   2. Fetch 'my_bucket/my_key' and check its value.
  //   3. Remove 'my_bucket/my_key.
  client.store(
      "my_bucket", "my_key", "hello", [&](std::error_code ec) {
    if (should_bail(ec)) return;
    std::cout << "Stored 'hello'." << std::endl;

    client.fetch(
        "my_bucket", "my_key", [&](riak::object object, std::error_code ec) {
      if (should_bail(ec)) return;
      std::cout << "Fetched '" << object.value() << "'." << std::endl;

      client.remove(object, [&](std::error_code ec) {
        if (should_bail(ec)) return;
        std::cout << "Removed." << std::endl;

        // Stopping the client will unblock the main thread.
        client.managed_stop();
      });
    });
  });

  // Block (and run callbacks) until .managed_stop() is called. If we didn't
  // have this line, the client will be destroyed without waiting for
  // operations to complete. Blocking destructors are trouble.
  client.managed_run();
  if (error) {
    std::cerr << "ERROR: " << error.message() << std::endl;
    return 1;
  }
  std::cout << "Everything ok, clean exit." << std::endl;
  return 0;
}
