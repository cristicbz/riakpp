#include "example_util.hpp"
#include "riakpp/blocking_group.hpp"
#include "riakpp/client.hpp"

// Store a value, fetch it and remove it using a blocking_group to wait between
// operations. This way of doing things is not recommended outside of small
// scripts and the like.
int main(int argc, char* argv[]) {
  // Parse [host:port] from the command line arguments.
  std::string hostname = "localhost";
  uint16_t port = 8087;
  if (!hostport_from_argv(argc, argv, hostname, port)) return 1;
  // Create the client object.
  riak::client client{hostname, port};

  // A blocking group allows you to block until a group of handlers have been
  // called. It starts in a 'pending' state where it allows handlers to be added
  // to the group using either .wrap() or .save(), see the examples below.
  auto blocking = riak::blocking_group{};
  std::error_code error;

  // A small helper to print error messages.
  auto should_bail = [&] (std::error_code to_check) -> bool {
    if (to_check) {
      // Make sure the blocking_group is not pending on destruction (doing so
      // aborts the process, similarly to destroying a thread without calling
      // .join()). Calling .wait() multiple times is harmless.
      blocking.wait();
      std::cerr << "ERROR: " << to_check.message() << std::endl;
      return true;
    }
    return false;
  };

  // A blocking group allows you to block until a group of handlers have been
  // called. The first syntax we can use is blocking.wrap(some_handler):
  client.store("example_bucket", "example_key", "hello",
               blocking.wrap([&](std::error_code ec) { error = ec; }));

  // Restting the group after waiting allows us to reuse it.
  blocking.wait_and_reset();
  if (should_bail(error)) return 1;
  std::cout << "Stored." << std::endl;

  // When all a handler does is save values to local variables, we can use
  // the convenience syntax blocking.save(var1, var2, ...).
  riak::object to_fetch{"example_bucket", "example_key"};
  client.fetch(to_fetch, blocking.save(to_fetch, error));
  blocking.wait_and_reset();
  if (should_bail(error)) return 1;
  std::cout << "Fetched '" << to_fetch.value() << "'." << std::endl;

  // Finally, let's remove the object. Notice we don't reset the blocking group
  // again otherwise we would be destroying a pending blocking group.
  client.remove(to_fetch, blocking.save(error));
  blocking.wait();
  if (should_bail(error)) return 1;

  std::cout << "Everything ok, clean exit." << std::endl;
  return 0;
}
