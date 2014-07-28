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
  riak::object object{"example_bucket", "example_key"};
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
  // called. If all we want to do with the results of an operation is save them
  // to local variables then we can use the blocking.save(var1, var2, ...)
  // syntax:
  client.async_fetch(object, blocking.save(error, object));
  blocking.wait_and_reset();  // Block until the fetch is done. Reset to reuse.
  if (should_bail(error)) return 1;
  if (!object.exists()) {
    std::cout << "Fetched new object, storing 'hello'..." << std::endl;
    object.value() = "hello";
  } else {
    std::cout << "Fetched existing object '" << object.value()
              << "'. Appending 'hello'." << std::endl;
    object.value() += "hello";
  }

  // Note that this isn't really a synchronous API, there still is a handler
  // being called in a different thread, but it is provided by 'blocking'. If
  // we want better control over that handler, but still block until after it's
  // called, we can use wrap() instead of save():
  object.value() = "hello";
  client.async_store(object, blocking.wrap([&](std::error_code ec) {
                               std::cerr << "Wohoo, I'm in another thread!\n";
                               error = ec;
                             }));
  if (should_bail(error)) return 1;
  std::cout << "Stored. Refetching..." << std::endl;

  // Clear the value and refetch the object to a local variable using save.
  object.value() = "";
  client.async_fetch(object, blocking.save(error, object));
  blocking.wait_and_reset();
  if (should_bail(error)) return 1;
  std::cout << "Fetched '" << object.value() << "'. Removing..." << std::endl;

  // Finally, let's remove the object. Notice we don't reset the blocking group
  // again otherwise we would be destroying a pending blocking group.
  client.async_remove(object, blocking.save(error));
  blocking.wait();
  if (should_bail(error)) return 1;
  std::cout << "Removed. Everything ok, clean exit." << std::endl;
  return 0;
}
