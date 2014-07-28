#include "example_util.hpp"
#include "riakpp/client.hpp"
#include "riakpp/completion_group.hpp"

#include <mutex>

// Simple logging function to avoid output interleaving between threads.
void log(std::string msg) {
  static std::mutex mutex;
  std::lock_guard<std::mutex> lock{mutex};
  std::cout << msg << std::endl;
}

bool do_operations(std::string hostname, uint16_t port,
                   size_t max_connections) {
  // Create the client and specify the maximum number of simultaneous
  // connections.
  riak::client client{hostname, port, &riak::client::pass_through_resolver,
                      riak::connection_options{}
                          .max_connections(max_connections)};

  // To run a handler once a group of operations are finished, we create a
  // completion_group and we add functions to it using the wrap() method.
  auto storers = riak::completion_group{};
  for (int i_request = 0; i_request < 20; ++i_request) {
    client.async_store("example_bucket",
                       "example_key_" + std::to_string(i_request), "hello",
                       storers.wrap([&, i_request](std::error_code ec) {
                         if (ec) {
                           log("error: " + ec.message());
                         } else {
                           log("added " + std::to_string(i_request));
                         }
                       }));
  }

  // To finalize a completion_group we call when_done with a handler to be
  // called when all the functions in a group have been called.
  storers.when_done([&] {
    // If we like, we can specify the handler on construction and the
    // completion_group may be able to inline the call to the handler.
    auto removers = riak::make_completion_group([&] { client.stop_managed(); });
    for (int i_request = 0; i_request < 20; ++i_request) {
      client.async_remove("example_bucket",
                          "example_key_" + std::to_string(i_request),
                          removers.wrap([&, i_request](std::error_code ec) {
                            if (ec) {
                              log("error: " + ec.message());
                            } else {
                              log("removed " + std::to_string(i_request));
                            }
                          }));
    }
  });
  client.run_managed();
  return true;
}

// Store and remove many values in parallel using completion groups.
int main(int argc, char* argv[]) {
  // Parse [host:port] from the command line arguments.
  std::string hostname = "localhost";
  uint16_t port = 8087;
  if (!hostport_from_argv(argc, argv, hostname, port)) return 1;

  // Set the max number of connections to 1. The operations will therefore be
  // performed in order.
  if (!do_operations(hostname, port, 1)) return 1;

  // Set the max number of connections to 20. This should have the effect of
  // displaying the message in a jumbled order, since they're all sent at the
  // same time.
  if (!do_operations(hostname, port, 20)) return 1;

  return 0;
}
