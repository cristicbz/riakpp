#include <boost/asio/io_service.hpp>
#include <riakpp/blocking_group.hpp>
#include <riakpp/client.hpp>

int example1() {
  riak::client client{"localhost", 8087};

  client.async_store("example_bucket", "example_key", "hello, world!",
                     [&](std::error_code error) {
                        if (error) std::cerr << error.message() << ".\n";
                        client.stop_managed();  // Unblocks main thread.
                      });
  client.run_managed();  // Block until client.stop_managed().
  return 0;
}

int example2() {
  boost::asio::io_service io_service;
  boost::asio::io_service::work work{io_service};
  riak::client client{io_service, "localhost", 8087};

  client.async_store("example_bucket", "example_key", "hello, world!",
                     [&](std::error_code error) {
                        if (error) std::cerr << error.message() << ".\n";
                        io_service.stop();
                      });
  io_service.run();
  return 0;
}

int example3() {
  riak::client client{"localhost", 8087};

  riak::blocking_group blocker;
  std::error_code error;
  client.async_store("example_bucket", "example_key", "hello, world!",
                     blocker.wrap([&](std::error_code store_error) {
                       error = store_error;  // Save to variable outside scope.
                     }));

  // Wait until all the wrapped handlers have been called.
  blocker.wait();
  if (error) { std::cerr << error.message() << std::endl; return 1; }
  blocker.reset();  // Reset the group to allow reuse.

  // Wrapping a handler just to save a variable would be cumbersome, so you
  // use for convenience, you can replace
  //    blocker.wrap([&] (type1 arg1, type2 arg2, ...) {
  //      var1 = arg1;
  //      var2 = arg2;
  //      ...
  //    });
  // with blocker.save(var1, var2, ...). For instance, the fetch handler has
  // signature void(std::error_code, riak::object), hence we can write:
  riak::object fetched{"example_bucket", "example_key"};
  client.async_fetch(fetched, blocker.save(error, fetched));
  blocker.wait();
  if (error) { std::cerr << error.message() << std::endl; return 1; }
  blocker.reset();

  std::cout << "Fetched value '" << fetched.value() << "'." << std::endl;

  // Finally, let's remove the object. Again we can use save() to get the error.
  client.async_remove(fetched, blocker.save(error));
  blocker.wait();
  if (error) { std::cerr << error.message() << std::endl; return 1; }

  // Notice we don't reset blocker again. If a 'pending' blocking_group is
  // destroyed the process is aborted -- think of it as destroying an unjoined
  // thread. A blocking_group is pending when it accepts calls to wrap() and
  // wait(): after construction or after a call to reset() and stops being
  // pending after a call to wait().
  //
  // Redundant calls to wait() are OK and simply don't do anything.
  return 0;
}

int example4() {
  riak::client client{"localhost", 8087};

  // Nested lambdas to fetch, modify and store the object.
  std::error_code exit_with;
  client.async_fetch(
      "example_bucket", "example_key",
      [&](std::error_code fetch_error, riak::object fetched) {
        if (fetch_error) {
          exit_with = fetch_error;
          client.stop_managed();
          return;
        }
        fetched.value() = "hello, world!";
        client.async_store(fetched,
                           [&](std::error_code store_error) {
                             exit_with = store_error;
                             client.stop_managed();
                           });
      });
  client.run_managed();  // Block until client.managed_stop().

  if (exit_with) {
    std::cerr << "Error: " << exit_with.message() << std::endl;
    return 1;
  }
  return 0;
}

riak::store_resolved_sibling max_length_resolution(riak::object& conflicted) {
  size_t max_length = 0;
  const riak::object::content* max_length_sibling = nullptr;
  for (auto& sibling : conflicted.siblings()) {
    if (sibling.value().length() >= max_length) {
      max_length = sibling.value().length();
      max_length_sibling = &sibling;
    }
  }
  conflicted.resolve_with(*max_length_sibling);

  // Returning yes means we want riakpp to make a store() call with the resolved
  // object before calling the fetch handler.
  return riak::store_resolved_sibling::yes;
}

int example5() {
  riak::client client{"localhost", 8087, &max_length_resolution};

  // Nested lambdas to fetch, modify and store the object.
  std::error_code exit_with;
  client.async_fetch(
      "example_bucket", "example_key",
      [&](std::error_code fetch_error, riak::object fetched) {
        if (fetch_error) {
          exit_with = fetch_error;
          client.stop_managed();
          return;
        }
        fetched.value() = "hello, world!";
        client.async_store(fetched,
                           [&](std::error_code store_error) {
                             exit_with = store_error;
                             client.stop_managed();
                           });
      });
  client.run_managed();  // Block until client.managed_stop().

  if (exit_with) {
    std::cerr << "Error: " << exit_with.message() << std::endl;
    return 1;
  }
  return 0;
}

int main() {
  int error;
  error = example1(); if (error) return error;
  error = example2(); if (error) return error;
  error = example3(); if (error) return error;
  error = example4(); if (error) return error;
  return 0;
}
