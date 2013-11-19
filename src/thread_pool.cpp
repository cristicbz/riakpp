#include "thread_pool.hpp"

#include "check.hpp"

namespace riak {

thread_pool::thread_pool(size_t num_threads) : work_{io_service_} {
  RIAKPP_CHECK_GT(num_threads, 0u);
  threads_.reserve(num_threads);
  for (size_t i = 0; i < num_threads; ++i) {
    // TODO(cristicbz): Do somthing about exceptions?
    threads_.emplace_back([&] { io_service_.run(); });
  }
}

thread_pool::~thread_pool() noexcept {
  io_service_.stop();
  for (auto& thread : threads_) thread.join();
}

}  // namespace riak
