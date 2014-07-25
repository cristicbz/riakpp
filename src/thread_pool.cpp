#include "thread_pool.hpp"

#include "check.hpp"

namespace riak {

thread_pool::thread_pool(size_t num_threads,
                         boost::asio::io_service* io_service)
    : io_service_ptr_{io_service ? nullptr : new boost::asio::io_service{}},
      io_service_(io_service_ptr_ ? *io_service_ptr_ : *io_service),
      work_{io_service_} {
  num_threads = num_threads == use_hardware_threads
                    ? std::thread::hardware_concurrency()
                    : num_threads;
  RIAKPP_CHECK_GT(num_threads, 0u);
  threads_.reserve(num_threads);
  for (size_t i = 0; i < num_threads; ++i) {
    threads_.emplace_back([&] { io_service_.run(); });
  }
}

thread_pool::~thread_pool() noexcept {
  if (io_service_ptr_) {
    io_service_.stop();
  } else {
    RIAKPP_CHECK(io_service_.stopped());
  }
  for (auto& thread : threads_) thread.join();
}

}  // namespace riak
