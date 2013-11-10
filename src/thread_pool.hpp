#ifndef RIAKPP_THREAD_POOL_HPP_
#define RIAKPP_THREAD_POOL_HPP_

#include <boost/asio/io_service.hpp>

#include <cstdint>
#include <thread>
#include <vector>

namespace riak {

class thread_pool {
 public:
  static const size_t default_num_threads = 16;

  explicit thread_pool(size_t num_threads = default_num_threads);
  virtual ~thread_pool() noexcept;

  boost::asio::io_service& io_service() noexcept { return io_service_; }

  const boost::asio::io_service& io_service() const noexcept {
    return io_service_;
  }

 private:
  boost::asio::io_service io_service_;
  boost::asio::io_service::work work_;
  std::vector<std::thread> threads_;
};

}  // namespace riak

#endif  // #ifndef RIAKPP_THREAD_POOL_HPP_
