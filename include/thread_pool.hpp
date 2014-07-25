#ifndef RIAKPP_THREAD_POOL_HPP_
#define RIAKPP_THREAD_POOL_HPP_

#include <boost/asio/io_service.hpp>

#include <cstdint>
#include <thread>
#include <vector>

namespace riak {

class thread_pool {
 public:
  static constexpr size_t use_hardware_threads = static_cast<size_t>(-1);

  thread_pool(size_t num_threads = use_hardware_threads,
              boost::asio::io_service* io_service = nullptr);
  ~thread_pool() noexcept;

  boost::asio::io_service& io_service() noexcept { return io_service_; }

  const boost::asio::io_service& io_service() const noexcept {
    return io_service_;
  }

 private:
  std::unique_ptr<boost::asio::io_service> io_service_ptr_;
  boost::asio::io_service& io_service_;
  boost::asio::io_service::work work_;
  std::vector<std::thread> threads_;
};

}  // namespace riak

#endif  // #ifndef RIAKPP_THREAD_POOL_HPP_
