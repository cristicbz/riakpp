#ifndef RIAKPP_BLOCKING_COUNTER_HPP_
#define RIAKPP_BLOCKING_COUNTER_HPP_

#include "check.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <type_traits>

namespace riak {

class blocking_counter {
 public:
  class item;
  friend class item;
  class item {
   public:
    explicit item(blocking_counter& counter) : counter_{&counter} {
      RIAKPP_CHECK(counter_->alive_);
      counter_->increment();
    }

    item(const item& other) : counter_{other.counter_} {
      if (counter_) counter_->increment();
    }

    item(item&& other) : counter_{other.counter_} { other.counter_ = nullptr; }

    ~item() { release(); }

    item& operator=(item other) {
      swap(other);
      return *this;
    }

    void release() {
      if (counter_) {
        counter_->decrement();
        counter_ = nullptr;
      }
    }

    void swap(item& other) { std::swap(counter_, other.counter_); }

   private:
    blocking_counter* counter_;
  };

  blocking_counter() = default;
  ~blocking_counter() { wait_and_disable(); }

  void wait_and_disable() {
    if (alive_.exchange(false)) {
      std::unique_lock<std::mutex> lock{count_mutex_};
      --count_;
      while (count_ > 0) zero_count_.wait(lock);
    }
  }

 private:
  void increment() { ++count_; }
  void decrement() {
    std::lock_guard<std::mutex> lock{count_mutex_};
    if (--count_ == 0) zero_count_.notify_all();
  }

  std::atomic<bool> alive_{true};
  std::atomic<uint32_t> count_{1};
  std::mutex count_mutex_;
  std::condition_variable zero_count_;
};

}  // namespace riak

#endif  // #ifndef RIAKPP_BLOCKING_COUNTER_HPP_
