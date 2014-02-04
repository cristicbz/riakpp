#ifndef RIAKPP_BLOCKING_QUEUE_HPP_
#define RIAKPP_BLOCKING_QUEUE_HPP_

#include "check.hpp"

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>

namespace riak {

// TODO(cristicbz): Add try_pop().
// TODO(cristicbz): Use circular_buffer (note that it currently doesn't support
// moveable-only objects :/).
template <class T>
class blocking_queue {
 public:
  typedef T value_type;

  explicit blocking_queue(size_t capacity) : capacity_{capacity} {
    RIAKPP_CHECK_GT(capacity_, 0);
  }

  bool push(value_type value);
  bool pop(value_type& value);
  void cancel() noexcept;

  bool cancelled() const noexcept { return cancelled_; }

  class cancel_unless_disarmed;

 private:
  std::mutex mutex_;
  std::condition_variable not_empty_condition_;
  std::condition_variable not_full_condition_;
  std::queue<value_type> queue_;

  bool cancelled_ = false;
  size_t capacity_ = 0;
};

template <class T>
class blocking_queue<T>::cancel_unless_disarmed {
 public:
  cancel_unless_disarmed(blocking_queue& queue) : queue_(queue) {}
  ~cancel_unless_disarmed() {
    if (!disarmed_) queue_.cancel();
  }

  void disarm() { disarmed_ = true; }

 private:
  blocking_queue& queue_;
  bool disarmed_ = false;
};

template <class T>
bool blocking_queue<T>::push(value_type value) {
  {
    std::unique_lock<std::mutex> lock{mutex_};
    if (cancelled_) return false;

    cancel_unless_disarmed cancel_on_exception{*this};
    while (queue_.size() >= capacity_) {
      not_full_condition_.wait(lock);
      if (cancelled_) {
        cancel_on_exception.disarm();
        return false;
      }
    }
    queue_.emplace(std::move(value));
    cancel_on_exception.disarm();
  }
  not_empty_condition_.notify_one();
  return true;
}

template <class T>
bool blocking_queue<T>::pop(value_type& value) {
  {
    std::unique_lock<std::mutex> lock{mutex_};
    if (cancelled_) return false;

    cancel_unless_disarmed cancel_on_exception{*this};
    while (queue_.empty()) {
      not_empty_condition_.wait(lock);
      if (cancelled_) {
        cancel_on_exception.disarm();
        return false;
      }
    }
    value = std::move(queue_.front());
    queue_.pop();
    cancel_on_exception.disarm();
  }
  not_full_condition_.notify_one();
  return true;
}

template <class T>
void blocking_queue<T>::cancel() noexcept {
  {
    std::lock_guard<std::mutex> lock{mutex_};
    if (cancelled_) return;

    cancelled_ = true;
    std::queue<value_type>().swap(queue_);
  }
  not_empty_condition_.notify_all();
  not_full_condition_.notify_all();
}

}  // namespace riak

#endif  // #ifndef RIAKPP_BLOCKING_QUEUE_HPP_
