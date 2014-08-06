#ifndef RIAKPP_ASYNC_QUEUE_HPP_
#define RIAKPP_ASYNC_QUEUE_HPP_

#include "check.hpp"

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <stack>

namespace riak {

template <class Element>
class async_queue {
 public:
  using value_type = Element;
  using handler_type = std::function<void(Element)>;

  inline async_queue(size_t max_element, size_t max_handlers);

  bool closed() const { return closed_; }

  template <class ...Args>
  inline void emplace(Args&& ...args);

  template <class HandlerConvertible>
  inline void async_pop(HandlerConvertible&& handler);

  inline void close();

 private:
  // TODO(cristicbz): Use ring buffer instead of heavyweight std::queue.
  std::queue<value_type> elements_;
  std::stack<handler_type> handlers_;

  std::mutex queues_mutex_;
  std::condition_variable elements_full_;
  std::condition_variable handlers_full_;
  const size_t max_elements_, max_handlers_;
  bool closed_;
};

template <class Element>
async_queue<Element>::async_queue(size_t max_element, size_t max_handlers)
    : max_elements_{max_element}, max_handlers_{max_handlers}, closed_{false} {
  RIAKPP_CHECK_GE(max_elements_, 0);
  RIAKPP_CHECK_GE(max_handlers_, 0);
}

template <class Element>
template <class ...Args>
inline void async_queue<Element>::emplace(Args&& ...args) {
  // TODO(cristicbz): Remove duplication.
  std::unique_lock<std::mutex> lock{queues_mutex_};
  if (closed_) return;
  if (handlers_.empty()) {
    while (elements_.size() == max_elements_) elements_full_.wait(lock);
    elements_.emplace(std::forward<Args>(args)...);
  } else {
    bool should_signal = handlers_.size() == max_handlers_;
    handler_type handler = std::move(handlers_.top());
    handlers_.pop();
    lock.unlock();
    if (should_signal) handlers_full_.notify_one();
    handler(value_type{std::forward<Args>(args)...});
  }
}

template <class Element>
template <class HandlerConvertible>
void async_queue<Element>::async_pop(HandlerConvertible&& handler) {
  std::unique_lock<std::mutex> lock{queues_mutex_};
  if (closed_) return;
  if (elements_.empty()) {
    while (handlers_.size() == max_handlers_) handlers_full_.wait(lock);
    handlers_.emplace(std::move(handler));
  } else {
    bool should_signal = elements_.size() == max_elements_;
    value_type element = std::move(elements_.front());
    elements_.pop();
    lock.unlock();
    if (should_signal) elements_full_.notify_one();
    handler(std::move(element));
  }
}

template <class Element>
void async_queue<Element>::close() {
  std::lock_guard<std::mutex> lock{queues_mutex_};
  closed_ = true;
}

}  // namespace riak

#endif  // #ifndef RIAKPP_ASYNC_QUEUE_HPP_
