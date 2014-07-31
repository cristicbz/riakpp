#ifndef RIAKPP_BLOCKING_GROUP_HPP_
#define RIAKPP_BLOCKING_GROUP_HPP_

#include "check.hpp"
#include "completion_group.hpp"
#include "store_handler.hpp"

#include <condition_variable>
#include <memory>
#include <mutex>

namespace riak {
class blocking_group {
 public:
  blocking_group() = default;

  blocking_group(const blocking_group&) = delete;
  blocking_group(blocking_group&&) = default;

  blocking_group& operator=(const blocking_group&) = delete;
  blocking_group& operator=(blocking_group&&) = default;

  inline ~blocking_group();

  inline void wait();
  inline void reset();
  inline void wait_and_reset();

  bool pending() const { return group_.pending(); }

 private:
  class latch_type;
  struct handler_type {
    inline handler_type();
    inline void operator()();

    std::shared_ptr<latch_type> latch;

    ~handler_type() {}  // This definition is required to avoid a bug in g++-4.8
  };

  static inline void do_nothing() {}

 public:
  template <typename Function>
  auto wrap(Function&& function)
      -> decltype(basic_completion_group<handler_type>{}.wrap(
          std::forward<Function>(function))) {
    return group_.wrap(std::forward<Function>(function));
  }

  auto wrap_notify() -> decltype(basic_completion_group<handler_type>{}.wrap(
      do_nothing)) {
    return group_.wrap(do_nothing);
  }

  template <typename... Args>
  auto save(Args&&... args)
      -> decltype(basic_completion_group<handler_type>{}.wrap(
          make_store_handler(std::forward<Args>(args)...))) {
    return group_.wrap(make_store_handler(std::forward<Args>(args)...));
  }

 private:
  basic_completion_group<handler_type> group_;
};

class blocking_group::latch_type {
 public:
  void trigger() {
    std::unique_lock<std::mutex> lock{mutex_};
    triggered_ = true;
    condition_.notify_all();
    lock.unlock();
  }

  void wait() {
    std::unique_lock<std::mutex> lock{mutex_};
    while (!triggered_) condition_.wait(lock);
  }

 private:
  std::mutex mutex_;
  std::condition_variable condition_;
  bool triggered_{false};
};

blocking_group::~blocking_group() {
  RIAKPP_CHECK(group_.pending())
    << "blocking_group destroyed before a call to wait()";
}

void blocking_group::wait() {
  if (!group_.pending()) {
    auto latch = group_.handler().latch;
    group_.notify();
    latch->wait();
  }
}

void blocking_group::reset() {
  RIAKPP_CHECK(group_.pending()) << "Called reset without waiting on group.";
  group_.reset();
}

void blocking_group::wait_and_reset() {
  wait();
  reset();
}

blocking_group::handler_type::handler_type()
    : latch{std::make_shared<latch_type>()} {}

void blocking_group::handler_type::operator()() { latch->trigger(); }

}  // namespace riak

#endif  // #ifndef RIAKPP_BLOCKING_GROUP_HPP_
