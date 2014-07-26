#ifndef RIAKPP_TRANSIENT_HPP_
#define RIAKPP_TRANSIENT_HPP_

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

#include "check.hpp"

namespace riak {

template <class Wrapped>
class transient;

template <class Wrapped>
class transient_ref;

template <typename TransientRef, typename Function>
class transient_function_wrapper;

namespace internal {
class blocking_counter {
 public:
  blocking_counter() = default;
  inline ~blocking_counter();

  blocking_counter(const blocking_counter&) = delete;
  blocking_counter(blocking_counter&&) = delete;

  blocking_counter& operator=(const blocking_counter&) = delete;
  blocking_counter& operator=(blocking_counter&&) = delete;

  inline bool try_acquire();
  inline void release();

  template <class Function>
  inline void wait_and_close(Function&& function);

 private:
  uint32_t count_{1};
  bool closed_ = false;

  std::mutex count_mutex_;
  std::condition_variable zero_count_;

};
}  // namespace internal

template <class Wrapped>
class transient {
 public:
  using wrapped_type = Wrapped;
  using ref_type = transient_ref<wrapped_type>;

  friend class transient_ref<wrapped_type>;

  inline explicit transient(wrapped_type& wrapped);

  transient(transient&&) = delete;
  transient(const transient&) = delete;

  inline transient& operator=(transient&&);
  transient& operator=(const transient&) = delete;

  ~transient() { reset(); }

  inline void reset(wrapped_type& wrapped);
  inline void reset();

  template <class Function>
  inline transient_function_wrapper<transient_ref<wrapped_type>,
                                    typename std::decay<Function>::type>
      wrap(Function&& function);

  inline transient_ref<wrapped_type> ref() const;

 private:
  struct payload {
    payload(wrapped_type& wrapped) : wrapped(&wrapped) {}

    wrapped_type* wrapped;
    internal::blocking_counter lock_counter;
  };

  std::shared_ptr<payload> payload_;
};

template <typename TransientRef, typename Function>
class transient_function_wrapper {
 public:
  template <class FunctionConv>
  transient_function_wrapper(TransientRef ref, FunctionConv&& fun)
    : transient_ref_{std::move(ref)},
      function_(std::forward<FunctionConv>(fun)) {}

  template <typename ...Args>
  void operator()(Args&&... args) const {
    if (auto lock = transient_ref_.lock()) {
      function_(std::forward<Args>(args)...);
    }
  }

  template <typename ...Args>
  void operator()(Args&&... args) {
    if (auto lock = transient_ref_.lock()) {
      function_(std::forward<Args>(args)...);
    }
  }

 private:
  TransientRef transient_ref_;
  Function function_;
};

template <class Wrapped>
class transient_ref {
 public:
  using wrapped_type = Wrapped;
  using transient_type = transient<wrapped_type>;

  explicit transient_ref(const transient_type& to) : payload_{to.payload_} {}

  class locked;
  friend class locked;

  class locked {
   public:
    inline locked(const transient_ref& ref);
    ~locked() { if (lock_counter_) lock_counter_->release(); }

    locked(const locked&) = delete;
    locked& operator=(const locked&) = delete;

    inline locked(locked&& rhs);
    inline locked& operator=(locked&&);

    explicit operator bool() const { return wrapped_ != nullptr; }
    inline wrapped_type& operator*() const;
    inline wrapped_type* operator->() const;

   private:
    Wrapped* wrapped_{nullptr};
    internal::blocking_counter* lock_counter_{nullptr};
  };

  locked lock() const { return locked{*this}; }

 private:
  std::shared_ptr<typename transient_type::payload> payload_;
};


// Template and inline implementations.

namespace internal {
blocking_counter::~blocking_counter() {
  RIAKPP_CHECK(closed_);
  RIAKPP_CHECK_EQ(0, count_);
}

template <class Function>
void blocking_counter::wait_and_close(Function&& function) {
  std::unique_lock<std::mutex> lock{count_mutex_};
  RIAKPP_CHECK(!closed_);
  RIAKPP_CHECK_GT(count_, 0);
  closed_ = true;
  --count_;
  while (count_ > 0) zero_count_.wait(lock);
  function();
}

bool blocking_counter::try_acquire() {
  std::lock_guard<std::mutex> lock{count_mutex_};
  if (closed_) return false;
  ++count_;
  return true;
}

void blocking_counter::release() {
  std::unique_lock<std::mutex> lock{count_mutex_};
  RIAKPP_CHECK_GT(count_, 0);
  if (--count_ == 0) {
    RIAKPP_CHECK(closed_);
    lock.unlock();
    zero_count_.notify_one();
  }
}
}  // namespace internal


template <class Wrapped>
transient<Wrapped>::transient(wrapped_type& wrapped)
    : payload_{std::make_shared<payload>(wrapped)} {}

template <class Wrapped>
transient<Wrapped>& transient<Wrapped>::operator=(transient&& rhs) {
  reset();
  payload_ = std::move(rhs.payload_);
  return *this;
}

template <class Wrapped>
void transient<Wrapped>::reset(wrapped_type& wrapped) {
  reset();
  payload_ = std::make_shared(wrapped);
}

template <class Wrapped>
void transient<Wrapped>::reset() {
  if (!payload_) return;
  auto ensure_live_payload = payload_;
  payload_->lock_counter.wait_and_close([&] {
    payload_->wrapped = nullptr;
    payload_.reset();
  });
}

template <class Wrapped>
template <class Function>
inline transient_function_wrapper<transient_ref<Wrapped>,
                                  typename std::decay<Function>::type>
    transient<Wrapped>::wrap(Function&& function) {
  return {ref(), std::forward<Function>(function)};
}

template <class Wrapped>
transient_ref<Wrapped> transient<Wrapped>::ref() const {
  return transient_ref<wrapped_type>{*this};
}

template <class Wrapped>
transient_ref<Wrapped>::locked::locked(const transient_ref& ref) {
  if (!ref.payload_) return;

  lock_counter_ = &ref.payload_->lock_counter;
  if (!lock_counter_->try_acquire()) {
    lock_counter_ = nullptr;
  } else {
    wrapped_ = ref.payload_->wrapped;
    RIAKPP_CHECK(wrapped_ != nullptr);
  }
}

template <class Wrapped>
transient_ref<Wrapped>::locked::locked(locked&& rhs)
    : wrapped_{rhs.wrapped_},
      lock_counter_{rhs.lock_counter_} {
  rhs.wrapped_ = nullptr;
  rhs.lock_counter_ = nullptr;
}

template <class Wrapped>
auto transient_ref<Wrapped>::locked::operator=(locked&& rhs) -> locked& {
  wrapped_ = rhs.wrapped_;
  lock_counter_ = rhs.lock_counter_;
  rhs.wrapped_ = nullptr;
  rhs.lock_counter_ = nullptr;
  return *this;
}

template <class Wrapped>
auto transient_ref<Wrapped>::locked::operator->() const -> wrapped_type* {
  RIAKPP_CHECK(wrapped_ != nullptr);
  return wrapped_;
}

template <class Wrapped>
auto transient_ref<Wrapped>::locked::operator*() const -> wrapped_type& {
  RIAKPP_CHECK(wrapped_ != nullptr);
  return *wrapped_;
}

}  // namespace riak

#endif  // #ifndef RIAKPP_TRANSIENT_HPP_
