#ifndef RIAKPP_BLOCKING_OBJECT_HPP_
#define RIAKPP_BLOCKING_OBJECT_HPP_

#include "check.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <type_traits>

namespace riak {

template <class T>
class basic_blocking_pointer;

template <class T>
class blocking_object {
 public:
  friend class basic_blocking_pointer<T>;
  friend class basic_blocking_pointer<const T>;

  typedef basic_blocking_pointer<T> pointer;
  typedef basic_blocking_pointer<const T> const_pointer;

  blocking_object(T& pointee) : pointee_{&pointee} {}
  ~blocking_object() { destroy(); }

  void destroy() {
    if (!destroyed_.exchange(true)) {
      RIAKPP_CHECK_GT(ptr_count_, 0);
      std::unique_lock<std::mutex> lock{mutex_};
      --ptr_count_;
      while (ptr_count_ > 0) zero_count_.wait(lock);
    }
  }

  pointer new_ptr() {
    return pointer{*this};
  }
  const_pointer new_ptr() const {
    return pointer{*this};
  }
  const_pointer new_const_ptr() const {
    return pointer{*this};
  }

 private:
  inline T* add_ptr();
  inline void remove_ptr();

  std::mutex mutex_;
  std::condition_variable zero_count_;
  std::atomic<bool> destroyed_{false};
  std::atomic<uint32_t> ptr_count_{1};
  T* pointee_;
};

template <class T>
T* blocking_object<T>::add_ptr() {
  RIAKPP_CHECK_GT(ptr_count_.fetch_add(1), 0);
  return pointee_;
}

template <class T>
void blocking_object<T>::remove_ptr() {
  std::unique_lock<std::mutex> lock{mutex_};
  if (--ptr_count_ == 0) zero_count_.notify_all();
}

template <class T>
class basic_blocking_pointer {
 public:
  friend class blocking_object<typename std::remove_const<T>::type>;
  typedef blocking_object<typename std::remove_const<T>::type> object_type;
  typedef basic_blocking_pointer self_type;

  basic_blocking_pointer() : obj_{nullptr} {}
  basic_blocking_pointer(const self_type& other)
      : obj_{other.obj_}, pointee_{obj_ ? obj_->add_ptr() : nullptr} {}
  basic_blocking_pointer(self_type&& other)
      : obj_{other.obj_}, pointee_{other.pointee_} {
    other.obj_ = nullptr;
    other.pointee_ = nullptr;
  }

  ~basic_blocking_pointer() {
    if (obj_) obj_->remove_ptr();
  }

  T& operator*() const { return *pointee_; }
  T* operator->() const { return pointee_; }

  self_type& operator=(self_type&& other) {
    obj_ = other.obj_;
    other.obj_ = nullptr;
    pointee_ = other.pointee_;
    other.pointee_ = nullptr;
  }

  self_type& operator=(const self_type& other) {
    obj_ = other.obj_;
    pointee_ = obj_ ? obj_->add_ptr() : nullptr;
    return *this;
  }

 private:
  basic_blocking_pointer(object_type& obj)
      : obj_{&obj}, pointee_{obj.add_ptr()} {}

  object_type* obj_;
  T* pointee_;
};

template <class T>
using blocking_ptr = typename blocking_object<T>::pointer;

template <class T>
using blocking_cptr = typename blocking_object<T>::const_pointer;

}  // namespace

#endif  // #ifndef RIAKPP_BLOCKING_OBJECT_HPP_
