#ifndef RIAKPP_COMPLETION_GROUP_HPP_
#define RIAKPP_COMPLETION_GROUP_HPP_

#include "check.hpp"

#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

namespace riak {
template <typename Handler>
class basic_completion_group;

template <class RefType, typename Function>
class completion_wrapper;

template <typename Handler>
inline basic_completion_group<typename std::decay<Handler>::type>
    make_completion_group(Handler&& handler);

template <typename Handler>
class basic_completion_group {
 public:
  using handler_type = Handler;

  class ref_type;

  inline basic_completion_group(handler_type handler = {});

  basic_completion_group(basic_completion_group&&) = default;
  basic_completion_group(const basic_completion_group&) = delete;

  basic_completion_group& operator=(basic_completion_group&&) = default;
  basic_completion_group& operator=(const basic_completion_group&) = delete;

  inline void when_done(handler_type handler);

  inline void set_handler(handler_type handler);
  inline handler_type& handler();
  inline void notify();

  inline bool pending() const;
  inline void reset(handler_type handler = {});

  inline ref_type ref();

  template <typename Function>
  inline auto wrap(Function&& function)
      -> completion_wrapper<ref_type, typename std::decay<Function>::type>;

 private:
  struct trigger {
    trigger(handler_type handler) : handler{std::move(handler)} {}
    ~trigger() { handler(); }
    handler_type handler;
  };

  std::shared_ptr<trigger> trigger_;
};

using completion_group = basic_completion_group<std::function<void(void)>>;

template <typename Handler>
inline basic_completion_group<typename std::decay<Handler>::type>
make_completion_group(Handler&& handler) {
  return {std::forward<Handler>(handler)};
}

template <typename Handler>
class basic_completion_group<Handler>::ref_type {
  friend class basic_completion_group;
 private:
  ref_type(const std::shared_ptr<trigger>& trigger) : trigger_{trigger} {}
  std::shared_ptr<const basic_completion_group::trigger> trigger_;
};

template <class RefType, typename Function>
class completion_wrapper {
 public:
  completion_wrapper(const RefType& ref, Function function)
    : ref_{ref}, function_{std::move(function)} {}

  template <typename... Args>
  auto operator()(Args&&... args) const
      -> decltype(std::declval<Function>()(std::forward<Args>(args)...)) {
    return function_(std::forward<Args>(args)...);
  }

  template <typename... Args>
  auto operator()(Args&&... args)
      -> decltype(std::declval<Function>()(std::forward<Args>(args)...)) {
    return function_(std::forward<Args>(args)...);
  }

 private:
  RefType ref_;
  Function function_;
};

template <typename Handler>
inline basic_completion_group<Handler>::basic_completion_group(
    handler_type handler) {
  reset(std::move(handler));
}

template <typename Handler>
inline void basic_completion_group<Handler>::when_done(handler_type handler) {
  set_handler(handler);
  notify();
}

template <typename Handler>
inline auto basic_completion_group<Handler>::handler() -> handler_type& {
  RIAKPP_CHECK(!pending());
  return trigger_->handler;
}

template <typename Handler>
void basic_completion_group<Handler>::set_handler(handler_type handler) {
  RIAKPP_CHECK(!pending());
  trigger_->handler = std::move(handler);
}

template <typename Handler>
void basic_completion_group<Handler>::notify() {
  trigger_.reset();
}

template <typename Handler>
inline bool basic_completion_group<Handler>::pending() const {
  return !trigger_;
}

template <typename Handler>
inline void basic_completion_group<Handler>::reset(handler_type handler) {
  RIAKPP_CHECK(pending());
  trigger_ = std::make_shared<trigger>(std::move(handler));
}

template <typename Handler>
inline auto basic_completion_group<Handler>::ref() -> ref_type {
  return {trigger_};
}

template <typename Handler>
template <typename Function>
inline auto basic_completion_group<Handler>::wrap(Function&& function)
    -> completion_wrapper<ref_type, typename std::decay<Function>::type> {
  return {ref(), std::forward<Function>(function)};
}

}  // namespace riak

#endif  // #ifndef RIAKPP_COMPLETION_GROUP_HPP_
