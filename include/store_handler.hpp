#ifndef RIAKPP_STORE_HANDLER_HPP_
#define RIAKPP_STORE_HANDLER_HPP_

#include <tuple>
#include <utility>

namespace riak {

template <typename ArgTuple>
class store_handler {
 public:
  enum from_args_t { from_args = 0 };

  template <typename... Args>
  store_handler(from_args_t, Args&&... args)
      : extract_to_{std::forward<Args>(args)...} {}

  store_handler(const store_handler&) = default;
  store_handler(store_handler&&) = default;
  store_handler& operator=(const store_handler&) = default;
  store_handler& operator=(store_handler&&) = default;

  template <typename... Args>
  void operator()(Args&&... args) const {
    extract_to_ = std::forward_as_tuple(std::forward<Args>(args)...);
  }

  template <typename... Args>
  void operator()(Args&&... args) {
    extract_to_ = std::forward_as_tuple(std::forward<Args>(args)...);
  }

 private:
  ArgTuple extract_to_;
};

template <typename... Args>
inline auto make_store_handler(Args&&... args)
    -> store_handler<std::tuple<Args&&...>> {
  return {store_handler<std::tuple<Args&&...>>::from_args,
          std::forward<Args>(args)...};
}

}  // namespace riak

#endif  // #ifndef RIAKPP_STORE_HANDLER_HPP_
