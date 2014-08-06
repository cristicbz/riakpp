#ifndef RIAKPP_OPTION_HPP_
#define RIAKPP_OPTION_HPP_

#include <utility>

#define RIAKPP_DEFINE_OPTION(type, name, dflt)                \
 private:                                                     \
  option<type> name##_{dflt};                                 \
                                                              \
 public:                                                      \
  auto name(type new_value)->decltype(*this) {                \
    name##_.set(new_value);                                   \
    return *this;                                             \
  }                                                           \
  auto move_##name()->type&& { return name##_.move_value(); } \
  auto name() const->const type& { return name##_.value(); }  \
  auto defaulted_##name() const->bool { return name##_.defaulted(); }

namespace riak {

template<typename Type>
class option {
 public:
  using value_type = Type;
  option(value_type default_value) : value_{std::move(default_value)} {}

  void set(value_type new_value) {
    value_ = std::move(new_value);
    defaulted_ = false;
  }

  bool defaulted() const { return defaulted_; }
  const value_type& value() const { return value_; }
  value_type&& move_value() { return std::move(value_); }

 private:
  value_type value_;
  bool defaulted_ = true;
};

}  // namespace riak

#endif  // #ifndef RIAKPP_OPTION_HPP_
