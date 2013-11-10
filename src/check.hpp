#ifndef RIAKPP_CHECK_HPP_
#define RIAKPP_CHECK_HPP_

#include <iostream>
#include <string>
#include <sstream>

#define RIAKPP_INTERNAL_XSTRINGIFY(x) #x
#define RIAKPP_INTERNAL_STRINGIFY(x) RIAKPP_INTERNAL_XSTRINGIFY(x)

#define RIAKPP_INTERNAL_CHECK_ERROR_STREAM(x)                 \
  ::riak::internal::check_error_stream {                      \
    __FILE__ ":" RIAKPP_INTERNAL_STRINGIFY(__LINE__) ": ", x \
  }
#define RIAKPP_CHECK(cond) \
  if (!(cond))             \
  RIAKPP_INTERNAL_CHECK_ERROR_STREAM(nullptr) << "Check '" #cond << "' failed"

#define RIAKPP_INTERNAL_CHECK_OP(id, op, a, b)                            \
  if (::std::string *message =                                            \
          ::riak::internal::Check##id##Impl((a), (b), #a " " #op " " #b)) \
  RIAKPP_INTERNAL_CHECK_ERROR_STREAM(message)

#define RIAKPP_CHECK_EQ(a, b) RIAKPP_INTERNAL_CHECK_OP(Eq, ==, a, b)
#define RIAKPP_CHECK_LE(a, b) RIAKPP_INTERNAL_CHECK_OP(Le, <=, a, b)
#define RIAKPP_CHECK_LT(a, b) RIAKPP_INTERNAL_CHECK_OP(Lt, <, a, b)
#define RIAKPP_CHECK_GE(a, b) RIAKPP_INTERNAL_CHECK_OP(Ge, >=, a, b)
#define RIAKPP_CHECK_GT(a, b) RIAKPP_INTERNAL_CHECK_OP(Gt, >, a, b)

namespace riak {
namespace internal {

#define RIAKPP_INTERNAL_DEFINE_CHECK_OP_IMPL(id, op)                        \
  template <class T, class U>                                               \
  inline std::string *Check##id##Impl(const T &a, const U &b,               \
                                      const char *condition) {              \
    if (a op b) return nullptr;                                             \
    std::stringstream stream{};                                             \
    stream << "Check '" << condition << "' failed ('" << a << "' vs '" << b \
           << "')";                                                         \
    return new std::string{stream.str()};                                   \
  }

RIAKPP_INTERNAL_DEFINE_CHECK_OP_IMPL(Eq, == );
RIAKPP_INTERNAL_DEFINE_CHECK_OP_IMPL(Le, <= );
RIAKPP_INTERNAL_DEFINE_CHECK_OP_IMPL(Lt, < );
RIAKPP_INTERNAL_DEFINE_CHECK_OP_IMPL(Ge, >= );
RIAKPP_INTERNAL_DEFINE_CHECK_OP_IMPL(Gt, > );

#undef RIAKPP_INTERNAL_DEFINE_CHECK_OP_IMPL

class check_error_stream {
 public:
  check_error_stream(const char *location,
                     std::string *message = nullptr) noexcept;

  template <class T>
  check_error_stream &operator<<(const T& message_piece) {
    if (!additional_message_) {
      additional_message_ = true;
      message_ << ": ";
    }
    message_ << message_piece;
    return *this;
  }

  ~check_error_stream() noexcept;

 private:
  std::stringstream message_;
  bool additional_message_ = false;
};

}  // namespace internal
}  // namespace riak

#endif  // #ifndef RIAKPP_CHECK_HPP_
