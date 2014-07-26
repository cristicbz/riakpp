#ifndef RIAKPP_DEBUG_LOG_HPP_
#define RIAKPP_DEBUG_LOG_HPP_

#include <sstream>

#ifdef RIAKPP_DEBUG_LOGGING
#define RIAKPP_DLOG \
  ::riak::internal::debug_log_stream(__FILE__, __FUNCTION__, __LINE__)
#else
#define RIAKPP_DLOG if (false) std::stringstream{}
#endif

#define RIAKPP_TRACE()                                                 \
  ::riak::internal::debug_log_stream(__FILE__, __FUNCTION__, __LINE__) \
      << "\x1b[32mTRACE \x1b[0m"

namespace riak {
namespace internal {

class debug_log_stream {
 public:
  debug_log_stream(const char* filename, const char* function, int line);
  ~debug_log_stream();

  template <class T>
  debug_log_stream& operator<<(const T& message_piece) {
    message_ << message_piece;
    return *this;
  }

 private:
  std::stringstream location_;
  std::stringstream message_;
};

}  // namespace internal
}  // namespace riak

#endif  // #ifndef RIAKPP_DEBUG_LOG_HPP_
