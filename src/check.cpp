#include "check.hpp"

#include <execinfo.h>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include <thread>

namespace riak {
namespace internal {
namespace {
// Adapted from here:
//   http://stackoverflow.com/questions/3151779
void __attribute__((noinline)) dump_trace() {
  void *trace[128];
  int i, trace_size = 0;
  char name_buf[512];
  name_buf[readlink("/proc/self/exe", name_buf, 511)] = 0;

  trace_size = backtrace(trace, 128);
  for (i = 2; i < trace_size; ++i) {
    std::cerr << (i == 2 ? "In " : "  called from ");
    std::cerr.flush();

    char syscom[256];
    sprintf(syscom, "addr2line %p -spfCe %s 1>&2", trace[i], name_buf);
    int ret = system(syscom);
    if (ret) std::cerr << "<unknown>" << std::endl;
  }
}

}  // namespace


check_error_stream::check_error_stream(const char *location,
                                       std::string *message) noexcept {
  message_ << location;
  if (message) {
    message_ << *message;
    delete message;
  }
}

__attribute__((noinline)) check_error_stream::~check_error_stream() noexcept {
  if (!additional_message_) message_ << ".";
  std::cerr << message_.str() << std::endl;
  dump_trace();
  std::abort();
}

}  // namespace internal
}  // namespace riak
