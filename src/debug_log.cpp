#include "debug_log.hpp"

#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>
#include <thread>

namespace riak {
namespace internal {

debug_log_stream::debug_log_stream(const char* filename, const char* function,
                                   int line) {
  std::string fn{filename};
  auto slash = fn.find_last_of("/\\");
  if (slash != std::string::npos) fn = fn.substr(slash + 1);
  location_ << fn << ":" << line << ":" << function << ": ";
}

debug_log_stream::~debug_log_stream() {
  static std::unordered_map<std::thread::id, uint64_t> id_map;
  static std::mutex m;
  std::lock_guard<std::mutex> l{m};

  auto thread_id =
      id_map.insert({std::this_thread::get_id(), id_map.size()}).first->second;
  std::cerr << "[" << thread_id << "] " << location_.str() << message_.str()
            << std::endl;
}

}  // namespace internal
}  // namespace riak
