#ifndef RIAKPP_CONNECTION_OPTIONS_HPP_
#define RIAKPP_CONNECTION_OPTIONS_HPP_

#include "option.hpp"

#include <cstddef>
#include <cstdint>

namespace riak {
class connection_options {
 public:
  RIAKPP_DEFINE_OPTION(size_t, highwatermark, 4096)
  RIAKPP_DEFINE_OPTION(size_t, max_sockets, 8)
  RIAKPP_DEFINE_OPTION(uint64_t, deadline_ms, 3000)
  RIAKPP_DEFINE_OPTION(uint64_t, connection_timeout_ms, 1500)
  RIAKPP_DEFINE_OPTION(size_t, num_worker_threads, 0)
};
}  // namespace riak

#endif  // #ifndef RIAKPP_CONNECTION_OPTIONS_HPP_
