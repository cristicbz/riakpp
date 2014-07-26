#ifndef RIAKPP_ENDPOINT_VECTOR_HPP_
#define RIAKPP_ENDPOINT_VECTOR_HPP_

#include <vector>
#include <boost/asio/ip/tcp.hpp>

namespace riak {
using endpoint_vector = std::vector<boost::asio::ip::tcp::endpoint>;
using endpoint_iterator = endpoint_vector::const_iterator;
}  // namespace riak

#endif  // #ifndef RIAKPP_ENDPOINT_VECTOR_HPP_
