#ifndef RIAKPP_BYTE_ORDER_HPP_
#define RIAKPP_BYTE_ORDER_HPP_

#include <boost/asio/detail/socket_ops.hpp>

namespace riak {
namespace byte_order {
using boost::asio::detail::socket_ops::host_to_network_long;
using boost::asio::detail::socket_ops::network_to_host_long;
using boost::asio::detail::socket_ops::host_to_network_short;
using boost::asio::detail::socket_ops::network_to_host_short;
}  // namespace byte_order
}  // namespace riak

#endif // #ifndef RIAKPP_BYTE_ORDER_HPP_
