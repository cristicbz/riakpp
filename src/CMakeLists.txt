include(rpcz_functions)

protobuf_generate_cpp(RIAK_PB_SRCS RIAK_PB_HDRS proto/riak.proto)
set(RIAK_PB ${RIAK_PB_SRCS} ${RIAK_PB_HDRS})

protobuf_generate_cpp(RIAK_KV_PB_SRCS RIAK_KV_PB_HDRS proto/riak_kv.proto)
set(RIAK_KV_PB ${RIAK_KV_PB_SRCS} ${RIAK_KV_PB_HDRS})

add_library(riak_pb_lib ${RIAK_PB} ${RIAK_KV_PB})

add_library(
  riakpp
    blocking_counter.hpp
    broker.hpp
    check.cpp
    check.hpp
    client.cpp
    client.hpp
    connection.hpp
    connection_pool.cpp
    connection_pool.hpp
    debug_log.cpp
    debug_log.hpp
    length_framed_unbuffered_connection.cpp
    length_framed_unbuffered_connection.hpp
    thread_pool.cpp
    thread_pool.hpp
)
target_link_libraries(
  riakpp
    riak_pb_lib
    ${Boost_LIBRARIES}
    ${PROTOBUF_LIBRARY}
)

add_executable(
  riaktool
    riaktool.cpp
)
target_link_libraries(
  riaktool
    riakpp
    ${Boost_LIBRARIES}
    ${PROTOBUF_LIBRARY}
)
