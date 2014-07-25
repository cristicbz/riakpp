protobuf_generate_cpp(RIAK_PB_SRCS RIAK_PB_HDRS proto/riak.proto)
set(RIAK_PB ${RIAK_PB_SRCS} ${RIAK_PB_HDRS})

protobuf_generate_cpp(RIAK_KV_PB_SRCS RIAK_KV_PB_HDRS proto/riak_kv.proto)
set(RIAK_KV_PB ${RIAK_KV_PB_SRCS} ${RIAK_KV_PB_HDRS})

add_library(
  riakpp SHARED
    check.cpp
    client.cpp
    connection_pool.cpp
    debug_log.cpp
    length_framed_connection.cpp
    thread_pool.cpp
    ${RIAK_PB} ${RIAK_KV_PB}
)
target_link_libraries(
  riakpp
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
)

install(TARGETS riakpp
  LIBRARY DESTINATION lib
)
