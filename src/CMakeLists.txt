include_directories("../include/")

protobuf_generate_cpp(RIAK_PB_SRCS RIAK_PB_HDRS proto/riak.proto)
set(RIAK_PB ${RIAK_PB_SRCS} ${RIAK_PB_HDRS})

protobuf_generate_cpp(RIAK_KV_PB_SRCS RIAK_KV_PB_HDRS proto/riak_kv.proto)
set(RIAK_KV_PB ${RIAK_KV_PB_SRCS} ${RIAK_KV_PB_HDRS})

add_library(
  riakpp SHARED
    check.cpp
    client.cpp
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

install(TARGETS riakpp
  LIBRARY DESTINATION lib
)
