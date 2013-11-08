set(RPCZ_PLUGIN_ROOT /usr/local/bin)

function(PROTOBUF_GENERATE_RPCZ SRCS HDRS)
  set(PLUGIN_BIN ${RPCZ_PLUGIN_ROOT}/protoc-gen-cpp_rpcz)
  PROTOBUF_GENERATE_MULTI(PLUGIN "cpp_rpcz" PROTOS ${ARGN}
                          OUTPUT_STRUCT "_SRCS:.rpcz.cc;_HDRS:.rpcz.h"
                          FLAGS "--plugin=protoc-gen-cpp_rpcz=${PLUGIN_BIN}"
                          DEPENDS ${PLUGIN_BIN})
  set(${SRCS} ${_SRCS} PARENT_SCOPE)
  set(${HDRS} ${_HDRS} PARENT_SCOPE)
endfunction()

function(PROTOBUF_GENERATE_PYTHON_RPCZ SRCS)
  set(PLUGIN_BIN ${RPCZ_PLUGIN_ROOT}/protoc-gen-python_rpcz)
  PROTOBUF_GENERATE_MULTI(PLUGIN "python_rpcz" PROTOS ${ARGN}
                          OUTPUT_STRUCT "_SRCS:_rpcz.py"
                          FLAGS "--plugin=protoc-gen-python_rpcz=${PLUGIN_BIN}"
                          DEPENDS ${PLUGIN_BIN})
  set(${SRCS} ${_SRCS} PARENT_SCOPE)
endfunction()
