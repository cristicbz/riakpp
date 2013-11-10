project(riakpp)
cmake_minimum_required (VERSION 2.8)

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake_modules")
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -Werror -g -fcolor-diagnostics")

# Package Dependencies
# ==============================================================================
find_package(
  Boost 1.54
    COMPONENTS
      program_options
      system
      thread
    REQUIRED
)

find_package(ProtobufPlugin REQUIRED)

include_directories(
  ${Boost_INCLUDE_DIR}
  ${PROTOBUF_INCLUDE_DIR}
)

add_subdirectory(src)
