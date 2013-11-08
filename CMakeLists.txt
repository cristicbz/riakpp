project(riakpp)
cmake_minimum_required (VERSION 2.8)


set(CMAKE_CXX_COMPILER "/usr/bin/clang++")
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake_modules")
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -g")


# Package Dependencies
# ==============================================================================
find_package(
  Boost 1.53
    COMPONENTS
      program_options
      system
      thread
    REQUIRED
)

find_package(ProtobufPlugin REQUIRED)
find_package(Glog REQUIRED)

include_directories(
  ${Boost_INCLUDE_DIR}
  ${GLOG_INCLUDE_DIR}
  ${PROTOBUF_INCLUDE_DIR}
)


add_subdirectory(src)
