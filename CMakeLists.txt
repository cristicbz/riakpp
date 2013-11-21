project(riakpp)
cmake_minimum_required (VERSION 2.8)

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake_modules")

# C++ Flags
# ==============================================================================

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -g -fcolor-diagnostics")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=c++11 -Werror -g")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Intel")
  # using Intel C++
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  # using Visual Studio C++
endif()

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
  "${Boost_INCLUDE_DIR}"
  "${PROTOBUF_INCLUDE_DIR}"
  "${PROJECT_BINARY_DIR}/src"
)

# Subdirectories
# ==============================================================================
include_directories("src" "include")

add_subdirectory("src")
add_subdirectory("test")
