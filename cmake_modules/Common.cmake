include(CMakeForceCompiler)

set(CMAKE_CXX_COMPILER "/usr/bin/clang++")

list(APPEND CMAKE_MODULE_PATH .)
list(APPEND CMAKE_CXX_FLAGS
     "-std=c++11 -Wall -fcolor-diagnostics -Wno-c++11-extensions")

if(CMAKE_BUILD_TYPE STREQUAL "release")
  list(APPEND CMAKE_CXX_FLAGS "-O4")
endif()
