option(ENABLE_CTEST OFF "In addition to the unittests binary generate tests
compatible with ctest.")

if (BUILD_TESTS)

include(ExternalProject)
ExternalProject_Add(
  GMock
    URL http://googlemock.googlecode.com/files/gmock-1.7.0.zip
    TIMEOUT 30
    INSTALL_COMMAND ""
)
ExternalProject_Get_Property(GMock binary_dir)
ExternalProject_Get_Property(GMock source_dir)
include_directories(SYSTEM ${source_dir}/include)
link_directories(${binary_dir})

include_directories(SYSTEM ${source_dir}/gtest/include)
link_directories(${binary_dir})
link_directories(${binary_dir}/gtest)

set(
  UNITTESTS
    blocking_group_test.cpp
    completion_group_test.cpp
    connection_pool_test.cpp
    length_framed_connection_test.cpp
    object_test.cpp
    store_handler_test.cpp)

add_executable(
  unittests
    ${UNITTESTS}
    test_length_framed_server.cpp
    unittests_main.cpp
)
add_dependencies(unittests GTest)
add_dependencies(unittests GMock)
target_link_libraries(
  unittests
    riakpp gmock gtest
)

if (ENABLE_CTEST)
  enable_testing()
  foreach(GTEST_SOURCE_FILE ${UNITTESTS})
    file(STRINGS ${GTEST_SOURCE_FILE} GTEST_NAMES REGEX ^TEST)
    foreach(GTEST_NAME ${GTEST_NAMES})
      string(REGEX REPLACE ["\) \(,"] ";" GTEST_NAME ${GTEST_NAME})
      list(GET GTEST_NAME 1 GTEST_GROUP_NAME)
      list(GET GTEST_NAME 3 GTEST_NAME)
      add_test(${GTEST_GROUP_NAME}.${GTEST_NAME} ${PROJECT_BINARY_DIR}/test/unittests --gtest_filter=${GTEST_GROUP_NAME}.${GTEST_NAME})
    endforeach()
  endforeach()
endif (ENABLE_CTEST)

endif (BUILD_TESTS)
