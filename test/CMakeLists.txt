include(ExternalProject)

ExternalProject_Add(
    GTest
    URL http://googletest.googlecode.com/files/gtest-1.7.0.zip
    TIMEOUT 30
    INSTALL_COMMAND ""
)
ExternalProject_Get_Property(GTest binary_dir)
ExternalProject_Get_Property(GTest source_dir)
include_directories(SYSTEM ${source_dir}/include)
link_directories(${binary_dir})
enable_testing(true)

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

add_executable(
  unittests
    blocking_group_test.cpp
    completion_group_test.cpp
    connection_pool_test.cpp
    length_framed_connection_test.cpp
    object_test.cpp
    store_handler_test.cpp

    test_length_framed_server.cpp
    unittests_main.cpp
)

add_dependencies(unittests GTest)
add_dependencies(unittests GMock)

target_link_libraries(
  unittests
    riakpp gmock gtest
)

add_custom_target(
    test unittests 1>&2
    DEPENDS unittests
)

