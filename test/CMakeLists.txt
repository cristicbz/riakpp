include(ExternalProject)

ExternalProject_Add(
    GTest
    URL http://googletest.googlecode.com/files/gtest-1.7.0.zip
    TIMEOUT 30
    INSTALL_COMMAND ""
)
ExternalProject_Get_Property(GTest binary_dir)
ExternalProject_Get_Property(GTest source_dir)
include_directories(${source_dir}/include)
link_directories(${binary_dir})
enable_testing(true)

#ExternalProject_Add(
#    GMock
#    URL http://googlemock.googlecode.com/files/gmock-1.7.0.zip
#    TIMEOUT 30
#    INSTALL_COMMAND ""
#)
#ExternalProject_Get_Property(GMock binary_dir)
#ExternalProject_Get_Property(GMock source_dir)
#include_directories(${source_dir}/include)
#link_directories(${binary_dir})

file(GLOB TEST_FILES *_test.cpp)
add_executable(
  unittests
    unittests_main.cpp
    ${TEST_FILES}
)

add_dependencies(unittests GTest)
#addidependencies(unittests GMock)

target_link_libraries(
  unittests
    riakpp gtest
)

add_custom_target(
    test unittests 1>&2
    DEPENDS unittests
)



