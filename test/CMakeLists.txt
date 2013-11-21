
find_package(GTest REQUIRED)

include_directories(${GTest_INCLUDE_DIR})

file(GLOB TEST_FILES *_test.cpp)
add_executable(
  unittests
    unittests_main.cpp
    ${TEST_FILES}
)

target_link_libraries(
  unittests
    riakpp gtest
)

add_custom_target(
    test unittests 1>&2
    DEPENDS unittests
)



