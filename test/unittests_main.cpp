#include <gtest/gtest.h>

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  testing::FLAGS_gtest_death_test_style = "threadsafe";
  testing::FLAGS_gtest_color = "yes";
  testing::FLAGS_gtest_catch_exceptions = true;
  return RUN_ALL_TESTS();
}
