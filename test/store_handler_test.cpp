#include "store_handler.hpp"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>
#include <sstream>

namespace riak {
namespace testing {
namespace {

TEST(StoreHandlerTest, Empty) {
  auto handler = make_store_handler();
  handler();
}

TEST(StoreHandlerTest, SomeInts) {
  int a = 1;
  int b = 2;
  int indirect = 3;
  int& c = indirect;

  auto handler = make_store_handler(a, b, c);
  handler(100, 200, 300);

  EXPECT_EQ(100, a);
  EXPECT_EQ(200, b);
  EXPECT_EQ(300, indirect);
}

TEST(StoreHandlerTest, FromOthers) {
  int a = 1;
  int b = 2;
  int indirect = 3;
  int& c = indirect;

  int x = 100;
  int y = 200;
  int indirect_src = 300;
  int& z = indirect_src;

  auto handler = make_store_handler(a, b, c);
  handler(x, y, z);

  EXPECT_EQ(100, a);
  EXPECT_EQ(200, b);
  EXPECT_EQ(300, indirect);

  EXPECT_EQ(100, x);
  EXPECT_EQ(200, y);
  EXPECT_EQ(300, indirect_src);
}

struct movable_only {
  movable_only(int x) : x{x} {}

  movable_only(movable_only&&) = default;
  movable_only& operator=(movable_only&&) = default;

  movable_only(const movable_only&) = delete;
  movable_only& operator=(const movable_only&) = delete;

  int x;
};

struct move_destroys {
  move_destroys(int x) : x{x} {}

  move_destroys(move_destroys&& rhs) : x{rhs.x} { rhs.x = -1; }
  move_destroys& operator=(move_destroys&& rhs) {
    x = rhs.x;
    rhs.x = -1;
    return *this;
  }

  move_destroys(const move_destroys&) = default;
  move_destroys& operator=(const move_destroys&) = default;

  int x;
};

TEST(StoreHandlerTest, Movable) {
  movable_only m = 5;
  auto handler = make_store_handler(m);
  handler(movable_only{10});
  EXPECT_EQ(10, m.x);

  move_destroys m2 = 5;
  move_destroys m3 = 10;
  auto handler2 = make_store_handler(m2);
  handler2(m3);
  EXPECT_EQ(10, m2.x);
  EXPECT_EQ(10, m3.x);

  m2.x = 5;
  handler2(std::move(m3));
  EXPECT_EQ(10, m2.x);
  EXPECT_EQ(-1, m3.x);
}

TEST(StoreHandlerTest, CopyHandler) {
  int a = 1;
  int b = 2;

  auto handler = make_store_handler(a, b);
  auto handler2 = handler;
  handler2(100, 200);

  EXPECT_EQ(100, a);
  EXPECT_EQ(200, b);
}


}  // namespace
}  // namespace testing
}  // namespace riak
