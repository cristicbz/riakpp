#include "completion_group.hpp"
#include <gtest/gtest.h>

namespace riak {
namespace testing {
namespace {

TEST(CompletionGroupTest, EmptyOk) {
  bool x = false;
  {
    auto wrapped = [&] { x = true; };
    auto group = make_completion_group(wrapped);
  }
  EXPECT_TRUE(x);
}

TEST(CompletionGroupTest, CopyOnce) {
  bool x = false;
  auto handler = [&] { x = true; };
  {
    auto group = make_completion_group(handler);
    auto group_ref1 = group.ref();
    {
      auto group_ref2 = group_ref1;
    }
    EXPECT_FALSE(x);
  }
  EXPECT_TRUE(x);
}

TEST(CompletionGroupTest, WrapFuncs) {
  bool x = false;
  int f1 = 1, f2 = 2;
  auto handler = [&] { x = true; };
  auto fun1 = [&] (int x) -> int { int k = f1; f1 = x; return k; };
  auto fun2 = [&] (int x) -> int { int k = f2; f2 = x; return k; };

  auto group = make_completion_group(handler);
  {
    auto wrapped1 = group.wrap(fun1);
    {
      auto wrapped2 = group.wrap(fun2);
      EXPECT_FALSE(x);
      group.notify();
      EXPECT_FALSE(x);

      EXPECT_EQ(1, wrapped1(10));
      EXPECT_EQ(10, f1);
      EXPECT_FALSE(x);

      EXPECT_EQ(2, wrapped2(20));
      EXPECT_EQ(20, f2);
    }
    EXPECT_FALSE(x);
  }
  EXPECT_TRUE(x);
}

struct movable_only {
  movable_only(int& x) : x(x) {}

  movable_only(const movable_only&) = delete;
  movable_only& operator=(const movable_only&) = delete;

  movable_only(movable_only&&) = default;
  movable_only& operator=(movable_only&&) = default;

  void operator()() { x = -1; }
  int operator()(int y) { int k = x; x = y; return k; }

  int& x;
};

TEST(CompletionGroupTest, MovableOnly) {
  int h = 0, f = 1;
  auto handler = movable_only(h);
  auto fun = movable_only(f);

  {
    auto group = make_completion_group(std::move(handler));
    auto wrapped = group.wrap(std::move(fun));
    auto other = std::move(wrapped);
    EXPECT_EQ(1, other(10));
    EXPECT_EQ(10, f);
    EXPECT_EQ(0, h);
    group.notify();
    EXPECT_EQ(0, h);
  }
  EXPECT_EQ(-1, h);
}

}  // namespace
}  // namespace testing
}  // namespace riak
