#include "blocking_group.hpp"
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>

namespace riak {
namespace testing {
namespace {

TEST(BlockingGroupTest, EmptyOk) {
  blocking_group blocking;
  blocking.wait();  // Just checking for deadlocks.
}

using closure = std::function<void(void)>;

TEST(BlockingGroupTest, Wrap) {
  blocking_group blocking;
  {
    std::atomic<int> x{0};
    closure f = blocking.wrap([&] { x = 1; });
    std::thread async{[&] {
      std::this_thread::sleep_for(std::chrono::milliseconds{20});
      f(); f = {};
    }};
    blocking.wait();
    EXPECT_EQ(1, x);
    async.join();
  }
  blocking.reset();
  {
    std::atomic<int> x{0};
    std::atomic<int> y{0};
    closure fx = blocking.wrap([&] { x = 1; });
    closure fy = blocking.wrap([&] { y = 2; });
    std::thread async_x{[&] {
      std::this_thread::sleep_for(std::chrono::milliseconds{20});
      fx(); fx = {};
    }};
    std::thread async_y = std::thread{[&] {
      std::this_thread::sleep_for(std::chrono::milliseconds{20});
      fy(); fy= {};
    }};
    blocking.wait();
    EXPECT_EQ(1, x);
    EXPECT_EQ(2, y);
    async_x.join();
    async_y.join();
  }
}

struct movable_only {
  movable_only(std::atomic<int>& x) : x(x) {}

  movable_only(const movable_only&) = delete;
  movable_only& operator=(const movable_only&) = delete;

  movable_only(movable_only&&) = default;
  movable_only& operator=(movable_only&&) = default;

  void operator()() { x = 1; }

  std::atomic<int>& x;
};

TEST(BlockingGroupTest, MovableOnly) {
  blocking_group blocking;
  std::atomic<int> x{0};
  auto wrapped = blocking.wrap(movable_only{x});
  std::unique_ptr<decltype(wrapped)> wrapped_ptr{
      new decltype(wrapped) {std::move(wrapped)}};
  std::thread async{[&] {
    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    (*wrapped_ptr)(); wrapped_ptr.reset();
  }};
  blocking.wait();
  EXPECT_EQ(1, x);
  async.join();
}

}  // namespace
}  // namespace testing
}  // namespace riak
