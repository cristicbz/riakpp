#include "object.hpp"

#include <gtest/gtest.h>

namespace riak {
namespace {

TEST(ObjectTest, EmptyIsInvalid) {
  object o;
  EXPECT_FALSE(o.valid());
}

TEST(ObjectTest, DieIfInvalid) {
  object o;
  const object co;
  object::content c;

  EXPECT_DEATH({ o.bucket(); }, "");
  EXPECT_DEATH({ o.key(); }, "");
  EXPECT_DEATH({ o.value(); }, "");
  EXPECT_DEATH({ co.value(); }, "");
  EXPECT_DEATH({ o.raw_content(); }, "");
  EXPECT_DEATH({ co.raw_content(); }, "");
  EXPECT_DEATH({ o.sibling(0); }, "");
  EXPECT_DEATH({ o.siblings(); }, "");
}

}  // namespace
}  // namespace riak
