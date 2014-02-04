#include "object.hpp"

#include <gtest/gtest.h>

#include <iostream>
#include <sstream>

namespace riak {
bool operator==(const object& a, const object& b) {
  if (a.valid() != b.valid())  return false;
  if (a.bucket() != b.bucket())  return false;
  if (a.key() != b.key())  return false;
  if (a.siblings().size() != b.siblings().size())  return false;

  for (size_t i_sibling = 0; i_sibling < a.siblings().size(); ++i_sibling) {
    if (a.sibling(i_sibling).SerializeAsString() !=
        b.sibling(i_sibling).SerializeAsString()) {
      return false;
    }
  }

  if (!a.in_conflict() && a.exists() != b.exists()) return false;
  return a.bucket() == b.bucket() && a.key() == b.key() &&
         a.vclock() == b.vclock();
}

bool operator!=(const object &a, const object& b) { return !(a == b); }

std::ostream& operator<<(std::ostream& os, const object& o) {
  if (!o.valid()) return os << "<invalid-object>";
  std::stringstream ss;
  ss << "object { bucket: '" << o.bucket() << "', key: '" << o.key()
     << "', exists: " << o.exists() << ", in_conflict: " << o.in_conflict()
     << " vclock: '" << o.vclock() << "', values: [";
  for (const auto& sibling : o.siblings())
    ss << "'" << sibling.value() << "', ";
  return os << ss.str().substr(0, ss.str().size() - 2) << "] }";
}

namespace {
object make_object(std::string bucket, std::string key, std::string vclock,
                   std::vector<std::string> sibling_values) {
  object::sibling_vector siblings;
  siblings.Reserve(sibling_values.size());
  for (auto& value : sibling_values) {
    *siblings.Add()->mutable_value() = std::move(value);
  }

  return {std::move(bucket), std::move(key),
          std::move(vclock), std::move(siblings)};
}

TEST(ObjectTest, ValidityConditions) {
  object o1{{}, {}};
  EXPECT_TRUE(o1.valid());
  o1.valid(false);
  object o2 = o1;
  object o3{"b", "k"};
  o3 = o1;

  EXPECT_FALSE(o1.valid());
  EXPECT_FALSE(o2.valid());
  EXPECT_FALSE(o3.valid());

  object o4 = std::move(o1);
  object o5{"b", "k"};
  o5 = std::move(o2);
  EXPECT_FALSE(o4.valid());
  EXPECT_FALSE(o5.valid());

  object p1{"b", "k"};
  object p2 = make_object("b", "k", "123", {});
  object p3 = p1;
  EXPECT_TRUE(p3.valid());
  object p4{{}, {}};
  p4.valid(false);
  p4 = p2;
  EXPECT_TRUE(p1.valid());
  EXPECT_TRUE(p2.valid());
  EXPECT_TRUE(p3.valid());
  EXPECT_TRUE(p4.valid());

  object p5 = std::move(p1);
  object p6{{}, {}};
  p6.valid(false);
  p6 = std::move(p2);
  EXPECT_TRUE(p5.valid());
  EXPECT_TRUE(p6.valid());
}

TEST(ObjectTest, CopyMoveAndAssign) {
  const object o1 = make_object("b1", "k1", "c1", {"v11", "v12"});
  const object o2 = make_object("b2", "k2", "c2", {"v21"});

  ASSERT_TRUE(o1.valid());
  ASSERT_TRUE(o2.valid());

  object p1 = o1;
  object p2{{}, {}};
  p2 = o2;
  EXPECT_EQ(p1, o1);
  EXPECT_EQ(p2, o2);

  object p3 = std::move(p1);
  object p4{{}, {}};
  p4 = std::move(p2);
  EXPECT_EQ(p3, o1);
  EXPECT_EQ(p4, o2);

  p3 = o2;
  EXPECT_EQ(p3, o2);
  EXPECT_EQ(p4, o2);

}

TEST(ObjectTest, NewObject) {
  const object expected_object = make_object("b", "k", "", {"v"});
  object new_object{"b", "k"};


  EXPECT_EQ(new_object.siblings().size(), 1);
  ASSERT_NE(new_object, expected_object);

  new_object.value() = "v";
  EXPECT_EQ(new_object.siblings().size(), 1);
  EXPECT_EQ(new_object, expected_object);
}

TEST(ObjectTest, ContentAlwaysInitialized) {
  {
    object o{"b", "k"};
    EXPECT_TRUE(o.raw_content().IsInitialized());
  }
  {
    object::sibling_vector siblings;
    siblings.Add();
    siblings.Add();
    object o{"b", "k", "", std::move(siblings)};
    o.resolve_with_sibling(1);
    EXPECT_TRUE(o.raw_content().IsInitialized());
  }
  {
    object::sibling_vector siblings;
    object o{"b", "k", "", std::move(siblings)};
    EXPECT_TRUE(o.raw_content().IsInitialized());
  }
}

TEST(ObjectTest, TombstoneObject) {
  object tombstone = make_object("b", "k", "c", {});  // has vclock but no value
  EXPECT_FALSE(tombstone.in_conflict());
  EXPECT_FALSE(tombstone.exists());

  tombstone.resolve_with_sibling(0);

  EXPECT_FALSE(tombstone.in_conflict());
  EXPECT_FALSE(tombstone.exists());
}

TEST(ObjectTest, SiblingResolution) {
  const object conflicted = make_object("b", "k", "c", {"t", "u", "v"});
  const object expected_object[] = {make_object("b", "k", "c", {"t"}),
                                    make_object("b", "k", "c", {"u"}),
                                    make_object("b", "k", "c", {"v"})};
  ASSERT_FALSE(expected_object[0].in_conflict());
  ASSERT_FALSE(expected_object[1].in_conflict());
  ASSERT_FALSE(expected_object[2].in_conflict());
  ASSERT_TRUE(conflicted.in_conflict());

  // Resolve with either one of the siblings.
  for (int i = 0; i < 3; ++i) {
    object o = conflicted;
    o.resolve_with_sibling(i);
    EXPECT_FALSE(o.in_conflict());
    EXPECT_EQ(o, expected_object[i]) << i;
  }

  for (int i = 0; i < 3; ++i) {
    object o = conflicted;
    o.resolve_with_sibling(o.siblings().begin() + i);
    EXPECT_FALSE(o.in_conflict());
    EXPECT_EQ(o, expected_object[i]) << i;
  }

  for (int i = 0; i < 3; ++i) {
    object o = conflicted;
    o.resolve_with(expected_object[i].raw_content());
    EXPECT_FALSE(o.in_conflict());
    EXPECT_EQ(o, expected_object[i]) << i;
  }

  for (int i = 0; i < 3; ++i) {
    object o = conflicted;
    object::content c = expected_object[i].raw_content();
    o.resolve_with(std::move(c));
    EXPECT_FALSE(o.in_conflict());
    EXPECT_EQ(o, expected_object[i]) << i;
  }

  { // Try resolving twice.
    object o = conflicted;
    o.resolve_with_sibling(1);
    o.resolve_with_sibling(0);
    EXPECT_EQ(o, expected_object[1]);
  }

  { // Try resolving with new content.
    object o = conflicted;
    object::content c;
    c.set_value("new");
    c.set_content_type("some/mime/type");
    o.resolve_with(std::move(c));
    EXPECT_EQ(o.value(), "new");
    EXPECT_EQ(o.raw_content().content_type(), "some/mime/type");
  }
}

TEST(ObjectDeathTest, DieIfInvalid) {
  object inv{{}, {}};
  inv.valid(false);
  const object cinv{inv};
  object::content content;

  ASSERT_FALSE(inv.valid());
  ASSERT_FALSE(cinv.valid());
  EXPECT_EQ(inv.bucket(), "");
  EXPECT_EQ(inv.key(), "");
  EXPECT_DEATH({ inv.value(); }, "");
  EXPECT_DEATH({ cinv.value(); }, "");
  EXPECT_DEATH({ inv.raw_content(); }, "");
  EXPECT_DEATH({ cinv.raw_content(); }, "");
  EXPECT_DEATH({ inv.sibling(0); }, "");
  EXPECT_DEATH({ inv.siblings(); }, "");
  EXPECT_DEATH({ inv.resolve_with_sibling(0); }, "");
  EXPECT_DEATH({ inv.resolve_with_sibling(inv.siblings().begin()); }, "");
  EXPECT_DEATH({ inv.resolve_with(content); }, "");
  EXPECT_DEATH({ inv.resolve_with(std::move(content)); }, "");
  EXPECT_DEATH({ inv.in_conflict(); }, "");
  EXPECT_DEATH({ inv.exists(); }, "");

  object valid{"b", "k"};
  const object cvalid{"b", "k"};
  ASSERT_TRUE(valid.valid());
  ASSERT_TRUE(cvalid.valid());
  valid.bucket();
  valid.key();
  valid.value();
  cvalid.value();
  valid.raw_content();
  cvalid.raw_content();
  valid.sibling(0);
  valid.siblings();
  valid.resolve_with_sibling(0);
  valid.resolve_with_sibling(valid.siblings().begin());
  valid.resolve_with(content);
  valid.resolve_with(std::move(content));
  valid.exists();
  valid.in_conflict();
}

TEST(ObjectDeathTest, DieIfInConflict) {
  auto conflicted = make_object("a", "b", "x", {"1", "2", "3"});

  ASSERT_TRUE(conflicted.valid());
  ASSERT_TRUE(conflicted.in_conflict());

  // These calls should be OK.
  conflicted.bucket();
  conflicted.valid();
  conflicted.key();

  // While these should die.
  EXPECT_DEATH({ conflicted.value(); }, "");
  EXPECT_DEATH({ conflicted.raw_content(); }, "");
}

}  // namespace
}  // namespace riak
