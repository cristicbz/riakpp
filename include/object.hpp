#ifndef RIAKPP_OBJECT_HPP_
#define RIAKPP_OBJECT_HPP_

#include "check.hpp"
#include "riak_kv.pb.h"

#include <chrono>
#include <string>

namespace riak {

class object {
 public:
  typedef pbc::RpbContent content;
  typedef google::protobuf::RepeatedPtrField<content> sibling_vector;

  object() : valid_{false} {}

  inline object(std::string bucket, std::string key);

  inline object(object&& other);
  inline object(const object& other) = default;

  inline object& operator=(object&& other);
  inline object& operator=(const object& other) = default;

  inline const std::string& bucket() const;
  inline const std::string& key() const;

  inline std::string& value() { return *raw_content().mutable_value(); }
  inline const std::string& value() const { return raw_content().value(); }

  inline content& raw_content();
  inline const content& raw_content() const;

  inline const content& sibling(size_t index) const;
  inline const sibling_vector& siblings() const;

  inline void resolve_with_sibling(size_t sibling_index);
  inline void resolve_with_sibling(
      sibling_vector::const_iterator sibling_iterator);

  inline void resolve_with(const content& new_content);
  inline void resolve_with(content&& new_content);

  bool valid() const { return valid_; }
  bool exists() const { check_no_conflict(); return exists_; }
  bool in_conflict() const { check_valid(); return siblings_.size() > 1; }

  const std::string& vclock() const { return vclock_; }

  inline object(std::string bucket, std::string key, std::string vclock,
                sibling_vector&& initial_siblings);

 private:
  friend class client;

  inline void check_valid() const;
  inline void check_no_conflict() const;
  inline void ensure_one_valid_sibling();
  inline void ensure_valid_content();

  sibling_vector siblings_;
  std::string bucket_, key_, vclock_;
  bool valid_ = true, exists_ = false;
};

object::object(std::string bucket, std::string key)
    : bucket_{std::move(bucket)}, key_{std::move(key)} {
  ensure_one_valid_sibling();
}

object::object(object&& other)
    : bucket_{std::move(other.bucket_)},
      key_{std::move(other.key_)},
      vclock_{std::move(other.vclock_)},
      valid_{other.valid_},
      exists_{other.exists_} {
  siblings_.Swap(&other.siblings_);
}

const std::string& object::bucket() const {
  check_valid();
  return bucket_;
}

const std::string& object::key() const {
  check_valid();
  return key_;
}

pbc::RpbContent& object::raw_content() {
  check_no_conflict();
  return *siblings_.Mutable(0);
}

const pbc::RpbContent& object::raw_content() const {
  check_no_conflict();
  return siblings_.Get(0);
}

const object::content& object::sibling(size_t index) const {
  check_valid();
  RIAKPP_CHECK_LT(index, siblings_.size());
  return siblings_.Get(index);
}

const object::sibling_vector& object::siblings() const {
  check_valid();
  return siblings_;
}

void object::resolve_with_sibling(size_t sibling_index) {
  check_valid();
  RIAKPP_CHECK_LT(sibling_index, siblings_.size());

  // Swap the desired sibling with the last one.
  std::swap(*(siblings_.pointer_begin() + sibling_index),
            *(siblings_.pointer_end() - 1));

  // Create a clean RepeatedPtrField containing only the desired element.
  sibling_vector new_vector;
  new_vector.AddAllocated(siblings_.ReleaseLast());
  siblings_.Swap(&new_vector);

  ensure_valid_content();
}

void object::resolve_with_sibling(
    sibling_vector::const_iterator sibling_iterator) {
  RIAKPP_CHECK(sibling_iterator >= siblings_.begin());
  RIAKPP_CHECK(sibling_iterator < siblings_.end());
  resolve_with_sibling(
      static_cast<size_t>(sibling_iterator - siblings_.begin()));
}

void object::resolve_with(const content& new_content) {
  check_valid();
  sibling_vector new_vector;
  new_vector.Add()->CopyFrom(new_content);
  siblings_.Swap(&new_vector);
  ensure_valid_content();
}

void object::resolve_with(content&& new_content) {
  check_valid();
  sibling_vector new_vector;
  new_vector.Add()->Swap(&new_content);
  siblings_.Swap(&new_vector);
  ensure_valid_content();
}

object& object::operator=(object&& other) {
  siblings_.Swap(&other.siblings_);
  bucket_ = std::move(other.bucket_);
  key_ = std::move(other.key_);
  vclock_ = std::move(other.vclock_);
  valid_ = other.valid_;
  exists_ = other.exists_;
  return *this;
}

object::object(std::string bucket, std::string key, std::string vclock,
               sibling_vector&& initial_siblings)
    : bucket_{std::move(bucket)},
      key_{std::move(key)},
      vclock_{std::move(vclock)} {
  exists_ = !vclock_.empty();
  siblings_.Swap(&initial_siblings);
  ensure_one_valid_sibling();
}

void object::check_valid() const {
  RIAKPP_CHECK(valid_)
      << "Invalid/unitialised riak::object used. Maybe you forgot to "
         "check an error code in a handler?";
}

void object::check_no_conflict() const {
  check_valid();
  RIAKPP_CHECK(!in_conflict())
      << "Cannot access conflicted object with bucket = '" << bucket_
      << "' and key ='" << key_ << "'. There are " << siblings_.size()
      << " siblings.";
}

void object::ensure_one_valid_sibling() {
  if (siblings_.size() == 0) {
    siblings_.Add();
    *siblings_.Mutable(0)->mutable_value() = {};
    exists_ = false;
  } else if (siblings_.size() == 1) {
    ensure_valid_content();
  }
}

void object::ensure_valid_content() {
  RIAKPP_CHECK_EQ(siblings_.size(), 1);
  auto& content = raw_content();
  if (!content.has_value()) *content.mutable_value() = {};
  if (content.deleted()) {
    exists_ = false;
    content.set_deleted(false);
  }
}

}  // namespace riak

#endif  // #ifndef RIAKPP_OBJECT_HPP_
