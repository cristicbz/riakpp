#ifndef RIAKPP_OBJECT_HPP_
#define RIAKPP_OBJECT_HPP_

#include "check.hpp"
#include "riak_kv.pb.h"

#include <string>

namespace riak {

class object {
 public:
  object() : valid_{false} {}

  inline object(std::string bucket, std::string key);

  inline object(object&& other);
  inline object(const object& other);

  inline object& operator=(object&& other);
  inline object& operator=(const object& other);

  const std::string& bucket() const { check(); return bucket_; }
  const std::string& key() const { check(); return key_; }

  std::string& value() { check(); return *content_.mutable_value(); }
  const std::string& value() const { check(); return content_.value(); }

  pbc::RpbContent& raw_content() { check(); return content_; }
  const pbc::RpbContent& raw_content() const { check(); return content_; }

  bool is_new() const { check(); return vclock_.empty(); }
  bool is_valid() const { return valid_; }

 private:
  friend class client;

  inline object(std::string bucket, std::string key, std::string vclock,
                pbc::RpbContent&& content);

  inline void check() const;

  pbc::RpbContent content_;
  std::string bucket_, key_, vclock_;
  bool valid_ = true;
};

object::object(std::string bucket, std::string key)
    : bucket_{std::move(bucket)}, key_{std::move(key)} {}

object::object(object&& other)
    : bucket_{std::move(other.bucket_)},
      key_{std::move(other.key_)},
      vclock_{std::move(other.vclock_)},
      valid_{other.valid_} {
  content_.Swap(&other.content_);
}

object::object(const object& other)
    : bucket_{other.bucket_},
      key_{other.key_},
      vclock_{other.vclock_},
      valid_{other.valid_} {
  content_.CopyFrom(other.content_);
}

object& object::operator=(object&& other) {
  content_.Swap(&other.content_);
  bucket_ = std::move(other.bucket_);
  key_ = std::move(other.key_);
  vclock_ = std::move(other.vclock_);
  valid_ = other.valid_;
  return *this;
}

object& object::operator=(const object& other) {
  bucket_ = other.bucket_;
  key_ = other.key_;
  vclock_ = other.vclock_;
  valid_ = other.valid_;
  content_.CopyFrom(other.content_);
  return *this;
}

object::object(std::string bucket, std::string key, std::string vclock,
               pbc::RpbContent&& content)
    : bucket_{std::move(bucket)},
      key_{std::move(key)},
      vclock_{std::move(vclock)} {
  content_.Swap(&content);
}

void object::check() const {
  RIAKPP_CHECK(valid_)
      << "Invalid/unitialised riak::object used. Maybe you forgot to "
         "check an error code in a handler?";
}

}  // namespace riak

#endif  // #ifndef RIAKPP_OBJECT_HPP_
