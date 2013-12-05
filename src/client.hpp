#ifndef RIAKPP_CLIENT_HPP_
#define RIAKPP_CLIENT_HPP_

#include "object.hpp"
#include "riak_kv.pb.h"

#include <cstdint>
#include <memory>
#include <string>
#include <system_error>

namespace riak {

class connection;

enum class store_resolved_sibling {
  no = 0,
  yes = 1
};

class client {
 public:
  typedef std::function<store_resolved_sibling(riak::object&)> sibling_resolver;

  client(const std::string& hostname, uint16_t port,
         sibling_resolver resolver = &pass_through_resolver,
         uint64_t deadline_ms = 3000);

  client(std::unique_ptr<connection> connection,
         sibling_resolver resolver = &pass_through_resolver,
         uint64_t deadline_ms = 3000);

  template <class Handler>
  void fetch(std::string bucket, std::string key, Handler handler) const;

  template <class Handler>
  void store(std::string bucket, std::string key, std::string value,
             Handler handler) const;

  template <class Handler>
  void store(riak::object object, Handler handler) const;

  template <class Handler>
  void remove(std::string bucket, std::string key, Handler handler) const;

  template <class Handler>
  void remove(riak::object object, Handler handler) const;

  static store_resolved_sibling pass_through_resolver(riak::object& conflicted);

 private:
  void send(pbc::RpbMessageCode code, const google::protobuf::Message& message,
            std::function<void(std::string&, std::error_code&)> handler) const;

  static void parse(pbc::RpbMessageCode code, const std::string& serialized,
                    google::protobuf::Message& message, std::error_code& error);

  template <class Handler>
  void fetch_wrapper(Handler& handler, std::string& bucket, std::string& key,
                     const std::string& serialized,
                     std::error_code& error) const;

  template <class Handler>
  static void store_wrapper(Handler& handler, const std::string& serialized,
                            std::error_code& error);

  template <class Handler>
  static void store_resolution_wrapper(Handler& handler, riak::object& object,
                                     const std::string& serialized,
                                     std::error_code& error);

  template <class Handler>
  static void remove_wrapper(Handler& handler, const std::string& serialized,
                             std::error_code& error);

  const std::unique_ptr<connection> connection_;
  const sibling_resolver resolver_;
  const uint64_t deadline_ms_;
};

template <class Handler>
void client::fetch(std::string bucket, std::string key, Handler handler) const {
  namespace ph = std::placeholders;
  pbc::RpbGetReq request;
  // TODO(cristicbz): These copies can be removed by reusing the strings after
  // serializing the request.
  *request.mutable_bucket() = bucket;
  *request.mutable_key() = key;
  request.set_deletedvclock(true);
  request.set_timeout(deadline_ms_);

  send(pbc::RpbMessageCode::GET_REQ, request,
       std::bind(&client::fetch_wrapper<Handler>, this, std::move(handler),
                 std::move(bucket), std::move(key), ph::_1, ph::_2));
}

template <class Handler>
void client::store(std::string bucket, std::string key, std::string value,
                   Handler handler) const {
  namespace ph = std::placeholders;
  pbc::RpbPutReq request;
  request.mutable_bucket()->swap(bucket);
  request.mutable_key()->swap(key);
  request.mutable_content()->mutable_value()->swap(value);
  request.set_timeout(deadline_ms_);
  send(pbc::RpbMessageCode::PUT_REQ, request,
       std::bind(&store_wrapper<Handler>, std::move(handler), ph::_1, ph::_2));
}

template <class Handler>
void client::store(riak::object object, Handler handler) const {
  namespace ph = std::placeholders;
  pbc::RpbPutReq request;
  request.mutable_bucket()->swap(object.bucket_);
  request.mutable_key()->swap(object.key_);
  request.mutable_vclock()->swap(object.vclock_);
  request.mutable_content()->Swap(&object.raw_content());
  request.mutable_content()->clear_deleted();
  request.mutable_content()->clear_last_mod();
  request.mutable_content()->clear_last_mod_usecs();
  request.set_timeout(deadline_ms_);
  send(pbc::RpbMessageCode::PUT_REQ, request,
       std::bind(&store_wrapper<Handler>, std::move(handler), ph::_1, ph::_2));
}

template <class Handler>
void client::remove(riak::object object, Handler handler) const {
  namespace ph = std::placeholders;
  pbc::RpbDelReq request;
  *request.mutable_bucket() = std::move(object.bucket_);
  *request.mutable_key() = std::move(object.key_);
  *request.mutable_vclock() = std::move(object.vclock_);
  send(pbc::RpbMessageCode::DEL_REQ, request,
       std::bind(&remove_wrapper<Handler>, std::move(handler), ph::_1, ph::_2));
}

template <class Handler>
void client::remove(std::string bucket, std::string key,
                    Handler handler) const {
  namespace ph = std::placeholders;
  pbc::RpbDelReq request;
  *request.mutable_bucket() = std::move(bucket);
  *request.mutable_key() = std::move(key);
  send(pbc::RpbMessageCode::DEL_REQ, request,
       std::bind(&remove_wrapper<Handler>, std::move(handler), ph::_1, ph::_2));
}

template <class Handler>
void client::fetch_wrapper(Handler& handler, std::string& bucket,
                           std::string& key, const std::string& serialized,
                           std::error_code& error) const {
  namespace ph = std::placeholders;
  pbc::RpbGetResp response;
  object fetched;

  parse(pbc::RpbMessageCode::GET_RESP, serialized, response, error);
  if (!error) {
    if (response.vclock().empty()) {
      fetched = object{std::move(bucket), std::move(key)};
    } else {
      fetched = object{std::move(bucket),
                       std::move(key),
                       std::move(*response.mutable_vclock()),
                       std::move(*response.mutable_content())};

      if (fetched.in_conflict() &&
          resolver_(fetched) == store_resolved_sibling::yes) {
        pbc::RpbPutReq put_request;
        *put_request.mutable_bucket() = fetched.bucket();
        *put_request.mutable_key() = fetched.key();
        *put_request.mutable_vclock() = std::move(fetched.vclock_);
        put_request.mutable_content()->CopyFrom(fetched.raw_content());
        if (!fetched.exists()) {
          put_request.mutable_content()->set_deleted(true);
        }
        put_request.set_timeout(deadline_ms_);
        put_request.set_return_head(true);
        send(pbc::RpbMessageCode::PUT_REQ, put_request,
             std::bind(&store_resolution_wrapper<Handler>, std::move(handler),
                       std::move(fetched), ph::_1, ph::_2));
        return;
      }
    }
  }

  handler(std::move(fetched), std::move(error));
}

template <class Handler>
void client::store_wrapper(Handler& handler, const std::string& serialized,
                           std::error_code& error) {
  pbc::RpbPutResp response;
  parse(pbc::PUT_RESP, serialized, response, error);
  handler(std::move(error));
}

template <class Handler>
void client::store_resolution_wrapper(Handler& handler, riak::object& resolved,
                                      const std::string& serialized,
                                      std::error_code& error) {
  pbc::RpbPutResp response;
  parse(pbc::PUT_RESP, serialized, response, error);
  if (error) {
    resolved = {};
  } else if (response.vclock().empty() || response.content_size() > 1) {
    resolved = {};
    error = std::make_error_code(std::errc::resource_unavailable_try_again);
  } else {
    resolved.vclock_ = std::move(*response.mutable_vclock());
  }
  handler(std::move(resolved), std::move(error));
}


template <class Handler>
void client::remove_wrapper(Handler& handler, const std::string& serialized,
                            std::error_code& error) {
  pbc::RpbDelResp response;
  parse(pbc::DEL_RESP, serialized, response, error);
  handler(std::move(error));
}

}  // namespace riak

#endif  // #ifndef RIAKPP_CLIENT_HPP_
