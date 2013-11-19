#include "client.hpp"

#include "connection.hpp"
#include "connection_pool.hpp"

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace riak {

client::client(const std::string& hostname, uint16_t port, uint64_t deadline_ms)
    : connection_{new connection_pool{hostname, port}},
      deadline_ms_{deadline_ms} {}

client::client(std::unique_ptr<connection> connection, uint64_t deadline_ms)
    : connection_{std::move(connection)}, deadline_ms_{deadline_ms} {}

void client::parse(pbc::RpbMessageCode code, const std::string& serialized,
                   google::protobuf::Message& message, std::error_code& error) {
  if (error) return;

  auto proto_begin = serialized.data() + 1;
  auto proto_size = serialized.size() - 1;
  if (serialized.empty()) {
    error = std::make_error_code(std::errc::io_error);
  } else if (serialized[0] == pbc::RpbMessageCode::ERROR_RESP) {
    pbc::RpbErrorResp response;
    if (!response.ParseFromArray(proto_begin, proto_size)) {
      error = std::make_error_code(std::errc::io_error);
    } else {
      // TODO(cristicbz): Do something else with the error message.
      DLOG << "RIAK ERROR: " << response.errmsg();
      error = std::make_error_code(std::errc::protocol_error);
    }
  } else if (serialized[0] != code ||
             !message.ParseFromArray(proto_begin, proto_size)) {
    error = std::make_error_code(std::errc::io_error);
  }
}

void client::send(pbc::RpbMessageCode code,
                  const google::protobuf::Message& message,
                  connection::response_handler handler) const {
  static const size_t min_message_size = 64;

  connection::request new_request;
  new_request.deadline_ms = deadline_ms_;
  new_request.on_response = std::move(handler);
  new_request.message.reserve(min_message_size);
  new_request.message.push_back(static_cast<char>(code));
  google::protobuf::io::StringOutputStream message_stream(&new_request.message);
  message.SerializeToZeroCopyStream(&message_stream);

  connection_->send_and_consume_request(new_request);
}

}  // namespace riak
