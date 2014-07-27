#include "client.hpp"

#include "connection_pool.hpp"
#include "debug_log.hpp"
#include "length_framed_connection.hpp"
#include "thread_pool.hpp"

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace riak {

client::client(const std::string& hostname, uint16_t port,
               sibling_resolver resolver, connection_options options)
    : threads_{new thread_pool{options.num_worker_threads()}},
      connection_{new connection{
          threads_->io_service(), hostname, port, options.max_sockets(),
          options.highwatermark(), options.connection_timeout_ms()}},
      io_service_{&threads_->io_service()},
      resolver_{std::move(resolver)},
      deadline_ms_{options.deadline_ms()} {}

client::client(boost::asio::io_service& io_service, const std::string& hostname,
               uint16_t port, sibling_resolver resolver,
               connection_options options)
    : connection_{new connection{
          io_service, hostname, port, options.max_sockets(),
          options.highwatermark(), options.connection_timeout_ms()}},
      io_service_{&io_service},
      resolver_{std::move(resolver)},
      deadline_ms_{options.deadline_ms()} {
  RIAKPP_CHECK(options.defaulted_num_worker_threads())
      << "When using an external io_service, no threads are spawned so the "
         "number of threads cannot be specified.";
}

client::~client() {}

void client::managed_run() {
  RIAKPP_CHECK(manages_io_service())
      << "managed_run() called on client with unmanaged io_service";
  io_service_->run();
}

void client::managed_stop() {
  RIAKPP_CHECK(manages_io_service())
      << "managed_stop() called on client with unmanaged io_service";
  io_service_->stop();
}

store_resolved_sibling client::pass_through_resolver(object& conflicted) {
  return store_resolved_sibling::no;
}

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
      RIAKPP_DLOG << "RIAK ERROR: " << response.errmsg();
      error = std::make_error_code(std::errc::protocol_error);
    }
  } else if (serialized[0] != code ||
             !message.ParseFromArray(proto_begin, proto_size)) {
    error = std::make_error_code(std::errc::io_error);
  }
}

void client::send(pbc::RpbMessageCode code,
                  const google::protobuf::Message& message,
                  connection::handler_type handler) const {
  static const size_t min_message_size = 64;

  connection::request_type new_request;
  new_request.deadline_ms = deadline_ms_;
  new_request.payload.reserve(min_message_size);
  new_request.payload.push_back(static_cast<char>(code));
  google::protobuf::io::StringOutputStream message_stream(&new_request.payload);
  message.SerializeToZeroCopyStream(&message_stream);

  connection_->async_send(new_request, std::move(handler));
}

}  // namespace riak
