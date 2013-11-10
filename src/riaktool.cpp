#include "debug_log.hpp"
#include "connection_pool.hpp"

#include <boost/asio/io_service.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/program_options.hpp>

namespace {
void wait_on_signal() {
  boost::asio::io_service signal_io_service;
  boost::asio::io_service::work work{signal_io_service};
  boost::asio::signal_set signals(signal_io_service, SIGINT, SIGTERM);
  signals.async_wait([&](...) { signal_io_service.stop(); });
  signal_io_service.run();
}

template<class T, class U>
inline const T max(T a, U b) { return a < b ? b : a; }
}  // namespace

int main(int argc, char *argv[]) {
  using namespace riak;
  namespace po = boost::program_options;

  // Parse arguments
  std::string hostname;
  uint16_t port, num_threads, num_sockets, highwatermark;
  uint32_t num_messages;
  po::options_description description{
    "Sends a lot of get_object requests to a Riak node using a connection pool."
  };
  description.add_options()
      ("help,h", "prints this help message")
      ("hostname,n",
       po::value<std::string>(&hostname)->default_value("localhost"),
       "hostname of Riak node")
      ("port,p",
       po::value<uint16_t>(&port)->default_value(10017),
       "port to connect on Riak node")
      ("num-threads,t",
       po::value<uint16_t>(&num_threads)->default_value(2),
       "number of I/O threads")
      ("num-sockets,s",
       po::value<uint16_t>(&num_sockets)->default_value(128),
       "number of sockets in pool")
      ("highwatermark,h",
       po::value<uint16_t>(&highwatermark)->default_value(1024),
       "max buffered requests")
      ("nmsgs,m",
       po::value<uint32_t>(&num_messages)->default_value(1000),
       "number of messages to send to the node");
  po::variables_map variables;
  try {
    po::store(po::parse_command_line(argc, argv, description), variables);
    po::notify(variables);
  } catch (const std::exception& e) {
    DLOG << e.what();
  }
  if (variables.count("help")) {
    std::cerr << description << std::endl;
    return -1;
  }


  // Simple connection_pool usage:
  //   riak::connection_pool conn(hostname, port, num_threads, num_sockets,
  //                              highwatermark);
  //   conn.send(string_message1, deadline_ms, handler);
  //   conn.send(string_message2, deadline_ms, handler);
  //   etc.
  //   
  // What follows is a mess because this is throwaway code.

  std::mutex num_sent_mutex;
  uint32_t num_sent = 0;
  auto start_clock = std::chrono::high_resolution_clock::now();
  auto first_response_clock = start_clock;
  
  std::string message{"\x09\x0A\01\x62\x12\x01\x6B", 7};
  DLOG << "Creating connection pool...";
  riak::connection_pool conn(hostname, port, num_threads, num_sockets,
                             highwatermark);

  DLOG << "Buffering messages...";
  for (int i = 0 ; i < num_messages ; ++ i) {
    conn.send(message, num_messages,
              [&](const std::string & response, std::error_code error) {
      std::lock_guard<std::mutex> lock{num_sent_mutex};
      ++num_sent;
      if (num_sent == 1) {
        first_response_clock = std::chrono::high_resolution_clock::now();
        DLOG << error.message() << " [first message took "
             << (std::chrono::duration_cast<std::chrono::milliseconds>(
                     first_response_clock - start_clock).count() /
                 1000.0) << " secs].";
      } else if (num_sent % max(1, num_messages / 20) == 0 ||
                 num_sent == num_messages - 1) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - first_response_clock);
        DLOG << error.message() << " [sent " << num_sent << " at "
             << (num_sent / (double(duration.count()) / 1000.0))
             << " messages/sec]";
      }
    });
    if (i % max(1, num_messages / 20) == 0)
      DLOG << "Buffered " << i << " messages.";
  }
  DLOG << "Buffered all the messages. Waiting on signal...";

  wait_on_signal();
  DLOG << "Signal caught.";

  return 0;
}
