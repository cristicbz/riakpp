#ifndef RIAKPP_EXAMPLE_UTIL_HPP_
#define RIAKPP_EXAMPLE_UTIL_HPP_

#include <cstdlib>
#include <iostream>
#include <string>

inline std::string basename(const std::string& path) {
  auto last_slash = path.find_last_of("/\\");
  return last_slash == std::string::npos ? path : path.substr(last_slash + 1);
}

inline bool hostport_from_argv(uint16_t argc, char* argv[],
                               std::string& hostname, uint16_t& port) {
  std::string progname = basename(argv[0]);

  if (argc == 1) {
    hostname = "localhost";
    port = 8087;
  } else if (argc == 2) {
    auto hostport = std::string{argv[1]};
    auto colon_index = hostport.find(':');
    if (colon_index > 0) hostname = hostport.substr(0, colon_index);
    if (colon_index == std::string::npos) {
      port = 8087;
    } else {
      auto portstring = hostport.substr(colon_index + 1);
      port = atoi(portstring.c_str());
      if (port == 0) {
        std::cerr << progname << ": invalid port '" << portstring << "'.\n";
        return false;
      }
    }
  } else {
    std::cerr << "usage:     " << progname
              << " [hostname[:port]]    (default=" << hostname << ":" << port
              << ")\nerror: too many arguments" << std::endl;
    return false;
  }
  return true;
}


#endif  // #ifndef RIAKPP_EXAMPLE_UTIL_HPP_
