include_directories(SYSTEM ".")
add_executable(
  store_fetch
    store_fetch.cpp
)
target_link_libraries(
  store_fetch
    riakpp
)
