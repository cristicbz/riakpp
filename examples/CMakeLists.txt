include_directories(SYSTEM ".")

function(riakpp_example name)
  add_executable(${name} ${name}.cpp)
  target_link_libraries(${name} riakpp)
endfunction()

riakpp_example(store_fetch)
riakpp_example(store_fetch_blocking)
riakpp_example(store_many)
