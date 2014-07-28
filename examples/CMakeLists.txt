include_directories(SYSTEM ".")

function(riakpp_example name)
  add_executable(${name} ${name}.cpp)
  target_link_libraries(${name} riakpp)
endfunction()

riakpp_example(1_store)
riakpp_example(2_store_io_service)
riakpp_example(3_store_fetch_remove)
riakpp_example(4_blocking_groups)
riakpp_example(5_completion_groups)
