#ifndef RIAKPP_BROKER_HPP_
#define RIAKPP_BROKER_HPP_

#include "blocking_queue.hpp"

#include <thread>
#include <functional>

namespace riak {

template <class Work>
class broker {
 public:
  typedef Work work_type;
  typedef std::function<void(Work&)> worker_function;

  broker(size_t max_work, size_t max_workers);
  ~broker();

  // TODO(cristicbz): Use try_pop to assign work immediatedly if available.
  void add_work(work_type work) { work_.push(std::move(work)); }
  void add_worker(worker_function worker) { workers_.push(std::move(worker)); }

  void stop();

 private:
  void assign_work_loop();

  blocking_queue<work_type> work_;
  blocking_queue<worker_function> workers_;
  std::thread thread_;
};

template <class W>
broker<W>::broker(size_t max_work, size_t max_workers)
    : work_{max_work},
      workers_{max_workers},
      thread_{std::bind(&broker::assign_work_loop, this)} {}

template <class W>
broker<W>::~broker() {
  work_.cancel();
  workers_.cancel();
  if (thread_.joinable()) thread_.join();
}

template <class W>
void broker<W>::assign_work_loop() {
  work_type work;
  worker_function worker;

  while (true) {
    if (!work_.pop(work)) break;
    if (!workers_.pop(worker)) break;
    
    worker(work);
  }
}
template <class W>
void broker<W>::stop() {
  work_.cancel();
  workers_.cancel();
  thread_.join();
}

}  // namespace riak

#endif  // #ifndef RIAKPP_BROKER_HPP_
