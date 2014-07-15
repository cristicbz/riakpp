#ifndef RIAKPP_BROKER_HPP_
#define RIAKPP_BROKER_HPP_

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>

namespace riak {

template <class Work>
class broker {
 public:
  using work_type = Work;
  using worker_function = std::function<void(Work&)>;

  inline broker(size_t max_work, size_t max_workers);

  bool closed() const { return closed_; }

  template <class WorkConvertible>
  inline void add_work(WorkConvertible&& work);

  template <class WorkerConvertible>
  inline void add_worker(WorkerConvertible&& worker);

  inline void close();

 private:
  // TODO(cristicbz): Use ring buffers instead of heavyweight std::queue.
  std::queue<work_type> work_;
  std::queue<worker_function> workers_;

  std::mutex queues_mutex_;
  std::condition_variable work_full_;
  std::condition_variable workers_full_;
  const size_t max_work_, max_workers_;
  bool closed_;
};

template <class Work>
broker<Work>::broker(size_t max_work, size_t max_workers)
    : max_work_{max_work}, max_workers_{max_workers}, closed_{false} {}

template <class Work>
template <class WorkConvertible>
void broker<Work>::add_work(WorkConvertible&& work) {
  std::unique_lock<std::mutex> lock{queues_mutex_};
  if (closed_) return;
  if (workers_.empty()) {
    while (work_.size() == max_work_) work_full_.wait(lock);
    work_.emplace(std::move(work));
  } else {
    bool should_signal = workers_.size() == max_workers_;
    worker_function worker = std::move(workers_.front());
    workers_.pop();
    lock.unlock();
    if (should_signal) workers_full_.notify_one();
    worker(work);
  }
}

template <class Work>
template <class WorkerConvertible>
void broker<Work>::add_worker(WorkerConvertible&& worker) {
  std::unique_lock<std::mutex> lock{queues_mutex_};
  if (closed_) return;
  if (work_.empty()) {
    while (workers_.size() == max_workers_) workers_full_.wait(lock);
    workers_.emplace(std::move(worker));
  } else {
    bool should_signal = work_.size() == max_work_;
    work_type work = std::move(work_.front());
    work_.pop();
    lock.unlock();
    if (should_signal) work_full_.notify_one();
    worker(work);
  }
}

template <class Work>
void broker<Work>::close() {
  std::lock_guard<std::mutex> lock{queues_mutex_};
  closed_ = true;
}

}  // namespace riak

#endif  // #ifndef RIAKPP_BROKER_HPP_
