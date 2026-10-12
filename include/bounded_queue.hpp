#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <utility>

// Blocking bounded multi-producer, multi-consumer queue.
// push waits while full and returns false after close().
// pop waits while empty, drains items that were already queued, then returns false.
// Waits use predicates so a spurious wakeup cannot observe a broken state.
template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(size_t capacity) : capacity_(capacity) {
        if (capacity_ == 0) {
            throw std::invalid_argument("capacity must be > 0");
        }
    }

    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    bool push(const T& value) {
        std::unique_lock<std::mutex> lock(mu_);
        not_full_.wait(lock, [&] { return closed_ || queue_.size() < capacity_; });
        if (closed_) {
            return false;
        }
        queue_.push_back(value);
        not_empty_.notify_one();
        return true;
    }

    bool pop(T& result) {
        std::unique_lock<std::mutex> lock(mu_);
        not_empty_.wait(lock, [&] { return closed_ || !queue_.empty(); });
        if (queue_.empty()) {
            return false;
        }
        result = std::move(queue_.front());
        queue_.pop_front();
        not_full_.notify_one();
        return true;
    }

    void close() {
        std::lock_guard<std::mutex> lock(mu_);
        closed_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }

private:
    const size_t capacity_;
    std::mutex mu_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<T> queue_;
    bool closed_ = false;
};
