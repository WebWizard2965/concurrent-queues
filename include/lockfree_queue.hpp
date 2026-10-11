#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

// Bounded MPMC queue from Dmitry Vyukov (1024cores), adapted to std::atomic.
// Lock-free, not wait-free: a thread can spin while another thread is between
// winning a slot and publishing the sequence number.
// try_push returns false when the queue is full. There is no close(); shutdown
// in the tests is "producers push a known count, consumers pop that many."
//
// The sequence number is the synchronization edge:
//   producer publishes payload, then release-stores sequence = pos + 1
//   consumer acquire-loads sequence, then reads payload
// enqueue_pos / dequeue_pos can be relaxed because a stale position is retried.
template <typename T, bool PadIndices = true>
class LockFreeQueue {
    static_assert(std::is_nothrow_copy_constructible<T>::value, "T copy must not throw");
    static_assert(std::is_nothrow_move_assignable<T>::value, "T move-assign must not throw");

    struct Cell {
        std::atomic<size_t> sequence;
        T data;
    };

    template <bool Pad>
    struct alignas(Pad ? 64 : alignof(std::atomic<size_t>)) Index {
        std::atomic<size_t> value{0};
    };

public:
    static size_t require_capacity(size_t capacity) {
        if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
            throw std::invalid_argument("capacity must be a power of two and at least 2");
        }
        return capacity;
    }

    explicit LockFreeQueue(size_t capacity)
        : capacity_(require_capacity(capacity)),
          mask_(capacity_ - 1),
          cells_(new Cell[capacity_]) {
        for (size_t i = 0; i < capacity_; ++i) {
            cells_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    LockFreeQueue(const LockFreeQueue&) = delete;
    LockFreeQueue& operator=(const LockFreeQueue&) = delete;

    bool try_push(const T& item) {
        size_t pos = enqueue_pos_.value.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell = cells_[pos & mask_];
            const size_t seq = cell.sequence.load(std::memory_order_acquire);
            const intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            if (dif == 0) {
                if (enqueue_pos_.value.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed)) {
                    cell.data = item;
                    cell.sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
            } else if (dif < 0) {
                return false;
            } else {
                pos = enqueue_pos_.value.load(std::memory_order_relaxed);
            }
        }
    }

    bool try_pop(T& item) {
        size_t pos = dequeue_pos_.value.load(std::memory_order_relaxed);
        for (;;) {
            Cell& cell = cells_[pos & mask_];
            const size_t seq = cell.sequence.load(std::memory_order_acquire);
            const intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
            if (dif == 0) {
                if (dequeue_pos_.value.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed)) {
                    item = std::move(cell.data);
                    cell.sequence.store(pos + mask_ + 1, std::memory_order_release);
                    return true;
                }
            } else if (dif < 0) {
                return false;
            } else {
                pos = dequeue_pos_.value.load(std::memory_order_relaxed);
            }
        }
    }

    static constexpr bool padded() { return PadIndices; }

private:
    size_t capacity_;
    size_t mask_;
    std::unique_ptr<Cell[]> cells_;
    Index<PadIndices> enqueue_pos_;
    Index<PadIndices> dequeue_pos_;
};
