#include "bounded_queue.hpp"
#include "cpu_relax.hpp"
#include "lockfree_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#define CHECK(cond)                                                                 \
    do {                                                                            \
        if (!(cond)) {                                                              \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__ << " " #cond "\n"; \
            std::exit(1);                                                           \
        }                                                                           \
    } while (0)

#define CHECK_THROWS(expr)                                                             \
    do {                                                                               \
        bool threw__ = false;                                                         \
        try {                                                                          \
            expr;                                                                      \
        } catch (const std::invalid_argument&) {                                       \
            threw__ = true;                                                           \
        }                                                                              \
        if (!threw__) {                                                                \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__ << " expected throw\n"; \
            std::exit(1);                                                              \
        }                                                                              \
    } while (0)

namespace {

void start_watchdog(std::chrono::seconds limit) {
    std::thread([limit] {
        std::this_thread::sleep_for(limit);
        std::cerr << "FAIL watchdog: test exceeded " << limit.count() << "s\n";
        std::_Exit(1);
    }).detach();
}

void test_mutex_api() {
    CHECK_THROWS(BoundedQueue<int>(0));
    BoundedQueue<int> queue(2);
    CHECK(queue.try_push(1));
    CHECK(queue.try_push(2));
    CHECK(!queue.try_push(3));
    int value = 0;
    CHECK(queue.try_pop(value) && value == 1);
    CHECK(queue.try_pop(value) && value == 2);
    CHECK(!queue.try_pop(value));
    CHECK(queue.push(7));
    CHECK(queue.pop(value) && value == 7);
}

void test_blocking_fifo() {
    BoundedQueue<int> queue(1);
    constexpr int kN = 2000;
    std::thread producer([&] {
        for (int i = 0; i < kN; ++i) {
            CHECK(queue.push(i));
        }
    });
    for (int i = 0; i < kN; ++i) {
        int value = -1;
        CHECK(queue.pop(value));
        CHECK(value == i);
    }
    producer.join();
}

void test_close_while_empty() {
    BoundedQueue<int> queue(2);
    std::atomic<bool> entered{false};
    std::atomic<bool> returned_false{false};
    std::thread consumer([&] {
        entered.store(true);
        int value = 0;
        returned_false.store(!queue.pop(value));
    });
    while (!entered.load()) {
        cpu_relax();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    queue.close();
    consumer.join();
    CHECK(returned_false.load());
}

void test_close_while_full() {
    BoundedQueue<int> queue(2);
    CHECK(queue.push(10));
    CHECK(queue.push(11));
    std::atomic<bool> entered{false};
    std::atomic<bool> push_rejected{false};
    std::thread producer([&] {
        entered.store(true);
        push_rejected.store(!queue.push(12));
    });
    while (!entered.load()) {
        cpu_relax();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    queue.close();
    producer.join();
    CHECK(push_rejected.load());
    int value = 0;
    CHECK(queue.pop(value) && value == 10);
    CHECK(queue.pop(value) && value == 11);
    CHECK(!queue.pop(value));
}

template <typename Queue>
void test_fifo_try(Queue& queue, int n) {
    std::thread producer([&] {
        for (int i = 0; i < n; ++i) {
            while (!queue.try_push(i)) {
                cpu_relax();
            }
        }
    });
    for (int i = 0; i < n; ++i) {
        int value = -1;
        while (!queue.try_pop(value)) {
            cpu_relax();
        }
        CHECK(value == i);
    }
    producer.join();
}

template <typename Queue>
void test_unique_try(Queue& queue, int producers, int consumers, int per_producer) {
    const int n = producers * per_producer;
    auto seen = std::make_unique<std::atomic<int>[]>(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) {
        seen[static_cast<size_t>(i)].store(0, std::memory_order_relaxed);
    }

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(producers + consumers));
    for (int producer = 0; producer < producers; ++producer) {
        threads.emplace_back([&, producer] {
            const int begin = producer * per_producer;
            for (int i = 0; i < per_producer; ++i) {
                while (!queue.try_push(begin + i)) {
                    cpu_relax();
                }
            }
        });
    }

    std::atomic<int> left{n};
    for (int consumer = 0; consumer < consumers; ++consumer) {
        threads.emplace_back([&] {
            while (left.load(std::memory_order_acquire) > 0) {
                int value = -1;
                if (!queue.try_pop(value)) {
                    cpu_relax();
                    continue;
                }
                CHECK(value >= 0 && value < n);
                seen[static_cast<size_t>(value)].fetch_add(1, std::memory_order_relaxed);
                left.fetch_sub(1, std::memory_order_acq_rel);
            }
        });
    }

    for (std::thread& thread : threads) {
        thread.join();
    }
    CHECK(left.load() == 0);
    for (int i = 0; i < n; ++i) {
        CHECK(seen[static_cast<size_t>(i)].load() == 1);
    }
}

void test_lockfree_capacity() {
    CHECK_THROWS(LockFreeQueue<int>(1));
    CHECK_THROWS(LockFreeQueue<int>(3));
    LockFreeQueue<int> queue(2);
    CHECK(queue.try_push(1));
    CHECK(queue.try_push(2));
    CHECK(!queue.try_push(3));
    int value = 0;
    CHECK(queue.try_pop(value) && value == 1);
    CHECK(queue.try_push(4));
    CHECK(queue.try_pop(value) && value == 2);
    CHECK(queue.try_pop(value) && value == 4);
    CHECK(!queue.try_pop(value));
}

}  // namespace

int main() {
    start_watchdog(std::chrono::seconds(45));

    test_mutex_api();
    test_blocking_fifo();
    test_close_while_empty();
    test_close_while_full();

    {
        BoundedQueue<int> queue(8);
        test_fifo_try(queue, 1000);
    }
    {
        BoundedQueue<int> queue(8);
        test_unique_try(queue, 4, 4, 4000);
    }
    {
        BoundedQueue<int> queue(1);
        test_unique_try(queue, 2, 2, 1500);
    }

    test_lockfree_capacity();
    {
        LockFreeQueue<int, true> queue(8);
        test_fifo_try(queue, 1000);
    }
    {
        LockFreeQueue<int, true> queue(2);
        test_unique_try(queue, 4, 4, 3000);
    }
    {
        LockFreeQueue<int, false> queue(1024);
        test_unique_try(queue, 8, 8, 2000);
    }

    std::cout << "PASS queue_stress\n";
    return 0;
}
