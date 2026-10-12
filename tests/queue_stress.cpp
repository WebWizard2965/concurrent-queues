#include "bounded_queue.hpp"

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

namespace {

void start_watchdog(std::chrono::seconds limit) {
    std::thread([limit] {
        std::this_thread::sleep_for(limit);
        std::cerr << "FAIL watchdog: test exceeded " << limit.count() << "s, probably a deadlock\n";
        std::_Exit(1);
    }).detach();
}

void test_zero_capacity_throws() {
    bool threw = false;
    try {
        BoundedQueue<int> queue(0);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    CHECK(threw);
}

void test_single_thread_order() {
    BoundedQueue<int> queue(3);
    CHECK(queue.push(1));
    CHECK(queue.push(2));
    CHECK(queue.push(3));
    int value = 0;
    CHECK(queue.pop(value) && value == 1);
    CHECK(queue.pop(value) && value == 2);
    CHECK(queue.pop(value) && value == 3);
}

// Capacity 1 forces the producer to wait on every push after the first.
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
    std::atomic<bool> returned_false{false};
    std::thread consumer([&] {
        int value = 0;
        returned_false.store(!queue.pop(value));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    queue.close();
    consumer.join();
    CHECK(returned_false.load());
}

void test_close_while_full() {
    BoundedQueue<int> queue(2);
    CHECK(queue.push(10));
    CHECK(queue.push(11));
    std::atomic<bool> push_rejected{false};
    std::thread producer([&] {
        push_rejected.store(!queue.push(12));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    queue.close();
    producer.join();
    CHECK(push_rejected.load());
    int value = 0;
    CHECK(queue.pop(value) && value == 10);
    CHECK(queue.pop(value) && value == 11);
    CHECK(!queue.pop(value));
}

// Every producer pushes its own range of ids. After all producers finish, close()
// lets the consumers drain and exit. Each id must be popped exactly once.
void test_each_item_once(size_t capacity, int producers, int consumers, int per_producer) {
    const int n = producers * per_producer;
    BoundedQueue<int> queue(capacity);
    auto seen = std::make_unique<std::atomic<int>[]>(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) {
        seen[static_cast<size_t>(i)].store(0);
    }

    std::vector<std::thread> consumer_threads;
    for (int c = 0; c < consumers; ++c) {
        consumer_threads.emplace_back([&] {
            int value = -1;
            while (queue.pop(value)) {
                CHECK(value >= 0 && value < n);
                seen[static_cast<size_t>(value)].fetch_add(1);
            }
        });
    }

    std::vector<std::thread> producer_threads;
    for (int p = 0; p < producers; ++p) {
        producer_threads.emplace_back([&, p] {
            const int begin = p * per_producer;
            for (int i = 0; i < per_producer; ++i) {
                CHECK(queue.push(begin + i));
            }
        });
    }

    for (std::thread& thread : producer_threads) {
        thread.join();
    }
    queue.close();
    for (std::thread& thread : consumer_threads) {
        thread.join();
    }

    for (int i = 0; i < n; ++i) {
        CHECK(seen[static_cast<size_t>(i)].load() == 1);
    }
    std::cout << "  " << producers << " producers, " << consumers << " consumers, capacity "
              << capacity << ": " << n << " items, each delivered once\n";
}

}  // namespace

int main() {
    start_watchdog(std::chrono::seconds(60));

    test_zero_capacity_throws();
    test_single_thread_order();
    test_blocking_fifo();
    test_close_while_empty();
    test_close_while_full();

    test_each_item_once(1, 2, 2, 2000);
    test_each_item_once(8, 4, 4, 5000);
    test_each_item_once(64, 8, 8, 5000);
    test_each_item_once(1024, 8, 8, 5000);

    std::cout << "PASS queue_stress\n";
    return 0;
}
