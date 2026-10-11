#include "bounded_queue.hpp"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

// Small driver: 4 producers, 4 consumers, every id delivered once, then close.

int main() {
    constexpr int kProducers = 4;
    constexpr int kConsumers = 4;
    constexpr int kPerProducer = 1000;
    constexpr int kItems = kProducers * kPerProducer;

    BoundedQueue<int> queue(32);
    std::atomic<int> received{0};
    std::vector<std::thread> threads;
    threads.reserve(kProducers + kConsumers);

    for (int producer = 0; producer < kProducers; ++producer) {
        threads.emplace_back([&, producer] {
            const int begin = producer * kPerProducer;
            for (int i = 0; i < kPerProducer; ++i) {
                if (!queue.push(begin + i)) {
                    std::cerr << "push failed before close\n";
                    std::exit(1);
                }
            }
        });
    }
    for (int consumer = 0; consumer < kConsumers; ++consumer) {
        threads.emplace_back([&] {
            int value = 0;
            while (queue.pop(value)) {
                received.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (int i = 0; i < kProducers; ++i) {
        threads[static_cast<size_t>(i)].join();
    }
    queue.close();
    for (int i = kProducers; i < kProducers + kConsumers; ++i) {
        threads[static_cast<size_t>(i)].join();
    }

    if (received.load() != kItems) {
        std::cerr << "expected " << kItems << " items, got " << received.load() << '\n';
        return 1;
    }
    std::cout << "PASS producer_consumer items=" << kItems << '\n';
    return 0;
}
