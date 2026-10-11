#include "bounded_queue.hpp"
#include "cpu_relax.hpp"
#include "lockfree_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Record {
    std::string impl;
    std::string mode;
    int producers = 0;
    int consumers = 0;
    size_t capacity = 0;
    std::string padded;
    uint64_t items = 0;
    double seconds = 0;
    std::string metric;
    double value = 0;
};

std::vector<Record> g_records;

void wait_go(const std::atomic<bool>& go) {
    while (!go.load(std::memory_order_acquire)) {
        cpu_relax();
    }
}

uint64_t expected_sum(uint64_t items) {
    return (items - 1) * items / 2;
}

template <typename Queue>
bool try_spin(const std::string& impl, const std::string& padded, size_t capacity,
              int producers, int consumers, uint64_t items) {
    if (items % static_cast<uint64_t>(producers) != 0) {
        std::cerr << "items must divide by producers\n";
        return false;
    }
    Queue queue(capacity);
    std::atomic<bool> go{false};
    std::atomic<uint64_t> sum{0};
    std::atomic<int> left{static_cast<int>(items)};
    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(producers + consumers));

    const uint64_t per = items / static_cast<uint64_t>(producers);
    for (int producer = 0; producer < producers; ++producer) {
        threads.emplace_back([&, producer] {
            wait_go(go);
            const int begin = static_cast<int>(per) * producer;
            for (uint64_t i = 0; i < per; ++i) {
                const int id = begin + static_cast<int>(i);
                while (!queue.try_push(id)) {
                    cpu_relax();
                }
            }
        });
    }
    for (int consumer = 0; consumer < consumers; ++consumer) {
        threads.emplace_back([&] {
            wait_go(go);
            while (left.load(std::memory_order_acquire) > 0) {
                int value = 0;
                if (!queue.try_pop(value)) {
                    cpu_relax();
                    continue;
                }
                sum.fetch_add(static_cast<uint64_t>(value), std::memory_order_relaxed);
                left.fetch_sub(1, std::memory_order_acq_rel);
            }
        });
    }

    const auto t0 = Clock::now();
    go.store(true, std::memory_order_release);
    for (std::thread& thread : threads) {
        thread.join();
    }
    const auto t1 = Clock::now();
    if (sum.load() != expected_sum(items) || left.load() != 0) {
        std::cerr << "checksum failed for " << impl << " producers=" << producers << '\n';
        return false;
    }
    const double seconds = std::chrono::duration<double>(t1 - t0).count();
    g_records.push_back(Record{impl, "try_spin", producers, consumers, capacity, padded, items,
                               seconds, "items_per_sec", items / seconds});
    return true;
}

bool blocking(size_t capacity, int producers, int consumers, uint64_t items) {
    if (items % static_cast<uint64_t>(producers) != 0) {
        return false;
    }
    BoundedQueue<int> queue(capacity);
    std::atomic<bool> go{false};
    std::atomic<uint64_t> sum{0};
    std::atomic<uint64_t> pops{0};
    std::vector<std::thread> consumers_threads;
    std::vector<std::thread> producer_threads;
    const uint64_t per = items / static_cast<uint64_t>(producers);

    for (int consumer = 0; consumer < consumers; ++consumer) {
        consumers_threads.emplace_back([&] {
            wait_go(go);
            int value = 0;
            while (queue.pop(value)) {
                sum.fetch_add(static_cast<uint64_t>(value), std::memory_order_relaxed);
                pops.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (int producer = 0; producer < producers; ++producer) {
        producer_threads.emplace_back([&, producer] {
            wait_go(go);
            const int begin = static_cast<int>(per) * producer;
            for (uint64_t i = 0; i < per; ++i) {
                if (!queue.push(begin + static_cast<int>(i))) {
                    std::cerr << "blocking push rejected\n";
                    std::_Exit(1);
                }
            }
        });
    }

    const auto t0 = Clock::now();
    go.store(true, std::memory_order_release);
    for (std::thread& thread : producer_threads) {
        thread.join();
    }
    queue.close();
    for (std::thread& thread : consumers_threads) {
        thread.join();
    }
    const auto t1 = Clock::now();
    if (pops.load() != items || sum.load() != expected_sum(items)) {
        std::cerr << "blocking checksum failed\n";
        return false;
    }
    const double seconds = std::chrono::duration<double>(t1 - t0).count();
    g_records.push_back(Record{"mutex", "blocking", producers, consumers, capacity, "n/a", items,
                               seconds, "items_per_sec", items / seconds});
    return true;
}

template <typename Queue>
void uncontended(const std::string& impl, const std::string& padded, size_t capacity, int ops) {
    Queue queue(capacity);
    const auto t0 = Clock::now();
    for (int i = 0; i < ops; ++i) {
        if (!queue.try_push(i)) {
            std::cerr << "uncontended push failed\n";
            std::_Exit(1);
        }
        int value = 0;
        if (!queue.try_pop(value) || value != i) {
            std::cerr << "uncontended pop failed\n";
            std::_Exit(1);
        }
    }
    const auto t1 = Clock::now();
    const double seconds = std::chrono::duration<double>(t1 - t0).count();
    const double ns = seconds * 1e9 / static_cast<double>(ops);
    g_records.push_back(Record{impl, "uncontended", 1, 1, capacity, padded,
                               static_cast<uint64_t>(ops), seconds, "ns_per_pair", ns});
}

void write_csv(const std::string& path) {
    std::filesystem::create_directories(std::filesystem::path(path).parent_path());
    std::ofstream out(path);
    if (!out) {
        std::cerr << "cannot write " << path << '\n';
        std::_Exit(1);
    }
    out << "impl,mode,producers,consumers,capacity,padded,items,seconds,metric,value\n";
    out.setf(std::ios::fixed);
    out.precision(6);
    for (const Record& row : g_records) {
        out << row.impl << ',' << row.mode << ',' << row.producers << ',' << row.consumers << ','
            << row.capacity << ',' << row.padded << ',' << row.items << ',' << row.seconds << ','
            << row.metric << ',' << row.value << '\n';
        std::cout << row.impl << ' ' << row.mode << " p=" << row.producers
                  << " cap=" << row.capacity << ' ' << row.metric << '=' << row.value << '\n';
    }
}

}  // namespace

int main() {
    std::thread([] {
        std::this_thread::sleep_for(std::chrono::seconds(120));
        std::cerr << "FAIL throughput watchdog\n";
        std::_Exit(1);
    }).detach();

    constexpr uint64_t kItems = 100000;
    constexpr int kOps = 200000;
    const int threads[] = {1, 2, 4, 8};

    uncontended<BoundedQueue<int>>("mutex", "n/a", 1024, kOps);
    uncontended<LockFreeQueue<int, true>>("lockfree", "yes", 1024, kOps);
    uncontended<LockFreeQueue<int, false>>("lockfree", "no", 1024, kOps);

    for (int n : threads) {
        if (!try_spin<BoundedQueue<int>>("mutex", "n/a", 1024, n, n, kItems)) {
            return 1;
        }
        if (!try_spin<LockFreeQueue<int, true>>("lockfree", "yes", 1024, n, n, kItems)) {
            return 1;
        }
        if (!try_spin<LockFreeQueue<int, false>>("lockfree", "no", 1024, n, n, kItems)) {
            return 1;
        }
        if (!blocking(1024, n, n, kItems)) {
            return 1;
        }
        if (!blocking(64, n, n, kItems)) {
            return 1;
        }
    }

    write_csv("results/throughput.csv");
    std::cout << "PASS throughput\n";
    return 0;
}
