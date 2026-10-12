#include "bounded_queue.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Row {
    int producers = 0;
    int consumers = 0;
    size_t capacity = 0;
    uint64_t items = 0;
    double seconds = 0;
    double items_per_sec = 0;
};

// Moves `items` ids through the queue and returns the elapsed seconds.
// Returns a negative value if the sum of popped ids is wrong.
double run_once(size_t capacity, int producers, int consumers, uint64_t items) {
    BoundedQueue<int> queue(capacity);
    const uint64_t per = items / static_cast<uint64_t>(producers);
    std::vector<uint64_t> sums(static_cast<size_t>(consumers), 0);
    std::vector<uint64_t> counts(static_cast<size_t>(consumers), 0);

    const auto t0 = Clock::now();
    std::vector<std::thread> consumer_threads;
    for (int c = 0; c < consumers; ++c) {
        consumer_threads.emplace_back([&, c] {
            int value = 0;
            while (queue.pop(value)) {
                sums[static_cast<size_t>(c)] += static_cast<uint64_t>(value);
                counts[static_cast<size_t>(c)] += 1;
            }
        });
    }
    std::vector<std::thread> producer_threads;
    for (int p = 0; p < producers; ++p) {
        producer_threads.emplace_back([&, p] {
            const int begin = static_cast<int>(per) * p;
            for (uint64_t i = 0; i < per; ++i) {
                queue.push(begin + static_cast<int>(i));
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
    const auto t1 = Clock::now();

    uint64_t sum = 0;
    uint64_t count = 0;
    for (int c = 0; c < consumers; ++c) {
        sum += sums[static_cast<size_t>(c)];
        count += counts[static_cast<size_t>(c)];
    }
    if (count != items || sum != (items - 1) * items / 2) {
        return -1;
    }
    return std::chrono::duration<double>(t1 - t0).count();
}

}  // namespace

int main() {
    constexpr uint64_t kItems = 100000;
    constexpr int kRepeats = 5;
    const size_t capacities[] = {1024, 64};
    const int thread_counts[] = {1, 2, 4, 8};

    std::vector<Row> rows;
    for (size_t capacity : capacities) {
        for (int n : thread_counts) {
            std::vector<double> times;
            for (int r = 0; r < kRepeats; ++r) {
                const double seconds = run_once(capacity, n, n, kItems);
                if (seconds < 0) {
                    std::cerr << "checksum failed: capacity " << capacity << ", " << n << "+" << n << '\n';
                    return 1;
                }
                times.push_back(seconds);
            }
            std::sort(times.begin(), times.end());
            const double median = times[kRepeats / 2];
            rows.push_back(Row{n, n, capacity, kItems, median, kItems / median});
            std::cout << "capacity " << capacity << ", " << n << " producers + " << n
                      << " consumers: " << kItems / median / 1e6 << "M items/s\n";
        }
    }

    std::filesystem::create_directories("results");
    std::ofstream out("results/throughput.csv");
    out << "producers,consumers,capacity,items,median_seconds,items_per_sec\n";
    out.setf(std::ios::fixed);
    out.precision(6);
    for (const Row& row : rows) {
        out << row.producers << ',' << row.consumers << ',' << row.capacity << ',' << row.items << ','
            << row.seconds << ',' << row.items_per_sec << '\n';
    }
    std::cout << "PASS throughput (wrote results/throughput.csv)\n";
    return 0;
}
