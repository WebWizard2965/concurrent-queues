# Concurrent queues

A bounded queue that many producer threads and many consumer threads can share safely. It uses one mutex and two condition variables. Stress tests check that every item is delivered exactly once, and a small benchmark measures throughput.

## The queue

`include/bounded_queue.hpp`

- `push(x)` waits while the queue is full. It returns false if the queue was closed.
- `pop(x)` waits while the queue is empty. After `close()`, it still returns the items already queued, then returns false.
- `close()` wakes every waiting thread. New pushes are rejected, and consumers drain what is left and exit.

How it works:

- One `std::mutex` protects a `std::deque` and a `closed_` flag. Only one thread touches them at a time.
- `not_full_` is the condition variable producers wait on. `not_empty_` is the one consumers wait on.
- After a push, the producer calls `not_empty_.notify_one()` to wake one consumer. After a pop, the consumer calls `not_full_.notify_one()` to wake one producer.
- Every wait uses a predicate, such as `wait(lock, [&] { return closed_ || !queue_.empty(); })`. A thread can wake up without being notified (a spurious wakeup), and the predicate makes it check the condition again before continuing.

## Tests

`tests/queue_stress.cpp` checks:

- capacity 0 is rejected
- single-thread first-in, first-out order
- a capacity-1 queue between one producer and one consumer, so the producer waits on every push, and the order stays correct
- `close()` wakes a consumer blocked on an empty queue
- `close()` wakes a producer blocked on a full queue, rejects its push, and still lets the queued items be popped
- 2+2, 4+4, 8+8 (capacity 64), and 8+8 (capacity 1024) producer and consumer threads: every producer pushes its own range of ids, and the test checks that each id was popped exactly once

A watchdog thread fails the test after 60 seconds, so a deadlock shows up as a failure instead of a hang.

`src/producer_consumer.cpp` is a small example with 4 producers and 4 consumers.

## How to run

In Linux or WSL, from this directory:

```bash
mkdir -p build
g++ -std=c++17 -O2 -Wall -Wextra -Wpedantic -pthread -Iinclude tests/queue_stress.cpp -o build/queue_stress
g++ -std=c++17 -O2 -Wall -Wextra -Wpedantic -pthread -Iinclude src/producer_consumer.cpp -o build/producer_consumer
g++ -std=c++17 -O2 -Wall -Wextra -Wpedantic -pthread -Iinclude bench/throughput.cpp -o build/throughput
./build/queue_stress
./build/producer_consumer
./build/throughput
```

`-pthread` is needed for `std::thread`. Run `throughput` from this directory so it writes `results/throughput.csv`.

## Benchmark

`bench/throughput.cpp` sends 100,000 ids through the queue with N producers and N consumers, for N = 1, 2, 4, 8 and capacities 1024 and 64. Each configuration runs 5 times and the median time is reported. Each run also checks that the popped ids add up to `0 + 1 + ... + 99,999`. A fast run with a wrong sum counts as a failure.

## Measured results

From `results/throughput.csv`, on a 16-thread laptop CPU in WSL:

| Threads | Capacity 1024 | Capacity 64 |
| --- | --- | --- |
| 1 + 1 | 1.99M items/s | 0.74M items/s |
| 2 + 2 | 1.16M items/s | 0.16M items/s |
| 4 + 4 | 1.28M items/s | 0.22M items/s |
| 8 + 8 | 1.19M items/s | 0.20M items/s |

Adding threads does not make it faster. Every push and pop takes the same mutex, so only one thread can work on the queue at a time, and more threads mostly add waiting. With capacity 64 the queue fills up more often, so producers sleep and get woken up more often, which costs more. Absolute numbers depend on the machine.

## What this does not prove

Passing stress tests is evidence that these runs worked. It does not prove the queue has no race condition. The queue is not lock-free: it is a conventional mutex queue, chosen because it is simple to reason about.
