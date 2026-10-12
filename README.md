# C++ concurrency lab

Two bounded multi-producer, multi-consumer queues. One blocks on a mutex and condition variables and can shut down cleanly. The other is Vyukov's lock-free ring. Stress tests check that every id is delivered once. A benchmark records throughput and a checksum, and it does not call the mutex queue lock-free.

This is the CPU-side companion to `cuda-kernel-verification`: same kind of question (did the parallel program do the right thing?), different machine. `parallel-ml-benchmark` is the measurement companion.

## How to run

From this directory, in WSL or Linux:

```bash
cmake -S . -B build
cmake --build build -j
./build/queue_stress
./build/producer_consumer
./build/throughput
./build/queue_stress_tsan
```

`./build/throughput` writes `results/throughput.csv` and also checks that the popped ids sum to `0 + ... + (n-1)`. Run it from this directory so the CSV lands in `results/`.

On the WSL kernel used while building this, `queue_stress_tsan` aborts at startup with `FATAL: ThreadSanitizer: unexpected memory mapping`. That is not a passed race check. Keep a sanitizer log only from a run that prints `PASS queue_stress`.

## How to keep results

See `results/README.md`. Commit `results/throughput.csv` after a run you trust, and keep the numbers in the "Measured" section below equal to that file. Do not commit `build/`.

## BoundedQueue

`include/bounded_queue.hpp`

- `push` blocks while the queue is full and returns false after `close()`.
- `pop` blocks while the queue is empty, returns queued items after `close()`, then returns false.
- `try_push` / `try_pop` do not wait for capacity. They still take the mutex.
- Waits use condition-variable predicates, so a spurious wakeup rechecks the invariant.
- Two condition variables: one for "not full", one for "not empty".

## LockFreeQueue

`include/lockfree_queue.hpp`

Vyukov's bounded MPMC ring. Capacity is a power of two and at least 2.

A producer claims a cell by bumping `enqueue_pos`, writes the value, then release-stores the sequence number. A consumer acquire-loads that sequence before reading the value. The position counters are relaxed because a stale value is retried. The queue is lock-free, not wait-free: a thread can spin while another thread sits between the claim and the release store.

`try_push` returns false when the ring is full. There is no `close()`. The tests publish a known number of ids instead of a shutdown flag.

`LockFreeQueue<int, false>` keeps the head and tail on one cache line. The benchmark records that variant next to the padded one. If the numbers are close, that is the result.

## Tests

`tests/queue_stress.cpp` checks:

- capacity rejection
- single-thread `try_push` / `try_pop` order
- blocking FIFO through a capacity-1 queue
- `close()` waking an empty pop and a full push, then draining what was already stored
- one producer, one consumer FIFO via `try_*`
- four-by-four and eight-by-eight runs where each id appears once
- the lock-free ring at capacity 2, which is the high-contention case

```bash
cmake -S . -B build
cmake --build build -j
./build/queue_stress
./build/queue_stress_tsan
./build/producer_consumer
./build/throughput
```

ThreadSanitizer is the `queue_stress_tsan` binary (`-O1 -g -fsanitize=thread`). A clean run is evidence about that execution, not a proof that no race exists.

## Measured results

Figures below are taken from `results/throughput.csv`. Every parallel row passed the checksum `0 + ... + (n-1)`. Absolute rates depend on the machine. The comparison is the result.

Uncontended push then pop, one thread, 200,000 pairs:

- mutex: 49 ns/pair
- lock-free, padded head and tail: 18 ns/pair
- lock-free, both counters on one cache line: 24 ns/pair

`try_spin`, 100,000 items, capacity 1024. The thread count is producers plus the same number of consumers:

- 1+1: mutex 2.29M items/s, padded lock-free 19.4M, unpadded lock-free 7.83M
- 8+8: mutex 1.05M items/s, padded lock-free 2.25M, unpadded lock-free 1.39M

Padding isolates false sharing on the head and tail. With one thread and no second core hitting the other counter, the gap is small. With a producer and a consumer, and again at 8+8, the padded ring is about 1.6x the unpadded ring (2.25M versus 1.39M items/s).

Blocking mutex queue, same 100,000 items:

- capacity 1024: 2.85M items/s at 1+1, 0.90M at 8+8
- capacity 64: 1.41M items/s at 1+1, 0.17M at 8+8

The smaller capacity is backpressure. `try_spin` burns CPU when the queue is full, so those rows are not a ranking against the blocking queue. The lock-free queue is lock-free, not wait-free, and it has no `close()`.

ThreadSanitizer did not start on the WSL kernel used here. `queue_stress_tsan` aborts with `FATAL: ThreadSanitizer: unexpected memory mapping` before any test runs. The functional `queue_stress` binary passed. A sanitizer result has to come from a run that gets past startup.

## Benchmark

`bench/throughput.cpp` writes `results/throughput.csv` when launched from this directory.

- `uncontended`: one thread, push then pop, `ns_per_pair`
- `try_spin`: producers and consumers spin on `try_push` / `try_pop`
- `blocking`: mutex queue only, real waits, capacities 1024 and 64

Every parallel row checks that the sum of popped ids equals `0 + ... + (n-1)`. A fast number with a bad checksum is a failed run.

## What this does not prove

The lock-free queue is not a drop-in replacement for the blocking queue. Shutdown, backpressure, and waiting are the mutex queue's job. The benchmark does not show that one design is universally faster.
