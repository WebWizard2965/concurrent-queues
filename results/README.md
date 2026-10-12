# Results

`throughput.csv` is written by `./build/throughput` when it runs from the repository root. Each row is one configuration: producers, consumers, capacity, item count, median time over 5 runs, and items per second.

If you rerun on another machine, commit the new CSV and update the table in the main README from it. `build/` is gitignored, so do not commit the executables.
