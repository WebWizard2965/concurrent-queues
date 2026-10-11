# What to keep

From this repository root:

```bash
cmake -S . -B build && cmake --build build -j
./build/throughput
```

That overwrites `throughput.csv`. Commit that file when the numbers are the ones you want to show. The README summary should match the CSV. If you rerun on another machine, commit the new CSV and update the README from it.

Also worth keeping, as text you paste into the README rather than a binary:

- the `PASS queue_stress` line
- a ThreadSanitizer log, only from a run that gets past startup

`build/` is gitignored. Do not commit the executables.
