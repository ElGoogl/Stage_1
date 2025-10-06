import time
import statistics
from memory_profiler import memory_usage
from crawler_v1 import download_book_v1
from crawler_v2 import download_book_v2
from indexer import build_inverted_index


def benchmark_function(func, *args, runs=10, **kwargs):
    """
    Benchmark a function over multiple runs and return average time + memory.
    - func: function to benchmark
    - args/kwargs: arguments for the function
    - runs: number of times to repeat the test
    """
    times = []
    memories = []

    print(f"\nBenchmarking {func.__name__} for {runs} runs...")
    for i in range(runs):
        start = time.perf_counter()
        mem_usage = memory_usage((func, args, kwargs))
        end = time.perf_counter()

        times.append(end - start)
        memories.append(max(mem_usage))

        # Optional: show progress every 10 runs
        if (i + 1) % 10 == 0:
            print(f"  ▶ Run {i+1}/{runs} done...")

    avg_time = statistics.mean(times)
    avg_memory = statistics.mean(memories)

    print(f"{func.__name__} completed {runs} runs.")
    print(f"   → Average time: {avg_time:.3f}s | Average memory: {avg_memory:.2f} MB\n")
    return avg_time, avg_memory


if __name__ == "__main__":
    results = []

    # Benchmark Crawler v1 (JSON Files)
    t, m = benchmark_function(download_book_v1, 76921)
    results.append(("Crawler v1 (JSON)", t, m))

    # Benchmark Crawler v2 (TXT structure)
    t, m = benchmark_function(download_book_v2, 76921)
    results.append(("Crawler v2 (TXT)", t, m))

    # Benchmark Indexer_v1
    t, m = benchmark_function(build_inverted_index)
    results.append(("Indexer (dict-based)", t, m))

    # Summary Table
    print("\nBenchmark Summary (average of 100 runs):")
    print(f"{'Component':30} {'Avg Time (s)':>15} {'Avg Memory (MB)':>20}")
    print("-" * 70)
    for name, t, m in results:
        print(f"{name:30} {t:15.3f} {m:20.2f}")
