import time
import statistics
from memory_profiler import memory_usage

# ❌ Deaktiviert: v1
# from crawler_v1 import download_book_v1
# from JSON_Indexer.indexer import build_inverted_index

# ✅ v2
from crawler_v2 import download_book_v2
from Hierarchical_Indext import build_hierarchical_index

def benchmark_function(func, *args, runs=5, **kwargs):
    times, memories = [], []
    print(f"\nBenchmarking {func.__name__} for {runs} runs...")
    for i in range(runs):
        start = time.perf_counter()
        mem_usage = memory_usage((func, args, kwargs))
        end = time.perf_counter()
        times.append(end - start)
        memories.append(max(mem_usage))
    avg_time = statistics.mean(times)
    avg_memory = statistics.mean(memories)
    print(f"{func.__name__}: {avg_time:.3f}s | {avg_memory:.2f} MB")
    return avg_time, avg_memory

if __name__ == "__main__":
    results = []
    # ✅ Benchmark: Crawler v2
    t, m = benchmark_function(download_book_v2, 76921, runs=3)
    results.append(("Crawler v2 (TXT)", t, m))

    # ✅ Benchmark: hierarchischer Indexer v2
    t, m = benchmark_function(build_hierarchical_index, runs=1, clean=True)
    results.append(("Indexer v2 (hierarchical)", t, m))

    print("\nBenchmark Summary:")
    print(f"{'Component':30} {'Avg Time (s)':>15} {'Avg Memory (MB)':>20}")
    print("-" * 70)
    for name, t, m in results:
        print(f"{name:30} {t:15.3f} {m:20.2f}")
