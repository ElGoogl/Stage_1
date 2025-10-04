import time
from memory_profiler import memory_usage
from crawler_v2 import download_book_v2
from crawler_v1 import download_book_v1
from indexer import build_inverted_index

def benchmark_function(func, *args, **kwargs):
    """Benchmark a function’s execution time and memory usage."""
    print(f"\n⚙️  Benchmarking {func.__name__}...")
    start = time.perf_counter()
    mem_usage = memory_usage((func, args, kwargs))
    end = time.perf_counter()
    print(f"{func.__name__} finished in {end - start:.2f}s | peak memory {max(mem_usage):.2f} MB")
    return end - start, max(mem_usage)

if __name__ == "__main__":
    results = []

    # Benchmark Crawler v1 (JSON)
    t, m = benchmark_function(download_book_v1, 76921)
    results.append(("Crawler v1 (JSON)", t, m))

    # Benchmark Crawler v2 (TXT)
    t, m = benchmark_function(download_book_v2, 76921)
    results.append(("Crawler v2 (TXT)", t, m))

    # Benchmark Indexer v1 (Monolithe JSON File)
    t, m = benchmark_function(build_inverted_index)
    results.append(("Indexer (dict-based)", t, m))

    print("\nBenchmark Summary:")
    print(f"{'Component':30} {'Time (s)':>10} {'Memory (MB)':>15}")
    print("-" * 55)
    for name, t, m in results:
        print(f"{name:30} {t:10.2f} {m:15.2f}")
