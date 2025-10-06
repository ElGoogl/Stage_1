import time
import statistics
import csv
from memory_profiler import memory_usage
from crawler_v1 import download_book_v1
from crawler_v2 import download_book_v2
from indexed_query_v1 import search_file_v1


# Generic benchmarking helper
def benchmark_function(func, *args, runs=1, **kwargs):
    """Run a function multiple times and return average time + memory."""
    times, memories = [], []
    for _ in range(runs):
        start = time.perf_counter()
        mem_usage = memory_usage((func, args, kwargs))
        end = time.perf_counter()
        times.append(end - start)
        memories.append(max(mem_usage))
    return statistics.mean(times), statistics.mean(memories)


# Crawler benchmark runner
def benchmark_crawler(func, label, n_books, runs):
    """
    Benchmark downloading N different (or same) books for a number of runs.
    func  : crawler function
    label : label for CSV
    n_books: number of unique book IDs per run
    runs   : how many times to repeat the benchmark
    """
    # Example book ID range (Project Gutenberg IDs 100–100 000)
    base_ids = list(range(76900, 76900 + n_books))
    times, memories = [], []
    print(f"\nBenchmarking {label} ...")

    for r in range(runs):
        start = time.perf_counter()
        mem_usage = []
        for bid in base_ids:
            mem_usage.extend(memory_usage((func, (bid,)), interval=0.1, timeout=None))
        end = time.perf_counter()
        times.append(end - start)
        memories.append(max(mem_usage))
        print(f"  ▶ Run {r+1}/{runs} done ({end-start:.2f}s)")

    avg_t, avg_m = statistics.mean(times), statistics.mean(memories)
    print(f"{label}: {avg_t:.2f}s avg time | {avg_m:.2f} MB avg mem")
    return label, avg_t, avg_m



# Benchmark scenarios
def run_all_benchmarks():
    results = []

    #results.append(benchmark_crawler(download_book_v1, "Crawler_v1 – 10 × 1 book", n_books=1, runs=10))
    #results.append(benchmark_crawler(download_book_v2, "Crawler_v2 – 10 × 1 book", n_books=1, runs=10))
#
    #results.append(benchmark_crawler(download_book_v1, "Crawler_v1 – 5 × 5 books", n_books=5, runs=5))
    #results.append(benchmark_crawler(download_book_v2, "Crawler_v2 – 5 × 5 books", n_books=5, runs=5))
#
    #results.append(benchmark_crawler(download_book_v1, "Crawler_v1 – 2 × 50 books", n_books=50, runs=2))
    #results.append(benchmark_crawler(download_book_v2, "Crawler_v2 – 2 × 50 books", n_books=50, runs=2))
#
    #results.append(benchmark_crawler(download_book_v1, "Crawler_v1 – 1 × 75 books", n_books=75, runs=1))
    #results.append(benchmark_crawler(download_book_v2, "Crawler_v2 – 1 × 75 books", n_books=75, runs=1))
#
    #results.append(benchmark_crawler(download_book_v1, "Crawler_v1 – 1 × 100 books", n_books=100, runs=1))
    #results.append(benchmark_crawler(download_book_v2, "Crawler_v2 – 1 × 100 books", n_books=100, runs=1))

    t, m = benchmark_function(search_file_v1, 'balls')
    results.append(("Search (term='balls')", t, m))


    return results


# Export results to CSV
def export_to_csv(results, filename="benchmark_results.csv"):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Scenario", "Average Time (s)", "Average Memory (MB)"])
        for name, t, m in results:
            writer.writerow([name, f"{t:.3f}", f"{m:.2f}"])
    print(f"\nResults saved to {filename}")


# Main entry
if __name__ == "__main__":
    print("Starting full crawler benchmarking ...")
    all_results = run_all_benchmarks()
    export_to_csv(all_results)

    # Optional: print summary
    print("\nBenchmark Summary")
    print(f"{'Scenario':40} {'Avg Time (s)':>15} {'Avg Mem (MB)':>15}")
    print("-" * 70)
    for name, t, m in all_results:
        print(f"{name:40} {t:15.2f} {m:15.2f}")
