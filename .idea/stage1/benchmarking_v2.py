# benchmarking_download_and_index.py
import time
import statistics
from memory_profiler import memory_usage
from pathlib import Path
import os

from crawler_v2 import download_book_v2
from Hierarchical_Indext import index_book_incremental, build_hierarchical_index
from Hierarchical_Indext.indexer_v2 import INDEX_ROOT_V2, DATA_LAKE_V2

TEST_ID = 76921  # feste ID für vergleichbare Läufe

def benchmark_function(func, *args, runs=10, **kwargs):  # ← Default auf 10
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

def _delete_index_dir():
    if INDEX_ROOT_V2.exists():
        for root, dirs, files in os.walk(INDEX_ROOT_V2, topdown=False):
            for name in files:
                (Path(root) / name).unlink(missing_ok=True)
            for name in dirs:
                try:
                    (Path(root) / name).rmdir()
                except OSError:
                    pass
        try:
            INDEX_ROOT_V2.rmdir()
        except OSError:
            pass

def _delete_book_from_datalake(book_id: int):
    bid = str(int(book_id))
    for part in ("content", "header", "footer"):
        for p in DATA_LAKE_V2.rglob(f"{bid}_{part}.txt"):
            p.unlink(missing_ok=True)

if __name__ == "__main__":
    results = []

    # --- Benchmark: reiner Download (v2) ---
    t, m = benchmark_function(download_book_v2, TEST_ID, runs=10)  # ← 10
    results.append(("Download v2 (single ID)", t, m))

    # --- Benchmark: inkrementelle Indexierung (pipeline-nah) ---
    download_book_v2(TEST_ID)   # sichert, dass die Files da sind
    _delete_index_dir()         # sauberer Start für die Messung
    t, m = benchmark_function(index_book_incremental, TEST_ID, runs=10)  # ← 10
    results.append(("Index v2 (incremental, single ID)", t, m))

    print("\nBenchmark Summary:")
    print(f"{'Component':35} {'Avg Time (s)':>15} {'Avg Memory (MB)':>20}")
    print("-" * 75)
    for name, t, m in results:
        print(f"{name:35} {t:15.3f} {m:20.2f}")