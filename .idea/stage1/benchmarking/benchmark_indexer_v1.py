import time
import csv
import os
from memory_profiler import memory_usage
from crawlers.crawler_v1 import download_book_v1
from indexers.indexer import build_inverted_index, save_index
from utils.cleanup_project import delete_indexed_file
from indexer_query.indexed_query_v1 import search_file_v1

# ───────────────────────────────────────────────
# Benchmark configuration
# ───────────────────────────────────────────────
START_ID = 70000
END_ID = 70100
TEST_SIZES = [70009, 70024, 70049, 70074, 70099]
CSV_PATH = "data_repository/benchmark_index_scaling.csv"
INDEX_PATH = "data_repository/datamart_indexer_v1/inverted_index.json"

SEARCH_TERMS = ["advantage", "house", "white"]
SEARCH_REPEATS = 20000


# ───────────────────────────────────────────────
# Helper functions
# ───────────────────────────────────────────────
def get_file_size_mb(filepath):
    """Return file size in MB (0 if file not found)."""
    if os.path.exists(filepath):
        return os.path.getsize(filepath) / (1024 * 1024)
    return 0.0


def benchmark_search_terms(index):
    """
    Run multiple searches on the given index and return
    the average search time per term in microseconds (µs).
    """
    avg_times = {}

    for term in SEARCH_TERMS:
        # Measure total time for many searches, then average
        start = time.perf_counter()
        for _ in range(SEARCH_REPEATS):
            _ = search_file_v1(term, index)
        end = time.perf_counter()

        avg_us = ((end - start) / SEARCH_REPEATS) * 1_000_000 # convert to microseconds
        avg_times[term] = avg_us

    return avg_times


# ───────────────────────────────────────────────
# Main benchmark function
# ───────────────────────────────────────────────
def benchmark_index_scaling():
    results = []

    print("🚀 Starting index-scaling benchmark...")
    print(f"📚 Downloading books {START_ID} to {END_ID}...")

    ## Step 1: Download all books (once)
    #for book_id in range(START_ID, END_ID + 1):
    #    success = download_book_v1(book_id)
    #    if not success:
    #        print(f"⚠️ Failed to download book {book_id}, skipping.")
    #print("✅ All available books downloaded.\n")

    # Step 2: Progressive indexing + search benchmark
    for cutoff in TEST_SIZES:
        print(f"⚙️ Benchmarking indexing up to book ID {cutoff}...")

        # Reset old index
        delete_indexed_file()

        # Measure indexing time & memory
        start = time.perf_counter()
        mem_usage = memory_usage((build_inverted_index,), interval=0.1, timeout=None)
        index = build_inverted_index()
        save_index(index)
        end = time.perf_counter()

        elapsed = end - start
        avg_mem = max(mem_usage)
        file_size = get_file_size_mb(INDEX_PATH)

        # Benchmark search performance
        print(f"🔍 Running {SEARCH_REPEATS} searches for each term...")
        search_times = benchmark_search_terms(index)

        num_books = cutoff - START_ID
        results.append(
            (
                num_books,
                elapsed,
                avg_mem,
                file_size,
                search_times["advantage"],
                search_times["house"],
                search_times["white"],
            )
        )

        print(
            f"✅ Indexed {num_books} books → {elapsed:.2f}s | {avg_mem:.2f} MB RAM | {file_size:.2f} MB file size"
        )
        print(
            f"   Avg search (ms): advantage={search_times['advantage']:.3f}, "
            f"house={search_times['house']:.3f}, white={search_times['white']:.3f}\n"
        )

    # Step 3: Write results to CSV
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Number of Books Indexed",
                "Indexing Time (s)",
                "Memory (MB)",
                "File Size (MB)",
                "Search (advantage) [ms]",
                "Search (house) [ms]",
                "Search (white) [ms]",
            ]
        )
        for row in results:
            writer.writerow(
                [
                    row[0],
                    f"{row[1]:.3f}",
                    f"{row[2]:.2f}",
                    f"{row[3]:.2f}",
                    f"{row[4]:.3f}",
                    f"{row[5]:.3f}",
                    f"{row[6]:.3f}",
                ]
            )

    print(f"📁 Results saved to {CSV_PATH}\n")

    # Step 4: Print summary
    print("📊 Benchmark Summary:")
    print(
        f"{'Books':>8} {'Time (s)':>10} {'Mem (MB)':>10} {'File(MB)':>10} {'adv(ms)':>10} {'house(ms)':>12} {'white(ms)':>12}"
    )
    print("-" * 80)
    for r in results:
        print(
            f"{r[0]:8d} {r[1]:10.2f} {r[2]:10.2f} {r[3]:10.2f} {r[4]:10.3f} {r[5]:12.3f} {r[6]:12.3f}"
        )


# ───────────────────────────────────────────────
# Main entry point
# ───────────────────────────────────────────────
if __name__ == "__main__":
    benchmark_index_scaling()
