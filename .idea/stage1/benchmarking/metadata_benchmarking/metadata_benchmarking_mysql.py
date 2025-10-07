#!/usr/bin/env python3
"""Simple Metadata Benchmarking - Tests for MySQL backend."""

import time, statistics, random, tempfile, shutil, json, sys, os
from pathlib import Path
from contextlib import contextmanager

@contextmanager
def quiet():
    """Suppress output."""
    import sys, os
    with open(os.devnull, 'w') as devnull:
        old = sys.stdout
        sys.stdout = devnull
        try: yield
        finally: sys.stdout = old

# Import MySQL metadata functions
sys.path.append(str(Path(__file__).parent.parent.parent))
from metadata_processing.metadata_mysql import parse_gutenberg_metadata, store_metadata_in_db, get_metadata_from_db, create_metadata_table, search_books

def get_books():
    """Get book IDs from V1 JSON files."""
    path = Path("data_repository/datalake_v1")
    books = [int(f.stem) for f in path.glob("*.json") if f.stem.isdigit()] if path.exists() else []
    return sorted(books)

def get_header(book_id):
    """Get header content from JSON file."""
    try:
        json_path = Path("data_repository/datalake_v1") / f"{book_id}.json"
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get("header", "")
    except:
        return ""

def benchmark(book_ids, db_path):
    """Benchmark metadata pipeline."""
    times = {"extract": [], "store": [], "query": []}
    successful = 0
    
    for book_id in book_ids:
        header = get_header(book_id)
        if not header: continue
        
        # Extract
        t = time.perf_counter()
        metadata = parse_gutenberg_metadata(header)
        times["extract"].append(time.perf_counter() - t)
        if not metadata: continue
            
        # Store
        t = time.perf_counter()
        with quiet():
            success = store_metadata_in_db(metadata, book_id=str(book_id))
        times["store"].append(time.perf_counter() - t)
        
        if success:
            successful += 1
            # Query
            t = time.perf_counter()
            get_metadata_from_db(str(book_id))
            times["query"].append(time.perf_counter() - t)
    
    return {
        "success_rate": successful / len(book_ids) if book_ids else 0,
        "extract_ms": statistics.mean(times["extract"]) * 1000 if times["extract"] else 0,
        "store_ms": statistics.mean(times["store"]) * 1000 if times["store"] else 0,
        "query_ms": statistics.mean(times["query"]) * 1000 if times["query"] else 0,
        "total_s": sum(times["extract"]) + sum(times["store"]) + sum(times["query"])
    }

def benchmark_search(db_path, num_searches=10):
    """Benchmark search operations."""
    search_queries = [
        ('Author', 'a'),      # Search for authors containing 'a'
        ('Title', 'the'),     # Search for titles containing 'the'
        ('Language', 'en'),   # Search for English books
        ('Subject', 'fiction'), # Search for fiction books
        ('Author', 'dickens'), # Search for Dickens
    ]
    
    times = []
    results_count = []
    
    for _ in range(num_searches):
        # Pick a random search query
        keyword, value = random.choice(search_queries)
        
        # Time the search
        t = time.perf_counter()
        with quiet():
            results = search_books(keyword, value)
        search_time = time.perf_counter() - t
        
        times.append(search_time)
        results_count.append(len(results))
    
    return {
        "avg_search_ms": statistics.mean(times) * 1000 if times else 0,
        "avg_results": statistics.mean(results_count) if results_count else 0,
        "total_searches": len(times)
    }

def test():
    """Test V1 JSON format with 5 runs and average results."""
    books = get_books()
    if len(books) < 10:
        print(f"V1 (JSON): Only {len(books)} books - skipping")
        return
        
    print(f"\nV1 (JSON) - 5 Runs Average")
    print("=" * 70)
    print(f"Books: {len(books)}, Testing: {[s for s in [10,25,50,75,100] if s <= len(books)]}")
    print("Size     Success%   Extract    Store      Query      Search     Total")
    print("-" * 70)
    
    temp_dir = tempfile.mkdtemp(prefix="bench_v1_")
    try:
        for size in [s for s in [10,25,50,75,100] if s <= len(books)]:
            # Run 5 benchmarks and collect results
            all_results = []
            sample_books = random.sample(books, size)  # Use same books for all runs
            
            for run in range(5):
                db_path = Path(temp_dir) / f"{size}_run_{run}.db"
                with quiet(): create_metadata_table()
                
                # Run standard benchmark
                results = benchmark(sample_books, str(db_path))
                
                # Run search benchmark on the populated database
                search_results = benchmark_search(str(db_path), num_searches=5)
                results["search_ms"] = search_results["avg_search_ms"]
                
                all_results.append(results)
            
            # Calculate averages
            avg_results = {
                "success_rate": statistics.mean([r["success_rate"] for r in all_results]),
                "extract_ms": statistics.mean([r["extract_ms"] for r in all_results]),
                "store_ms": statistics.mean([r["store_ms"] for r in all_results]),
                "query_ms": statistics.mean([r["query_ms"] for r in all_results]),
                "search_ms": statistics.mean([r["search_ms"] for r in all_results]),
                "total_s": statistics.mean([r["total_s"] for r in all_results])
            }
            
            print(f"{size:<8} {avg_results['success_rate']:<10.1%} "
                  f"{avg_results['extract_ms']:<10.2f} {avg_results['store_ms']:<10.2f} "
                  f"{avg_results['query_ms']:<10.2f} {avg_results['search_ms']:<10.2f} {avg_results['total_s']:<8.2f}")
    finally:
        shutil.rmtree(temp_dir)

def main():
    """Run benchmarks."""
    print("METADATA BENCHMARKING - V1 JSON Format")
    print("=" * 55)
    test()
    print("\n" + "=" * 55)

if __name__ == "__main__":
    main()