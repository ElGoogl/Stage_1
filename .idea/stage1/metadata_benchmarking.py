#!/usr/bin/env python3
"""Simple Metadata Benchmarking - Tests [10,25,50,75,100] books for V1 and V2 formats."""

import time, statistics, random, tempfile, shutil, json, sys, os
from pathlib import Path
from contextlib import contextmanager

@contextmanager
def quiet():
    """Suppress output."""
    with open(os.devnull, 'w') as devnull:
        old = sys.stdout
        sys.stdout = devnull
        try: yield
        finally: sys.stdout = old

from metadata_sqlite import parse_gutenberg_metadata, store_metadata_in_db, get_metadata_from_db, create_metadata_table
from crawler_v2 import get_subfolder

def get_books(version):
    """Get book IDs for version."""
    if version == "v1":
        path = Path("data_repository/datalake_v1")
        books = [int(f.stem) for f in path.glob("*.json") if f.stem.isdigit()] if path.exists() else []
        return sorted(books)
    else:
        path = Path("data_repository/datalake_v2")
        books = []
        if path.exists():
            for f in path.glob("*/*_header.txt"):
                try: books.append(int(f.stem.split('_')[0]))
                except: pass
        return sorted(set(books))

def get_header(book_id, version):
    """Get header content."""
    try:
        if version == "v1":
            return json.loads((Path("data_repository/datalake_v1") / f"{book_id}.json").read_text())["header"]
        else:
            return (get_subfolder(book_id) / f"{book_id}_header.txt").read_text()
    except:
        return ""

def benchmark(book_ids, version, db_path):
    """Benchmark metadata pipeline."""
    times = {"extract": [], "store": [], "query": []}
    successful = 0
    
    for book_id in book_ids:
        header = get_header(book_id, version)
        if not header: continue
        
        # Extract
        t = time.perf_counter()
        metadata = parse_gutenberg_metadata(header)
        times["extract"].append(time.perf_counter() - t)
        if not metadata: continue
            
        # Store
        t = time.perf_counter()
        with quiet():
            success = store_metadata_in_db(metadata, db_path=db_path, book_id=str(book_id))
        times["store"].append(time.perf_counter() - t)
        
        if success:
            successful += 1
            # Query
            t = time.perf_counter()
            get_metadata_from_db(str(book_id), db_path=db_path)
            times["query"].append(time.perf_counter() - t)
    
    return {
        "success_rate": successful / len(book_ids) if book_ids else 0,
        "extract_ms": statistics.mean(times["extract"]) * 1000 if times["extract"] else 0,
        "store_ms": statistics.mean(times["store"]) * 1000 if times["store"] else 0,
        "query_ms": statistics.mean(times["query"]) * 1000 if times["query"] else 0,
        "total_s": sum(times["extract"]) + sum(times["store"]) + sum(times["query"])
    }

def test(name, version):
    """Test one version."""
    books = get_books(version)
    if len(books) < 10:
        print(f"{name}: Only {len(books)} books - skipping")
        return
        
    print(f"\n{name}")
    print("=" * 55)
    print(f"Books: {len(books)}, Testing: {[s for s in [10,25,50,75,100] if s <= len(books)]}")
    print("Size     Success%   Extract    Store      Query      Total")
    print("-" * 55)
    
    temp_dir = tempfile.mkdtemp(prefix=f"bench_{version}_")
    try:
        for size in [s for s in [10,25,50,75,100] if s <= len(books)]:
            db_path = Path(temp_dir) / f"{size}.db"
            with quiet(): create_metadata_table(str(db_path))
            
            results = benchmark(random.sample(books, size), version, str(db_path))
            print(f"{size:<8} {results['success_rate']:<10.1%} "
                  f"{results['extract_ms']:<10.2f} {results['store_ms']:<10.2f} "
                  f"{results['query_ms']:<10.2f} {results['total_s']:<8.2f}")
    finally:
        shutil.rmtree(temp_dir)

def main():
    """Run benchmarks."""
    print("METADATA BENCHMARKING")
    print("=" * 55)
    test("V1 (JSON)", "v1")
    test("V2 (HEADERS)", "v2")
    print("\n" + "=" * 55)

if __name__ == "__main__":
    main()