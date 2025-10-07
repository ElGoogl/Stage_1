#!/usr/bin/env python3
"""
Minimal Metadata Benchmarking Script

Measures basic performance of metadata extraction and storage across different dataset sizes.
"""

import time
import statistics
from pathlib import Path
from typing import List, Dict

# Memory profiling
import psutil
def get_memory_usage_mb():
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024

from metadata_sqlite import parse_gutenberg_metadata, store_metadata_in_db, get_metadata_from_db
from crawler_v2 import get_subfolder


def get_header_content_v1(book_id: int) -> str:
    """Get header content from v1 JSON file."""
    try:
        import json
        json_path = Path("data_repository/datalake_v1") / f"{book_id}.json"
        if json_path.exists():
            with open(json_path, "r", encoding="utf-8") as f:
                book_data = json.load(f)
            return book_data.get("header", "")
    except:
        pass
    return ""


def get_header_content_v2(book_id: int) -> str:
    """Get header content from v2 header file."""
    try:
        subfolder = get_subfolder(book_id)
        header_path = subfolder / f"{book_id}_header.txt"
        if header_path.exists():
            return header_path.read_text(encoding="utf-8")
    except:
        pass
    return ""


def get_header_content(book_id: int, version: str = "auto") -> str:
    """Get header content from specified version or auto-detect."""
    if version == "v1":
        return get_header_content_v1(book_id)
    elif version == "v2":
        return get_header_content_v2(book_id)
    else:  # auto
        # Try v2 first, then v1
        content = get_header_content_v2(book_id)
        if not content:
            content = get_header_content_v1(book_id)
        return content


def bench_metadata_pipeline(book_ids: List[int], version: str = "auto") -> Dict:
    """
    Benchmark complete metadata pipeline: extract → store → query
    """
    import sys
    from io import StringIO
    
    extraction_times = []
    storage_times = []
    query_times = []
    memory_samples = []
    successful = 0
    
    start_memory = get_memory_usage_mb()
    
    # Suppress stdout during benchmarking to avoid clutter
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    try:
        for book_id in book_ids:
            # 1. Extract metadata
            header = get_header_content(book_id, version)
            if not header:
                continue
                
            start_time = time.perf_counter()
            metadata = parse_gutenberg_metadata(header)
            extract_time = time.perf_counter() - start_time
            
            if not metadata:
                continue
                
            extraction_times.append(extract_time)
            
            # 2. Store metadata
            start_time = time.perf_counter()
            success = store_metadata_in_db(metadata, book_id=str(book_id))
            store_time = time.perf_counter() - start_time
            
            if success:
                storage_times.append(store_time)
                successful += 1
                
                # 3. Query metadata
                start_time = time.perf_counter()
                result = get_metadata_from_db(str(book_id))
                query_time = time.perf_counter() - start_time
                
                if result:
                    query_times.append(query_time)
            
            # Sample memory usage
            current_memory = get_memory_usage_mb()
            memory_samples.append(current_memory)
    
    finally:
        # Restore stdout
        sys.stdout = old_stdout
    
    end_memory = get_memory_usage_mb()
    memory_delta = end_memory - start_memory
    peak_memory = max(memory_samples) if memory_samples else end_memory
    
    return {
        'total_books': len(book_ids),
        'successful': successful,
        'success_rate': successful / len(book_ids),
        'avg_extract_ms': statistics.mean(extraction_times) * 1000 if extraction_times else 0,
        'avg_store_ms': statistics.mean(storage_times) * 1000 if storage_times else 0,
        'avg_query_ms': statistics.mean(query_times) * 1000 if query_times else 0,
        'total_time_s': sum(extraction_times) + sum(storage_times) + sum(query_times),
        'memory_delta_mb': memory_delta,
        'peak_memory_mb': peak_memory,
        'avg_memory_mb': statistics.mean(memory_samples) if memory_samples else 0
    }


def get_v1_book_ids():
    """Get list of available book IDs from v1 data (JSON files)."""
    book_ids = []
    v1_path = Path("data_repository/datalake_v1")
    if v1_path.exists():
        for json_file in v1_path.glob("*.json"):
            try:
                book_id = int(json_file.stem)
                book_ids.append(book_id)
            except ValueError:
                continue
    return sorted(book_ids)


def get_v2_book_ids():
    """Get list of available book IDs from v2 data (header files)."""
    book_ids = []
    v2_path = Path("data_repository/datalake_v2")
    if v2_path.exists():
        for header_file in v2_path.glob("*/*_header.txt"):
            try:
                book_id = int(header_file.stem.split('_')[0])
                book_ids.append(book_id)
            except (ValueError, IndexError):
                continue
    return sorted(list(set(book_ids)))  # Remove duplicates and sort


def get_available_book_ids():
    """Get list of available book IDs from both v1 and v2 data."""
    v1_ids = get_v1_book_ids()
    v2_ids = get_v2_book_ids()
    return sorted(list(set(v1_ids + v2_ids)))  # Remove duplicates and sort


def main():
    """Run metadata benchmark across different dataset sizes (combined v1+v2)."""
    # Get available book IDs
    available_books = get_available_book_ids()
    print(f"Found {len(available_books)} available books: {available_books[:10]}{'...' if len(available_books) > 10 else ''}")
    
    if len(available_books) < 5:
        print(f"Not enough books for meaningful benchmark (need at least 5, have {len(available_books)})")
        return
    
    # Create test sizes
    book_counts = create_test_sizes(len(available_books))
    book_counts = sorted(list(set(book_counts)))
    print(f"Testing with sizes: {book_counts}")
    
    print("METADATA BENCHMARK - Multiple Dataset Sizes (Combined)")
    print("=" * 80)
    print(f"{'Books':<8} {'Success%':<10} {'Extract(ms)':<12} {'Store(ms)':<11} {'Query(ms)':<11} {'Total(s)':<9} {'Memory(MB)':<10}")
    print("-" * 80)
    
    for book_count in book_counts:
        print(f"Testing {book_count} books...", end=" ", flush=True)
        
        # Use first N available books
        book_ids = available_books[:book_count]
        results = bench_metadata_pipeline(book_ids)
        
        print(f"\r{book_count:<8} "
              f"{results['success_rate']:<10.1%} "
              f"{results['avg_extract_ms']:<12.2f} "
              f"{results['avg_store_ms']:<11.2f} "
              f"{results['avg_query_ms']:<11.2f} "
              f"{results['total_time_s']:<9.2f} "
              f"{results['memory_delta_mb']:<10.1f}")
    
    print("=" * 80)


def create_test_sizes(max_books: int) -> List[int]:
    """Create reasonable test sizes based on available data."""
    if max_books >= 100:
        return [10, 25, 50, 75, 100]
    elif max_books >= 50:
        return [5, 10, 20, max_books]
    elif max_books >= 20:
        return [5, 10, max_books//2, max_books]
    else:
        return [5, max_books//2, max_books]


def run_benchmark_for_version(version_name: str, book_ids: List[int], version_flag: str):
    """Run benchmark for a specific version."""
    if not book_ids:
        print(f"No books available for {version_name}")
        return
    
    print(f"\n{version_name.upper()} BENCHMARK")
    print("=" * 80)
    print(f"Found {len(book_ids)} books: {book_ids[:5]}{'...' if len(book_ids) > 5 else ''}")
    
    if len(book_ids) < 3:
        print(f"Not enough books for meaningful benchmark (need at least 3, have {len(book_ids)})")
        return
    
    # Create test sizes
    book_counts = create_test_sizes(len(book_ids))
    book_counts = sorted(list(set([count for count in book_counts if count <= len(book_ids)])))
    print(f"Testing with sizes: {book_counts}")
    
    print(f"{'Books':<8} {'Success%':<10} {'Extract(ms)':<12} {'Store(ms)':<11} {'Query(ms)':<11} {'Total(s)':<9} {'Memory(MB)':<10}")
    print("-" * 80)
    
    for book_count in book_counts:
        print(f"Testing {book_count} books...", end=" ", flush=True)
        
        # Use first N available books
        test_books = book_ids[:book_count]
        results = bench_metadata_pipeline(test_books, version_flag)
        
        print(f"\r{book_count:<8} "
              f"{results['success_rate']:<10.1%} "
              f"{results['avg_extract_ms']:<12.2f} "
              f"{results['avg_store_ms']:<11.2f} "
              f"{results['avg_query_ms']:<11.2f} "
              f"{results['total_time_s']:<9.2f} "
              f"{results['memory_delta_mb']:<10.1f}")
    
    print("=" * 80)


def main_separate():
    """Run metadata benchmark for both v1 and v2 separately."""
    print("METADATA BENCHMARKING - SEPARATE VERSION TESTING")
    print("=" * 80)
    
    # Get available book IDs for each version
    v1_books = get_v1_book_ids()
    v2_books = get_v2_book_ids()
    
    print(f"V1 books available: {len(v1_books)}")
    print(f"V2 books available: {len(v2_books)}")
    
    # Test V1 format (JSON files)
    run_benchmark_for_version("V1 (JSON)", v1_books, "v1")
    
    # Test V2 format (header files)
    run_benchmark_for_version("V2 (Header Files)", v2_books, "v2")
    
    print("\nNote: Install 'psutil' for accurate memory measurements: pip install psutil")


if __name__ == "__main__":
    main_separate()