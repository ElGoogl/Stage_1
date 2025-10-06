import os
import time
import statistics
from pathlib import Path
from typing import Iterable, Optional, Sequence

from memory_profiler import memory_usage

from control.control_panel_v2 import (
    ControlPanelV2,
    CONTROL_PATH,
    DOWNLOADED_FILE,
    INDEXED_FILE,
)
from Hierarchical_Indext.indexer_v2 import INDEX_ROOT_V2, DATA_LAKE_V2


# --------- Hilfen zum Bereinigen für reproduzierbare Runs ---------

def _delete_index_dir():
    if INDEX_ROOT_V2.exists():
        for root, dirs, files in os.walk(INDEX_ROOT_V2, topdown=False):
            for name in files:
                try:
                    (Path(root) / name).unlink()
                except FileNotFoundError:
                    pass
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
    # Entfernt alle Teile dieses Buches aus dem datalake_v2
    for part in ("content", "header", "footer"):
        for p in DATA_LAKE_V2.rglob(f"{bid}_{part}.txt"):
            try:
                p.unlink()
            except FileNotFoundError:
                pass

def _filter_ids_file(path: Path, ids_to_remove: Iterable[int]):
    if not path.exists():
        return
    remove = {str(int(x)) for x in ids_to_remove}
    lines = [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines()]
    kept = [ln for ln in lines if ln and ln not in remove]
    path.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")

def prepare_fair_run(book_ids: Sequence[int],
                     clean_index: bool = True,
                     clean_datalake: bool = True,
                     clean_state: bool = True):
    """
    Stellt sicher, dass genau diese book_ids im Run neu erzeugt und indexiert werden,
    damit die Messung vergleichbar ist.
    """
    if clean_index:
        _delete_index_dir()
    if clean_datalake:
        for bid in book_ids:
            _delete_book_from_datalake(bid)
    if clean_state:
        CONTROL_PATH.mkdir(parents=True, exist_ok=True)
        _filter_ids_file(DOWNLOADED_FILE, book_ids)
        _filter_ids_file(INDEXED_FILE, book_ids)

# --------- Benchmark-Helfer (identische Metriken & Ausgabe) ---------

def benchmark_function(func, *args, runs=3, **kwargs):
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

# --------- Zu benchmarkende Funktion: komplette Pipeline ---------

def run_pipeline_batch(batch_size: int = 5,
                       fixed_book_ids: Optional[Sequence[int]] = None,
                       queue_size: int = 4,
                       throttle_seconds: float = 0.5):
    """
    Startet die v2-Pipeline (Downloader + Indexer) und wartet bis Ende.
    Wenn fixed_book_ids gesetzt ist, werden genau diese IDs (bis batch_size) verarbeitet.
    """
    cp = ControlPanelV2(
        batch_size=batch_size,
        queue_size=queue_size,
        throttle_seconds=throttle_seconds,
        max_random_tries=500,
        fixed_book_ids=fixed_book_ids,
    )
    cp.start()
    cp.join()

# --------- Direkter Start (Beispiel) ---------

if __name__ == "__main__":
    results = []

    # Feste Test-IDs (5 Stück) — so ist jeder Run vergleichbar
    test_ids = [76921, 76922, 76947, 20345, 20346]

    # Vorbereitung für faire Messung (Index & diese Bücher entfernen)
    prepare_fair_run(test_ids, clean_index=True, clean_datalake=True, clean_state=True)

    # ✅ Benchmark: v2 Pipeline (5 Bücher, feste IDs)
    t, m = benchmark_function(
        run_pipeline_batch,
        runs=3,
        batch_size=5,
        fixed_book_ids=test_ids,
        queue_size=4,
        throttle_seconds=0.5
    )
    results.append(("Pipeline v2 (5 books, fixed)", t, m))

    print("\nBenchmark Summary:")
    print(f"{'Component':35} {'Avg Time (s)':>15} {'Avg Memory (MB)':>20}")
    print("-" * 75)
    for name, t, m in results:
        print(f"{name:35} {t:15.3f} {m:20.2f}")
