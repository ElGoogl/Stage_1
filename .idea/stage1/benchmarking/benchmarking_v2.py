# stage1/benchmarking_v2.py
import os
import csv
import time
import random
import statistics
from pathlib import Path
from typing import List, Dict

from memory_profiler import memory_usage

from crawlers.crawler_v2 import download_book_v2
from Hierarchical_Indext import (
    index_book_incremental,
    search_postings,
    clear_postings_cache,
)
from Hierarchical_Indext.indexer_v2 import INDEX_ROOT_V2, DATA_LAKE_V2

ID_START = 70000
END_IDS = [70009]
#, 70024, 70049, 70074, 70099]

TERMS = ["advantage", "house", "white"]

# Throttle zwischen Downloads (zur Server-Schonung); wird NICHT in Zeiten eingerechnet
DOWNLOAD_THROTTLE_SEC = 0.20      # 0.0 = ohne Pause
DOWNLOAD_THROTTLE_JITTER_PCT = 0.2  # ±20% Zufallsjitter auf die Pause

SEARCH_RUNS_PER_TERM  = 20     # 20 Kaltruns je Wort
CSV_PATH = Path("../stage1_benchmark_results.csv")


# ---------- Utilities ----------
def _delete_index_dir():
    """Entfernt den kompletten Index-Ordner (sauberer Start)."""
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


def _download_range_only(start_id: int, end_id: int, throttle: float = 0.25, jitter_pct: float = 0.2):
    """
    Lädt alle Bücher in [start_id, end_id].
    Rückgabe:
      - ok_ids: Liste erfolgreich geladener IDs
      - per_book_times: Zeit pro erfolgreiches Buch (Sek., EXKL. Pausen)
      - per_book_mems:  Peak-Memory pro erfolgreiches Buch (MB)
      - sleep_total:    Summe aller Pausen (Sek.) – nur Info, NICHT in Zeiten enthalten
    Hinweis:
      - Die Download-Gesamtzeit für die Benchmark-Ergebnisse berechnet sich als sum(per_book_times).
      - Ggf. angewandte Pausen (throttle + jitter) werden separat als sleep_total geliefert.
    """
    ok_ids: List[int] = []
    per_book_times: List[float] = []
    per_book_mems: List[float] = []
    sleep_total: float = 0.0

    for bid in range(int(start_id), int(end_id) + 1):
        # reine Arbeitszeit messen (ohne Pause)
        t0 = time.perf_counter()
        ok = download_book_v2(bid)
        dt = time.perf_counter() - t0

        # leichtgewichtige Peak-Memory-Schätzung (Sample)
        mem_samples = memory_usage(max_iterations=1)
        pm = max(mem_samples) if mem_samples else 0.0

        if ok:
            ok_ids.append(bid)
            per_book_times.append(dt)
            per_book_mems.append(pm)

        # optionale Pause – wird aus den Ergebnissen EXKLUDIERT
        if throttle > 0:
            # Jitter um Burst-Muster zu vermeiden
            jitter = 1.0 + random.uniform(-jitter_pct, jitter_pct)
            pause = max(0.0, throttle * jitter)
            time.sleep(pause)
            sleep_total += pause

    return ok_ids, per_book_times, per_book_mems, sleep_total


def _index_ids(ids, verbose_missing: bool = False):
    """
    Indexiert ausschließlich die übergebene ID-Liste.
    Rückgabe:
      - indexed_ids: Liste der erfolgreich indexierten IDs
      - per_index_times: Zeit pro erfolgreiches Buch (Sek.)
      - per_index_mems:  Peak-Memory pro erfolgreiches Buch (MB)
      - total_index_time: Summe der Index-Zeiten (Sek.)
    """
    indexed_ids: List[int] = []
    per_index_times: List[float] = []
    per_index_mems: List[float] = []

    for bid in ids:
        try:
            t0 = time.perf_counter()
            index_book_incremental(bid)
            dt = time.perf_counter() - t0

            mem_samples = memory_usage(max_iterations=1)
            pm = max(mem_samples) if mem_samples else 0.0

            indexed_ids.append(bid)
            per_index_times.append(dt)
            per_index_mems.append(pm)
        except FileNotFoundError as e:
            if verbose_missing:
                print(f"[skip] {e}")
            continue

    total_index_time = sum(per_index_times)
    return indexed_ids, per_index_times, per_index_mems, total_index_time


def bench_search_per_term_cold(terms, runs: int = 20):
    """
    Misst je Term 'runs' COLD-Läufe.
    Rückgabe:
      - per_term_stats: Liste Dicts: {"term", "total_time", "avg_time", "avg_mem"}
      - overall_total_time: Summe aller Laufzeiten (alle Terms × runs)
      - overall_avg_time:   Durchschnitt über alle einzelnen Läufe
      - overall_avg_mem:    Durchschnitt des Peak-Memory über alle einzelnen Läufe
    """
    per_term_stats: List[Dict[str, float]] = []
    all_time_samples: List[float] = []
    all_mem_samples: List[float] = []

    for term in terms:
        time_samples: List[float] = []
        mem_samples_term: List[float] = []

        for _ in range(runs):
            clear_postings_cache()  # LRU leeren ⇒ Datei-Read (OS-Cache kann weiter wirken)
            t0 = time.perf_counter()
            _ = search_postings(term)
            dt = time.perf_counter() - t0

            mem_now = memory_usage(max_iterations=1)
            pm = max(mem_now) if mem_now else 0.0

            time_samples.append(dt)
            mem_samples_term.append(pm)
            all_time_samples.append(dt)
            all_mem_samples.append(pm)

        per_term_stats.append({
            "term": term,
            "total_time": sum(time_samples),
            "avg_time": statistics.mean(time_samples) if time_samples else 0.0,
            "avg_mem": statistics.mean(mem_samples_term) if mem_samples_term else 0.0,
        })

    overall_total_time = sum(all_time_samples) if all_time_samples else 0.0
    overall_avg_time = statistics.mean(all_time_samples) if all_time_samples else 0.0
    overall_avg_mem  = statistics.mean(all_mem_samples) if all_mem_samples else 0.0
    return per_term_stats, overall_total_time, overall_avg_time, overall_avg_mem


def _fmt(num, width=12, prec=6):
    return f"{num:{width}.{prec}f}"


# ---------- Main ----------
if __name__ == "__main__":
    # CSV vorbereiten (überschreiben)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as fcsv:
        writer = csv.writer(fcsv)
        writer.writerow(["SizeLabel", "Metric", "Total Time (s)", "Average Time (s)", "Average Memory (MB)", "SuccDownloads", "Indexed", "Sleep Total (s)"])

    print(f"INDEX_ROOT_V2 = {INDEX_ROOT_V2}")
    print(f"DATA_LAKE_V2  = {DATA_LAKE_V2}")
    print("Tipp: aus dem Projekt-Root ausführen:  python -m stage1.benchmarking_v2")

    for end_id in END_IDS:
        # Sauberer Start je Testgröße
        _delete_index_dir()

        size_n = end_id - ID_START + 1
        size_label = f"{ID_START}-{end_id} (n={size_n})"
        print(f"\n=== Benchmark for size: {size_label} ===")

        # 1) Download – EXKL. Pausen in allen Kennzahlen
        ok_ids, dl_times, dl_mems, dl_sleep_total = _download_range_only(
            ID_START, end_id,
            throttle=DOWNLOAD_THROTTLE_SEC,
            jitter_pct=DOWNLOAD_THROTTLE_JITTER_PCT
        )
        dl_total = sum(dl_times)                                # ohne Pausen!
        dl_avg_time = statistics.mean(dl_times) if dl_times else 0.0
        dl_avg_mem  = statistics.mean(dl_mems)  if dl_mems  else 0.0

        # 2) Index
        if ok_ids:
            indexed_ids, idx_times, idx_mems, idx_total = _index_ids(ok_ids)
            idx_avg_time = statistics.mean(idx_times) if idx_times else 0.0
            idx_avg_mem  = statistics.mean(idx_mems)  if idx_mems  else 0.0
        else:
            indexed_ids, idx_times, idx_mems, idx_total = [], [], [], 0.0
            idx_avg_time = 0.0
            idx_avg_mem  = 0.0

        # 3) Search (20 COLD-Runs je Wort)
        per_term_stats, search_overall_total, search_overall_avg, search_overall_mem = bench_search_per_term_cold(
            TERMS, runs=SEARCH_RUNS_PER_TERM
        )

        # 4) Ausgabe als Tabelle je Größe
        print("\nResults (times in seconds, memory in MB):")
        header = f"{'Metric':36} {'Total Time (s)':>16} {'Average Time (s)':>18} {'Average Memory (MB)':>22}"
        print(header)
        print("-" * len(header))

        # Download (alle Werte exklusiv Pausen)
        print(f"{'Download (all)':36} {_fmt(dl_total, prec=3)} {_fmt(dl_avg_time)} {_fmt(dl_avg_mem, prec=2)}")
        # Index
        print(f"{'Index (all)':36} {_fmt(idx_total, prec=3)} {_fmt(idx_avg_time)} {_fmt(idx_avg_mem, prec=2)}")

        # Search per term
        for s in per_term_stats:
            label = f"Search – {s['term']} ({SEARCH_RUNS_PER_TERM} runs)"
            print(f"{label:36} {_fmt(s['total_time'])} {_fmt(s['avg_time'])} {_fmt(s['avg_mem'], prec=2)}")

        # Search overall
        print(f"{'Search – overall (all runs)':36} {_fmt(search_overall_total)} {_fmt(search_overall_avg)} {_fmt(search_overall_mem, prec=2)}")

        # 5) CSV anhängen (Sleep-Zeit nur informativ)
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as fcsv:
            writer = csv.writer(fcsv)
            # Download/Index
            writer.writerow([size_label, "Download (all)", f"{dl_total:.6f}", f"{dl_avg_time:.6f}", f"{dl_avg_mem:.2f}", f"{len(ok_ids)}", f"{len(indexed_ids)}", f"{dl_sleep_total:.3f}"])
            writer.writerow([size_label, "Index (all)",    f"{idx_total:.6f}", f"{idx_avg_time:.6f}", f"{idx_avg_mem:.2f}", f"{len(ok_ids)}", f"{len(indexed_ids)}", f"0.000"])
            # Search per term
            for s in per_term_stats:
                writer.writerow([size_label, f"Search – {s['term']} ({SEARCH_RUNS_PER_TERM} runs)",
                                 f"{s['total_time']:.6f}", f"{s['avg_time']:.6f}", f"{s['avg_mem']:.2f}",
                                 f"{len(ok_ids)}", f"{len(indexed_ids)}", f"0.000"])
            # Search overall
            writer.writerow([size_label, "Search – overall (all runs)",
                             f"{search_overall_total:.6f}", f"{search_overall_avg:.6f}", f"{search_overall_mem:.2f}",
                             f"{len(ok_ids)}", f"{len(indexed_ids)}", f"0.000"])

        # Hinweis zur Pause (nur Info, nicht in Metriken enthalten)
        if DOWNLOAD_THROTTLE_SEC > 0:
            print(f"\n[Info] Total throttle sleep excluded from metrics: {dl_sleep_total:.3f}s "
                  f"(base={DOWNLOAD_THROTTLE_SEC:.2f}s, jitter=±{int(DOWNLOAD_THROTTLE_JITTER_PCT*100)}%)")

    print(f"\nCSV gespeichert unter: {CSV_PATH.resolve()}")